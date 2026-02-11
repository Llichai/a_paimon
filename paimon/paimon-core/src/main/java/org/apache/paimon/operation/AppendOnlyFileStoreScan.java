/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.operation;

import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.fileindex.FileIndexPredicate;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStatsEvolution;
import org.apache.paimon.stats.SimpleStatsEvolutions;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 仅追加表（Append-Only表）的文件存储扫描实现。
 *
 * <p>仅追加表的特点：
 * <ul>
 *   <li><b>无主键</b>：数据只追加，不更新</li>
 *   <li><b>无删除</b>：不支持删除操作（除非使用删除向量）</li>
 *   <li><b>简单过滤</b>：只需要对所有字段统一过滤，无需区分主键和值</li>
 *   <li><b>Limit 下推</b>：简单累计行数即可提前终止扫描</li>
 * </ul>
 *
 * <p>与主键表的区别：
 * <ul>
 *   <li>主键表需要分别过滤主键和值字段</li>
 *   <li>主键表需要考虑文件重叠和合并</li>
 *   <li>仅追加表的过滤逻辑更简单直接</li>
 * </ul>
 *
 * @see AbstractFileStoreScan
 * @see AppendOnlyFileStore
 */
public class AppendOnlyFileStoreScan extends AbstractFileStoreScan {

    /** 桶选择转换器，从谓词中提取桶过滤条件 */
    private final BucketSelectConverter bucketSelectConverter;
    /** 统计信息转换器（用于 schema evolution） */
    private final SimpleStatsEvolutions simpleStatsEvolutions;

    /** 是否启用文件索引读取 */
    private final boolean fileIndexReadEnabled;
    /** 是否启用删除向量 */
    private final boolean deletionVectorsEnabled;

    /** 输入过滤谓词 */
    protected Predicate inputFilter;

    /** 缓存未演化的过滤器（按 schema id） */
    private final Map<Long, Predicate> notEvolvedFilterMapping = new ConcurrentHashMap<>();

    /** 缓存已演化的过滤器（按 schema id） */
    private final Map<Long, Predicate> evolvedFilterMapping = new ConcurrentHashMap<>();

    public AppendOnlyFileStoreScan(
            ManifestsReader manifestsReader,
            BucketSelectConverter bucketSelectConverter,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            TableSchema schema,
            ManifestFile.Factory manifestFileFactory,
            Integer scanManifestParallelism,
            boolean fileIndexReadEnabled,
            boolean deletionVectorsEnabled) {
        super(
                manifestsReader,
                snapshotManager,
                schemaManager,
                schema,
                manifestFileFactory,
                scanManifestParallelism);
        this.bucketSelectConverter = bucketSelectConverter;
        this.simpleStatsEvolutions =
                new SimpleStatsEvolutions(sid -> scanTableSchema(sid).fields(), schema.id());
        this.fileIndexReadEnabled = fileIndexReadEnabled;
        this.deletionVectorsEnabled = deletionVectorsEnabled;
    }

    /** 设置过滤谓词，同时尝试提取桶过滤条件 */
    public AppendOnlyFileStoreScan withFilter(Predicate predicate) {
        this.inputFilter = predicate;
        this.bucketSelectConverter.convert(predicate).ifPresent(this::withTotalAwareBucketFilter);
        return this;
    }

    /** 是否启用后置过滤（用于 Limit 下推） */
    @Override
    protected boolean postFilterManifestEntriesEnabled() {
        return limit != null && limit > 0 && !deletionVectorsEnabled;
    }

    /**
     * 对 Manifest 条目应用 Limit 下推。
     *
     * <p>简单累计每个文件的行数，达到 limit 时停止。
     * 由于是仅追加表，无需考虑更新和删除。
     */
    @Override
    protected List<ManifestEntry> postFilterManifestEntries(List<ManifestEntry> entries) {
        checkArgument(limit != null && limit > 0 && !deletionVectorsEnabled);
        List<ManifestEntry> result = new ArrayList<>();
        long accumulatedRowCount = 0;
        for (ManifestEntry entry : entries) {
            result.add(entry);
            accumulatedRowCount += entry.file().rowCount();
            if (accumulatedRowCount >= limit) {
                break;
            }
        }
        return result;
    }

    /**
     * 根据统计信息过滤 Manifest 条目（线程安全）。
     *
     * <p>过滤流程：
     * <ol>
     *   <li>应用统计信息过滤（min/max/null count）</li>
     *   <li>如果启用，应用文件索引过滤</li>
     * </ol>
     */
    @Override
    protected boolean filterByStats(ManifestEntry entry) {
        // 获取或创建未演化的过滤器
        Predicate notEvolvedFilter =
                notEvolvedFilterMapping.computeIfAbsent(
                        entry.file().schemaId(),
                        id ->
                                // keepNewFieldFilter 为 true 以处理新增字段
                                // 例如，新增字段 'c'，条件 'c > 3'：旧文件可以被过滤
                                simpleStatsEvolutions.filterUnsafeFilter(
                                        entry.file().schemaId(), inputFilter, true));
        if (notEvolvedFilter == null) {
            return true;
        }

        // 演化统计信息以匹配当前 schema
        SimpleStatsEvolution evolution = simpleStatsEvolutions.getOrCreate(entry.file().schemaId());
        SimpleStatsEvolution.Result stats =
                evolution.evolution(
                        entry.file().valueStats(),
                        entry.file().rowCount(),
                        entry.file().valueStatsCols());

        // 根据 min/max 过滤
        boolean result =
                notEvolvedFilter.test(
                        entry.file().rowCount(),
                        stats.minValues(),
                        stats.maxValues(),
                        stats.nullCounts());

        if (!result) {
            return false;
        }

        // 如果启用文件索引，进一步过滤
        if (!fileIndexReadEnabled) {
            return true;
        }

        return testFileIndex(entry.file().embeddedIndex(), entry);
    }

    /**
     * 使用文件索引测试过滤条件。
     *
     * @param embeddedIndexBytes 嵌入式索引字节
     * @param entry Manifest 条目
     * @return 是否通过过滤
     */
    private boolean testFileIndex(@Nullable byte[] embeddedIndexBytes, ManifestEntry entry) {
        if (embeddedIndexBytes == null) {
            return true;
        }

        // 获取数据行类型
        RowType dataRowType = scanTableSchema(entry.file().schemaId()).logicalRowType();

        // 获取或创建已演化的过滤器
        Predicate dataPredicate =
                evolvedFilterMapping.computeIfAbsent(
                        entry.file().schemaId(),
                        id ->
                                simpleStatsEvolutions.tryDevolveFilter(
                                        entry.file().schemaId(), inputFilter));

        // 使用文件索引评估谓词
        try (FileIndexPredicate predicate =
                new FileIndexPredicate(embeddedIndexBytes, dataRowType)) {
            return predicate.evaluate(dataPredicate).remain();
        } catch (IOException e) {
            throw new RuntimeException("Exception happens while checking predicate.", e);
        }
    }
}
