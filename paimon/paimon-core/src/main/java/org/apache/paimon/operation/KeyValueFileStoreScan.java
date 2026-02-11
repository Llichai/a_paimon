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

import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fileindex.FileIndexPredicate;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FilteredManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStatsEvolution;
import org.apache.paimon.stats.SimpleStatsEvolutions;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.MergeEngine.AGGREGATE;
import static org.apache.paimon.CoreOptions.MergeEngine.PARTIAL_UPDATE;

/**
 * 主键表（KeyValue表）的文件存储扫描实现。
 *
 * <p>相比于 AppendOnly 表，主键表的扫描具有以下特点：
 * <ul>
 *   <li><b>双重过滤</b>：支持对主键字段和值字段分别进行过滤</li>
 *   <li><b>全桶过滤</b>：在某些条件下可以过滤整个桶的数据</li>
 *   <li><b>Limit 下推</b>：在无重叠文件时可以提前终止扫描</li>
 *   <li><b>文件索引</b>：利用嵌入式文件索引加速过滤</li>
 * </ul>
 *
 * <p>值过滤器的启用条件：
 * <ul>
 *   <li><b>批量模式（ALL）</b>：仅当显式启用时才应用值过滤</li>
 *   <li><b>增量模式（DELTA）</b>：不应用值过滤（需要保留所有变更）</li>
 *   <li><b>变更日志模式（CHANGELOG）</b>：仅对 LOOKUP 和 FULL_COMPACTION 模式启用</li>
 * </ul>
 *
 * <p>全桶过滤优化：
 * <ul>
 *   <li><b>无重叠文件</b>：逐文件过滤，只保留满足条件的文件</li>
 *   <li><b>有重叠文件</b>：如果任一文件满足条件，则保留整个桶</li>
 * </ul>
 *
 * @see AbstractFileStoreScan
 * @see KeyValueFileStore
 */
public class KeyValueFileStoreScan extends AbstractFileStoreScan {

    /** 主键字段的统计信息转换器（用于 schema evolution） */
    private final SimpleStatsEvolutions fieldKeyStatsConverters;
    /** 值字段的统计信息转换器（用于 schema evolution） */
    private final SimpleStatsEvolutions fieldValueStatsConverters;
    /** 桶选择转换器，从谓词中提取桶过滤条件 */
    private final BucketSelectConverter bucketSelectConverter;
    /** 是否启用删除向量 */
    private final boolean deletionVectorsEnabled;
    /** 合并引擎类型 */
    private final MergeEngine mergeEngine;
    /** 变更日志生成器类型 */
    private final ChangelogProducer changelogProducer;
    /** 是否启用文件索引读取 */
    private final boolean fileIndexReadEnabled;

    /** 主键过滤谓词 */
    private Predicate keyFilter;
    /** 值字段过滤谓词 */
    private Predicate valueFilter;
    /** 是否强制启用值过滤 */
    private boolean valueFilterForceEnabled = false;

    /** 缓存未演化的主键过滤器（按 schema id） */
    private final Map<Long, Predicate> notEvolvedKeyFilterMapping = new ConcurrentHashMap<>();

    /** 缓存未演化的值过滤器（按 schema id） */
    private final Map<Long, Predicate> notEvolvedValueFilterMapping = new ConcurrentHashMap<>();

    /** 缓存已演化的值过滤器（按 schema id） */
    private final Map<Long, Predicate> evolvedValueFilterMapping = new ConcurrentHashMap<>();

    public KeyValueFileStoreScan(
            ManifestsReader manifestsReader,
            BucketSelectConverter bucketSelectConverter,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            TableSchema schema,
            KeyValueFieldsExtractor keyValueFieldsExtractor,
            ManifestFile.Factory manifestFileFactory,
            Integer scanManifestParallelism,
            boolean deletionVectorsEnabled,
            MergeEngine mergeEngine,
            ChangelogProducer changelogProducer,
            boolean fileIndexReadEnabled) {
        super(
                manifestsReader,
                snapshotManager,
                schemaManager,
                schema,
                manifestFileFactory,
                scanManifestParallelism);
        this.bucketSelectConverter = bucketSelectConverter;
        // NOTE: don't add key prefix to field names because fieldKeyStatsConverters is used for
        // filter conversion
        this.fieldKeyStatsConverters =
                new SimpleStatsEvolutions(
                        sid -> scanTableSchema(sid).trimmedPrimaryKeysFields(), schema.id());
        this.fieldValueStatsConverters =
                new SimpleStatsEvolutions(
                        sid -> keyValueFieldsExtractor.valueFields(scanTableSchema(sid)),
                        schema.id());
        this.deletionVectorsEnabled = deletionVectorsEnabled;
        this.mergeEngine = mergeEngine;
        this.changelogProducer = changelogProducer;
        this.fileIndexReadEnabled = fileIndexReadEnabled;
    }

    /** 设置主键过滤谓词，同时尝试提取桶过滤条件 */
    public KeyValueFileStoreScan withKeyFilter(Predicate predicate) {
        this.keyFilter = predicate;
        this.bucketSelectConverter.convert(predicate).ifPresent(this::withTotalAwareBucketFilter);
        return this;
    }

    /** 设置值字段过滤谓词 */
    public KeyValueFileStoreScan withValueFilter(Predicate predicate) {
        this.valueFilter = predicate;
        return this;
    }

    /** 强制启用值过滤 */
    @Override
    public FileStoreScan enableValueFilter() {
        this.valueFilterForceEnabled = true;
        return this;
    }

    /**
     * 根据统计信息过滤 Manifest 条目（线程安全）。
     *
     * <p>过滤流程：
     * <ol>
     *   <li>如果启用值过滤，先应用值过滤器</li>
     *   <li>应用主键过滤器，利用主键统计信息</li>
     * </ol>
     */
    @Override
    protected boolean filterByStats(ManifestEntry entry) {
        // 应用值过滤器
        if (isValueFilterEnabled() && !filterByValueFilter(entry)) {
            return false;
        }

        // 获取或创建未演化的主键过滤器
        Predicate notEvolvedFilter =
                notEvolvedKeyFilterMapping.computeIfAbsent(
                        entry.file().schemaId(),
                        id ->
                                // keepNewFieldFilter 为 true 以处理新增字段
                                // 例如，新增字段 'c'，条件 'c > 3'：旧文件可以被过滤
                                fieldKeyStatsConverters.filterUnsafeFilter(
                                        entry.file().schemaId(), keyFilter, true));
        if (notEvolvedFilter == null) {
            return true;
        }

        // 应用主键过滤器到文件统计信息
        DataFileMeta file = entry.file();
        SimpleStatsEvolution.Result stats =
                fieldKeyStatsConverters
                        .getOrCreate(file.schemaId())
                        .evolution(file.keyStats(), file.rowCount(), null);
        return notEvolvedFilter.test(
                file.rowCount(), stats.minValues(), stats.maxValues(), stats.nullCounts());
    }

    @Override
    protected ManifestEntry dropStats(ManifestEntry entry) {
        if (!isValueFilterEnabled() && postFilterManifestEntriesEnabled()) {
            return new FilteredManifestEntry(entry.copyWithoutStats(), filterByValueFilter(entry));
        }
        return entry.copyWithoutStats();
    }

    private boolean filterByFileIndex(@Nullable byte[] embeddedIndexBytes, ManifestEntry entry) {
        if (embeddedIndexBytes == null) {
            return true;
        }

        RowType dataRowType = scanTableSchema(entry.file().schemaId()).logicalRowType();
        try (FileIndexPredicate predicate =
                new FileIndexPredicate(embeddedIndexBytes, dataRowType)) {
            Predicate dataPredicate =
                    evolvedValueFilterMapping.computeIfAbsent(
                            entry.file().schemaId(),
                            id ->
                                    fieldValueStatsConverters.tryDevolveFilter(
                                            entry.file().schemaId(), valueFilter));
            return predicate.evaluate(dataPredicate).remain();
        } catch (IOException e) {
            throw new RuntimeException("Exception happens while checking fileIndex predicate.", e);
        }
    }

    /** 判断是否启用值过滤器，根据扫描模式和配置决定 */
    private boolean isValueFilterEnabled() {
        if (valueFilter == null) {
            return false;
        }

        switch (scanMode) {
            case ALL:
                // 批量模式：仅在显式启用时应用值过滤
                return valueFilterForceEnabled;
            case DELTA:
                // 增量模式：不应用值过滤（需要保留所有变更）
                return false;
            case CHANGELOG:
                // 变更日志模式：仅对 LOOKUP 和 FULL_COMPACTION 启用
                return changelogProducer == ChangelogProducer.LOOKUP
                        || changelogProducer == ChangelogProducer.FULL_COMPACTION;
            default:
                throw new UnsupportedOperationException("Unsupported scan mode: " + scanMode);
        }
    }

    @Override
    protected boolean postFilterManifestEntriesEnabled() {
        return wholeBucketFilterEnabled() || limitPushdownEnabled();
    }

    /** 判断是否启用全桶过滤 */
    private boolean wholeBucketFilterEnabled() {
        return valueFilter != null && scanMode == ScanMode.ALL;
    }

    /**
     * 判断是否启用 Limit 下推。
     *
     * <p>启用条件：
     * <ul>
     *   <li>设置了 limit 且大于 0</li>
     *   <li>合并引擎不是 PARTIAL_UPDATE 或 AGGREGATE</li>
     *   <li>未启用删除向量</li>
     * </ul>
     */
    @VisibleForTesting
    public boolean limitPushdownEnabled() {
        if (limit == null || limit <= 0) {
            return false;
        }

        return mergeEngine != PARTIAL_UPDATE && mergeEngine != AGGREGATE && !deletionVectorsEnabled;
    }

    /**
     * 对 Manifest 条目进行后置过滤。
     *
     * <p>处理流程：
     * <ol>
     *   <li>按（分区，桶）分组文件</li>
     *   <li>对每个桶应用全桶过滤（如果启用）</li>
     *   <li>应用 Limit 下推优化（如果启用且文件无重叠）</li>
     * </ol>
     */
    @Override
    protected List<ManifestEntry> postFilterManifestEntries(List<ManifestEntry> files) {
        // 按桶分组，保持顺序
        Map<Pair<BinaryRow, Integer>, List<ManifestEntry>> buckets = groupByBucket(files);
        List<ManifestEntry> result = new ArrayList<>();
        AtomicLong currentRowCount = new AtomicLong(0);
        for (List<ManifestEntry> entries : buckets.values()) {
            boolean noOverlapping = noOverlapping(entries);
            // 应用全桶过滤
            if (wholeBucketFilterEnabled()) {
                entries =
                        noOverlapping
                                ? filterWholeBucketPerFile(entries)
                                : filterWholeBucketAllFiles(entries);
            }

            // 应用 Limit 下推
            if (limitPushdownEnabled() && noOverlapping) {
                if (currentRowCount.get() >= limit) {
                    break;
                }
                entries = applyLimitWhenNoOverlapping(entries, currentRowCount);
            }
            result.addAll(entries);
        }
        return result;
    }

    /**
     * 在无重叠文件时应用 Limit。
     *
     * <p>累计文件的行数，达到 limit 时停止。
     * 如果遇到有删除行的文件，则返回所有文件（无法准确计算）。
     */
    private List<ManifestEntry> applyLimitWhenNoOverlapping(
            List<ManifestEntry> entries, AtomicLong currentRowCount) {
        List<ManifestEntry> result = new ArrayList<>();
        for (ManifestEntry entry : entries) {
            boolean hasDeleteRows =
                    entry.file().deleteRowCount().map(count -> count > 0L).orElse(false);
            if (hasDeleteRows) {
                // 有删除行，无法准确计算，返回所有文件
                return entries;
            }
            result.add(entry);
            long fileRowCount = entry.file().rowCount();
            currentRowCount.addAndGet(fileRowCount);
            if (currentRowCount.get() >= limit) {
                break;
            }
        }
        return result;
    }

    /**
     * 逐文件过滤（用于无重叠文件）。
     *
     * <p>只保留满足值过滤器条件的文件。
     */
    private List<ManifestEntry> filterWholeBucketPerFile(List<ManifestEntry> entries) {
        List<ManifestEntry> filtered = new ArrayList<>();
        for (ManifestEntry entry : entries) {
            if (filterByValueFilter(entry)) {
                filtered.add(entry);
            }
        }
        return filtered;
    }

    /**
     * 全桶过滤（用于有重叠文件）。
     *
     * <p>如果桶中任一文件满足条件，则保留整个桶的所有文件。
     * 对于 PARTIAL_UPDATE 和 AGGREGATE 引擎（未启用删除向量时），
     * 无法精确过滤，返回所有文件。
     */
    private List<ManifestEntry> filterWholeBucketAllFiles(List<ManifestEntry> entries) {
        if (!deletionVectorsEnabled
                && (mergeEngine == PARTIAL_UPDATE || mergeEngine == AGGREGATE)) {
            return entries;
        }

        // entries 来自同一个桶，如果任一文件满足条件，保留整个桶
        for (ManifestEntry entry : entries) {
            if (filterByValueFilter(entry)) {
                return entries;
            }
        }
        return Collections.emptyList();
    }

    /**
     * 应用值过滤器到文件。
     *
     * <p>利用文件的值统计信息和嵌入式索引进行过滤。
     */
    private boolean filterByValueFilter(ManifestEntry entry) {
        // 如果是已过滤的条目，直接返回结果
        if (entry instanceof FilteredManifestEntry) {
            return ((FilteredManifestEntry) entry).selected();
        }

        // 获取或创建未演化的值过滤器
        Predicate notEvolvedFilter =
                notEvolvedValueFilterMapping.computeIfAbsent(
                        entry.file().schemaId(),
                        id ->
                                // keepNewFieldFilter 为 true 以处理新增字段
                                fieldValueStatsConverters.filterUnsafeFilter(
                                        entry.file().schemaId(), valueFilter, true));
        if (notEvolvedFilter == null) {
            return true;
        }

        // 应用过滤器到值统计信息
        DataFileMeta file = entry.file();
        SimpleStatsEvolution.Result result =
                fieldValueStatsConverters
                        .getOrCreate(file.schemaId())
                        .evolution(file.valueStats(), file.rowCount(), file.valueStatsCols());
        return notEvolvedFilter.test(
                        file.rowCount(),
                        result.minValues(),
                        result.maxValues(),
                        result.nullCounts())
                // 如果启用文件索引，还要检查文件索引
                && (!fileIndexReadEnabled
                        || filterByFileIndex(entry.file().embeddedIndex(), entry));
    }

    /**
     * 判断文件列表是否无重叠。
     *
     * <p>无重叠条件：
     * <ul>
     *   <li>只有一个文件</li>
     *   <li>所有文件都不是 level 0（level 0 文件有重叠）</li>
     *   <li>所有文件在同一层级</li>
     * </ul>
     */
    private static boolean noOverlapping(List<ManifestEntry> entries) {
        if (entries.size() <= 1) {
            return true;
        }

        Integer previousLevel = null;
        for (ManifestEntry entry : entries) {
            int level = entry.file().level();
            // level 0 文件有重叠
            if (level == 0) {
                return false;
            }

            if (previousLevel == null) {
                previousLevel = level;
            } else {
                // 不同层级，有重叠
                if (previousLevel != level) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * 按（分区，桶）分组 Manifest 条目，同时保持顺序。
     *
     * <p>使用 LinkedHashMap 保持插入顺序。
     */
    private Map<Pair<BinaryRow, Integer>, List<ManifestEntry>> groupByBucket(
            List<ManifestEntry> entries) {
        return entries.stream()
                .collect(
                        Collectors.groupingBy(
                                // 使用 LinkedHashMap 避免乱序
                                file -> Pair.of(file.partition(), file.bucket()),
                                LinkedHashMap::new,
                                Collectors.toList()));
    }
}
