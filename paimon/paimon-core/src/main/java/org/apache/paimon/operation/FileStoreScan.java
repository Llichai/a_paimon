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

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.BucketEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.metrics.ScanMetrics;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BiFilter;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 文件存储扫描操作接口，用于生成扫描计划。
 *
 * <p>该接口支持三种扫描模式：
 * <ul>
 *   <li><b>批量扫描（BATCH）</b>：读取单个快照的完整数据</li>
 *   <li><b>增量扫描（INCREMENTAL）</b>：读取两个快照之间的变更数据</li>
 *   <li><b>流式扫描（STREAMING）</b>：持续读取新产生的数据变更</li>
 * </ul>
 *
 * <p>扫描过程包括以下阶段：
 * <ol>
 *   <li>根据分区、桶、层级等条件过滤 Manifest 文件</li>
 *   <li>读取过滤后的 Manifest 文件获取数据文件列表</li>
 *   <li>根据谓词和统计信息过滤数据文件</li>
 *   <li>生成最终的扫描计划 {@link Plan}</li>
 * </ol>
 */
public interface FileStoreScan {

    /** 设置分区过滤谓词，使用表达式进行过滤。 */
    FileStoreScan withPartitionFilter(Predicate predicate);

    /** 设置分区过滤，指定要扫描的分区列表。 */
    FileStoreScan withPartitionFilter(List<BinaryRow> partitions);

    /** 设置分区过滤，使用字符串映射表示的分区列表。 */
    FileStoreScan withPartitionsFilter(List<Map<String, String>> partitions);

    /** 设置分区过滤谓词，使用 PartitionPredicate 进行过滤。 */
    FileStoreScan withPartitionFilter(PartitionPredicate predicate);

    /** 设置桶过滤，只扫描指定的桶。 */
    FileStoreScan withBucket(int bucket);

    /** 设置只读取实际存在的桶（用于延迟分桶表）。 */
    FileStoreScan onlyReadRealBuckets();

    /** 设置桶过滤器，根据桶号过滤。 */
    FileStoreScan withBucketFilter(Filter<Integer> bucketFilter);

    /** 设置桶过滤器，同时考虑桶号和总桶数。 */
    FileStoreScan withTotalAwareBucketFilter(BiFilter<Integer, Integer> bucketFilter);

    /** 设置分区和桶的组合过滤。 */
    FileStoreScan withPartitionBucket(BinaryRow partition, int bucket);

    /** 设置要扫描的快照 ID。 */
    FileStoreScan withSnapshot(long snapshotId);

    /** 设置要扫描的快照对象。 */
    FileStoreScan withSnapshot(Snapshot snapshot);

    /**
     * 设置扫描模式。
     *
     * @param scanMode 扫描模式：BATCH（批量）、INCREMENTAL（增量）、STREAMING（流式）
     */
    FileStoreScan withKind(ScanMode scanMode);

    /** 设置只扫描指定层级的文件。 */
    FileStoreScan withLevel(int level);

    /** 设置层级过滤器。 */
    FileStoreScan withLevelFilter(Filter<Integer> levelFilter);

    /** 设置层级范围过滤器，根据最小和最大层级过滤。 */
    FileStoreScan withLevelMinMaxFilter(BiFilter<Integer, Integer> minMaxFilter);

    /** 启用基于值的过滤（使用数据文件的统计信息）。 */
    FileStoreScan enableValueFilter();

    /** 设置 Manifest 条目过滤器，直接过滤 ManifestEntry。 */
    FileStoreScan withManifestEntryFilter(Filter<ManifestEntry> filter);

    /** 设置数据文件名过滤器。 */
    FileStoreScan withDataFileNameFilter(Filter<String> fileNameFilter);

    /** 设置扫描指标收集器。 */
    FileStoreScan withMetrics(ScanMetrics metrics);

    /** 丢弃文件统计信息以减少内存使用。 */
    FileStoreScan dropStats();

    /** 保留文件统计信息。 */
    FileStoreScan keepStats();

    /** 设置行号范围过滤（用于行级别的读取）。 */
    FileStoreScan withRowRanges(List<Range> rowRanges);

    /** 设置读取类型（用于 schema evolution）。 */
    FileStoreScan withReadType(RowType readType);

    /** 设置扫描结果的最大行数限制。 */
    FileStoreScan withLimit(long limit);

    /** 获取扫描的并行度（如果有）。 */
    @Nullable
    Integer parallelism();

    /** 获取 Manifest 读取器。 */
    ManifestsReader manifestsReader();

    /** 读取单个 Manifest 文件的内容。 */
    List<ManifestEntry> readManifest(ManifestFileMeta manifest);

    /**
     * 生成扫描计划。
     *
     * @return 包含所有要读取文件的扫描计划
     */
    Plan plan();

    /**
     * 读取简化的文件条目列表。
     *
     * <p>SimpleFileEntry 只保留关键信息，因此无法基于统计信息进行过滤，
     * 但内存占用更小，适合于只需要文件路径等基本信息的场景。
     */
    List<SimpleFileEntry> readSimpleEntries();

    /** 读取分区条目列表。 */
    List<PartitionEntry> readPartitionEntries();

    /** 读取桶条目列表。 */
    List<BucketEntry> readBucketEntries();

    /** 读取文件条目的迭代器（避免一次性加载所有条目）。 */
    Iterator<ManifestEntry> readFileIterator();

    /** 读取指定 Manifest 文件列表的文件条目迭代器。 */
    Iterator<ManifestEntry> readFileIterator(List<ManifestFileMeta> manifestFileMetas);

    /**
     * 列出所有分区。
     *
     * @return 分区列表
     */
    default List<BinaryRow> listPartitions() {
        return readPartitionEntries().stream()
                .map(PartitionEntry::partition)
                .collect(Collectors.toList());
    }

    /**
     * 扫描计划结果接口。
     *
     * <p>包含了本次扫描要读取的所有文件信息，以及相关的快照和水位线信息。
     */
    interface Plan {

        /**
         * 获取水位线（watermark）。
         *
         * @return 水位线值，如果没有则返回 null
         */
        @Nullable
        Long watermark();

        /**
         * 获取本次扫描对应的快照。
         *
         * @return 快照对象，如果表为空或直接指定了 Manifest 列表则返回 null
         */
        @Nullable
        Snapshot snapshot();

        /**
         * 获取扫描结果的所有文件条目。
         *
         * @return ManifestEntry 列表，每个条目代表一个数据文件
         */
        List<ManifestEntry> files();

        /**
         * 获取指定文件类型的文件条目。
         *
         * @param kind 文件类型（ADD 或 DELETE）
         * @return 过滤后的 ManifestEntry 列表
         */
        default List<ManifestEntry> files(FileKind kind) {
            return files().stream().filter(e -> e.kind() == kind).collect(Collectors.toList());
        }

        /**
         * 将文件按分区和桶分组。
         *
         * @param files 文件条目列表
         * @return 按分区和桶分组的文件映射表
         */
        static Map<BinaryRow, Map<Integer, List<ManifestEntry>>> groupByPartFiles(
                List<ManifestEntry> files) {
            Map<BinaryRow, Map<Integer, List<ManifestEntry>>> groupBy = new LinkedHashMap<>();
            for (ManifestEntry entry : files) {
                // 按分区分组
                groupBy.computeIfAbsent(entry.partition(), k -> new LinkedHashMap<>())
                        // 按桶分组
                        .computeIfAbsent(entry.bucket(), k -> new ArrayList<>())
                        .add(entry);
            }
            return groupBy;
        }
    }
}
