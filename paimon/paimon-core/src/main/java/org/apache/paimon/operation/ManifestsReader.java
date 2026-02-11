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
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BiFilter;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.partition.PartitionPredicate.createBinaryPartitions;

/**
 * Manifest 文件读取器工具类。
 *
 * <p>该类负责从快照中读取和过滤 Manifest 文件列表，是扫描操作的第一阶段。
 *
 * <p>读取流程：
 * <ol>
 *   <li>从快照获取 Manifest 列表（根据扫描模式选择 data/delta/changelog）</li>
 *   <li>根据过滤条件（分区、桶、层级等）过滤 Manifest 文件</li>
 *   <li>返回过滤后的 Manifest 列表供后续读取</li>
 * </ol>
 *
 * <p>过滤条件包括：
 * <ul>
 *   <li><b>分区过滤</b>：根据分区谓词过滤，利用 Manifest 的分区统计信息</li>
 *   <li><b>桶过滤</b>：根据桶号范围过滤，避免读取不相关的桶</li>
 *   <li><b>层级过滤</b>：根据文件层级过滤，用于选择特定层级的文件</li>
 *   <li><b>行号范围过滤</b>：根据行号范围过滤（用于行跟踪）</li>
 * </ul>
 *
 * <p>该类是线程安全的。
 *
 * @see AbstractFileStoreScan
 */
@ThreadSafe
public class ManifestsReader {

    /** 分区类型 */
    private final RowType partitionType;
    /** 分区默认值 */
    private final String partitionDefaultValue;
    /** 快照管理器 */
    private final SnapshotManager snapshotManager;
    /** Manifest 列表工厂 */
    private final ManifestList.Factory manifestListFactory;

    /** 是否只读取实际存在的桶 */
    private boolean onlyReadRealBuckets = false;
    /** 指定的桶号 */
    @Nullable private Integer specifiedBucket = null;
    /** 指定的层级 */
    @Nullable private Integer specifiedLevel = null;
    /** 分区过滤谓词 */
    @Nullable private PartitionPredicate partitionFilter = null;
    /** 层级范围过滤器 */
    @Nullable private BiFilter<Integer, Integer> levelMinMaxFilter = null;
    /** 行号范围过滤 */
    @Nullable protected List<Range> rowRanges;

    public ManifestsReader(
            RowType partitionType,
            String partitionDefaultValue,
            SnapshotManager snapshotManager,
            ManifestList.Factory manifestListFactory) {
        this.partitionType = partitionType;
        this.partitionDefaultValue = partitionDefaultValue;
        this.snapshotManager = snapshotManager;
        this.manifestListFactory = manifestListFactory;
    }

    public ManifestsReader onlyReadRealBuckets() {
        this.onlyReadRealBuckets = true;
        return this;
    }

    public ManifestsReader withBucket(int bucket) {
        this.specifiedBucket = bucket;
        return this;
    }

    public ManifestsReader withLevel(int level) {
        this.specifiedLevel = level;
        return this;
    }

    public ManifestsReader withLevelMinMaxFilter(BiFilter<Integer, Integer> minMaxFilter) {
        this.levelMinMaxFilter = minMaxFilter;
        return this;
    }

    public ManifestsReader withPartitionFilter(Predicate predicate) {
        this.partitionFilter = PartitionPredicate.fromPredicate(partitionType, predicate);
        return this;
    }

    public ManifestsReader withPartitionFilter(List<BinaryRow> partitions) {
        this.partitionFilter = PartitionPredicate.fromMultiple(partitionType, partitions);
        return this;
    }

    public ManifestsReader withPartitionsFilter(List<Map<String, String>> partitions) {
        return withPartitionFilter(
                createBinaryPartitions(partitions, partitionType, partitionDefaultValue));
    }

    public ManifestsReader withPartitionFilter(PartitionPredicate predicate) {
        this.partitionFilter = predicate;
        return this;
    }

    public ManifestsReader withRowRanges(List<Range> rowRanges) {
        this.rowRanges = rowRanges;
        return this;
    }

    /** 获取分区过滤谓词 */
    @Nullable
    public PartitionPredicate partitionFilter() {
        return partitionFilter;
    }

    /**
     * 读取 Manifest 文件列表。
     *
     * @param specifiedSnapshot 指定的快照，null 表示使用最新快照
     * @param scanMode 扫描模式（ALL/DELTA/CHANGELOG）
     * @return 包含快照、所有 Manifest 和过滤后 Manifest 的结果
     */
    public Result read(@Nullable Snapshot specifiedSnapshot, ScanMode scanMode) {
        List<ManifestFileMeta> manifests;
        // 获取快照
        Snapshot snapshot =
                specifiedSnapshot == null ? snapshotManager.latestSnapshot() : specifiedSnapshot;
        if (snapshot == null) {
            manifests = Collections.emptyList();
        } else {
            // 根据扫描模式读取 Manifest
            manifests = readManifests(snapshot, scanMode);
        }

        // 过滤 Manifest 文件
        List<ManifestFileMeta> filtered =
                manifests.stream()
                        .filter(this::filterManifestFileMeta)
                        .collect(Collectors.toList());
        return new Result(snapshot, manifests, filtered);
    }

    /**
     * 根据扫描模式从快照中读取 Manifest 列表。
     *
     * @param snapshot 快照
     * @param scanMode 扫描模式
     * @return Manifest 文件列表
     */
    private List<ManifestFileMeta> readManifests(Snapshot snapshot, ScanMode scanMode) {
        ManifestList manifestList = manifestListFactory.create();
        switch (scanMode) {
            case ALL:
                // 批量模式：读取数据 Manifest
                return manifestList.readDataManifests(snapshot);
            case DELTA:
                // 增量模式：读取增量 Manifest
                return manifestList.readDeltaManifests(snapshot);
            case CHANGELOG:
                // 变更日志模式：读取 Changelog Manifest
                return manifestList.readChangelogManifests(snapshot);
            default:
                throw new UnsupportedOperationException("Unknown scan kind " + scanMode.name());
        }
    }

    /**
     * 根据行号范围过滤 Manifest。
     *
     * <p>如果 Manifest 的行号范围与任何指定范围相交，则保留。
     */
    private boolean filterManifestByRowRanges(ManifestFileMeta manifest) {
        if (rowRanges == null) {
            return true;
        }
        Long min = manifest.minRowId();
        Long max = manifest.maxRowId();
        if (min == null || max == null) {
            return true;
        }

        Range manifestRowRange = new Range(min, max);

        // 检查是否与任何范围相交
        for (Range expected : rowRanges) {
            if (Range.intersection(manifestRowRange, expected) != null) {
                return true;
            }
        }
        return false;
    }

    /**
     * 过滤 Manifest 文件元数据（线程安全）。
     *
     * <p>根据桶、层级、分区和行号范围进行过滤。
     */
    private boolean filterManifestFileMeta(ManifestFileMeta manifest) {
        // 桶过滤
        Integer minBucket = manifest.minBucket();
        Integer maxBucket = manifest.maxBucket();
        if (minBucket != null && maxBucket != null) {
            if (onlyReadRealBuckets && maxBucket < 0) {
                return false;
            }
            if (specifiedBucket != null
                    && (specifiedBucket < minBucket || specifiedBucket > maxBucket)) {
                return false;
            }
        }

        // 层级过滤
        Integer minLevel = manifest.minLevel();
        Integer maxLevel = manifest.maxLevel();
        if (minLevel != null && maxLevel != null) {
            if (specifiedLevel != null
                    && (specifiedLevel < minLevel || specifiedLevel > maxLevel)) {
                return false;
            }
            if (levelMinMaxFilter != null && !levelMinMaxFilter.test(minLevel, maxLevel)) {
                return false;
            }
        }

        // 分区过滤
        if (partitionFilter != null) {
            SimpleStats stats = manifest.partitionStats();
            if (!partitionFilter.test(
                    manifest.numAddedFiles() + manifest.numDeletedFiles(),
                    stats.minValues(),
                    stats.maxValues(),
                    stats.nullCounts())) {
                return false;
            }
        }

        // 行号范围过滤
        if (!filterManifestByRowRanges(manifest)) {
            return false;
        }

        return true;
    }

    /**
     * 读取 Manifest 文件的结果。
     *
     * <p>包含快照、所有 Manifest 和过滤后的 Manifest。
     */
    public static final class Result {

        /** 快照 */
        public final Snapshot snapshot;
        /** 所有 Manifest 文件 */
        public final List<ManifestFileMeta> allManifests;
        /** 过滤后的 Manifest 文件 */
        public final List<ManifestFileMeta> filteredManifests;

        public Result(
                Snapshot snapshot,
                List<ManifestFileMeta> allManifests,
                List<ManifestFileMeta> filteredManifests) {
            this.snapshot = snapshot;
            this.allManifests = allManifests;
            this.filteredManifests = filteredManifests;
        }
    }

    /** 返回空结果 */
    public static Result emptyResult() {
        return new Result(null, Collections.emptyList(), Collections.emptyList());
    }
}
