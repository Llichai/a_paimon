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

package org.apache.paimon.operation.metrics;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 提交统计信息
 *
 * <p>封装单次提交操作的详细统计数据。
 *
 * <h2>统计内容</h2>
 * <p>该类记录提交操作的全面统计信息：
 * <ul>
 *   <li><b>提交基本信息</b>：
 *       <ul>
 *         <li>持续时间（duration）- 提交耗时
 *         <li>重试次数（attempts）- 因冲突导致的重试次数
 *         <li>生成的快照数（generatedSnapshots）- 本次提交生成的快照数量
 *       </ul>
 *   <li><b>表文件统计</b>：
 *       <ul>
 *         <li>追加的文件数（tableFilesAppended）- 包括新增和删除
 *         <li>新增的文件数（tableFilesAdded）- 只包括ADD类型
 *         <li>删除的文件数（tableFilesDeleted）- 只包括DELETE类型
 *         <li>压缩的文件数（tableFilesCompacted）- 压缩操作的文件数
 *       </ul>
 *   <li><b>Changelog文件统计</b>：
 *       <ul>
 *         <li>追加的Changelog文件数（changelogFilesAppended）
 *         <li>压缩的Changelog文件数（changelogFilesCompacted）
 *       </ul>
 *   <li><b>记录数统计</b>：
 *       <ul>
 *         <li>Delta记录追加数（deltaRecordsAppended）
 *         <li>Changelog记录追加数（changelogRecordsAppended）
 *         <li>Delta记录压缩数（deltaRecordsCompacted）
 *         <li>Changelog记录压缩数（changelogRecordsCompacted）
 *       </ul>
 *   <li><b>分区和桶统计</b>：
 *       <ul>
 *         <li>写入的分区数（numPartitionsWritten）
 *         <li>写入的桶数（numBucketsWritten）
 *       </ul>
 *   <li><b>压缩文件大小</b>：
 *       <ul>
 *         <li>压缩输入文件大小（compactionInputFileSize）
 *         <li>压缩输出文件大小（compactionOutputFileSize）
 *       </ul>
 * </ul>
 *
 * <h2>文件分类逻辑</h2>
 * <p>该类从不同类型的Manifest条目中提取统计信息：
 * <ul>
 *   <li><b>追加操作</b>：appendTableFiles 包含新增和删除的文件
 *   <li><b>压缩操作</b>：compactTableFiles 包含压缩前后的文件
 *   <li><b>Changelog</b>：appendChangelogFiles 和 compactChangelogFiles
 * </ul>
 *
 * @see CommitMetrics 提交指标
 * @see ManifestEntry Manifest条目
 */
public class CommitStats {

    /** 提交持续时间（毫秒） */
    private final long duration;

    /** 提交重试次数 */
    private final int attempts;

    /** 追加的表文件数（包括新增和删除） */
    private final long tableFilesAppended;

    /** 新增的表文件数 */
    private final long tableFilesAdded;

    /** 删除的表文件数 */
    private final long tableFilesDeleted;

    /** 追加的Changelog文件数 */
    private final long changelogFilesAppended;

    /** 压缩输入文件大小（字节） */
    private final long compactionInputFileSize;

    /** 压缩输出文件大小（字节） */
    private final long compactionOutputFileSize;

    /** 压缩的Changelog文件数 */
    private final long changelogFilesCompacted;

    /** 压缩的Changelog记录数 */
    private final long changelogRecordsCompacted;

    /** 压缩的Delta记录数 */
    private final long deltaRecordsCompacted;

    /** 追加的Changelog记录数 */
    private final long changelogRecordsAppended;

    /** 追加的Delta记录数 */
    private final long deltaRecordsAppended;

    /** 压缩的表文件数 */
    private final long tableFilesCompacted;

    /** 生成的快照数 */
    private final long generatedSnapshots;

    /** 写入的分区数 */
    private final long numPartitionsWritten;

    /** 写入的桶数 */
    private final long numBucketsWritten;

    /**
     * 构造提交统计信息
     *
     * <p>从Manifest条目列表中提取统计信息。
     *
     * @param appendTableFiles 追加的表文件条目
     * @param appendChangelogFiles 追加的Changelog文件条目
     * @param compactTableFiles 压缩的表文件条目
     * @param compactChangelogFiles 压缩的Changelog文件条目
     * @param commitDuration 提交持续时间（毫秒）
     * @param generatedSnapshots 生成的快照数
     * @param attempts 重试次数
     */
    public CommitStats(
            List<ManifestEntry> appendTableFiles,
            List<ManifestEntry> appendChangelogFiles,
            List<ManifestEntry> compactTableFiles,
            List<ManifestEntry> compactChangelogFiles,
            long commitDuration,
            int generatedSnapshots,
            int attempts) {
        // 从追加文件中分离新增和删除的文件
        List<ManifestEntry> addedTableFiles =
                appendTableFiles.stream()
                        .filter(f -> FileKind.ADD.equals(f.kind()))
                        .collect(Collectors.toList());
        List<ManifestEntry> deletedTableFiles =
                appendTableFiles.stream()
                        .filter(f -> FileKind.DELETE.equals(f.kind()))
                        .collect(Collectors.toList());

        // 压缩后的文件（ADD）
        List<ManifestEntry> compactAfterFiles =
                compactTableFiles.stream()
                        .filter(f -> FileKind.ADD.equals(f.kind()))
                        .collect(Collectors.toList());
        addedTableFiles.addAll(compactAfterFiles);

        // 压缩输入文件（DELETE）
        List<ManifestEntry> compactionInputFiles =
                compactTableFiles.stream()
                        .filter(f -> FileKind.DELETE.equals(f.kind()))
                        .collect(Collectors.toList());
        deletedTableFiles.addAll(compactionInputFiles);

        // 计算压缩输入和输出文件大小
        this.compactionInputFileSize =
                compactionInputFiles.stream()
                        .map(ManifestEntry::file)
                        .map(DataFileMeta::fileSize)
                        .reduce(Long::sum)
                        .orElse(0L);
        this.compactionOutputFileSize =
                compactAfterFiles.stream()
                        .map(ManifestEntry::file)
                        .map(DataFileMeta::fileSize)
                        .reduce(Long::sum)
                        .orElse(0L);

        // 文件数统计
        this.tableFilesAppended = appendTableFiles.size();
        this.tableFilesAdded = addedTableFiles.size();
        this.tableFilesDeleted = deletedTableFiles.size();
        this.tableFilesCompacted = compactTableFiles.size();
        this.changelogFilesAppended = appendChangelogFiles.size();
        this.changelogFilesCompacted = compactChangelogFiles.size();

        // 分区和桶统计
        this.numPartitionsWritten = numChangedPartitions(appendTableFiles, compactTableFiles);
        this.numBucketsWritten = numChangedBuckets(appendTableFiles, compactTableFiles);

        // 记录数统计
        this.changelogRecordsCompacted = getRowCounts(compactChangelogFiles);
        this.deltaRecordsCompacted = getRowCounts(compactTableFiles);
        this.changelogRecordsAppended = getRowCounts(appendChangelogFiles);
        this.deltaRecordsAppended = getRowCounts(appendTableFiles);

        // 基本信息
        this.duration = commitDuration;
        this.generatedSnapshots = generatedSnapshots;
        this.attempts = attempts;
    }

    /**
     * 计算变更的分区数
     *
     * @param changes 变更的Manifest条目列表
     * @return 不同分区的数量
     */
    @VisibleForTesting
    protected static long numChangedPartitions(List<ManifestEntry>... changes) {
        return Arrays.stream(changes)
                .flatMap(Collection::stream)
                .map(ManifestEntry::partition)
                .distinct()
                .count();
    }

    /**
     * 计算变更的桶数
     *
     * @param changes 变更的Manifest条目列表
     * @return 不同桶的数量
     */
    @VisibleForTesting
    protected static long numChangedBuckets(List<ManifestEntry>... changes) {
        return changedPartBuckets(changes).values().stream().mapToLong(Set::size).sum();
    }

    /**
     * 获取变更的分区列表
     *
     * @param changes 变更的Manifest条目列表
     * @return 不同分区的列表
     */
    @VisibleForTesting
    protected static List<BinaryRow> changedPartitions(List<ManifestEntry>... changes) {
        return Arrays.stream(changes)
                .flatMap(Collection::stream)
                .map(ManifestEntry::partition)
                .distinct()
                .collect(Collectors.toList());
    }

    /**
     * 获取变更的分区-桶映射
     *
     * @param changes 变更的Manifest条目列表
     * @return 分区 → 桶集合 的映射
     */
    @VisibleForTesting
    protected static Map<BinaryRow, Set<Integer>> changedPartBuckets(
            List<ManifestEntry>... changes) {
        Map<BinaryRow, Set<Integer>> changedPartBuckets = new LinkedHashMap<>();
        Arrays.stream(changes)
                .flatMap(Collection::stream)
                .forEach(
                        entry ->
                                changedPartBuckets
                                        .computeIfAbsent(
                                                entry.partition(), k -> new LinkedHashSet<>())
                                        .add(entry.bucket()));
        return changedPartBuckets;
    }

    /**
     * 计算文件列表的总行数
     *
     * @param files Manifest条目列表
     * @return 总行数
     */
    private long getRowCounts(List<ManifestEntry> files) {
        return files.stream().mapToLong(file -> file.file().rowCount()).sum();
    }

    // ==================== Getter方法（用于测试和指标报告） ====================

    @VisibleForTesting
    protected long getTableFilesAdded() {
        return tableFilesAdded;
    }

    @VisibleForTesting
    protected long getTableFilesDeleted() {
        return tableFilesDeleted;
    }

    @VisibleForTesting
    protected long getTableFilesAppended() {
        return tableFilesAppended;
    }

    @VisibleForTesting
    protected long getTableFilesCompacted() {
        return tableFilesCompacted;
    }

    @VisibleForTesting
    protected long getChangelogFilesAppended() {
        return changelogFilesAppended;
    }

    @VisibleForTesting
    protected long getChangelogFilesCompacted() {
        return changelogFilesCompacted;
    }

    @VisibleForTesting
    protected long getGeneratedSnapshots() {
        return generatedSnapshots;
    }

    @VisibleForTesting
    protected long getDeltaRecordsAppended() {
        return deltaRecordsAppended;
    }

    @VisibleForTesting
    protected long getChangelogRecordsAppended() {
        return changelogRecordsAppended;
    }

    @VisibleForTesting
    protected long getDeltaRecordsCompacted() {
        return deltaRecordsCompacted;
    }

    @VisibleForTesting
    protected long getChangelogRecordsCompacted() {
        return changelogRecordsCompacted;
    }

    @VisibleForTesting
    protected long getNumPartitionsWritten() {
        return numPartitionsWritten;
    }

    @VisibleForTesting
    protected long getNumBucketsWritten() {
        return numBucketsWritten;
    }

    @VisibleForTesting
    protected long getDuration() {
        return duration;
    }

    @VisibleForTesting
    protected int getAttempts() {
        return attempts;
    }

    /**
     * 获取压缩输入文件大小
     *
     * @return 压缩输入文件大小（字节）
     */
    public long getCompactionInputFileSize() {
        return compactionInputFileSize;
    }

    /**
     * 获取压缩输出文件大小
     *
     * @return 压缩输出文件大小（字节）
     */
    public long getCompactionOutputFileSize() {
        return compactionOutputFileSize;
    }
}
