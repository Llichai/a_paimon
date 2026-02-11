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
import org.apache.paimon.metrics.Histogram;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.MetricRegistry;

/**
 * 提交指标类
 *
 * <p>用于测量和报告提交操作的性能指标。
 *
 * <h2>提交性能监控</h2>
 * <p>该类收集提交操作的关键指标，包括：
 * <ul>
 *   <li><b>提交耗时</b>：单次提交的持续时间（毫秒）
 *   <li><b>提交重试次数</b>：因冲突导致的重试次数
 *   <li><b>文件变更统计</b>：新增、删除、追加、压缩的文件数量
 *   <li><b>记录数统计</b>：Delta和Changelog的记录数
 *   <li><b>分区和桶统计</b>：涉及的分区数和桶数
 *   <li><b>快照数量</b>：生成的快照数量
 *   <li><b>压缩文件大小</b>：压缩输入和输出的文件大小
 * </ul>
 *
 * <h2>支持的指标类型</h2>
 * <p>该类使用两种类型的指标：
 * <ul>
 *   <li><b>Gauge（测量值）</b>：实时查询最新提交的统计信息
 *   <li><b>Histogram（直方图）</b>：跟踪最近100次提交的耗时分布
 * </ul>
 *
 * <h2>指标名称</h2>
 * <p>所有指标都注册在 "commit" 指标组下，包括：
 * <ul>
 *   <li>lastCommitDuration - 最后一次提交的耗时
 *   <li>commitDuration - 提交耗时的直方图
 *   <li>lastCommitAttempts - 最后一次提交的重试次数
 *   <li>lastTableFilesAdded - 最后一次提交新增的表文件数
 *   <li>lastTableFilesDeleted - 最后一次提交删除的表文件数
 *   <li>lastTableFilesAppended - 最后一次提交追加的表文件数
 *   <li>lastTableFilesCommitCompacted - 最后一次提交压缩的表文件数
 *   <li>lastChangelogFilesAppended - 最后一次提交追加的Changelog文件数
 *   <li>lastChangelogFileCommitCompacted - 最后一次提交压缩的Changelog文件数
 *   <li>lastGeneratedSnapshots - 最后一次提交生成的快照数
 *   <li>lastDeltaRecordsAppended - 最后一次提交追加的Delta记录数
 *   <li>lastChangelogRecordsAppended - 最后一次提交追加的Changelog记录数
 *   <li>lastDeltaRecordsCommitCompacted - 最后一次提交压缩的Delta记录数
 *   <li>lastChangelogRecordsCommitCompacted - 最后一次提交压缩的Changelog记录数
 *   <li>lastPartitionsWritten - 最后一次提交写入的分区数
 *   <li>lastBucketsWritten - 最后一次提交写入的桶数
 *   <li>lastCompactionInputFileSize - 最后一次压缩的输入文件大小
 *   <li>lastCompactionOutputFileSize - 最后一次压缩的输出文件大小
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>性能监控</b>：跟踪提交操作的性能趋势
 *   <li><b>故障诊断</b>：分析提交失败和重试的原因
 *   <li><b>容量规划</b>：评估系统的吞吐能力
 *   <li><b>优化建议</b>：根据指标调整配置参数
 * </ul>
 *
 * @see CommitStats 提交统计数据
 */
public class CommitMetrics {

    /** 直方图窗口大小，保留最近100次提交的统计 */
    private static final int HISTOGRAM_WINDOW_SIZE = 100;

    /** 指标组名称 */
    public static final String GROUP_NAME = "commit";

    /** 指标组，用于注册和管理所有提交相关指标 */
    private final MetricGroup metricGroup;

    /**
     * 构造提交指标收集器
     *
     * @param registry 指标注册器
     * @param tableName 表名
     */
    public CommitMetrics(MetricRegistry registry, String tableName) {
        this.metricGroup = registry.createTableMetricGroup(GROUP_NAME, tableName);
        registerGenericCommitMetrics();
    }

    /**
     * 获取指标组（仅用于测试）
     *
     * @return 指标组对象
     */
    @VisibleForTesting
    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    /** 提交耗时直方图 */
    private Histogram durationHistogram;

    /** 最新提交的统计信息 */
    private CommitStats latestCommit;

    // ==================== 指标名称常量 ====================

    /** 最后一次提交的持续时间（毫秒） */
    public static final String LAST_COMMIT_DURATION = "lastCommitDuration";

    /** 提交持续时间的直方图 */
    public static final String COMMIT_DURATION = "commitDuration";

    /** 最后一次提交的重试次数 */
    public static final String LAST_COMMIT_ATTEMPTS = "lastCommitAttempts";

    /** 最后一次提交新增的表文件数 */
    public static final String LAST_TABLE_FILES_ADDED = "lastTableFilesAdded";

    /** 最后一次提交删除的表文件数 */
    public static final String LAST_TABLE_FILES_DELETED = "lastTableFilesDeleted";

    /** 最后一次提交追加的表文件数 */
    public static final String LAST_TABLE_FILES_APPENDED = "lastTableFilesAppended";

    /** 最后一次提交压缩的表文件数 */
    public static final String LAST_TABLE_FILES_COMMIT_COMPACTED = "lastTableFilesCommitCompacted";

    /** 最后一次提交追加的Changelog文件数 */
    public static final String LAST_CHANGELOG_FILES_APPENDED = "lastChangelogFilesAppended";

    /** 最后一次提交压缩的Changelog文件数 */
    public static final String LAST_CHANGELOG_FILES_COMMIT_COMPACTED =
            "lastChangelogFileCommitCompacted";

    /** 最后一次提交生成的快照数 */
    public static final String LAST_GENERATED_SNAPSHOTS = "lastGeneratedSnapshots";

    /** 最后一次提交追加的Delta记录数 */
    public static final String LAST_DELTA_RECORDS_APPENDED = "lastDeltaRecordsAppended";

    /** 最后一次提交追加的Changelog记录数 */
    public static final String LAST_CHANGELOG_RECORDS_APPENDED = "lastChangelogRecordsAppended";

    /** 最后一次提交压缩的Delta记录数 */
    public static final String LAST_DELTA_RECORDS_COMMIT_COMPACTED =
            "lastDeltaRecordsCommitCompacted";

    /** 最后一次提交压缩的Changelog记录数 */
    public static final String LAST_CHANGELOG_RECORDS_COMMIT_COMPACTED =
            "lastChangelogRecordsCommitCompacted";

    /** 最后一次提交写入的分区数 */
    public static final String LAST_PARTITIONS_WRITTEN = "lastPartitionsWritten";

    /** 最后一次提交写入的桶数 */
    public static final String LAST_BUCKETS_WRITTEN = "lastBucketsWritten";

    /** 最后一次压缩的输入文件大小（字节） */
    public static final String LAST_COMPACTION_INPUT_FILE_SIZE = "lastCompactionInputFileSize";

    /** 最后一次压缩的输出文件大小（字节） */
    public static final String LAST_COMPACTION_OUTPUT_FILE_SIZE = "lastCompactionOutputFileSize";

    /**
     * 注册通用提交指标
     *
     * <p>为所有提交相关的指标注册Gauge和Histogram。
     */
    private void registerGenericCommitMetrics() {
        metricGroup.gauge(
                LAST_COMMIT_DURATION, () -> latestCommit == null ? 0L : latestCommit.getDuration());
        metricGroup.gauge(
                LAST_COMMIT_ATTEMPTS, () -> latestCommit == null ? 0L : latestCommit.getAttempts());
        metricGroup.gauge(
                LAST_GENERATED_SNAPSHOTS,
                () -> latestCommit == null ? 0L : latestCommit.getGeneratedSnapshots());
        metricGroup.gauge(
                LAST_PARTITIONS_WRITTEN,
                () -> latestCommit == null ? 0L : latestCommit.getNumPartitionsWritten());
        metricGroup.gauge(
                LAST_BUCKETS_WRITTEN,
                () -> latestCommit == null ? 0L : latestCommit.getNumBucketsWritten());
        durationHistogram = metricGroup.histogram(COMMIT_DURATION, HISTOGRAM_WINDOW_SIZE);
        metricGroup.gauge(
                LAST_TABLE_FILES_ADDED,
                () -> latestCommit == null ? 0L : latestCommit.getTableFilesAdded());
        metricGroup.gauge(
                LAST_TABLE_FILES_DELETED,
                () -> latestCommit == null ? 0L : latestCommit.getTableFilesDeleted());
        metricGroup.gauge(
                LAST_TABLE_FILES_APPENDED,
                () -> latestCommit == null ? 0L : latestCommit.getTableFilesAppended());
        metricGroup.gauge(
                LAST_TABLE_FILES_COMMIT_COMPACTED,
                () -> latestCommit == null ? 0L : latestCommit.getTableFilesCompacted());
        metricGroup.gauge(
                LAST_CHANGELOG_FILES_APPENDED,
                () -> latestCommit == null ? 0L : latestCommit.getChangelogFilesAppended());
        metricGroup.gauge(
                LAST_CHANGELOG_FILES_COMMIT_COMPACTED,
                () -> latestCommit == null ? 0L : latestCommit.getChangelogFilesCompacted());
        metricGroup.gauge(
                LAST_DELTA_RECORDS_APPENDED,
                () -> latestCommit == null ? 0L : latestCommit.getDeltaRecordsAppended());
        metricGroup.gauge(
                LAST_CHANGELOG_RECORDS_APPENDED,
                () -> latestCommit == null ? 0L : latestCommit.getChangelogRecordsAppended());
        metricGroup.gauge(
                LAST_DELTA_RECORDS_COMMIT_COMPACTED,
                () -> latestCommit == null ? 0L : latestCommit.getDeltaRecordsCompacted());
        metricGroup.gauge(
                LAST_CHANGELOG_RECORDS_COMMIT_COMPACTED,
                () -> latestCommit == null ? 0L : latestCommit.getChangelogRecordsCompacted());
        metricGroup.gauge(
                LAST_COMPACTION_INPUT_FILE_SIZE,
                () -> latestCommit == null ? 0L : latestCommit.getCompactionInputFileSize());
        metricGroup.gauge(
                LAST_COMPACTION_OUTPUT_FILE_SIZE,
                () -> latestCommit == null ? 0L : latestCommit.getCompactionOutputFileSize());
    }

    /**
     * 报告提交统计信息
     *
     * <p>更新最新提交的统计信息，并将耗时添加到直方图中。
     *
     * @param commitStats 提交统计数据
     */
    public void reportCommit(CommitStats commitStats) {
        latestCommit = commitStats;
        durationHistogram.update(commitStats.getDuration());
    }
}
