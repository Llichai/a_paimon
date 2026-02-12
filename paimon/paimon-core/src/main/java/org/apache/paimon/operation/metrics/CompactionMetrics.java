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
import org.apache.paimon.metrics.Counter;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.MetricRegistry;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

/**
 * 压缩指标类
 *
 * <p>用于测量和报告LSM树压缩操作的性能指标。
 *
 * <h2>压缩性能监控</h2>
 * <p>该类收集压缩操作的关键指标，包括：
 * <ul>
 *   <li><b>Level0文件数</b>：最大和平均Level0文件数量
 *   <li><b>压缩线程繁忙度</b>：压缩线程在最近60秒内的使用率
 *   <li><b>压缩耗时</b>：平均压缩时间
 *   <li><b>压缩计数</b>：已完成、总数、队列中的压缩任务数
 *   <li><b>压缩文件大小</b>：最大和平均的输入/输出文件大小
 *   <li><b>总文件大小</b>：最大和平均的总文件大小
 * </ul>
 *
 * <h2>指标聚合策略</h2>
 * <p>该类为每个分区-桶组合创建独立的Reporter：
 * <ul>
 *   <li>每个分区-桶独立报告其压缩指标
 *   <li>全局指标通过聚合所有分区-桶的指标计算
 *   <li>支持细粒度的性能分析
 * </ul>
 *
 * <h2>线程繁忙度计算</h2>
 * <p>通过 {@link CompactTimer} 跟踪压缩线程的活跃时间：
 * <ul>
 *   <li>每个线程有独立的计时器
 *   <li>计算最近60秒内的繁忙百分比
 *   <li>繁忙度 = 压缩活跃时间 / 测量时间窗口 * 100%
 * </ul>
 *
 * <h2>指标名称</h2>
 * <p>所有指标都注册在 "compaction" 指标组下：
 * <ul>
 *   <li>maxLevel0FileCount - 最大Level0文件数
 *   <li>avgLevel0FileCount - 平均Level0文件数
 *   <li>compactionThreadBusy - 压缩线程繁忙度百分比
 *   <li>avgCompactionTime - 平均压缩时间（毫秒）
 *   <li>compactionCompletedCount - 已完成的压缩任务数
 *   <li>compactionTotalCount - 总压缩任务数
 *   <li>compactionQueuedCount - 队列中的压缩任务数
 *   <li>maxCompactionInputSize - 最大压缩输入文件大小
 *   <li>maxCompactionOutputSize - 最大压缩输出文件大小
 *   <li>avgCompactionInputSize - 平均压缩输入文件大小
 *   <li>avgCompactionOutputSize - 平均压缩输出文件大小
 *   <li>maxTotalFileSize - 最大总文件大小
 *   <li>avgTotalFileSize - 平均总文件大小
 * </ul>
 *
 * @see CompactTimer 压缩计时器
 */
public class CompactionMetrics {

    /**
     * 指标组名称
     */
    private static final String GROUP_NAME = "compaction";

    // ==================== 指标名称常量 ====================

    /**
     * 最大Level0文件数量
     */
    public static final String MAX_LEVEL0_FILE_COUNT = "maxLevel0FileCount";

    /**
     * 平均Level0文件数量
     */
    public static final String AVG_LEVEL0_FILE_COUNT = "avgLevel0FileCount";

    /**
     * 压缩线程繁忙度百分比
     */
    public static final String COMPACTION_THREAD_BUSY = "compactionThreadBusy";

    /**
     * 平均压缩时间（毫秒）
     */
    public static final String AVG_COMPACTION_TIME = "avgCompactionTime";

    /**
     * 已完成的压缩任务数
     */
    public static final String COMPACTION_COMPLETED_COUNT = "compactionCompletedCount";

    /**
     * 总压缩任务数
     */
    public static final String COMPACTION_TOTAL_COUNT = "compactionTotalCount";

    /**
     * 队列中的压缩任务数
     */
    public static final String COMPACTION_QUEUED_COUNT = "compactionQueuedCount";

    /**
     * 最大压缩输入文件大小（字节）
     */
    public static final String MAX_COMPACTION_INPUT_SIZE = "maxCompactionInputSize";

    /**
     * 最大压缩输出文件大小（字节）
     */
    public static final String MAX_COMPACTION_OUTPUT_SIZE = "maxCompactionOutputSize";

    /**
     * 平均压缩输入文件大小（字节）
     */
    public static final String AVG_COMPACTION_INPUT_SIZE = "avgCompactionInputSize";

    /**
     * 平均压缩输出文件大小（字节）
     */
    public static final String AVG_COMPACTION_OUTPUT_SIZE = "avgCompactionOutputSize";

    /**
     * 最大总文件大小（字节）
     */
    public static final String MAX_TOTAL_FILE_SIZE = "maxTotalFileSize";

    /**
     * 平均总文件大小（字节）
     */
    public static final String AVG_TOTAL_FILE_SIZE = "avgTotalFileSize";

    /**
     * 繁忙度测量时间窗口：60秒
     */
    private static final long BUSY_MEASURE_MILLIS = 60_000;

    /**
     * 压缩时间统计窗口大小：最近100次压缩
     */
    private static final int COMPACTION_TIME_WINDOW = 100;

    /**
     * 指标组，用于注册和管理所有压缩相关指标
     */
    private final MetricGroup metricGroup;

    /**
     * 分区-桶 → Reporter 的映射
     */
    private final Map<PartitionAndBucket, ReporterImpl> reporters;

    /**
     * 线程ID → 压缩计时器 的映射，用于跟踪每个线程的繁忙度
     */
    private final Map<Long, CompactTimer> compactTimers;

    /**
     * 压缩时间队列，保存最近的压缩耗时
     */
    private final Queue<Long> compactionTimes;

    /**
     * 已完成的压缩计数器
     */
    private Counter compactionsCompletedCounter;

    /**
     * 总压缩计数器
     */
    private Counter compactionsTotalCounter;

    /**
     * 队列中的压缩计数器
     */
    private Counter compactionsQueuedCounter;

    /**
     * 构造压缩指标收集器
     *
     * @param registry  指标注册器
     * @param tableName 表名
     */
    public CompactionMetrics(MetricRegistry registry, String tableName) {
        this.metricGroup = registry.createTableMetricGroup(GROUP_NAME, tableName);
        this.reporters = new HashMap<>();
        this.compactTimers = new ConcurrentHashMap<>();
        this.compactionTimes = new ConcurrentLinkedQueue<>();

        registerGenericCompactionMetrics();
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

    /**
     * 注册通用压缩指标
     *
     * <p>为所有压缩相关的指标注册Gauge和Counter。
     */
    private void registerGenericCompactionMetrics() {
        metricGroup.gauge(MAX_LEVEL0_FILE_COUNT, () -> getLevel0FileCountStream().max().orElse(-1));
        metricGroup.gauge(
                AVG_LEVEL0_FILE_COUNT, () -> getLevel0FileCountStream().average().orElse(-1));
        metricGroup.gauge(
                MAX_COMPACTION_INPUT_SIZE, () -> getCompactionInputSizeStream().max().orElse(-1));
        metricGroup.gauge(
                MAX_COMPACTION_OUTPUT_SIZE, () -> getCompactionOutputSizeStream().max().orElse(-1));
        metricGroup.gauge(
                AVG_COMPACTION_INPUT_SIZE,
                () -> getCompactionInputSizeStream().average().orElse(-1));
        metricGroup.gauge(
                AVG_COMPACTION_OUTPUT_SIZE,
                () -> getCompactionOutputSizeStream().average().orElse(-1));

        metricGroup.gauge(
                AVG_COMPACTION_TIME, () -> getCompactionTimeStream().average().orElse(0.0));
        metricGroup.gauge(COMPACTION_THREAD_BUSY, () -> getCompactBusyStream().sum());

        compactionsCompletedCounter = metricGroup.counter(COMPACTION_COMPLETED_COUNT);
        compactionsTotalCounter = metricGroup.counter(COMPACTION_TOTAL_COUNT);
        compactionsQueuedCounter = metricGroup.counter(COMPACTION_QUEUED_COUNT);

        metricGroup.gauge(MAX_TOTAL_FILE_SIZE, () -> getTotalFileSizeStream().max().orElse(-1));
        metricGroup.gauge(AVG_TOTAL_FILE_SIZE, () -> getTotalFileSizeStream().average().orElse(-1));
    }

    /**
     * 获取Level0文件数量流
     *
     * @return 所有分区-桶的Level0文件数量流
     */
    private LongStream getLevel0FileCountStream() {
        return reporters.values().stream().mapToLong(r -> r.level0FileCount);
    }

    private LongStream getCompactionInputSizeStream() {
        return reporters.values().stream().mapToLong(r -> r.compactionInputSize);
    }

    private LongStream getCompactionOutputSizeStream() {
        return reporters.values().stream().mapToLong(r -> r.compactionOutputSize);
    }

    private DoubleStream getCompactBusyStream() {
        return compactTimers.values().stream()
                .mapToDouble(t -> 100.0 * t.calculateLength() / BUSY_MEASURE_MILLIS);
    }

    private DoubleStream getCompactionTimeStream() {
        return compactionTimes.stream().mapToDouble(Long::doubleValue);
    }

    @VisibleForTesting
    public LongStream getTotalFileSizeStream() {
        return reporters.values().stream().mapToLong(r -> r.totalFileSize);
    }

    /**
     * 关闭指标组
     */
    public void close() {
        metricGroup.close();
    }

    /**
     * 压缩指标报告器接口
     *
     * <p>用于向 {@link CompactionMetrics} 报告指标值。
     */
    public interface Reporter {

        /**
         * 获取压缩计时器
         *
         * @return 当前线程的压缩计时器
         */
        CompactTimer getCompactTimer();

        /**
         * 报告Level0文件数量
         *
         * @param count Level0文件数量
         */
        void reportLevel0FileCount(long count);

        /**
         * 报告压缩时间
         *
         * @param time 压缩耗时（毫秒）
         */
        void reportCompactionTime(long time);

        /**
         * 增加已完成的压缩计数
         */
        void increaseCompactionsCompletedCount();

        /**
         * 增加总压缩计数
         */
        void increaseCompactionsTotalCount();

        /**
         * 增加队列中的压缩计数
         */
        void increaseCompactionsQueuedCount();

        /**
         * 减少队列中的压缩计数
         */
        void decreaseCompactionsQueuedCount();

        /**
         * 报告压缩输入文件大小
         *
         * @param bytes 输入文件大小（字节）
         */
        void reportCompactionInputSize(long bytes);

        /**
         * 报告压缩输出文件大小
         *
         * @param bytes 输出文件大小（字节）
         */
        void reportCompactionOutputSize(long bytes);

        /**
         * 报告总文件大小
         *
         * @param bytes 总文件大小（字节）
         */
        void reportTotalFileSize(long bytes);

        /**
         * 注销该Reporter
         */
        void unregister();
    }

    /**
     * 压缩指标报告器实现
     *
     * <p>每个分区-桶有一个独立的Reporter实例。
     */
    private class ReporterImpl implements Reporter {

        /**
         * 分区-桶标识
         */
        private final PartitionAndBucket key;

        /**
         * Level0文件数量
         */
        private long level0FileCount;

        /**
         * 压缩输入文件大小
         */
        private long compactionInputSize = 0;

        /**
         * 压缩输出文件大小
         */
        private long compactionOutputSize = 0;

        /**
         * 总文件大小
         */
        private long totalFileSize = 0;

        /**
         * 构造Reporter实现
         *
         * @param key 分区-桶标识
         */
        private ReporterImpl(PartitionAndBucket key) {
            this.key = key;
            this.level0FileCount = 0;
        }

        @Override
        public CompactTimer getCompactTimer() {
            return compactTimers.computeIfAbsent(
                    Thread.currentThread().getId(),
                    ignore -> new CompactTimer(BUSY_MEASURE_MILLIS));
        }

        @Override
        public void reportCompactionTime(long time) {
            synchronized (compactionTimes) {
                compactionTimes.add(time);
                if (compactionTimes.size() > COMPACTION_TIME_WINDOW) {
                    compactionTimes.poll();
                }
            }
        }

        @Override
        public void reportCompactionInputSize(long bytes) {
            this.compactionInputSize = bytes;
        }

        @Override
        public void reportCompactionOutputSize(long bytes) {
            this.compactionOutputSize = bytes;
        }

        @Override
        public void reportTotalFileSize(long bytes) {
            this.totalFileSize = bytes;
        }

        @Override
        public void reportLevel0FileCount(long count) {
            this.level0FileCount = count;
        }

        @Override
        public void increaseCompactionsCompletedCount() {
            compactionsCompletedCounter.inc();
        }

        @Override
        public void increaseCompactionsTotalCount() {
            compactionsTotalCounter.inc();
        }

        @Override
        public void increaseCompactionsQueuedCount() {
            compactionsQueuedCounter.inc();
        }

        @Override
        public void decreaseCompactionsQueuedCount() {
            compactionsQueuedCounter.dec();
        }

        @Override
        public void unregister() {
            reporters.remove(key);
        }
    }

    /**
     * 创建分区-桶的Reporter
     *
     * @param partition 分区
     * @param bucket    桶ID
     * @return Reporter实例
     */
    public Reporter createReporter(BinaryRow partition, int bucket) {
        PartitionAndBucket key = new PartitionAndBucket(partition, bucket);
        ReporterImpl reporter = new ReporterImpl(key);
        reporters.put(key, reporter);
        return reporter;
    }

    /**
     * 分区-桶标识类
     *
     * <p>用作Reporter映射的键。
     */
    private static class PartitionAndBucket {

        /**
         * 分区
         */
        private final BinaryRow partition;

        /**
         * 桶ID
         */
        private final int bucket;

        /**
         * 构造分区-桶标识
         *
         * @param partition 分区
         * @param bucket    桶ID
         */
        private PartitionAndBucket(BinaryRow partition, int bucket) {
            this.partition = partition;
            this.bucket = bucket;
        }

        @Override
        public int hashCode() {
            return Objects.hash(partition, bucket);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PartitionAndBucket)) {
                return false;
            }
            PartitionAndBucket other = (PartitionAndBucket) o;
            return Objects.equals(partition, other.partition) && bucket == other.bucket;
        }
    }
}
