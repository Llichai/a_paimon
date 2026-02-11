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

package org.apache.paimon.compact;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.operation.metrics.MetricUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * 压缩任务抽象类
 *
 * <p>实现了 {@link Callable} 接口，可以在线程池中异步执行。
 *
 * <p>核心功能：
 * <ul>
 *   <li>执行压缩：调用子类的 {@link #doCompact()} 方法
 *   <li>指标收集：记录压缩时间、输入输出大小等指标
 *   <li>日志记录：记录压缩过程的详细信息
 * </ul>
 *
 * <p>执行流程：
 * <ol>
 *   <li>启动计时器
 *   <li>调用 doCompact() 执行实际压缩
 *   <li>收集压缩指标（时间、文件大小等）
 *   <li>记录日志
 *   <li>停止计时器并清理
 * </ol>
 *
 * <p>指标统计：
 * <ul>
 *   <li>压缩时间：从开始到结束的耗时
 *   <li>输入文件大小：压缩前文件的总大小
 *   <li>输出文件大小：压缩后文件的总大小
 *   <li>完成计数：成功完成的压缩次数
 * </ul>
 *
 * <p>子类需要实现 {@link #doCompact()} 方法来定义具体的压缩逻辑。
 */
public abstract class CompactTask implements Callable<CompactResult> {

    private static final Logger LOG = LoggerFactory.getLogger(CompactTask.class);

    /** 压缩指标报告器（可选） */
    @Nullable private final CompactionMetrics.Reporter metricsReporter;

    /**
     * 构造压缩任务
     *
     * @param metricsReporter 压缩指标报告器，可为 null
     */
    public CompactTask(@Nullable CompactionMetrics.Reporter metricsReporter) {
        this.metricsReporter = metricsReporter;
    }

    /**
     * 执行压缩任务
     *
     * <p>实现 {@link Callable#call()} 方法，完成以下工作：
     * <ol>
     *   <li>启动计时器
     *   <li>执行实际压缩（doCompact）
     *   <li>收集和上报指标
     *   <li>记录日志
     *   <li>停止计时器和清理队列计数
     * </ol>
     *
     * @return 压缩结果，包含压缩前后的文件列表
     * @throws Exception 如果压缩过程发生异常
     */
    @Override
    public CompactResult call() throws Exception {
        // 启动计时器
        MetricUtils.safeCall(this::startTimer, LOG);
        try {
            long startMillis = System.currentTimeMillis();
            // 执行实际压缩
            CompactResult result = doCompact();

            // 收集和上报指标
            MetricUtils.safeCall(
                    () -> {
                        if (metricsReporter != null) {
                            // 上报压缩时间
                            metricsReporter.reportCompactionTime(
                                    System.currentTimeMillis() - startMillis);
                            // 增加完成计数
                            metricsReporter.increaseCompactionsCompletedCount();
                            // 上报输入文件大小
                            metricsReporter.reportCompactionInputSize(
                                    result.before().stream()
                                            .map(DataFileMeta::fileSize)
                                            .reduce(Long::sum)
                                            .orElse(0L));
                            // 上报输出文件大小
                            metricsReporter.reportCompactionOutputSize(
                                    result.after().stream()
                                            .map(DataFileMeta::fileSize)
                                            .reduce(Long::sum)
                                            .orElse(0L));
                        }
                    },
                    LOG);

            // 记录调试日志
            if (LOG.isDebugEnabled()) {
                LOG.debug(logMetric(startMillis, result.before(), result.after()));
            }
            return result;
        } finally {
            // 停止计时器
            MetricUtils.safeCall(this::stopTimer, LOG);
            // 减少队列计数
            MetricUtils.safeCall(this::decreaseCompactionsQueuedCount, LOG);
        }
    }

    /**
     * 减少队列中的压缩任务计数
     */
    private void decreaseCompactionsQueuedCount() {
        if (metricsReporter != null) {
            metricsReporter.decreaseCompactionsQueuedCount();
        }
    }

    /**
     * 启动压缩计时器
     */
    private void startTimer() {
        if (metricsReporter != null) {
            metricsReporter.getCompactTimer().start();
        }
    }

    /**
     * 停止压缩计时器
     */
    private void stopTimer() {
        if (metricsReporter != null) {
            metricsReporter.getCompactTimer().finish();
        }
    }

    /**
     * 生成压缩指标日志消息
     *
     * <p>记录以下信息：
     * <ul>
     *   <li>压缩前文件数量
     *   <li>压缩后文件数量
     *   <li>压缩耗时
     *   <li>输入文件总大小
     *   <li>输出文件总大小
     * </ul>
     *
     * @param startMillis 压缩开始时间（毫秒）
     * @param compactBefore 压缩前的文件列表
     * @param compactAfter 压缩后的文件列表
     * @return 格式化的日志消息
     */
    protected String logMetric(
            long startMillis, List<DataFileMeta> compactBefore, List<DataFileMeta> compactAfter) {
        return String.format(
                "Done compacting %d files to %d files in %dms. "
                        + "Rewrite input file size = %d, output file size = %d",
                compactBefore.size(),
                compactAfter.size(),
                System.currentTimeMillis() - startMillis,
                collectRewriteSize(compactBefore),
                collectRewriteSize(compactAfter));
    }

    /**
     * 执行压缩（子类实现）
     *
     * <p>子类需要实现此方法来定义具体的压缩逻辑，例如：
     * <ul>
     *   <li>读取输入文件
     *   <li>合并数据
     *   <li>写入输出文件
     *   <li>生成 changelog（如果需要）
     * </ul>
     *
     * @return 压缩结果，包含压缩前后的文件列表
     * @throws Exception 如果压缩过程发生异常
     */
    protected abstract CompactResult doCompact() throws Exception;

    /**
     * 收集文件列表的总大小
     *
     * @param files 文件列表
     * @return 文件总大小（字节）
     */
    private long collectRewriteSize(List<DataFileMeta> files) {
        return files.stream().mapToLong(DataFileMeta::fileSize).sum();
    }
}
