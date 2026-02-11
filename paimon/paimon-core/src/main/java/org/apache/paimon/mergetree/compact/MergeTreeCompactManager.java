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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.compact.CompactFutureManager;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactTask;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.RecordLevelExpire;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.operation.metrics.MetricUtils;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * MergeTree 压缩管理器
 *
 * <p>管理 {@link KeyValueFileStore} 的压缩任务生命周期。
 *
 * <p>核心职责：
 * <ul>
 *   <li>触发压缩：根据策略选择需要压缩的文件
 *   <li>任务管理：提交和跟踪压缩任务执行
 *   <li>结果处理：更新 Levels 结构
 *   <li>指标上报：上报压缩相关指标
 * </ul>
 *
 * <p>工作流程：
 * <pre>
 * 1. triggerCompaction：选择压缩单元（CompactUnit）
 * 2. submitCompaction：创建并提交压缩任务
 * 3. getCompactionResult：获取压缩结果并更新 Levels
 * </pre>
 */
public class MergeTreeCompactManager extends CompactFutureManager {

    private static final Logger LOG = LoggerFactory.getLogger(MergeTreeCompactManager.class);

    /** 异步执行器 */
    private final ExecutorService executor;
    /** LSM Tree 的层级结构 */
    private final Levels levels;
    /** 压缩策略 */
    private final CompactStrategy strategy;
    /** 键比较器 */
    private final Comparator<InternalRow> keyComparator;
    /** 压缩文件大小阈值 */
    private final long compactionFileSize;
    /** Sorted Run 数量停止触发阈值 */
    private final int numSortedRunStopTrigger;
    /** 压缩重写器 */
    private final CompactRewriter rewriter;

    /** 压缩指标上报器 */
    @Nullable private final CompactionMetrics.Reporter metricsReporter;
    /** Deletion Vector 维护器 */
    @Nullable private final BucketedDvMaintainer dvMaintainer;
    /** 是否延迟生成删除文件 */
    private final boolean lazyGenDeletionFile;
    /** 是否需要 Lookup（LOOKUP changelog 模式） */
    private final boolean needLookup;
    /** 是否强制重写所有文件 */
    private final boolean forceRewriteAllFiles;
    /** 是否强制保留删除记录 */
    private final boolean forceKeepDelete;

    /** 记录级别过期策略 */
    @Nullable private final RecordLevelExpire recordLevelExpire;

    /**
     * 构造 MergeTree 压缩管理器
     *
     * @param executor 异步执行器
     * @param levels LSM Tree 层级结构
     * @param strategy 压缩策略
     * @param keyComparator 键比较器
     * @param compactionFileSize 压缩文件大小阈值
     * @param numSortedRunStopTrigger Sorted Run 数量停止触发阈值
     * @param rewriter 压缩重写器
     * @param metricsReporter 指标上报器
     * @param dvMaintainer Deletion Vector 维护器
     * @param lazyGenDeletionFile 是否延迟生成删除文件
     * @param needLookup 是否需要 Lookup
     * @param recordLevelExpire 记录级别过期策略
     * @param forceRewriteAllFiles 是否强制重写所有文件
     * @param forceKeepDelete 是否强制保留删除记录
     */
    public MergeTreeCompactManager(
            ExecutorService executor,
            Levels levels,
            CompactStrategy strategy,
            Comparator<InternalRow> keyComparator,
            long compactionFileSize,
            int numSortedRunStopTrigger,
            CompactRewriter rewriter,
            @Nullable CompactionMetrics.Reporter metricsReporter,
            @Nullable BucketedDvMaintainer dvMaintainer,
            boolean lazyGenDeletionFile,
            boolean needLookup,
            @Nullable RecordLevelExpire recordLevelExpire,
            boolean forceRewriteAllFiles,
            boolean forceKeepDelete) {
        this.executor = executor;
        this.levels = levels;
        this.strategy = strategy;
        this.compactionFileSize = compactionFileSize;
        this.numSortedRunStopTrigger = numSortedRunStopTrigger;
        this.keyComparator = keyComparator;
        this.rewriter = rewriter;
        this.metricsReporter = metricsReporter;
        this.dvMaintainer = dvMaintainer;
        this.lazyGenDeletionFile = lazyGenDeletionFile;
        this.recordLevelExpire = recordLevelExpire;
        this.needLookup = needLookup;
        this.forceRewriteAllFiles = forceRewriteAllFiles;
        this.forceKeepDelete = forceKeepDelete;

        // 初始化时上报指标
        MetricUtils.safeCall(this::reportMetrics, LOG);
    }

    /**
     * 是否应该等待最新的压缩任务完成
     *
     * <p>当 Sorted Run 数量超过阈值时，需要等待压缩完成
     *
     * @return 是否等待
     */
    @Override
    public boolean shouldWaitForLatestCompaction() {
        return levels.numberOfSortedRuns() > numSortedRunStopTrigger;
    }

    /**
     * 是否应该等待准备检查点
     *
     * <p>当 Sorted Run 数量超过阈值+1时，需要等待
     * （使用 long 强制转换避免数值溢出）
     *
     * @return 是否等待
     */
    @Override
    public boolean shouldWaitForPreparingCheckpoint() {
        // cast to long to avoid Numeric overflow
        return levels.numberOfSortedRuns() > (long) numSortedRunStopTrigger + 1;
    }

    /**
     * 添加新文件到 Levels
     *
     * <p>如果覆盖空分区，快照会变为 APPEND，文件可能升级到高层级，
     * 因此使用 #update 方法
     *
     * @param file 新文件
     */
    @Override
    public void addNewFile(DataFileMeta file) {
        // if overwrite an empty partition, the snapshot will be changed to APPEND, then its files
        // might be upgraded to high level, thus we should use #update
        levels.update(Collections.emptyList(), Collections.singletonList(file));
        MetricUtils.safeCall(this::reportMetrics, LOG);
    }

    /**
     * 获取所有文件
     *
     * @return 所有文件列表
     */
    @Override
    public List<DataFileMeta> allFiles() {
        return levels.allFiles();
    }

    /**
     * 触发压缩任务
     *
     * <p>根据压缩类型选择压缩单元：
     * <ul>
     *   <li>全量压缩：强制压缩所有文件到最高层级
     *   <li>普通压缩：根据策略选择需要压缩的文件
     * </ul>
     *
     * <p>压缩单元选择条件：
     * <ul>
     *   <li>文件列表不为空
     *   <li>多个文件，或单个文件需要升级层级
     * </ul>
     *
     * <p>删除记录处理策略：
     * <ul>
     *   <li>输出层级为0：可能有未参与压缩的旧数据，保留删除记录
     *   <li>输出层级>0且为最高层级：输出是最旧的数据，可以丢弃删除记录
     *   <li>有 DV 维护器：可以丢弃删除记录
     * </ul>
     *
     * @param fullCompaction 是否全量压缩
     */
    @Override
    public void triggerCompaction(boolean fullCompaction) {
        Optional<CompactUnit> optionalUnit;
        List<LevelSortedRun> runs = levels.levelSortedRuns();
        if (fullCompaction) {
            // 全量压缩：强制压缩所有文件
            Preconditions.checkState(
                    taskFuture == null,
                    "A compaction task is still running while the user "
                            + "forces a new compaction. This is unexpected.");
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Trigger forced full compaction. Picking from the following runs\n{}",
                        runs);
            }
            optionalUnit =
                    CompactStrategy.pickFullCompaction(
                            levels.numberOfLevels(),
                            runs,
                            recordLevelExpire,
                            dvMaintainer,
                            forceRewriteAllFiles);
        } else {
            // 普通压缩：根据策略选择
            if (taskFuture != null) {
                return; // 已有任务在运行，直接返回
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Trigger normal compaction. Picking from the following runs\n{}", runs);
            }
            optionalUnit =
                    strategy.pick(levels.numberOfLevels(), runs)
                            .filter(unit -> !unit.files().isEmpty()) // 文件不为空
                            .filter(
                                    unit ->
                                            unit.files().size() > 1 // 多个文件
                                                    || unit.files().get(0).level()
                                                            != unit.outputLevel()); // 或需要升级层级
        }

        optionalUnit.ifPresent(
                unit -> {
                    /*
                     * As long as there is no older data, We can drop the deletion.
                     * If the output level is 0, there may be older data not involved in compaction.
                     * If the output level is bigger than 0, as long as there is no older data in
                     * the current levels, the output is the oldest, so we can drop the deletion.
                     * See CompactStrategy.pick.
                     */
                    // 判断是否可以丢弃删除记录
                    boolean dropDelete =
                            !forceKeepDelete // 未强制保留删除记录
                                    && unit.outputLevel() != 0 // 输出层级不为0
                                    && (unit.outputLevel() >= levels.nonEmptyHighestLevel() // 输出为最高层级
                                            || dvMaintainer != null); // 或有 DV 维护器

                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Submit compaction with files (name, level, size): "
                                        + levels.levelSortedRuns().stream()
                                                .flatMap(lsr -> lsr.run().files().stream())
                                                .map(
                                                        file ->
                                                                String.format(
                                                                        "(%s, %d, %d)",
                                                                        file.fileName(),
                                                                        file.level(),
                                                                        file.fileSize()))
                                                .collect(Collectors.joining(", ")));
                    }
                    submitCompaction(unit, dropDelete); // 提交压缩任务
                });
    }

    /**
     * 获取 Levels（测试可见）
     *
     * @return Levels 结构
     */
    @VisibleForTesting
    public Levels levels() {
        return levels;
    }

    /**
     * 提交压缩任务
     *
     * <p>根据压缩单元类型创建不同的任务：
     * <ul>
     *   <li>文件重写：{@link FileRewriteCompactTask}（简单重写）
     *   <li>归并压缩：{@link MergeTreeCompactTask}（区间分区+归并）
     * </ul>
     *
     * @param unit 压缩单元
     * @param dropDelete 是否丢弃删除记录
     */
    private void submitCompaction(CompactUnit unit, boolean dropDelete) {
        // 创建删除文件供应商（Deletion Vector 相关）
        Supplier<CompactDeletionFile> compactDfSupplier = () -> null;
        if (dvMaintainer != null) {
            compactDfSupplier =
                    lazyGenDeletionFile
                            ? () -> CompactDeletionFile.lazyGeneration(dvMaintainer) // 延迟生成
                            : () -> CompactDeletionFile.generateFiles(dvMaintainer); // 立即生成
        }

        CompactTask task;
        if (unit.fileRewrite()) {
            // 文件重写任务（简单重写）
            task = new FileRewriteCompactTask(rewriter, unit, dropDelete, metricsReporter);
        } else {
            // 归并压缩任务（区间分区+归并）
            task =
                    new MergeTreeCompactTask(
                            keyComparator,
                            compactionFileSize,
                            rewriter,
                            unit,
                            dropDelete,
                            levels.maxLevel(),
                            metricsReporter,
                            compactDfSupplier,
                            recordLevelExpire,
                            forceRewriteAllFiles);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Pick these files (name, level, size) for {} compaction: {}",
                    task.getClass().getSimpleName(),
                    unit.files().stream()
                            .map(
                                    file ->
                                            String.format(
                                                    "(%s, %d, %d)",
                                                    file.fileName(), file.level(), file.fileSize()))
                            .collect(Collectors.joining(", ")));
        }
        taskFuture = executor.submit(task); // 提交到执行器
        if (metricsReporter != null) {
            metricsReporter.increaseCompactionsQueuedCount(); // 队列计数+1
            metricsReporter.increaseCompactionsTotalCount(); // 总数+1
        }
    }

    /**
     * 完成当前任务，并将结果文件更新到 {@link Levels}
     *
     * @param blocking 是否阻塞等待
     * @return 压缩结果
     * @throws ExecutionException 执行异常
     * @throws InterruptedException 中断异常
     */
    @Override
    public Optional<CompactResult> getCompactionResult(boolean blocking)
            throws ExecutionException, InterruptedException {
        Optional<CompactResult> result = innerGetCompactionResult(blocking);
        result.ifPresent(
                r -> {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Update levels in compact manager with these changes:\nBefore:\n{}\nAfter:\n{}",
                                r.before(),
                                r.after());
                    }
                    levels.update(r.before(), r.after()); // 更新 Levels
                    MetricUtils.safeCall(this::reportMetrics, LOG); // 上报指标
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Levels in compact manager updated. Current runs are\n{}",
                                levels.levelSortedRuns());
                    }
                });
        return result;
    }

    /**
     * 压缩是否未完成
     *
     * <p>对于 LOOKUP 压缩，需要确保所有 Level-0 文件都被消费，
     * 因此这里需要让外部认为还有未完成的压缩工作
     *
     * @return 是否未完成
     */
    @Override
    public boolean compactNotCompleted() {
        // If it is a lookup compaction, we should ensure that all level 0 files are consumed, so
        // here we need to make the outside think that we still need to do unfinished compact
        // working
        return super.compactNotCompleted() || (needLookup && !levels().level0().isEmpty());
    }

    /**
     * 上报压缩指标
     */
    private void reportMetrics() {
        if (metricsReporter != null) {
            metricsReporter.reportLevel0FileCount(levels.level0().size()); // Level-0 文件数
            metricsReporter.reportTotalFileSize(levels.totalFileSize()); // 总文件大小
        }
    }

    /**
     * 关闭压缩管理器
     *
     * @throws IOException IO 异常
     */
    @Override
    public void close() throws IOException {
        rewriter.close(); // 关闭重写器
        if (metricsReporter != null) {
            MetricUtils.safeCall(metricsReporter::unregister, LOG); // 注销指标上报器
        }
    }

    /**
     * 获取压缩策略（测试可见）
     *
     * @return 压缩策略
     */
    @VisibleForTesting
    public CompactStrategy getStrategy() {
        return strategy;
    }
}
