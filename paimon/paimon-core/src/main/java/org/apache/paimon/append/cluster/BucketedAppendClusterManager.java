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

package org.apache.paimon.append.cluster;

import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactFutureManager;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactTask;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * 分桶追加表的聚类管理器
 *
 * <p>BucketedAppendClusterManager 负责管理 Append-Only 表单个桶的聚类(Clustering)任务,
 * 通过增量聚类策略对数据进行空间排序,提升查询性能。
 *
 * <p>聚类 vs 压缩:
 * <ul>
 *   <li><b>压缩(Compaction)</b>:合并小文件,减少文件数量,不改变数据顺序
 *   <li><b>聚类(Clustering)</b>:对数据重新排序,优化空间局部性,提升查询性能
 * </ul>
 *
 * <p>增量聚类:
 * 使用 LSM 树结构管理文件层级:
 * <pre>
 * Level 0: 新写入的文件,未排序
 * Level 1: 第一次聚类的文件,部分有序
 * Level 2: 第二次聚类的文件,全局有序
 * ...
 * Level N: 最终层级,完全有序
 * </pre>
 *
 * <p>聚类流程:
 * <pre>
 * 1. 添加文件:
 *    addNewFile() → 添加到 Level 0
 *
 * 2. 触发聚类:
 *    triggerCompaction(false) → 自动聚类
 *    - 使用 UniversalCompaction 策略选择文件
 *    - 条件:文件数量 >= numRunCompactionTrigger
 *
 *    triggerCompaction(true) → 全量聚类
 *    - 强制聚类所有文件到最高层级
 *
 * 3. 执行聚类任务:
 *    BucketedAppendClusterTask.doCompact()
 *    - 读取文件数据
 *    - 通过 CompactRewriter 重写并排序
 *    - 升级(upgrade)文件到输出层级
 *
 * 4. 更新层级:
 *    getCompactionResult() → 更新 levels 结构
 *    - 移除 before 文件
 *    - 添加 after 文件到新层级
 * </pre>
 *
 * <p>聚类策略:
 * {@link IncrementalClusterStrategy} 基于 {@link UniversalCompaction}:
 * <ul>
 *   <li>大小放大率(maxSizeAmp):限制总文件大小的增长
 *   <li>大小比率(sizeRatio):选择大小相近的文件一起聚类
 *   <li>触发阈值(numRunCompactionTrigger):最少文件数量
 * </ul>
 *
 * <p>全量聚类:
 * 当 fullCompaction=true 时:
 * <ul>
 *   <li>选择所有层级的所有文件
 *   <li>输出到最高层级(maxLevel)
 *   <li>如果已经只有一个 run 在最高层级,跳过聚类
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建聚类管理器
 * BucketedAppendClusterManager manager = new BucketedAppendClusterManager(
 *     executor,
 *     restoredFiles,
 *     schemaManager,
 *     clusterKeys,  // ["region", "city"]
 *     maxSizeAmp,
 *     sizeRatio,
 *     numRunCompactionTrigger,
 *     numLevels,
 *     files -> clusterRewrite(files)
 * );
 *
 * // 添加新文件
 * manager.addNewFile(newFile);
 *
 * // 触发聚类
 * manager.triggerCompaction(false);
 *
 * // 获取聚类结果
 * Optional<CompactResult> result = manager.getCompactionResult(true);
 * }</pre>
 *
 * @see AppendOnlyFileStore Append-Only 文件存储
 * @see IncrementalClusterStrategy 增量聚类策略
 * @see BucketedAppendLevels 分桶追加层级结构
 * @see UniversalCompaction 通用压缩策略
 */
public class BucketedAppendClusterManager extends CompactFutureManager {

    private static final Logger LOG = LoggerFactory.getLogger(BucketedAppendClusterManager.class);

    private final ExecutorService executor;
    private final BucketedAppendLevels levels;
    private final IncrementalClusterStrategy strategy;
    private final CompactRewriter rewriter;

    public BucketedAppendClusterManager(
            ExecutorService executor,
            List<DataFileMeta> restored,
            SchemaManager schemaManager,
            List<String> clusterKeys,
            int maxSizeAmp,
            int sizeRatio,
            int numRunCompactionTrigger,
            int numLevels,
            CompactRewriter rewriter) {
        this.executor = executor;
        this.levels = new BucketedAppendLevels(restored, numLevels);
        this.strategy =
                new IncrementalClusterStrategy(
                        schemaManager, clusterKeys, maxSizeAmp, sizeRatio, numRunCompactionTrigger);
        this.rewriter = rewriter;
    }

    @Override
    public boolean shouldWaitForLatestCompaction() {
        return false;
    }

    @Override
    public boolean shouldWaitForPreparingCheckpoint() {
        return false;
    }

    @Override
    public void addNewFile(DataFileMeta file) {
        levels.addLevel0File(file);
    }

    @Override
    public List<DataFileMeta> allFiles() {
        return levels.allFiles();
    }

    @Override
    public void triggerCompaction(boolean fullCompaction) {
        Optional<CompactUnit> optionalUnit;
        List<LevelSortedRun> runs = levels.levelSortedRuns();
        if (fullCompaction) {
            Preconditions.checkState(
                    taskFuture == null,
                    "A compaction task is still running while the user "
                            + "forces a new compaction. This is unexpected.");
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Trigger forced full compaction. Picking from the following runs\n{}",
                        runs);
            }
            optionalUnit = strategy.pick(levels.numberOfLevels(), runs, true);
        } else {
            if (taskFuture != null) {
                return;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Trigger normal compaction. Picking from the following runs\n{}", runs);
            }
            optionalUnit =
                    strategy.pick(levels.numberOfLevels(), runs, false)
                            .filter(unit -> !unit.files().isEmpty());
        }

        optionalUnit.ifPresent(
                unit -> {
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
                    submitCompaction(unit);
                });
    }

    private void submitCompaction(CompactUnit unit) {

        BucketedAppendClusterTask task =
                new BucketedAppendClusterTask(unit.files(), unit.outputLevel(), rewriter);

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
        taskFuture = executor.submit(task);
    }

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
                    levels.update(r.before(), r.after());
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Levels in compact manager updated. Current runs are\n{}",
                                levels.levelSortedRuns());
                    }
                });
        return result;
    }

    @Override
    public void close() throws IOException {}

    @VisibleForTesting
    public BucketedAppendLevels levels() {
        return levels;
    }

    /** A {@link CompactTask} impl for clustering of append bucketed table. */
    public static class BucketedAppendClusterTask extends CompactTask {

        private final List<DataFileMeta> toCluster;
        private final int outputLevel;
        private final CompactRewriter rewriter;

        public BucketedAppendClusterTask(
                List<DataFileMeta> toCluster, int outputLevel, CompactRewriter rewriter) {
            super(null);
            this.toCluster = toCluster;
            this.outputLevel = outputLevel;
            this.rewriter = rewriter;
        }

        @Override
        protected CompactResult doCompact() throws Exception {
            List<DataFileMeta> rewrite = rewriter.rewrite(toCluster);
            return new CompactResult(toCluster, upgrade(rewrite));
        }

        protected List<DataFileMeta> upgrade(List<DataFileMeta> files) {
            return files.stream()
                    .map(file -> file.upgrade(outputLevel))
                    .collect(Collectors.toList());
        }
    }

    /** Compact rewriter for append-only table. */
    public interface CompactRewriter {
        List<DataFileMeta> rewrite(List<DataFileMeta> compactBefore) throws Exception;
    }
}
