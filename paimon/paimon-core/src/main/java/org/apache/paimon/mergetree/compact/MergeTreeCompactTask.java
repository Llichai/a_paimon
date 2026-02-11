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

import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactTask;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.RecordLevelExpire;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.operation.metrics.CompactionMetrics;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

/**
 * MergeTree 压缩任务
 *
 * <p>负责执行 MergeTree 的归并压缩逻辑。
 *
 * <p>核心特性：
 * <ul>
 *   <li>区间分区：使用 {@link IntervalPartition} 将文件划分为不重叠的区间
 *   <li>智能升级：大文件直接升级层级，避免重写开销
 *   <li>合并压缩：小文件和重叠区间进行归并压缩
 *   <li>删除记录处理：最高层级可以丢弃删除记录
 * </ul>
 *
 * <p>工作流程：
 * <pre>
 * 1. 区间分区：将输入文件划分为多个 Section（键范围不重叠）
 * 2. 遍历 Section：
 *    a. 单个文件的 Section：
 *       - 大文件（>=minFileSize）：直接升级层级
 *       - 小文件（<minFileSize）：加入候选队列等待压缩
 *    b. 多个文件的 Section：加入候选队列进行归并压缩
 * 3. 压缩候选队列中的文件
 * </pre>
 *
 * <p>示例：
 * <pre>
 * 输入文件：[A(10MB), B(1MB), C(2MB), D(15MB), E(1MB)]
 * 区间分区：Section1[A], Section2[B,C], Section3[D], Section4[E]
 *
 * 处理：
 * - A(10MB)：大文件，直接升级
 * - B(1MB),C(2MB)：多文件，归并压缩
 * - D(15MB)：大文件，直接升级
 * - E(1MB)：小文件，归并压缩
 * </pre>
 */
public class MergeTreeCompactTask extends CompactTask {

    /** 最小文件大小阈值（小于此值的文件需要压缩） */
    private final long minFileSize;
    /** 压缩重写器 */
    private final CompactRewriter rewriter;
    /** 输出层级 */
    private final int outputLevel;
    /** 删除文件供应商（Deletion Vector 相关） */
    private final Supplier<CompactDeletionFile> compactDfSupplier;
    /** 区间分区结果（每个 Section 是一个不重叠的键范围） */
    private final List<List<SortedRun>> partitioned;
    /** 是否丢弃删除记录 */
    private final boolean dropDelete;
    /** 最大层级 */
    private final int maxLevel;
    /** 记录级别过期策略 */
    @Nullable private final RecordLevelExpire recordLevelExpire;
    /** 是否强制重写所有文件 */
    private final boolean forceRewriteAllFiles;

    /** 指标：升级文件数量 */
    private int upgradeFilesNum;

    /**
     * 构造 MergeTree 压缩任务
     *
     * @param keyComparator 键比较器
     * @param minFileSize 最小文件大小阈值
     * @param rewriter 压缩重写器
     * @param unit 压缩单元
     * @param dropDelete 是否丢弃删除记录
     * @param maxLevel 最大层级
     * @param metricsReporter 指标上报器
     * @param compactDfSupplier 删除文件供应商
     * @param recordLevelExpire 记录级别过期策略
     * @param forceRewriteAllFiles 是否强制重写所有文件
     */
    public MergeTreeCompactTask(
            Comparator<InternalRow> keyComparator,
            long minFileSize,
            CompactRewriter rewriter,
            CompactUnit unit,
            boolean dropDelete,
            int maxLevel,
            @Nullable CompactionMetrics.Reporter metricsReporter,
            Supplier<CompactDeletionFile> compactDfSupplier,
            @Nullable RecordLevelExpire recordLevelExpire,
            boolean forceRewriteAllFiles) {
        super(metricsReporter);
        this.minFileSize = minFileSize;
        this.rewriter = rewriter;
        this.outputLevel = unit.outputLevel();
        this.compactDfSupplier = compactDfSupplier;
        // 使用 IntervalPartition 将文件划分为不重叠的区间
        this.partitioned = new IntervalPartition(unit.files(), keyComparator).partition();
        this.dropDelete = dropDelete;
        this.maxLevel = maxLevel;
        this.recordLevelExpire = recordLevelExpire;
        this.forceRewriteAllFiles = forceRewriteAllFiles;

        this.upgradeFilesNum = 0;
    }

    /**
     * 执行压缩
     *
     * <p>遍历区间分区的每个 Section，根据文件数量和大小决定处理策略：
     * <ol>
     *   <li>多文件 Section：全部加入候选队列进行压缩
     *   <li>单文件 Section：
     *       <ul>
     *         <li>大文件（>=minFileSize）：直接升级层级
     *         <li>小文件（<minFileSize）：加入候选队列压缩
     *       </ul>
     * </ol>
     *
     * <p>注意：不能跳过中间文件进行压缩，这会破坏整体有序性
     *
     * @return 压缩结果
     * @throws Exception 压缩异常
     */
    @Override
    protected CompactResult doCompact() throws Exception {
        List<List<SortedRun>> candidate = new ArrayList<>();
        CompactResult result = new CompactResult();

        // Checking the order and compacting adjacent and contiguous files
        // Note: can't skip an intermediate file to compact, this will destroy the overall
        // orderliness
        // 遍历每个 Section（键范围不重叠）
        for (List<SortedRun> section : partitioned) {
            if (section.size() > 1) {
                // 多个文件：需要归并压缩
                candidate.add(section);
            } else {
                // 单个文件：根据大小决定升级或压缩
                SortedRun run = section.get(0);
                // No overlapping:
                // We can just upgrade the large file and just change the level instead of
                // rewriting it
                // But for small files, we will try to compact it
                for (DataFileMeta file : run.files()) {
                    if (file.fileSize() < minFileSize) {
                        // Smaller files are rewritten along with the previous files
                        // 小文件：加入候选队列，与前面的文件一起压缩
                        candidate.add(singletonList(SortedRun.fromSingle(file)));
                    } else {
                        // Large file appear, rewrite previous and upgrade it
                        // 大文件出现：先压缩前面累积的候选文件，然后升级当前大文件
                        rewrite(candidate, result);
                        upgrade(file, result);
                    }
                }
            }
        }
        rewrite(candidate, result); // 压缩剩余的候选文件
        result.setDeletionFile(compactDfSupplier.get()); // 设置删除文件
        return result;
    }

    /**
     * 记录指标日志
     *
     * @param startMillis 开始时间（毫秒）
     * @param compactBefore 压缩前文件
     * @param compactAfter 压缩后文件
     * @return 日志信息
     */
    @Override
    protected String logMetric(
            long startMillis, List<DataFileMeta> compactBefore, List<DataFileMeta> compactAfter) {
        return String.format(
                "%s, upgrade file num = %d",
                super.logMetric(startMillis, compactBefore, compactAfter), upgradeFilesNum);
    }

    /**
     * 升级文件层级
     *
     * <p>在以下情况需要重写而不是直接升级：
     * <ul>
     *   <li>输出到最高层级且包含删除记录：需要丢弃删除记录
     *   <li>强制重写所有文件：配置要求
     *   <li>包含过期记录：需要清理过期数据
     * </ul>
     *
     * <p>否则，只需改变文件的层级元数据，避免重写开销
     *
     * @param file 待升级文件
     * @param toUpdate 压缩结果（累积更新）
     * @throws Exception 升级异常
     */
    private void upgrade(DataFileMeta file, CompactResult toUpdate) throws Exception {
        if ((outputLevel == maxLevel && containsDeleteRecords(file)) // 最高层级且有删除记录
                || forceRewriteAllFiles // 强制重写
                || containsExpiredRecords(file)) { // 包含过期记录
            // 需要重写文件
            List<List<SortedRun>> candidate = new ArrayList<>();
            candidate.add(singletonList(SortedRun.fromSingle(file)));
            rewriteImpl(candidate, toUpdate);
            return;
        }

        // 直接升级层级（只改元数据）
        if (file.level() != outputLevel) {
            CompactResult upgradeResult = rewriter.upgrade(outputLevel, file);
            toUpdate.merge(upgradeResult);
            upgradeFilesNum++; // 升级计数+1
        }
    }

    /**
     * 重写候选文件
     *
     * <p>优化策略：
     * <ul>
     *   <li>空队列：直接返回
     *   <li>单个 Section + 单个文件：尝试升级
     *   <li>其他情况：执行归并压缩
     * </ul>
     *
     * @param candidate 候选文件队列
     * @param toUpdate 压缩结果（累积更新）
     * @throws Exception 重写异常
     */
    private void rewrite(List<List<SortedRun>> candidate, CompactResult toUpdate) throws Exception {
        if (candidate.isEmpty()) {
            return; // 空队列，直接返回
        }
        if (candidate.size() == 1) {
            List<SortedRun> section = candidate.get(0);
            if (section.size() == 0) {
                return;
            } else if (section.size() == 1) {
                // 单个 Section + 单个文件：尝试升级
                for (DataFileMeta file : section.get(0).files()) {
                    upgrade(file, toUpdate);
                }
                candidate.clear();
                return;
            }
        }
        // 执行归并压缩
        rewriteImpl(candidate, toUpdate);
    }

    /**
     * 重写实现（归并压缩）
     *
     * @param candidate 候选文件队列
     * @param toUpdate 压缩结果（累积更新）
     * @throws Exception 重写异常
     */
    private void rewriteImpl(List<List<SortedRun>> candidate, CompactResult toUpdate)
            throws Exception {
        CompactResult rewriteResult = rewriter.rewrite(outputLevel, dropDelete, candidate);
        toUpdate.merge(rewriteResult); // 合并结果
        candidate.clear(); // 清空候选队列
    }

    /**
     * 判断文件是否包含删除记录
     *
     * @param file 文件元数据
     * @return 是否包含删除记录
     */
    private boolean containsDeleteRecords(DataFileMeta file) {
        return file.deleteRowCount().map(d -> d > 0).orElse(true);
    }

    /**
     * 判断文件是否包含过期记录
     *
     * @param file 文件元数据
     * @return 是否包含过期记录
     */
    private boolean containsExpiredRecords(DataFileMeta file) {
        return recordLevelExpire != null && recordLevelExpire.isExpireFile(file);
    }
}
