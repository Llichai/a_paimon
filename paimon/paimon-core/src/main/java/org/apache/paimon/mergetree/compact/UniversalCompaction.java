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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.SortedRun;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * 通用压缩策略（Universal Compaction）- Paimon 的核心压缩策略实现
 *
 * <p>这是一种被广泛应用的 LSM Tree 压缩策略，目标是降低写放大（Write Amplification），
 * 但会增加读放大和空间放大。这种策略参考了 RocksDB 的实现方式。
 *
 * <p><b>核心思想：</b>
 * <ul>
 *   <li>按文件大小和比例选择压缩单元，而不是按照固定的层级关系
 *   <li>优先合并大小相近的文件，避免频繁重写大文件
 *   <li>同时控制空间放大（防止冗余数据过多）和文件数量（防止读取延迟）
 *   <li>支持非峰值时段更激进的压缩，平衡性能和资源利用
 * </ul>
 *
 * <p><b>三种压缩触发条件（按优先级，满足任意一个即触发）：</b>
 * <ol>
 *   <li><b>空间放大（Size Amplification）</b>
 *       <ul>
 *           <li>公式：candidateSize * 100 > maxSizeAmp% * earliestRunSize
 *           <li>目的：防止低层级文件堆积导致空间浪费
 *           <li>示例：maxSizeAmp=200，当低层级数据超过最后一个文件的 200% 时触发全量压缩
 *       </ul>
 *   <li><b>大小比例（Size Ratio）</b>
 *       <ul>
 *           <li>公式：candidateSize * (100 + sizeRatio)% < nextFileSize
 *           <li>目的：合并大小相近的文件，避免频繁重写大文件
 *           <li>示例：sizeRatio=1，相邻文件大小相差 1% 时进行合并
 *       </ul>
 *   <li><b>文件数量（File Num Trigger）</b>
 *       <ul>
 *           <li>条件：runs.size() > numRunCompactionTrigger
 *           <li>目的：控制文件数量，防止读取延迟过高
 *           <li>示例：numRunCompactionTrigger=4，文件超过 4 个时触发压缩
 *       </ul>
 * </ol>
 *
 * <p><b>参数说明：</b>
 * <ul>
 *   <li>maxSizeAmp：最大空间放大百分比，default=200（允许 200% 的额外空间）
 *   <li>sizeRatio：大小比例阈值，default=1（1% 的比例差异）
 *   <li>numRunCompactionTrigger：触发压缩的文件数量，default=4
 *   <li>earlyFullCompact：提前全量压缩策略（可选），用于定期全量压缩
 *   <li>offPeakHours：非峰值时段策略（可选），在非峰值时段更激进地压缩
 * </ul>
 *
 * <p><b>参考：</b>
 * RocksDB Universal Compaction: https://github.com/facebook/rocksdb/wiki/Universal-Compaction
 *
 * @see CompactStrategy
 * @see EarlyFullCompaction
 * @see OffPeakHours
 */
public class UniversalCompaction implements CompactStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(UniversalCompaction.class);

    /**
     * 最大空间放大百分比（例如 200 表示允许 200% 的额外空间）
     */
    private final int maxSizeAmp;
    /**
     * 大小比例阈值（例如 1 表示 1% 的比例差异）
     */
    private final int sizeRatio;
    /**
     * 触发压缩的文件数量阈值
     */
    private final int numRunCompactionTrigger;

    /**
     * 提前全量压缩策略（可选）
     */
    @Nullable
    private final EarlyFullCompaction earlyFullCompact;
    /**
     * 非峰值时段策略（可选，用于调整压缩激进程度）
     */
    @Nullable
    private final OffPeakHours offPeakHours;

    /**
     * 构造通用压缩策略
     *
     * @param maxSizeAmp              最大空间放大百分比
     * @param sizeRatio               大小比例阈值
     * @param numRunCompactionTrigger 触发压缩的文件数量
     * @param earlyFullCompact        提前全量压缩策略
     * @param offPeakHours            非峰值时段策略
     */
    public UniversalCompaction(
            int maxSizeAmp,
            int sizeRatio,
            int numRunCompactionTrigger,
            @Nullable EarlyFullCompaction earlyFullCompact,
            @Nullable OffPeakHours offPeakHours) {
        this.maxSizeAmp = maxSizeAmp;
        this.sizeRatio = sizeRatio;
        this.numRunCompactionTrigger = numRunCompactionTrigger;
        this.earlyFullCompact = earlyFullCompact;
        this.offPeakHours = offPeakHours;
    }

    /**
     * 选择压缩单元 - 通用压缩策略的核心决策方法
     *
     * <p><b>决策流程（按优先级）：</b>
     * <ol>
     *   <li><b>提前全量压缩</b>：检查是否满足提前全量压缩的条件
     *       <ul>
     *           <li>时间间隔：距离上次全量压缩超过阈值
     *           <li>总大小阈值：所有文件总大小小于阈值（小数据集快速合并）
     *           <li>增量大小阈值：非最高层级文件超过阈值（增量数据过多）
     *       </ul>
     *   <li><b>空间放大检查</b>：if (candidateSize * 100 > maxSizeAmp * earliestRunSize)
     *       <ul>
     *           <li>目标：防止低层级文件堆积导致空间浪费
     *           <li>触发时：选择所有文件进行全量压缩（到 maxLevel）
     *       </ul>
     *   <li><b>大小比例检查</b>：if (candidateSize * (100 + sizeRatio) < nextFileSize)
     *       <ul>
     *           <li>目标：合并大小相近的文件
     *           <li>触发时：选择连续的相近大小文件进行增量压缩
     *       </ul>
     *   <li><b>文件数量检查</b>：if (runs.size() > numRunCompactionTrigger)
     *       <ul>
     *           <li>目标：控制读取延迟，防止查询需要访问过多文件
     *           <li>触发时：选择最新的若干文件进行强制压缩
     *       </ul>
     * </ol>
     *
     * <p><b>执行示例：</b>
     * <pre>{@code
     * // 假设当前有 5 个文件：[10MB, 15MB, 20MB, 25MB, 100MB]
     * // 配置：maxSizeAmp=200, sizeRatio=1, numRunCompactionTrigger=4
     *
     * // 检查空间放大：
     * // candidateSize = 10+15+20+25 = 70MB
     * // earliestRunSize = 100MB
     * // 70 * 100 = 7000 > 200 * 100 = 20000? 否，不触发
     *
     * // 检查大小比例：
     * // step1: candidateSize = 10MB, 检查 10 * 1.01 = 10.1 < 15? 是，加入
     * // step2: candidateSize = 25MB, 检查 25 * 1.01 = 25.25 < 20? 否，停止
     * // 结果：选择 [10MB, 15MB] 进行压缩
     *
     * // 返回 CompactUnit(outputLevel=?, files=[10MB, 15MB])
     * }</pre>
     *
     * @param numLevels 总层级数（通常为 2-10）
     * @param runs 当前所有文件的 LevelSortedRun 列表
     *            （按时间排序，最新的在前；如果文件来自不同层级，Level-0 在最前）
     * @return 选中的压缩单元（Optional），如果不需要压缩返回 empty
     *
     * @see #pickForSizeAmp(int, List) 空间放大检查
     * @see #pickForSizeRatio(int, List) 大小比例检查
     */
    @Override
    public Optional<CompactUnit> pick(int numLevels, List<LevelSortedRun> runs) {
        int maxLevel = numLevels - 1;

        // 步骤0：尝试提前全量压缩（可选策略，如果启用）
        if (earlyFullCompact != null) {
            Optional<CompactUnit> unit = earlyFullCompact.tryFullCompact(numLevels, runs);
            if (unit.isPresent()) {
                // 满足提前全量压缩条件，直接返回
                return unit;
            }
        }

        // 步骤1：检查空间放大
        // 当低层级文件堆积过多时，触发全量压缩以释放空间
        CompactUnit unit = pickForSizeAmp(maxLevel, runs);
        if (unit != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Universal compaction due to size amplification");
            }
            return Optional.of(unit);
        }

        // 步骤2：检查大小比例
        // 合并大小相近的文件，降低写放大
        unit = pickForSizeRatio(maxLevel, runs);
        if (unit != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Universal compaction due to size ratio");
            }
            return Optional.of(unit);
        }

        // 步骤3：检查文件数量
        // 文件数过多，强制压缩以减少读取延迟
        if (runs.size() > numRunCompactionTrigger) {
            int candidateCount = runs.size() - numRunCompactionTrigger + 1;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Universal compaction due to file num");
            }
            return Optional.ofNullable(pickForSizeRatio(maxLevel, runs, candidateCount));
        }

        // 不满足任何压缩条件
        return Optional.empty();
    }

    /**
     * 强制选择所有 Level-0 文件进行压缩
     *
     * <p>用于 LOOKUP changelog 模式：确保 Level-0 文件及时压缩以生成 changelog
     *
     * @param numLevels 总层级数
     * @param runs      当前所有文件
     * @return 压缩单元（包含所有 Level-0 文件）
     */
    Optional<CompactUnit> forcePickL0(int numLevels, List<LevelSortedRun> runs) {
        // 收集所有 Level-0 文件
        int candidateCount = 0;
        for (int i = candidateCount; i < runs.size(); i++) {
            if (runs.get(i).level() > 0) {
                break; // 遇到非 Level-0 文件，停止
            }
            candidateCount++;
        }

        return candidateCount == 0
                ? Optional.empty()
                : Optional.of(pickForSizeRatio(numLevels - 1, runs, candidateCount, true));
    }

    /**
     * 基于空间放大选择压缩单元
     *
     * <p>空间放大定义：额外空间的百分比
     * <pre>
     * 空间放大 = (候选文件总大小 / 最早文件大小) * 100
     * </pre>
     *
     * <p>触发条件：
     * <pre>
     * candidateSize * 100 > maxSizeAmp * earliestRunSize
     * </pre>
     *
     * <p>示例：
     * <pre>
     * maxSizeAmp = 200（允许 200% 额外空间）
     * earliestRunSize = 100MB
     * candidateSize = 250MB
     * → 空间放大 = 250%，超过阈值，触发全量压缩
     * </pre>
     *
     * @param maxLevel 最大层级
     * @param runs     当前所有文件
     * @return 压缩单元（全量压缩）或 null
     */
    @VisibleForTesting
    CompactUnit pickForSizeAmp(int maxLevel, List<LevelSortedRun> runs) {
        if (runs.size() < numRunCompactionTrigger) {
            return null; // 文件数不足，不触发
        }

        // 候选文件总大小（除了最早的文件）
        long candidateSize =
                runs.subList(0, runs.size() - 1).stream()
                        .map(LevelSortedRun::run)
                        .mapToLong(SortedRun::totalSize)
                        .sum();

        // 最早文件的大小
        long earliestRunSize = runs.get(runs.size() - 1).run().totalSize();

        // 空间放大检查
        if (candidateSize * 100 > maxSizeAmp * earliestRunSize) {
            if (earlyFullCompact != null) {
                earlyFullCompact.updateLastFullCompaction(); // 更新全量压缩时间
            }
            return CompactUnit.fromLevelRuns(maxLevel, runs); // 全量压缩
        }

        return null;
    }

    /**
     * 基于大小比例选择压缩单元（默认从第一个文件开始）
     *
     * @param maxLevel 最大层级
     * @param runs     当前所有文件
     * @return 压缩单元或 null
     */
    @VisibleForTesting
    CompactUnit pickForSizeRatio(int maxLevel, List<LevelSortedRun> runs) {
        if (runs.size() < numRunCompactionTrigger) {
            return null;
        }

        return pickForSizeRatio(maxLevel, runs, 1);
    }

    /**
     * 基于大小比例选择压缩单元（指定候选文件数量）
     *
     * @param maxLevel       最大层级
     * @param runs           当前所有文件
     * @param candidateCount 初始候选文件数量
     * @return 压缩单元或 null
     */
    private CompactUnit pickForSizeRatio(
            int maxLevel, List<LevelSortedRun> runs, int candidateCount) {
        return pickForSizeRatio(maxLevel, runs, candidateCount, false);
    }

    /**
     * 基于大小比例选择压缩单元（核心算法实现）
     *
     * <p><b>算法核心思想：</b>
     * 从初始候选文件开始，不断扩展候选集合，直到遇到大小差异过大的文件为止。
     * 这样可以合并大小相近的文件，避免频繁重写大文件。
     *
     * <p><b>算法流程：</b>
     * <ol>
     *   <li>初始化：candidateSize 为前 candidateCount 个文件的总大小
     *   <li>循环扩展：从第 candidateCount 个文件开始遍历
     *   <li>判断条件：
     *       <pre>
     *       if (candidateSize * (100 + sizeRatio + offPeakRatio) / 100 < nextFileSize)
     *           break  // 下一个文件太大，停止扩展
     *       </pre>
     *   <li>扩展操作：将下一个文件加入候选集合，更新 candidateSize 和 candidateCount
     *   <li>输出：如果候选文件数 > 1 或强制选择，创建压缩单元
     * </ol>
     *
     * <p><b>参数含义：</b>
     * <ul>
     *   <li><b>maxLevel</b>：最大层级号（输出层级的上界）
     *   <li><b>runs</b>：所有文件列表（按时间排序）
     *   <li><b>candidateCount</b>：初始候选文件数（通常为 1）
     *   <li><b>forcePick</b>：是否强制选择（即使只有 1 个候选文件）
     *       <ul>
     *           <li>true：用于文件数量超限的场景，必须进行压缩
     *           <li>false：用于正常的大小比例选择，至少需要 2 个文件才压缩
     *       </ul>
     * </ul>
     *
     * <p><b>详细示例（sizeRatio = 1，offPeakRatio = 0）：</b>
     * <pre>{@code
     * 文件大小列表：[10MB, 11MB, 12MB, 50MB, 100MB]
     * candidateCount = 1（初始选择第一个文件）
     *
     * 步骤1：candidateSize = 10MB
     * 步骤2：检查第 2 个文件（11MB）
     *        10 * (100 + 1) / 100 = 10.1 < 11? 是，加入
     *        candidateSize = 21MB, candidateCount = 2
     *
     * 步骤3：检查第 3 个文件（12MB）
     *        21 * (100 + 1) / 100 = 21.21 < 12? 否，停止
     *        （因为 21.21 > 12，说明现有候选文件已经足够大，
     *         继续加入小文件不符合"大小相近"的原则）
     *
     * 结果：选择前 2 个文件 [10MB, 11MB] 进行压缩
     *
     * 注意：虽然第 3 个文件（12MB）也很小，但它不会被选中，
     * 因为与已选文件的大小比例关系不满足。这样的设计是为了
     * 避免混合不同大小级别的文件。
     * }</pre>
     *
     * <p><b>与非峰值时段的交互：</b>
     * <pre>{@code
     * 如果当前是非峰值时段（如夜间），offPeakRatio 可能为 10（即增加 10%）
     * 则判断条件变为：
     *   candidateSize * (100 + 1 + 10) / 100 < nextFileSize
     *   = candidateSize * 1.11 < nextFileSize
     * 这使得更多的文件会被选中进行压缩，提高压缩激进性
     * }</pre>
     *
     * @param maxLevel 最大层级
     * @param runs 文件列表
     * @param candidateCount 初始候选文件数量
     * @param forcePick 是否强制选择（即使只有 1 个候选文件）
     * @return 压缩单元，或 null 如果不满足压缩条件
     *
     * @see #ratioForOffPeak() 获取非峰值时段的比例调整
     * @see #createUnit(List, int, int) 创建压缩单元
     */
    public CompactUnit pickForSizeRatio(
            int maxLevel, List<LevelSortedRun> runs, int candidateCount, boolean forcePick) {
        // 计算初始候选文件的总大小
        long candidateSize = candidateSize(runs, candidateCount);

        // 尝试扩展候选集合
        for (int i = candidateCount; i < runs.size(); i++) {
            LevelSortedRun next = runs.get(i);

            // 判断下一个文件是否适合加入候选
            // 公式：candidateSize * (100 + sizeRatio + offPeakRatio) / 100 < nextFileSize
            // 如果成立，说明下一个文件明显更大，不适合合并
            if (candidateSize * (100.0 + sizeRatio + ratioForOffPeak()) / 100.0
                    < next.run().totalSize()) {
                // 下一个文件太大，停止扩展
                break;
            }

            // 下一个文件大小合适，加入候选
            candidateSize += next.run().totalSize();
            candidateCount++;
        }

        // 决定是否创建压缩单元
        // 要么强制选择（forcePick=true），要么至少有 2 个候选文件
        if (forcePick || candidateCount > 1) {
            return createUnit(runs, maxLevel, candidateCount);
        }

        // 候选文件不足 2 个，不进行压缩（避免频繁重写单个大文件）
        return null;
    }

    /**
     * 获取非峰值时段的比例调整
     *
     * <p>在非峰值时段，可以增加 sizeRatio 以更激进地压缩
     *
     * @return 额外的比例值（峰值时段为0）
     */
    private int ratioForOffPeak() {
        return offPeakHours == null ? 0 : offPeakHours.currentRatio(LocalDateTime.now().getHour());
    }

    /**
     * 计算候选文件的总大小
     *
     * @param runs           文件列表
     * @param candidateCount 候选文件数量
     * @return 总大小（字节）
     */
    private long candidateSize(List<LevelSortedRun> runs, int candidateCount) {
        long size = 0;
        for (int i = 0; i < candidateCount; i++) {
            size += runs.get(i).run().totalSize();
        }
        return size;
    }

    /**
     * 创建压缩单元
     *
     * <p>决定输出层级的规则：
     * <ul>
     *   <li>压缩所有文件 → 输出到 maxLevel（全量压缩）
     *   <li>部分压缩 → 输出到下一个未压缩文件的层级 - 1
     *   <li>避免输出到 Level-0：如果计算结果是 Level-0，继续包含 Level-0 文件直到非 Level-0
     * </ul>
     *
     * @param runs     文件列表
     * @param maxLevel 最大层级
     * @param runCount 压缩的文件数量
     * @return 压缩单元
     */
    @VisibleForTesting
    CompactUnit createUnit(List<LevelSortedRun> runs, int maxLevel, int runCount) {
        int outputLevel;
        if (runCount == runs.size()) {
            // 压缩所有文件 → 全量压缩
            outputLevel = maxLevel;
        } else {
            // 部分压缩 → 输出到下一个文件的层级 - 1
            outputLevel = Math.max(0, runs.get(runCount).level() - 1);
        }

        // 避免输出到 Level-0
        if (outputLevel == 0) {
            for (int i = runCount; i < runs.size(); i++) {
                LevelSortedRun next = runs.get(i);
                runCount++;
                if (next.level() != 0) {
                    outputLevel = next.level();
                    break;
                }
            }
        }

        // 如果最终压缩了所有文件，输出到 maxLevel
        if (runCount == runs.size()) {
            if (earlyFullCompact != null) {
                earlyFullCompact.updateLastFullCompaction();
            }
            outputLevel = maxLevel;
        }

        return CompactUnit.fromLevelRuns(outputLevel, runs.subList(0, runCount));
    }
}
