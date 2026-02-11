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
 * 通用压缩策略（Universal Compaction）
 *
 * <p>这是一种压缩策略，目标是降低写放大（Write Amplification），但会增加读放大和空间放大。
 *
 * <p>核心思想：
 * <ul>
 *   <li>按文件大小和比例选择压缩单元
 *   <li>优先合并大小相近的文件（size ratio）
 *   <li>控制空间放大（size amplification）
 *   <li>限制文件数量（num run trigger）
 * </ul>
 *
 * <p>三种压缩触发条件（按优先级）：
 * <ol>
 *   <li><b>空间放大</b>（Size Amplification）：
 *       候选文件总大小 > maxSizeAmp% * 最早文件大小 → 全量压缩
 *   <li><b>大小比例</b>（Size Ratio）：
 *       连续文件大小比例满足条件 → 增量压缩
 *   <li><b>文件数量</b>（File Num）：
 *       文件数量 > numRunCompactionTrigger → 强制压缩
 * </ol>
 *
 * <p>参考：RocksDB Universal Compaction
 * https://github.com/facebook/rocksdb/wiki/Universal-Compaction
 *
 * @see CompactStrategy
 * @see EarlyFullCompaction
 * @see OffPeakHours
 */
public class UniversalCompaction implements CompactStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(UniversalCompaction.class);

    /** 最大空间放大百分比（例如 200 表示允许 200% 的额外空间） */
    private final int maxSizeAmp;
    /** 大小比例阈值（例如 1 表示 1% 的比例差异） */
    private final int sizeRatio;
    /** 触发压缩的文件数量阈值 */
    private final int numRunCompactionTrigger;

    /** 提前全量压缩策略（可选） */
    @Nullable private final EarlyFullCompaction earlyFullCompact;
    /** 非峰值时段策略（可选，用于调整压缩激进程度） */
    @Nullable private final OffPeakHours offPeakHours;

    /**
     * 构造通用压缩策略
     *
     * @param maxSizeAmp 最大空间放大百分比
     * @param sizeRatio 大小比例阈值
     * @param numRunCompactionTrigger 触发压缩的文件数量
     * @param earlyFullCompact 提前全量压缩策略
     * @param offPeakHours 非峰值时段策略
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
     * 选择压缩单元
     *
     * <p>按优先级尝试三种压缩策略：
     * <ol>
     *   <li>提前全量压缩（earlyFullCompact）：基于时间间隔或大小阈值
     *   <li>空间放大检查（pickForSizeAmp）：防止空间浪费过大
     *   <li>大小比例检查（pickForSizeRatio）：合并大小相近的文件
     *   <li>文件数量检查：文件数过多时强制压缩
     * </ol>
     *
     * @param numLevels 总层级数
     * @param runs 当前所有文件（按时间排序，最新的在前）
     * @return 压缩单元（如果需要压缩）
     */
    @Override
    public Optional<CompactUnit> pick(int numLevels, List<LevelSortedRun> runs) {
        int maxLevel = numLevels - 1;

        // 步骤0：尝试提前全量压缩
        if (earlyFullCompact != null) {
            Optional<CompactUnit> unit = earlyFullCompact.tryFullCompact(numLevels, runs);
            if (unit.isPresent()) {
                return unit;
            }
        }

        // 步骤1：检查空间放大
        CompactUnit unit = pickForSizeAmp(maxLevel, runs);
        if (unit != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Universal compaction due to size amplification");
            }
            return Optional.of(unit);
        }

        // 步骤2：检查大小比例
        unit = pickForSizeRatio(maxLevel, runs);
        if (unit != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Universal compaction due to size ratio");
            }
            return Optional.of(unit);
        }

        // 步骤3：检查文件数量
        if (runs.size() > numRunCompactionTrigger) {
            // 文件数过多，强制压缩
            int candidateCount = runs.size() - numRunCompactionTrigger + 1;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Universal compaction due to file num");
            }
            return Optional.ofNullable(pickForSizeRatio(maxLevel, runs, candidateCount));
        }

        return Optional.empty();
    }

    /**
     * 强制选择所有 Level-0 文件进行压缩
     *
     * <p>用于 LOOKUP changelog 模式：确保 Level-0 文件及时压缩以生成 changelog
     *
     * @param numLevels 总层级数
     * @param runs 当前所有文件
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
     * @param runs 当前所有文件
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
     * @param runs 当前所有文件
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
     * @param maxLevel 最大层级
     * @param runs 当前所有文件
     * @param candidateCount 初始候选文件数量
     * @return 压缩单元或 null
     */
    private CompactUnit pickForSizeRatio(
            int maxLevel, List<LevelSortedRun> runs, int candidateCount) {
        return pickForSizeRatio(maxLevel, runs, candidateCount, false);
    }

    /**
     * 基于大小比例选择压缩单元（核心实现）
     *
     * <p>大小比例算法：
     * <pre>
     * 1. 从初始候选文件开始（candidateCount）
     * 2. 计算候选文件总大小（candidateSize）
     * 3. 遍历后续文件：
     *    - 如果 candidateSize * (100 + sizeRatio + offPeakRatio) / 100 < nextFileSize
     *      → 停止（下一个文件太大，不适合合并）
     *    - 否则，将下一个文件加入候选
     * 4. 如果候选文件数 > 1，创建压缩单元
     * </pre>
     *
     * <p>示例：
     * <pre>
     * sizeRatio = 1（1% 比例阈值）
     * 文件大小：[10MB, 11MB, 50MB, 100MB]
     *
     * 步骤1：candidateSize = 10MB
     * 步骤2：10 * 1.01 = 10.1 < 11，加入 → candidateSize = 21MB
     * 步骤3：21 * 1.01 = 21.21 < 50，加入 → candidateSize = 71MB
     * 步骤4：71 * 1.01 = 71.71 < 100，加入 → candidateSize = 171MB
     * 结果：压缩所有4个文件
     * </pre>
     *
     * @param maxLevel 最大层级
     * @param runs 当前所有文件
     * @param candidateCount 初始候选文件数量
     * @param forcePick 是否强制选择（即使只有1个候选文件）
     * @return 压缩单元或 null
     */
    public CompactUnit pickForSizeRatio(
            int maxLevel, List<LevelSortedRun> runs, int candidateCount, boolean forcePick) {
        long candidateSize = candidateSize(runs, candidateCount);
        for (int i = candidateCount; i < runs.size(); i++) {
            LevelSortedRun next = runs.get(i);
            // 检查下一个文件是否适合加入候选
            if (candidateSize * (100.0 + sizeRatio + ratioForOffPeak()) / 100.0
                    < next.run().totalSize()) {
                break; // 下一个文件太大，停止
            }

            candidateSize += next.run().totalSize();
            candidateCount++;
        }

        if (forcePick || candidateCount > 1) {
            return createUnit(runs, maxLevel, candidateCount);
        }

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
     * @param runs 文件列表
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
     * @param runs 文件列表
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
