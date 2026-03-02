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

import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.RecordLevelExpire;
import org.apache.paimon.mergetree.LevelSortedRun;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 压缩策略接口 - LSM Tree 压缩决策的核心抽象
 *
 * <p>在 LSM Tree 中，数据文件分布在不同层级（Level），压缩策略负责决定：
 * <ul>
 *   <li><b>何时压缩</b>：根据当前 LSM Tree 的状态判断是否需要压缩
 *   <li><b>压缩什么</b>：选择哪些文件组合进行压缩
 *   <li><b>压缩到哪里</b>：指定压缩后文件输出到哪个层级
 * </ul>
 *
 * <p><b>核心概念：</b>
 * <ul>
 *   <li><b>Run</b>：一组有序的数据文件，键范围无重叠或有限重叠
 *       <ul>
 *           <li>Level-0：一个文件一个 run（因为 L0 键可能重叠）
 *           <li>其他 Level：一个层级一个 run（键区间明确）
 *       </ul>
 *   <li><b>LevelSortedRun</b>：为 SortedRun 添加层级信息，便于策略判断
 *   <li><b>CompactUnit</b>：压缩单元，表示一次压缩操作的完整信息
 *       <ul>
 *           <li>输入：files（需要压缩的文件）
 *           <li>输出：outputLevel（压缩结果的目标层级）
 *       </ul>
 * </ul>
 *
 * <p><b>压缩方向：</b>
 * 通常从小层级（Level-0）向大层级（Level-N）顺序压缩。这样可以：
 * <ul>
 *   <li>降低读取延迟：新数据先进入 Level-0，压缩后向下沉
 *   <li>平衡写入性能：高层级的大文件相对稳定，很少被重写
 *   <li>控制空间放大：防止低层级文件堆积
 * </ul>
 *
 * <p><b>典型实现：</b>
 * <ul>
 *   <li>{@link UniversalCompaction}：通用压缩策略，基于文件大小和数量
 *       <ul>
 *           <li>降低写放大，但增加读放大和空间放大
 *           <li>适合大多数场景，特别是写入负载较重的场景
 *       </ul>
 *   <li>自定义策略：可根据业务需求实现特定的压缩策略
 *       <ul>
 *           <li>基于时间的策略：定期进行全量压缩
 *           <li>基于数据分布的策略：根据键值范围选择压缩
 *       </ul>
 * </ul>
 *
 * <p><b>使用流程：</b>
 * <pre>{@code
 * 1. CompactManager 定期调用 pick() 方法
 * 2. CompactStrategy 根据当前 LSM Tree 状态判断是否需要压缩
 * 3. 如果需要，pick() 返回 Optional.of(CompactUnit)
 * 4. CompactManager 将 CompactUnit 分配给 MergeTreeCompactTask 执行
 * 5. 执行完成后，Levels.update() 被调用以更新层级结构
 * }</pre>
 *
 * @see UniversalCompaction 通用压缩策略实现
 * @see CompactUnit 压缩单元定义
 * @see LevelSortedRun 带层级信息的 SortedRun
 */
public interface CompactStrategy {

    Logger LOG = LoggerFactory.getLogger(CompactStrategy.class);

    /**
     * 从 runs 中选择压缩单元
     *
     * <p><b>这是压缩策略的核心决策方法</b>
     *
     * <p><b>工作原理：</b>
     * <ol>
     *   <li>输入：当前 LSM Tree 的所有 LevelSortedRun（代表每个层级的文件集合）
     *   <li>决策：根据策略的特定逻辑（如大小、数量、时间等）判断是否需要压缩
     *   <li>输出：
     *       <ul>
     *           <li>Optional.of(unit)：需要压缩，返回压缩单元定义
     *           <li>Optional.empty()：不需要压缩，等待下一个检查周期
     *       </ul>
     * </ol>
     *
     * <p><b>关键设计要点：</b>
     * <ul>
     *   <li><b>压缩是基于 run 的</b>，不是基于单个文件
     *       <ul>
     *           <li>Level-0：每个文件是一个独立的 run
     *           <li>其他层级：整个层级是一个 run
     *           <li>CompactUnit 包含的文件可能来自多个不同的 run
     *       </ul>
     *   <li><b>压缩方向是向下（upward）</b>
     *       <ul>
     *           <li>通常从小层级（L0）的 run 开始
     *           <li>与大层级（L1、L2...）的 run 进行合并
     *           <li>最终输出到更高的层级（L1、L2...）
     *       </ul>
     *   <li><b>压缩不一定全量进行</b>
     *       <ul>
     *           <li>可以选择部分 run 进行增量压缩
     *           <li>也可以选择所有 run 进行全量压缩
     *           <li>取决于具体的策略实现
     *       </ul>
     * </ul>
     *
     * <p><b>参数说明：</b>
     * <ul>
     *   <li><b>numLevels</b>：LSM Tree 的总层级数
     *       <ul>
     *           <li>通常为 2-10
     *           <li>maxLevel = numLevels - 1（最高层级的索引）
     *           <li>压缩的最终输出通常到 maxLevel
     *       </ul>
     *   <li><b>runs</b>：当前所有层级的 SortedRun 列表
     *       <ul>
     *           <li>顺序：Level-0 的 run（按序列号排序）→ Level-1 的 run → ... → Level-N 的 run
     *           <li>如果某个层级为空（没有文件），则不包含该层级的 run
     *           <li>集合中的元素类型是 LevelSortedRun（包含层级号和 SortedRun）
     *           <li>大小通常 < 100（很少有超过 100 个 run 的情况）
     *       </ul>
     * </ul>
     *
     * <p><b>返回值详解：</b>
     * <ul>
     *   <li><b>Optional.of(unit)</b>：返回压缩单元
     *       <ul>
     *           <li>unit.files()：需要压缩的文件列表
     *           <li>unit.outputLevel()：压缩后输出到哪个层级
     *           <li>通常满足以下条件：
     *               <ul>
     *                   <li>files 中的文件来自 runs，按照压缩顺序排列
     *                   <li>outputLevel >= 最高 input run 的层级
     *                   <li>outputLevel <= maxLevel（最高层级）
     *               </ul>
     *       </ul>
     *   <li><b>Optional.empty()</b>：不需要压缩
     *       <ul>
     *           <li>当前 LSM Tree 处于健康状态
     *           <li>等待更多的数据积累后再进行压缩
     *           <li>下一个检查周期会再次调用此方法
     *       </ul>
     * </ul>
     *
     * <p><b>调用频率：</b>
     * <ul>
     *   <li>CompactManager 通常每次新文件生成或定时调用
     *   <li>频率较高（可能每秒多次），所以实现应该尽可能高效
     *   <li>返回 Optional.empty() 的调用很频繁，应该尽快返回
     * </ul>
     *
     * <p><b>实现示例（UniversalCompaction）：</b>
     * <pre>{@code
     * // 伪代码，展示实现的大致逻辑
     * @Override
     * public Optional<CompactUnit> pick(int numLevels, List<LevelSortedRun> runs) {
     *     // 检查 1：空间放大
     *     if (checkSpaceAmplification(runs)) {
     *         return Optional.of(createFullCompaction(runs));
     *     }
     *
     *     // 检查 2：大小比例
     *     CompactUnit unit = pickBySizeRatio(runs);
     *     if (unit != null) {
     *         return Optional.of(unit);
     *     }
     *
     *     // 检查 3：文件数量
     *     if (runs.size() > threshold) {
     *         return Optional.of(pickByFileNumber(runs));
     *     }
     *
     *     // 不需要压缩
     *     return Optional.empty();
     * }
     * }</pre>
     *
     * @param numLevels LSM Tree 的总层级数（通常 2-10）
     * @param runs 当前 LSM Tree 的所有 LevelSortedRun 列表
     *             （按层级顺序，Level-0 → Level-N）
     * @return 选中的压缩单元，或 Optional.empty() 如果不需要压缩
     *
     * @see CompactUnit 压缩单元定义
     * @see LevelSortedRun 带层级信息的 SortedRun
     * @see UniversalCompaction 通用压缩策略的具体实现
     */
    Optional<CompactUnit> pick(int numLevels, List<LevelSortedRun> runs);

    /**
     * 选择全量压缩单元（包含所有现有文件）
     *
     * <p>全量压缩策略：
     * <ul>
     *   <li>如果只有最大层级的文件，检查是否需要重写（过期数据、删除向量等）
     *   <li>否则，压缩所有层级的所有文件到最大层级
     * </ul>
     *
     * <p>触发重写的条件：
     * <ul>
     *   <li>强制重写所有文件（forceRewriteAllFiles = true）
     *   <li>文件包含需要清理的过期记录（recordLevelExpire）
     *   <li>文件关联了删除向量（deletion vector）
     * </ul>
     *
     * @param numLevels 总层级数
     * @param runs 当前所有层级的 sorted run 列表
     * @param recordLevelExpire 记录级别过期策略（可选）
     * @param dvMaintainer 删除向量维护器（可选）
     * @param forceRewriteAllFiles 是否强制重写所有文件
     * @return 选中的压缩单元，如果不需要压缩返回 empty
     */
    static Optional<CompactUnit> pickFullCompaction(
            int numLevels,
            List<LevelSortedRun> runs,
            @Nullable RecordLevelExpire recordLevelExpire,
            @Nullable BucketedDvMaintainer dvMaintainer,
            boolean forceRewriteAllFiles) {
        int maxLevel = numLevels - 1; // 最大层级索引
        if (runs.isEmpty()) {
            // no sorted run, no need to compact
            // 没有文件，无需压缩
            return Optional.empty();
        }

        // only max level files
        // 情况1：只有最大层级的文件
        if ((runs.size() == 1 && runs.get(0).level() == maxLevel)) {
            List<DataFileMeta> filesToBeCompacted = new ArrayList<>();

            // 检查每个文件是否需要重写
            for (DataFileMeta file : runs.get(0).run().files()) {
                if (forceRewriteAllFiles) {
                    // add all files when force compacted
                    // 强制压缩模式：添加所有文件
                    filesToBeCompacted.add(file);
                } else if (recordLevelExpire != null && recordLevelExpire.isExpireFile(file)) {
                    // check record level expire for large files
                    // 检查记录级别过期：文件包含过期数据
                    filesToBeCompacted.add(file);
                } else if (dvMaintainer != null
                        && dvMaintainer.deletionVectorOf(file.fileName()).isPresent()) {
                    // check deletion vector for large files
                    // 检查删除向量：文件关联了删除向量
                    filesToBeCompacted.add(file);
                }
            }

            if (filesToBeCompacted.isEmpty()) {
                // 没有需要重写的文件
                return Optional.empty();
            }

            // 创建部分重写的压缩单元
            return Optional.of(CompactUnit.fromFiles(maxLevel, filesToBeCompacted, true));
        }

        // full compaction
        // 情况2：多层级压缩，合并所有 runs 到最大层级
        return Optional.of(CompactUnit.fromLevelRuns(maxLevel, runs));
    }
}
