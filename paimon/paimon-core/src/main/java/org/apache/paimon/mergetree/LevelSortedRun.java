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

package org.apache.paimon.mergetree;

import java.util.Objects;

/**
 * 带层级的 Sorted Run - LSM Tree 层级管理的关键数据结构
 *
 * <p>这个类为 {@link SortedRun} 添加了层级（level）信息，使得压缩策略和读取操作
 * 能够更加精确地理解每个 SortedRun 在 LSM Tree 中的位置。
 *
 * <p><b>设计目的：</b>
 * <ul>
 *   <li><b>分层管理</b>：区分同一个 SortedRun 在哪个层级，便于层级管理
 *   <li><b>压缩决策</b>：压缩策略需要知道每个 run 的层级，以做出更好的决策
 *   <li><b>读取优化</b>：读取器根据层级顺序读取（Level-0 最新，高层级更旧）
 *   <li><b>状态跟踪</b>：跟踪每层的文件分布情况
 * </ul>
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li><b>压缩策略选择</b>
 *       <ul>
 *           <li>CompactStrategy.pick() 接收 List<LevelSortedRun>
 *           <li>根据每个 run 的 level，判断是否需要向上压缩
 *           <li>避免同层级文件之间的不必要压缩
 *       </ul>
 *   <li><b>文件读取</b>
 *       <ul>
 *           <li>MergeTreeReaders 根据 level 确定读取顺序
 *           <li>Level-0 优先读取（最新数据）
 *           <li>逐层向下读取，确保数据一致性
 *       </ul>
 *   <li><b>状态查询</b>
 *       <ul>
 *           <li>Levels.levelSortedRuns() 返回所有层级的 run 列表
 *           <li>调用者可以按层级分组、统计、过滤
 *           <li>用于日志、监控、诊断
 *       </ul>
 * </ul>
 *
 * <p><b>示例：</b>
 * <pre>{@code
 * // 获取所有层级的 SortedRun
 * List<LevelSortedRun> allRuns = levels.levelSortedRuns();
 *
 * for (LevelSortedRun lsr : allRuns) {
 *     int level = lsr.level();          // 0 = Level-0, 1 = Level-1, ...
 *     SortedRun run = lsr.run();        // 该层级的 SortedRun
 *     List<DataFileMeta> files = run.files();
 *
 *     // 按层级处理，例如：
 *     if (level == 0) {
 *         // Level-0 文件需要特殊处理（键可能重叠）
 *         processLevel0(run);
 *     } else {
 *         // 高层级文件的键不重叠
 *         processHigherLevel(level, run);
 *     }
 * }
 * }</pre>
 *
 * <p><b>与 SortedRun 的关系：</b>
 * <ul>
 *   <li>LevelSortedRun = level 信息 + SortedRun 对象
 *   <li>SortedRun：只关心文件的顺序和大小
 *   <li>LevelSortedRun：额外关心这些文件在 LSM Tree 中的位置
 * </ul>
 *
 * @see SortedRun 有序的文件集合（不包含层级信息）
 * @see Levels 层级管理器（管理所有的 LevelSortedRun）
 * @see CompactStrategy 压缩策略（使用 LevelSortedRun 进行决策）
 */
public class LevelSortedRun {

    /** 层级号
     *
     * <p><b>取值范围：</b>
     * <ul>
     *   <li>0：Level-0（特殊层级，文件按序列号排序）
     *   <li>1：Level-1（常规层级，文件按键排序，键不重叠）
     *   <li>2：Level-2
     *   <li>...：更高层级
     *   <li>最大值：numLevels - 1
     * </ul>
     *
     * <p><b>Layer 与 Level 的区别：</b>
     * <ul>
     *   <li>Layer：通常指物理存储层，从新到旧
     *   <li>Level：通常指 LSM Tree 的逻辑层，从小编号到大编号
     * </ul>
     */
    private final int level;

    /** Sorted Run - 代表这一层级的有序文件集合
     *
     * <p><b>特点：</b>
     * <ul>
     *   <li>Level-0：SortedRun 的 size 通常为 1（因为 L0 中一个文件一个 run）
     *   <li>高层级：SortedRun 的 size 通常为多个（该层级的所有文件）
     *   <li>键范围：高层级的 SortedRun 中，文件的键范围不重叠
     * </ul>
     */
    private final SortedRun run;

    /**
     * 构造带层级的 Sorted Run
     *
     * @param level 层级
     * @param run Sorted Run
     */
    public LevelSortedRun(int level, SortedRun run) {
        this.level = level;
        this.run = run;
    }

    /**
     * 获取层级
     *
     * @return 层级
     */
    public int level() {
        return level;
    }

    /**
     * 获取 Sorted Run
     *
     * @return Sorted Run
     */
    public SortedRun run() {
        return run;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LevelSortedRun that = (LevelSortedRun) o;
        return level == that.level && Objects.equals(run, that.run);
    }

    @Override
    public int hashCode() {
        return Objects.hash(level, run);
    }

    @Override
    public String toString() {
        return "LevelSortedRun{" + "level=" + level + ", run=" + run + '}';
    }
}
