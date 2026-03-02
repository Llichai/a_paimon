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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.utils.Preconditions;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Levels（层级管理器）- LSM Tree 的核心层级组织管理类
 *
 * <p>这是 Paimon LSM Tree 实现中最重要的类之一，负责管理所有数据文件在不同层级的组织和分布。
 *
 * <p><b>层级结构：</b>
 * <ul>
 *   <li><b>Level-0</b>：特殊层级，使用 TreeSet 存储，文件按序列号降序排列（最新的在前）
 *       <ul>
 *           <li>键可能重叠（允许新写入的数据与已有数据有重叠）
 *           <li>一个文件对应一个 SortedRun
 *           <li>写入性能最高，读取需要查询所有 Level-0 文件
 *       </ul>
 *   <li><b>Level-1~N</b>：常规层级，每层是一个 SortedRun，文件按键排序且键区间不重叠
 *       <ul>
 *           <li>数据相对稳定，压缩频率较低
 *           <li>读取性能好，键范围明确
 *           <li>空间放大最低
 *       </ul>
 * </ul>
 *
 * <p><b>Level-0 的排序规则（优先级从高到低）：</b>
 * <ol>
 *   <li><b>maxSequenceNumber</b>：序列号越大越靠前（最新数据优先）
 *       <ul>
 *           <li>确保读取时首先查询最新的数据
 *           <li>相同主键时，较新的记录会被优先读取
 *       </ul>
 *   <li><b>minSequenceNumber</b>：最小序列号升序
 *       <ul>
 *           <li>当 maxSequenceNumber 相同时比较
 *           <li>多个并发写入可能生成相同 maxSequenceNumber 的文件
 *       </ul>
 *   <li><b>creationTime</b>：创建时间升序
 *       <ul>
 *           <li>进一步区分文件的先后顺序
 *           <li>时间戳精确到毫秒级
 *       </ul>
 *   <li><b>fileName</b>：文件名升序（保证 TreeSet 的唯一性）
 *       <ul>
 *           <li>最后的比较器，确保 TreeSet 不会认为两个不同的文件重复
 *           <li>虽然前面的字段已经可以区分，但 TreeSet 要求 compareTo() 返回 0 仅当两个对象相同
 *       </ul>
 * </ol>
 *
 * <p><b>核心操作：</b>
 * <ul>
 *   <li>{@link #addLevel0File(DataFileMeta)}：添加新文件到 Level-0（写入时使用）
 *   <li>{@link #update(List, List)}：更新层级（压缩完成后更新）
 *   <li>{@link #levelSortedRuns()}：获取所有层级的 Sorted Run（用于读取和压缩决策）
 *   <li>{@link #nonEmptyHighestLevel()}：获取最高非空层级（压缩策略决策）
 *   <li>{@link #numberOfSortedRuns()}：获取总的 SortedRun 数量（用于触发压缩）
 * </ul>
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li>写入阶段：MergeTreeWriter 刷新新文件时调用 addLevel0File()
 *   <li>读取阶段：MergeTreeReaders 根据 levelSortedRuns() 确定要读取的文件
 *   <li>压缩阶段：CompactStrategy 根据层级结构做出压缩决策，压缩完成后调用 update()
 *   <li>状态管理：跟踪每层的文件分布，计算统计指标
 * </ul>
 *
 * @see LevelSortedRun
 * @see SortedRun
 * @see DataFileMeta
 */
public class Levels {

    /**
     * 键比较器 - 用于比较键值大小
     * 在压缩和读取时用于确定文件的键顺序，确保数据有序性
     */
    private final Comparator<InternalRow> keyComparator;

    /**
     * Level-0 文件集合（按序列号降序排列，使用 TreeSet 自动排序）
     *
     * <p><b>存储策略：</b>
     * <ul>
     *   <li>TreeSet 自动维护排序顺序，新文件添加时自动插入正确位置
     *   <li>按 maxSequenceNumber 降序：最新的数据文件在集合前面
     *   <li>元素总数通常较少（通常 10-100 个文件），TreeSet 的 log(n) 性能可接受
     * </ul>
     *
     * <p><b>生命周期：</b>
     * <ul>
     *   <li>MergeTreeWriter.flushWriteBuffer()：生成新的 Level-0 文件，添加到此集合
     *   <li>UniversalCompaction.pick()：选择 Level-0 文件进行压缩
     *   <li>MergeTreeCompactTask.doCompact()：压缩 Level-0 文件到更高层级
     *   <li>update()：压缩完成后删除旧文件
     * </ul>
     */
    private final TreeSet<DataFileMeta> level0;

    /**
     * Level-1 到 Level-N 的 SortedRun 列表（ArrayList，索引 i 对应 Level i+1）
     *
     * <p><b>数据组织：</b>
     * <ul>
     *   <li>levels.get(0) = Level-1 的 SortedRun
     *   <li>levels.get(1) = Level-2 的 SortedRun
     *   <li>levels.get(i) = Level-(i+1) 的 SortedRun
     *   <li>每层最多一个 SortedRun（文件按键排序，键不重叠）
     * </ul>
     *
     * <p><b>特点：</b>
     * <ul>
     *   <li>键区间不重叠：同一层的所有文件的键范围是分离的
     *   <li>相对稳定：只在压缩时更新，写入性能稳定
     *   <li>读取高效：可以通过二分查找快速定位要读取的文件
     * </ul>
     */
    private final List<SortedRun> levels;

    /**
     * 文件删除回调列表 - 当文件从 LSM Tree 中删除时回调
     *
     * <p><b>用途：</b>
     * <ul>
     *   <li>通知外部组件（如 Lookup 缓存）文件已被删除
     *   <li>清理相关的索引或其他辅助数据结构
     *   <li>用于 Bloom Filter 缓存的失效处理
     * </ul>
     */
    private final List<DropFileCallback> dropFileCallbacks = new ArrayList<>();

    /**
     * 构造 Levels
     *
     * @param keyComparator 键比较器
     * @param inputFiles 输入文件列表
     * @param numLevels 层级数量（至少为2）
     */
    public Levels(
            Comparator<InternalRow> keyComparator, List<DataFileMeta> inputFiles, int numLevels) {
        this.keyComparator = keyComparator;

        // in case the num of levels is not specified explicitly
        // 如果层级数未明确指定，使用输入文件中的最大层级+1
        int restoredNumLevels =
                Math.max(
                        numLevels,
                        inputFiles.stream().mapToInt(DataFileMeta::level).max().orElse(-1) + 1);
        checkArgument(restoredNumLevels > 1, "Number of levels must be at least 2.");
        // 初始化 Level-0（TreeSet 用于自动排序）
        this.level0 =
                new TreeSet<>(
                        (a, b) -> {
                            if (a.maxSequenceNumber() != b.maxSequenceNumber()) {
                                // file with larger sequence number should be in front
                                // 第一级：按最大序列号降序（最新的在前）
                                return Long.compare(b.maxSequenceNumber(), a.maxSequenceNumber());
                            } else {
                                // When two or more jobs are writing the same merge tree, it is
                                // possible that multiple files have the same maxSequenceNumber. In
                                // this case we have to compare their file names so that files with
                                // same maxSequenceNumber won't be "de-duplicated" by the tree set.
                                // 第二级：按最小序列号升序
                                int minSeqCompare =
                                        Long.compare(a.minSequenceNumber(), b.minSequenceNumber());
                                if (minSeqCompare != 0) {
                                    return minSeqCompare;
                                }
                                // If minSequenceNumber is also the same, use creation time
                                // 第三级：按创建时间升序
                                int timeCompare = a.creationTime().compareTo(b.creationTime());
                                if (timeCompare != 0) {
                                    return timeCompare;
                                }
                                // Final fallback: filename (to ensure uniqueness in TreeSet)
                                // 第四级：按文件名升序（保证 TreeSet 的唯一性）
                                return a.fileName().compareTo(b.fileName());
                            }
                        });
        // 初始化 Level-1 到 Level-N（空的 SortedRun）
        this.levels = new ArrayList<>();
        for (int i = 1; i < restoredNumLevels; i++) {
            levels.add(SortedRun.empty());
        }

        // 将输入文件按层级分组并添加
        Map<Integer, List<DataFileMeta>> levelMap = new HashMap<>();
        for (DataFileMeta file : inputFiles) {
            levelMap.computeIfAbsent(file.level(), level -> new ArrayList<>()).add(file);
        }
        levelMap.forEach((level, files) -> updateLevel(level, emptyList(), files));

        // 验证文件数量正确
        Preconditions.checkState(
                level0.size() + levels.stream().mapToInt(r -> r.files().size()).sum()
                        == inputFiles.size(),
                "Number of files stored in Levels does not equal to the size of inputFiles. This is unexpected.");
    }

    /**
     * 获取 Level-0 文件集合
     *
     * @return Level-0 文件集合（按序列号降序排列）
     */
    public TreeSet<DataFileMeta> level0() {
        return level0;
    }

    /**
     * 添加文件删除回调
     *
     * @param callback 回调
     */
    public void addDropFileCallback(DropFileCallback callback) {
        dropFileCallbacks.add(callback);
    }

    /**
     * 添加文件到 Level-0
     *
     * @param file 文件元数据（必须是 Level-0）
     */
    public void addLevel0File(DataFileMeta file) {
        checkArgument(file.level() == 0);
        level0.add(file);
    }

    /**
     * 获取指定层级的 SortedRun
     *
     * @param level 层级（必须 > 0，Level-0 没有单一的 SortedRun）
     * @return SortedRun
     */
    public SortedRun runOfLevel(int level) {
        checkArgument(level > 0, "Level0 does not have one single sorted run.");
        return levels.get(level - 1);
    }

    /**
     * 获取层级总数（包括 Level-0）
     *
     * @return 层级总数
     */
    public int numberOfLevels() {
        return levels.size() + 1;
    }

    /**
     * 获取最大层级（不包括 Level-0）
     *
     * @return 最大层级
     */
    public int maxLevel() {
        return levels.size();
    }

    /**
     * 获取 Sorted Run 总数
     *
     * <p>Level-0 中每个文件算一个 Sorted Run，
     * Level-1~N 中每个非空层级算一个 Sorted Run
     *
     * @return Sorted Run 总数
     */
    public int numberOfSortedRuns() {
        int numberOfSortedRuns = level0.size(); // Level-0 每个文件一个 Run
        for (SortedRun run : levels) {
            if (run.nonEmpty()) {
                numberOfSortedRuns++; // 非空层级一个 Run
            }
        }
        return numberOfSortedRuns;
    }

    /**
     * 获取最高非空层级
     *
     * @return 最高非空层级，如果所有层级都为空则返回 -1
     */
    public int nonEmptyHighestLevel() {
        int i;
        // 从最高层级向下查找非空层级
        for (i = levels.size() - 1; i >= 0; i--) {
            if (levels.get(i).nonEmpty()) {
                return i + 1;
            }
        }
        // 所有高层级都为空，检查 Level-0
        return level0.isEmpty() ? -1 : 0;
    }

    /**
     * 获取总文件大小
     *
     * @return 总文件大小（字节）
     */
    public long totalFileSize() {
        return level0.stream().mapToLong(DataFileMeta::fileSize).sum()
                + levels.stream().mapToLong(SortedRun::totalSize).sum();
    }

    /**
     * 获取所有文件列表
     *
     * @return 所有文件列表
     */
    public List<DataFileMeta> allFiles() {
        List<DataFileMeta> files = new ArrayList<>();
        List<LevelSortedRun> runs = levelSortedRuns();
        for (LevelSortedRun run : runs) {
            files.addAll(run.run().files());
        }
        return files;
    }

    /**
     * 获取所有层级的 Sorted Run
     *
     * <p>顺序：Level-0 中的每个文件 + Level-1~N 中的非空层级
     *
     * @return LevelSortedRun 列表
     */
    public List<LevelSortedRun> levelSortedRuns() {
        List<LevelSortedRun> runs = new ArrayList<>();
        // Level-0 中的每个文件作为单独的 Run
        level0.forEach(file -> runs.add(new LevelSortedRun(0, SortedRun.fromSingle(file))));
        // Level-1~N 中的非空层级
        for (int i = 0; i < levels.size(); i++) {
            SortedRun run = levels.get(i);
            if (run.nonEmpty()) {
                runs.add(new LevelSortedRun(i + 1, run));
            }
        }
        return runs;
    }

    /**
     * 更新层级（删除旧文件，添加新文件）
     *
     * <p><b>这是压缩流程中的关键步骤</b>
     *
     * <p><b>工作流程：</b>
     * <ol>
     *   <li>将输入的文件按层级分组（before 和 after）
     *   <li>对每个层级执行 updateLevel() 操作
     *   <li>通知文件删除回调，告知外部组件文件已删除
     * </ol>
     *
     * <p><b>压缩的示例流程：</b>
     * <pre>{@code
     * // 压缩前：Level-0 中有 4 个文件，Level-1 中有 1 个文件
     * before = [file1(L0), file2(L0), file3(L0), file4(L0), file5(L1)]
     *
     * // 压缩选择了 L0 的 4 个文件和 L1 的 1 个文件，合并到 L2
     * after = [file6(L2), file7(L2)]  // 新生成的文件
     *
     * // 调用 update(before, after)
     * update(before, after)
     *
     * // 结果：
     * // - Level-0：4 个文件删除
     * // - Level-1：1 个文件删除
     * // - Level-2：添加 2 个新文件
     * }</pre>
     *
     * <p><b>注意事项：</b>
     * <ul>
     *   <li>相同文件名的文件在 after 中表示升级（只改层级，不删除），不会触发删除回调
     *   <li>调用此方法前，压缩任务应已完成，确保新文件已持久化
     *   <li>此方法是原子操作，不支持分步执行
     * </ul>
     *
     * @param before 需要删除的文件列表（来自压缩前的文件）
     * @param after 需要添加的文件列表（来自压缩后的文件，可能在不同的层级）
     *
     * @see MergeTreeCompactTask#doCompact() 压缩执行
     * @see CompactManager#commit(CompactResult) 压缩结果提交
     */
    public void update(List<DataFileMeta> before, List<DataFileMeta> after) {
        // 步骤1：按层级分组，便于逐层更新
        Map<Integer, List<DataFileMeta>> groupedBefore = groupByLevel(before);
        Map<Integer, List<DataFileMeta>> groupedAfter = groupByLevel(after);

        // 步骤2：遍历所有层级，执行层级更新
        for (int i = 0; i < numberOfLevels(); i++) {
            updateLevel(
                    i,
                    groupedBefore.getOrDefault(i, emptyList()),
                    groupedAfter.getOrDefault(i, emptyList()));
        }

        // 步骤3：通知文件删除回调（外部组件可以收到删除通知）
        if (dropFileCallbacks.size() > 0) {
            // 收集所有被删除的文件名
            Set<String> droppedFiles =
                    before.stream().map(DataFileMeta::fileName).collect(Collectors.toSet());

            // 排除升级文件（升级是指文件层级改变但文件本身未被删除，只是改变了级别）
            // 例如：file1 从 Level-0 升级到 Level-1，此时 file1 既在 before 也在 after
            after.stream().map(DataFileMeta::fileName).forEach(droppedFiles::remove);

            // 通知所有回调
            for (DropFileCallback callback : dropFileCallbacks) {
                droppedFiles.forEach(callback::notifyDropFile);
            }
        }
    }

    /**
     * 更新单个层级
     *
     * @param level 层级
     * @param before 需要删除的文件
     * @param after 需要添加的文件
     */
    private void updateLevel(int level, List<DataFileMeta> before, List<DataFileMeta> after) {
        if (before.isEmpty() && after.isEmpty()) {
            return; // 无变化，直接返回
        }

        if (level == 0) {
            // Level-0：直接删除和添加
            before.forEach(level0::remove);
            level0.addAll(after);
        } else {
            // Level-1~N：从 SortedRun 中删除和添加，然后重新排序
            List<DataFileMeta> files = new ArrayList<>(runOfLevel(level).files());
            files.removeAll(before); // 删除旧文件
            files.addAll(after); // 添加新文件
            levels.set(level - 1, SortedRun.fromUnsorted(files, keyComparator)); // 重新排序
        }
    }

    /**
     * 将文件列表按层级分组
     *
     * @param files 文件列表
     * @return 层级到文件列表的映射
     */
    private Map<Integer, List<DataFileMeta>> groupByLevel(List<DataFileMeta> files) {
        return files.stream()
                .collect(Collectors.groupingBy(DataFileMeta::level, Collectors.toList()));
    }

    /**
     * 文件删除回调接口
     *
     * <p>用于通知外部组件文件已被删除
     */
    public interface DropFileCallback {

        /**
         * 通知文件被删除
         *
         * @param file 文件名
         */
        void notifyDropFile(String file);
    }
}
