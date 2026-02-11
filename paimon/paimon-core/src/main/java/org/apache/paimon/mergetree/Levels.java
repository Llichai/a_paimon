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
 * Levels（层级管理器）
 *
 * <p>存储 LSM Tree 所有层级的文件。
 *
 * <p>层级结构：
 * <ul>
 *   <li>Level-0：特殊层级，文件按序列号降序排列（最新的在前），键可能重叠
 *   <li>Level-1~N：每层是一个 {@link SortedRun}，文件按键排序且键区间不重叠
 * </ul>
 *
 * <p>Level-0 的排序规则（优先级从高到低）：
 * <ol>
 *   <li>maxSequenceNumber：序列号越大越靠前（最新数据）
 *   <li>minSequenceNumber：最小序列号升序
 *   <li>creationTime：创建时间升序
 *   <li>fileName：文件名升序（保证唯一性）
 * </ol>
 *
 * <p>核心操作：
 * <ul>
 *   <li>addLevel0File：添加新文件到 Level-0
 *   <li>update：更新层级（删除旧文件，添加新文件）
 *   <li>levelSortedRuns：获取所有层级的 Sorted Run
 *   <li>nonEmptyHighestLevel：获取最高非空层级
 * </ul>
 */
public class Levels {

    /** 键比较器 */
    private final Comparator<InternalRow> keyComparator;

    /** Level-0 文件集合（按序列号降序排列） */
    private final TreeSet<DataFileMeta> level0;

    /** Level-1 到 Level-N 的 SortedRun 列表 */
    private final List<SortedRun> levels;

    /** 文件删除回调列表 */
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
     * <p>压缩后的核心操作：
     * <ul>
     *   <li>before：压缩前的文件（需要删除）
     *   <li>after：压缩后的文件（需要添加）
     * </ul>
     *
     * @param before 需要删除的文件
     * @param after 需要添加的文件
     */
    public void update(List<DataFileMeta> before, List<DataFileMeta> after) {
        // 按层级分组
        Map<Integer, List<DataFileMeta>> groupedBefore = groupByLevel(before);
        Map<Integer, List<DataFileMeta>> groupedAfter = groupByLevel(after);
        // 更新每个层级
        for (int i = 0; i < numberOfLevels(); i++) {
            updateLevel(
                    i,
                    groupedBefore.getOrDefault(i, emptyList()),
                    groupedAfter.getOrDefault(i, emptyList()));
        }

        // 通知文件删除回调
        if (dropFileCallbacks.size() > 0) {
            Set<String> droppedFiles =
                    before.stream().map(DataFileMeta::fileName).collect(Collectors.toSet());
            // exclude upgrade files
            // 排除升级文件（只改层级，不删除）
            after.stream().map(DataFileMeta::fileName).forEach(droppedFiles::remove);
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
