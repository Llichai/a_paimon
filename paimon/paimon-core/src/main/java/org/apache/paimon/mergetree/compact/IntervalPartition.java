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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

/**
 * 区间分区算法
 *
 * <p>用于将多个数据文件分区到最少数量的 {@link SortedRun} 中。
 *
 * <p>核心思想：
 * <ul>
 *   <li>将数据文件按键范围分组，形成 Section（键范围不重叠的分区）
 *   <li>在每个 Section 内部，进一步分区为多个 {@link SortedRun}（最小化 Run 数量）
 *   <li>这种分层设计可以最小化同时处理的 SortedRun 数量，提高压缩效率
 * </ul>
 *
 * <p>算法特点：
 * <ul>
 *   <li>两层结构：外层是 Section 列表，内层是 SortedRun 列表
 *   <li>Section 之间键范围不重叠（通过右边界判断）
 *   <li>使用贪心算法最小化 SortedRun 数量
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>压缩时对文件进行分组，减少同时合并的文件数量
 *   <li>优化读取性能，减少同时打开的文件句柄数量
 * </ul>
 */
public class IntervalPartition {

    private final List<DataFileMeta> files; // 已排序的数据文件列表
    private final Comparator<InternalRow> keyComparator; // 键比较器

    /**
     * 构造区间分区算法
     *
     * @param inputFiles 输入文件列表
     * @param keyComparator 键比较器
     */
    public IntervalPartition(List<DataFileMeta> inputFiles, Comparator<InternalRow> keyComparator) {
        this.files = new ArrayList<>(inputFiles);
        // 对文件按 minKey 和 maxKey 排序
        this.files.sort(
                (o1, o2) -> {
                    int leftResult = keyComparator.compare(o1.minKey(), o2.minKey());
                    // 如果 minKey 相同，按 maxKey 排序
                    return leftResult == 0
                            ? keyComparator.compare(o1.maxKey(), o2.maxKey())
                            : leftResult;
                });
        this.keyComparator = keyComparator;
    }

    /**
     * 将数据文件分区为二维 {@link SortedRun} 列表
     *
     * <p>返回值结构：
     * <ul>
     *   <li>外层列表：Section 列表（键范围不重叠的分区）
     *   <li>内层列表：Section 内部的 {@link SortedRun} 列表
     * </ul>
     *
     * <p>这种分层设计的目的：
     * 最小化同时处理的 {@link SortedRun} 数量，提高压缩效率。
     *
     * <p>使用方式：
     *
     * <pre>{@code
     * for (List<SortedRun> section : algorithm.partition()) {
     *     // 在 section 内部进行合并排序
     * }
     * }</pre>
     *
     * @return 二维 SortedRun 列表（Section -> SortedRuns）
     */
    public List<List<SortedRun>> partition() {
        List<List<SortedRun>> result = new ArrayList<>();
        List<DataFileMeta> section = new ArrayList<>(); // 当前 Section 的文件列表
        BinaryRow bound = null; // 当前 Section 的右边界（maxKey）

        for (DataFileMeta meta : files) {
            // 如果当前文件的 minKey 大于 Section 右边界，说明键范围不重叠，需要创建新的 Section
            if (!section.isEmpty() && keyComparator.compare(meta.minKey(), bound) > 0) {
                // 将当前 Section 分区为 SortedRuns 并添加到结果
                result.add(partition(section));
                section.clear();
                bound = null;
            }
            // 将文件添加到当前 Section
            section.add(meta);
            // 更新 Section 的右边界（取当前文件 maxKey 和旧边界的较大值）
            if (bound == null || keyComparator.compare(meta.maxKey(), bound) > 0) {
                bound = meta.maxKey();
            }
        }
        if (!section.isEmpty()) {
            // 处理最后一个 Section
            result.add(partition(section));
        }

        return result;
    }

    /**
     * 将一个 Section 内的文件分区为最少数量的 {@link SortedRun}
     *
     * <p>算法思想（贪心算法）：
     * <ol>
     *   <li>使用优先队列维护已有的 SortedRun 分区（按每个分区的最大键排序）
     *   <li>对于每个新文件，选择 maxKey 最小的分区
     *   <li>如果该分区的 maxKey < 新文件的 minKey，则追加到该分区
     *   <li>否则，创建新的分区（因为键范围重叠）
     * </ol>
     *
     * <p>时间复杂度：O(n log n)，其中 n 是文件数量
     *
     * @param metas Section 内的文件列表（已排序）
     * @return 分区后的 SortedRun 列表
     */
    private List<SortedRun> partition(List<DataFileMeta> metas) {
        // 优先队列，按每个分区的最后一个文件的 maxKey 排序
        PriorityQueue<List<DataFileMeta>> queue =
                new PriorityQueue<>(
                        (o1, o2) ->
                                // 按最后一个文件的 maxKey 排序
                                keyComparator.compare(
                                        o1.get(o1.size() - 1).maxKey(),
                                        o2.get(o2.size() - 1).maxKey()));
        // 创建初始分区，包含第一个文件
        List<DataFileMeta> firstRun = new ArrayList<>();
        firstRun.add(metas.get(0));
        queue.add(firstRun);

        for (int i = 1; i < metas.size(); i++) {
            DataFileMeta meta = metas.get(i);
            // 选择 maxKey 最小的分区（任何 maxKey < meta.minKey() 的分区都可以，为了方便选择最小的）
            List<DataFileMeta> top = queue.poll();
            if (keyComparator.compare(meta.minKey(), top.get(top.size() - 1).maxKey()) > 0) {
                // 新文件的 minKey 大于分区的 maxKey，可以追加到该分区
                top.add(meta);
            } else {
                // 键范围重叠，需要创建新分区
                List<DataFileMeta> newRun = new ArrayList<>();
                newRun.add(meta);
                queue.add(newRun);
            }
            queue.add(top); // 将分区放回队列
        }

        // 分区之间的顺序不重要，只要保证每个分区内的文件有序即可
        return queue.stream().map(SortedRun::fromSorted).collect(Collectors.toList());
    }
}
