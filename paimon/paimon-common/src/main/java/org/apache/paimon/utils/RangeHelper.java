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

package org.apache.paimon.utils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.ToLongFunction;

/**
 * 范围辅助类，用于处理具有起始和结束位置的对象范围。
 *
 * <p>RangeHelper 是一个通用的范围处理工具，可以处理任何具有起始和结束位置的对象类型。
 * 它通过函数式接口提取对象的起始和结束位置，然后执行范围相关的操作。
 *
 * <p>主要特性：
 * <ul>
 *   <li>泛型支持：可以处理任何类型的对象
 *   <li>函数式设计：通过 ToLongFunction 提取范围边界
 *   <li>范围合并：自动合并重叠的范围对象
 *   <li>范围比较：检查多个范围是否相同
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 定义一个包含范围信息的类
 * class DataRange {
 *     long start;
 *     long end;
 *     String data;
 * }
 *
 * // 创建 RangeHelper
 * RangeHelper<DataRange> helper = new RangeHelper<>(
 *     r -> r.start,  // 起始位置提取函数
 *     r -> r.end     // 结束位置提取函数
 * );
 *
 * // 检查所有范围是否相同
 * List<DataRange> ranges = Arrays.asList(
 *     new DataRange(0, 100, "A"),
 *     new DataRange(0, 100, "B")
 * );
 * boolean allSame = helper.areAllRangesSame(ranges); // true
 *
 * // 合并重叠的范围
 * List<DataRange> overlappingRanges = Arrays.asList(
 *     new DataRange(0, 50, "A"),
 *     new DataRange(30, 80, "B"),
 *     new DataRange(100, 150, "C")
 * );
 * List<List<DataRange>> merged = helper.mergeOverlappingRanges(overlappingRanges);
 * // 结果: [[A, B], [C]]
 * }</pre>
 *
 * <p>应用场景：
 * <ul>
 *   <li>文件范围合并：合并重叠的文件块
 *   <li>数据分区处理：处理分区的范围信息
 *   <li>快照范围管理：合并重叠的快照范围
 *   <li>时间窗口合并：合并重叠的时间窗口
 * </ul>
 *
 * <p>合并算法：
 * <ol>
 *   <li>按起始位置对所有范围进行排序（起始位置相同时按结束位置排序）
 *   <li>遍历排序后的范围，检测重叠
 *   <li>将重叠的范围分组
 *   <li>每个分组内按原始顺序排列
 * </ol>
 *
 * <p>性能特性：
 * <ul>
 *   <li>范围相同检查：O(n)，其中 n 是范围数量
 *   <li>范围合并：O(n log n)，主要开销在排序
 *   <li>空间复杂度：O(n)，需要存储索引信息
 * </ul>
 *
 * <p>注意事项：
 * <ul>
 *   <li>范围边界是包含的（闭区间）
 *   <li>null 元素被视为不同的范围
 *   <li>合并后的分组内保持原始输入顺序
 *   <li>重叠判断：当前范围的起始 <= 上一个范围的结束
 * </ul>
 *
 * @param <T> 包含范围信息的对象类型
 */
public class RangeHelper<T> {

    /** 用于提取对象起始位置的函数。 */
    private final ToLongFunction<T> startFunction;

    /** 用于提取对象结束位置的函数。 */
    private final ToLongFunction<T> endFunction;

    /**
     * 创建一个 RangeHelper 实例。
     *
     * @param startFunction 用于提取对象起始位置的函数
     * @param endFunction 用于提取对象结束位置的函数
     */
    public RangeHelper(ToLongFunction<T> startFunction, ToLongFunction<T> endFunction) {
        this.startFunction = startFunction;
        this.endFunction = endFunction;
    }

    /**
     * 检查列表中的所有范围是否完全相同。
     *
     * <p>如果列表为空或只有一个元素，返回 true。
     * 如果任何元素为 null，返回 false。
     * 所有范围的起始和结束位置都必须相同才返回 true。
     *
     * @param ranges 要检查的范围列表
     * @return 如果所有范围完全相同返回 true，否则返回 false
     */
    public boolean areAllRangesSame(List<T> ranges) {
        if (ranges == null || ranges.isEmpty()) {
            return true;
        }

        // Get the first range as reference
        T first = ranges.get(0);

        // Compare all other ranges with the first one
        for (int i = 1; i < ranges.size(); i++) {
            T current = ranges.get(i);
            if (current == null || first == null) {
                return false; // Null elements are considered different
            }
            if (startFunction.applyAsLong(current) != startFunction.applyAsLong(first)
                    || endFunction.applyAsLong(current) != endFunction.applyAsLong(first)) {
                return false; // Found a different range
            }
        }

        return true; // All ranges are identical
    }

    /**
     * 合并重叠的范围对象。
     *
     * <p>此方法将输入的范围列表按起始位置排序，然后将重叠的范围分组。
     * 每个分组包含所有相互重叠的范围，分组内的范围按原始输入顺序排列。
     *
     * <p>重叠判断：如果当前范围的起始位置 <= 上一个范围的结束位置，
     * 则认为两个范围重叠。
     *
     * <p>示例：
     * <pre>{@code
     * // 输入范围（起始-结束）：
     * // [0-50], [30-80], [100-150], [120-180]
     *
     * // 输出分组：
     * // 组1: [[0-50], [30-80]]  (重叠：30 <= 50)
     * // 组2: [[100-150], [120-180]]  (重叠：120 <= 150)
     * }</pre>
     *
     * @param ranges 要合并的范围列表
     * @return 分组后的范围列表，每个分组包含一组相互重叠的范围
     */
    public List<List<T>> mergeOverlappingRanges(List<T> ranges) {
        int n = ranges.size();
        if (n == 0) {
            return new ArrayList<>();
        }

        // Create a list of IndexedValue to keep track of original indices
        List<IndexedValue> indexedRanges = new ArrayList<>();
        for (int i = 0; i < ranges.size(); i++) {
            indexedRanges.add(new IndexedValue(ranges.get(i), i));
        }

        // Sort the ranges by their start value
        indexedRanges.sort(
                Comparator.comparingLong(IndexedValue::start).thenComparingLong(IndexedValue::end));

        List<List<IndexedValue>> groups = new ArrayList<>();
        // Initialize with the first range
        List<IndexedValue> currentGroup = new ArrayList<>();
        currentGroup.add(indexedRanges.get(0));
        long currentEnd = indexedRanges.get(0).end();

        // Iterate through the sorted ranges and merge overlapping ones
        for (int i = 1; i < indexedRanges.size(); i++) {
            IndexedValue current = indexedRanges.get(i);
            long start = current.start();
            long end = current.end();

            // If the current range overlaps with the current group, merge it
            if (start <= currentEnd) {
                currentGroup.add(current);
                // Update the current end to the maximum end of the merged ranges
                if (end > currentEnd) {
                    currentEnd = end;
                }
            } else {
                // Otherwise, start a new group
                groups.add(currentGroup);
                currentGroup = new ArrayList<>();
                currentGroup.add(current);
                currentEnd = end;
            }
        }
        // Add the last group
        groups.add(currentGroup);

        // Convert the groups to the required format and sort each group by original index
        List<List<T>> result = new ArrayList<>();
        for (List<IndexedValue> group : groups) {
            // Sort the group by original index to maintain the input order
            group.sort(Comparator.comparingInt(ip -> ip.originalIndex));
            // Extract the pairs
            List<T> sortedGroup = new ArrayList<>();
            for (IndexedValue ip : group) {
                sortedGroup.add(ip.value);
            }
            result.add(sortedGroup);
        }

        return result;
    }

    /**
     * 内部辅助类，用于跟踪原始索引。
     *
     * <p>在范围排序和合并过程中，需要保持原始输入顺序的信息，
     * 此类将范围对象与其原始索引关联起来。
     */
    private class IndexedValue {

        /** 范围对象。 */
        private final T value;

        /** 在原始列表中的索引位置。 */
        private final int originalIndex;

        /**
         * 创建索引值对象。
         *
         * @param value 范围对象
         * @param originalIndex 原始索引
         */
        private IndexedValue(T value, int originalIndex) {
            this.value = value;
            this.originalIndex = originalIndex;
        }

        /**
         * 获取范围的起始位置。
         *
         * @return 起始位置
         */
        private long start() {
            return startFunction.applyAsLong(value);
        }

        /**
         * 获取范围的结束位置。
         *
         * @return 结束位置
         */
        private long end() {
            return endFunction.applyAsLong(value);
        }
    }
}
