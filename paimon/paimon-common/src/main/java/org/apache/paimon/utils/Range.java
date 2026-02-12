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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * 数值范围类，表示一个闭区间 [from, to]，即包含起始和结束位置的范围。
 *
 * <p>Range 类用于表示连续或离散的数值区间，广泛应用于数据分区、文件范围、快照 ID 范围等场景。
 * 该类提供了丰富的范围操作方法，包括合并、交集、排除等。
 *
 * <p>主要特性：
 * <ul>
 *   <li>闭区间表示：范围包含起始值和结束值，即 [from, to]
 *   <li>可序列化：支持在网络传输和持久化场景中使用
 *   <li>不可变性：一旦创建，范围的边界不可修改
 *   <li>范围运算：支持交集、并集、排除等集合运算
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建一个范围 [0, 100]
 * Range range = new Range(0, 100);
 *
 * // 检查范围交集
 * Range other = new Range(50, 150);
 * boolean hasIntersection = range.hasIntersection(other); // true
 *
 * // 合并多个重叠的范围
 * List<Range> ranges = Arrays.asList(
 *     new Range(0, 10),
 *     new Range(5, 15),
 *     new Range(20, 30)
 * );
 * List<Range> merged = Range.sortAndMergeOverlap(ranges);
 * // 结果: [0, 15], [20, 30]
 *
 * // 从范围中排除某些子范围
 * Range base = new Range(0, 10000);
 * List<Range> toExclude = Arrays.asList(
 *     new Range(1000, 2000),
 *     new Range(5000, 6000)
 * );
 * List<Range> remaining = base.exclude(toExclude);
 * // 结果: [0, 999], [2001, 4999], [6001, 10000]
 *
 * // 计算两个范围列表的交集
 * List<Range> left = Arrays.asList(new Range(0, 10), new Range(20, 30));
 * List<Range> right = Arrays.asList(new Range(5, 15), new Range(25, 35));
 * List<Range> intersection = Range.and(left, right);
 * // 结果: [5, 10], [25, 30]
 * }</pre>
 *
 * <p>应用场景：
 * <ul>
 *   <li>快照 ID 范围管理：表示要查询的快照 ID 区间
 *   <li>分区范围计算：表示数据分区的范围
 *   <li>文件偏移范围：表示要读取的文件字节范围
 *   <li>行号范围：表示要处理的行号区间
 *   <li>时间戳范围：表示时间窗口的边界
 * </ul>
 *
 * <p>性能特性：
 * <ul>
 *   <li>范围合并：O(n log n)，其中 n 是范围数量
 *   <li>范围交集：O(n + m)，其中 n 和 m 是两个列表的长度
 *   <li>范围排除：O(n)，其中 n 是要排除的范围数量
 *   <li>所有操作都是无锁的，线程安全取决于调用者
 * </ul>
 *
 * <p>注意事项：
 * <ul>
 *   <li>不支持空范围：创建时必须满足 from <= to
 *   <li>范围是包含边界的：[0, 0] 表示只包含一个元素 0
 *   <li>范围偏移：使用 addOffset 可以平移整个范围
 *   <li>相邻范围：两个范围是否视为可合并取决于 adjacent 参数
 * </ul>
 */
public class Range implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 范围的起始位置（包含）。 */
    public final long from;

    /** 范围的结束位置（包含）。 */
    public final long to;

    /**
     * 创建一个范围 [from, to]（from 和 to 都包含在范围内；不允许空范围）。
     *
     * @param from 范围的起始位置（包含）
     * @param to 范围的结束位置（包含）
     * @throws AssertionError 如果 from > to（空范围）
     */
    public Range(long from, long to) {
        assert from <= to;
        this.from = from;
        this.to = to;
    }

    /**
     * 返回此范围中包含的元素数量。
     *
     * @return 元素数量（to - from + 1）
     */
    public long count() {
        return to - from + 1;
    }

    /**
     * 创建一个新的范围，将当前范围的两个边界都偏移指定的量。
     *
     * <p>示例：
     * <pre>{@code
     * Range range = new Range(10, 20);
     * Range offset = range.addOffset(100);
     * // offset = [110, 120]
     * }</pre>
     *
     * @param offset 偏移量，可以是正数或负数
     * @return 偏移后的新范围
     */
    public Range addOffset(long offset) {
        return new Range(from + offset, to + offset);
    }

    /**
     * 检查此范围是否与另一个范围有交集。
     *
     * <p>两个范围有交集的条件是：
     * <ul>
     *   <li>this.from <= range.to（此范围的起始不晚于另一范围的结束）
     *   <li>this.to >= range.from（此范围的结束不早于另一范围的起始）
     * </ul>
     *
     * @param range 要检查的范围
     * @return 如果两个范围有交集返回 true，否则返回 false
     */
    public boolean hasIntersection(Range range) {
        return from <= range.to && to >= range.from;
    }

    /**
     * 检查此范围是否完全在另一个范围之前（即此范围的结束位置小于另一范围的起始位置）。
     *
     * @param other 要比较的范围
     * @return 如果此范围完全在另一范围之前返回 true
     */
    public boolean isBefore(Range other) {
        return to < other.from;
    }

    /**
     * 检查此范围是否完全在另一个范围之后（即此范围的起始位置大于另一范围的结束位置）。
     *
     * @param other 要比较的范围
     * @return 如果此范围完全在另一范围之后返回 true
     */
    public boolean isAfter(Range other) {
        return from > other.to;
    }

    /**
     * 将此范围转换为包含所有元素的 Long 列表。
     *
     * <p>警告：此方法会创建一个包含范围内所有值的列表，
     * 如果范围很大可能会导致内存问题。仅适用于小范围。
     *
     * @return 包含范围内所有值的列表
     */
    public List<Long> toListLong() {
        List<Long> longs = new ArrayList<>();
        for (long i = from; i <= to; i++) {
            longs.add(i);
        }
        return longs;
    }

    /**
     * Excludes the given ranges from this range and returns the remaining ranges.
     *
     * <p>For example, if this range is [0, 10000] and ranges to exclude are [1000, 2000], [3000,
     * 4000], [5000, 6000], then the result is [0, 999], [2001, 2999], [4001, 4999], [6001, 10000].
     *
     * @param ranges the ranges to exclude (can be unsorted and overlapping)
     * @return the remaining ranges after exclusion
     */
    public List<Range> exclude(List<Range> ranges) {
        if (ranges.isEmpty()) {
            return Collections.singletonList(this);
        }

        // Sort ranges by from
        List<Range> sorted = new ArrayList<>(ranges);
        sorted.sort(Comparator.comparingLong(a -> a.from));

        List<Range> result = new ArrayList<>();
        long current = this.from;

        for (Range exclude : sorted) {
            // Compute intersection with the current range
            Range intersect = Range.intersection(new Range(current, this.to), exclude);
            if (intersect == null) {
                continue;
            }
            // Add the part before the intersection (if any)
            if (current < intersect.from) {
                result.add(new Range(current, intersect.from - 1));
            }
            // Move current position past the intersection
            current = intersect.to + 1;
            if (current > this.to) {
                break;
            }
        }

        // Add the remaining part after all exclusions (if any)
        if (current <= this.to) {
            result.add(new Range(current, this.to));
        }

        return result;
    }

    /**
     * 对范围列表进行排序并合并重叠的范围。
     *
     * <p>此方法是 {@link #sortAndMergeOverlap(List, boolean)} 的简化版本，
     * 不合并相邻的范围（只合并重叠的范围）。
     *
     * @param ranges 要处理的范围列表
     * @return 排序并合并后的范围列表
     */
    public static List<Range> sortAndMergeOverlap(List<Range> ranges) {
        return sortAndMergeOverlap(ranges, false);
    }

    /**
     * 对范围列表进行排序并合并重叠（或相邻）的范围。
     *
     * <p>算法步骤：
     * <ol>
     *   <li>按照 from 值对范围进行排序
     *   <li>遍历排序后的范围，检查相邻范围是否可以合并
     *   <li>如果可以合并（重叠或相邻），则扩展当前范围
     *   <li>如果不能合并，则将当前范围加入结果并开始新范围
     * </ol>
     *
     * <p>示例：
     * <pre>{@code
     * // 不合并相邻范围
     * List<Range> ranges = Arrays.asList(
     *     new Range(0, 5),
     *     new Range(6, 10),  // 与前一个范围相邻但不重叠
     *     new Range(8, 15)   // 与前一个范围重叠
     * );
     * List<Range> merged = Range.sortAndMergeOverlap(ranges, false);
     * // 结果: [0, 5], [6, 15]
     *
     * // 合并相邻范围
     * merged = Range.sortAndMergeOverlap(ranges, true);
     * // 结果: [0, 15]
     * }</pre>
     *
     * @param ranges 要处理的范围列表，可以是无序且重叠的
     * @param adjacent 是否合并相邻的范围（true = 合并相邻，false = 只合并重叠）
     * @return 排序并合并后的范围列表
     */
    public static List<Range> sortAndMergeOverlap(List<Range> ranges, boolean adjacent) {
        if (ranges == null || ranges.isEmpty()) {
            return Collections.emptyList();
        }

        if (ranges.size() == 1) {
            return new ArrayList<>(ranges);
        }

        // Sort ranges by from
        List<Range> sorted = new ArrayList<>(ranges);
        sorted.sort(Comparator.comparingLong(r -> r.from));

        List<Range> result = new ArrayList<>();
        Range current = sorted.get(0);

        for (int i = 1; i < sorted.size(); i++) {
            Range next = sorted.get(i);
            // Check if current and next overlap (not just adjacent)
            if (current.to + (adjacent ? 1 : 0) >= next.from) {
                // Merge: extend current range
                current = new Range(current.from, Math.max(current.to, next.to));
            } else {
                // No overlap: add current to result and move to next
                result.add(current);
                current = next;
            }
        }
        // Add the last range
        result.add(current);

        return result;
    }

    /**
     * 尽可能合并已排序的范围列表（不要求完全排序）。
     *
     * <p>此方法假设输入的范围列表基本有序，但可能存在部分重叠。
     * 它会尝试将相邻的可合并范围进行合并。
     *
     * <p>与 {@link #sortAndMergeOverlap(List)} 的区别：
     * <ul>
     *   <li>此方法不会对输入进行排序，假设输入已经基本有序
     *   <li>此方法只检查相邻的范围是否可以合并
     *   <li>如果输入无序，可能无法合并所有可合并的范围
     * </ul>
     *
     * @param ranges 基本有序的范围列表
     * @return 合并后的范围列表
     */
    public static List<Range> mergeSortedAsPossible(List<Range> ranges) {
        if (ranges == null || ranges.isEmpty()) {
            return Collections.emptyList();
        }

        if (ranges.size() == 1) {
            return new ArrayList<>(ranges);
        }

        List<Range> result = new ArrayList<>();
        Range current = ranges.get(0);

        for (int i = 1; i < ranges.size(); i++) {
            Range next = ranges.get(i);
            // Try to merge current and next
            Range merged = Range.union(current, next);
            if (merged != null) {
                // Merged successfully
                current = merged;
            } else {
                // Cannot merge: add current to result and move to next
                result.add(current);
                current = next;
            }
        }
        // Add the last range
        result.add(current);

        return result;
    }

    /**
     * 将一个 ID 的可迭代集合转换为范围列表。
     *
     * <p>此方法会将连续的 ID 合并为一个范围。例如：
     * <pre>{@code
     * List<Long> ids = Arrays.asList(1L, 2L, 3L, 5L, 6L, 10L);
     * List<Range> ranges = Range.toRanges(ids);
     * // 结果: [1, 3], [5, 6], [10, 10]
     * }</pre>
     *
     * <p>注意：此方法假设输入的 ID 是有序的。
     *
     * @param ids 要转换的 ID 集合（应该是有序的）
     * @return 转换后的范围列表
     */
    public static List<Range> toRanges(Iterable<Long> ids) {
        List<Range> ranges = new ArrayList<>();
        Iterator<Long> iterator = ids.iterator();

        if (!iterator.hasNext()) {
            return ranges;
        }

        long rangeStart = iterator.next();
        long rangeEnd = rangeStart;

        while (iterator.hasNext()) {
            long current = iterator.next();
            if (current != rangeEnd + 1) {
                // Save the current range and start a new one
                ranges.add(new Range(rangeStart, rangeEnd));
                rangeStart = current;
            }
            rangeEnd = current;
        }
        // Add the last range
        ranges.add(new Range(rangeStart, rangeEnd));

        return ranges;
    }

    /**
     * Computes the intersection of two lists of ranges.
     *
     * <p>Assumes that both left and right are sorted and merged already (no overlaps within each
     * list).
     *
     * <p>For example, left=[0,10],[20,30] and right=[5,15],[25,35] will return [5,10],[25,30].
     *
     * @param left the first list of ranges (must be sorted and merged)
     * @param right the second list of ranges (must be sorted and merged)
     * @return the intersection of the two lists
     */
    public static List<Range> and(List<Range> left, List<Range> right) {
        if (left == null || right == null || left.isEmpty() || right.isEmpty()) {
            return Collections.emptyList();
        }

        List<Range> result = new ArrayList<>();
        int i = 0;
        int j = 0;

        while (i < left.size() && j < right.size()) {
            Range l = left.get(i);
            Range r = right.get(j);

            // Compute intersection of current ranges
            Range intersect = Range.intersection(l, r);
            if (intersect != null) {
                result.add(intersect);
            }

            // Advance the pointer of the range that ends earlier
            if (l.to <= r.to) {
                i++;
            } else {
                j++;
            }
        }

        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Range range = (Range) o;
        return from == range.from && to == range.to;
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to);
    }

    @Override
    public String toString() {
        return "[" + from + ", " + to + ']';
    }

    /**
     * 计算两个范围的并集，如果它们之间有间隙则返回 null。
     *
     * <p>此方法会尝试合并两个范围。如果两个范围重叠或相邻，则返回它们的并集；
     * 否则返回 null。
     *
     * <p>示例：
     * <pre>{@code
     * Range r1 = new Range(0, 10);
     * Range r2 = new Range(5, 15);
     * Range union = Range.union(r1, r2);
     * // union = [0, 15]
     *
     * Range r3 = new Range(0, 10);
     * Range r4 = new Range(20, 30);
     * Range union2 = Range.union(r3, r4);
     * // union2 = null（因为有间隙）
     * }</pre>
     *
     * @param left 第一个范围
     * @param right 第二个范围
     * @return 两个范围的并集，如果它们之间有间隙则返回 null
     */
    public static Range union(Range left, Range right) {
        if (left.from <= right.from) {
            if (left.to + 1 >= right.from) {
                return new Range(left.from, Math.max(left.to, right.to));
            }
        } else if (right.to + 1 >= left.from) {
            return new Range(right.from, Math.max(left.to, right.to));
        }
        return null;
    }

    /**
     * 计算两个范围的交集，如果它们不重叠则返回 null。
     *
     * <p>此方法会计算两个范围的重叠部分。如果两个范围有交集，返回交集范围；
     * 否则返回 null。
     *
     * <p>示例：
     * <pre>{@code
     * Range r1 = new Range(0, 10);
     * Range r2 = new Range(5, 15);
     * Range intersection = Range.intersection(r1, r2);
     * // intersection = [5, 10]
     *
     * Range r3 = new Range(0, 10);
     * Range r4 = new Range(20, 30);
     * Range intersection2 = Range.intersection(r3, r4);
     * // intersection2 = null（不重叠）
     * }</pre>
     *
     * @param left 第一个范围
     * @param right 第二个范围
     * @return 两个范围的交集，如果它们不重叠则返回 null
     */
    public static Range intersection(Range left, Range right) {
        if (left.from <= right.from) {
            if (left.to >= right.from) {
                return new Range(right.from, Math.min(left.to, right.to));
            }
        } else if (right.to >= left.from) {
            return new Range(left.from, Math.min(left.to, right.to));
        }
        return null;
    }

    /**
     * 检查两个原始范围（由起始和结束位置定义）是否有交集。
     *
     * <p>这是一个静态工具方法，用于快速检查两个范围是否重叠，
     * 而无需创建 Range 对象。
     *
     * @param start1 第一个范围的起始位置
     * @param end1 第一个范围的结束位置
     * @param start2 第二个范围的起始位置
     * @param end2 第二个范围的结束位置
     * @return 如果两个范围有交集返回 true
     */
    public static boolean intersect(long start1, long end1, long start2, long end2) {
        long intersectionStart = Math.max(start1, start2);
        long intersectionEnd = Math.min(end1, end2);
        return intersectionStart <= intersectionEnd;
    }
}
