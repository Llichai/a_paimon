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

package org.apache.paimon.statistics;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.utils.Preconditions;

import java.util.regex.Pattern;

/**
 * 截断统计信息收集器。
 *
 * <p>该收集器收集截断后的最小值/最大值统计信息。主要用于字符串类型的列，通过截断长字符串来减少统计信息的存储开销。
 *
 * <p>主要功能：
 * <ul>
 *   <li>收集空值计数
 *   <li>收集截断后的最小值（保证小于等于原始值）
 *   <li>收集截断后的最大值（保证大于等于原始值）
 *   <li>支持字符串的 Unicode 码点级别截断
 * </ul>
 *
 * <p>截断策略：
 * <ul>
 *   <li><b>最小值截断</b>：直接截断为前 N 个字符，保证结果 ≤ 原始值
 *   <li><b>最大值截断</b>：截断后尝试递增，保证结果 ≥ 原始值
 *     <ul>
 *       <li>如果原字符串长度 ≤ N，不需要截断
 *       <li>如果可以成功递增最后一个字符，使用递增后的值
 *       <li>如果无法递增（如溢出），标记失败并返回 null
 *     </ul>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建截断长度为 16 的收集器
 * TruncateSimpleColStatsCollector collector = new TruncateSimpleColStatsCollector(16);
 *
 * // 收集字符串统计信息
 * Serializer<Object> serializer = ...;
 * collector.collect(BinaryString.fromString("short"), serializer);
 * collector.collect(BinaryString.fromString("this is a very long string"), serializer);
 * collector.collect(null, serializer);
 *
 * // 获取结果
 * SimpleColStats stats = collector.result();
 * // min: "short" (长度 < 16，不截断)
 * // max: "this is a very l" + 递增的最后一个字符 (截断并递增)
 * // nullCount: 1
 *
 * // 从配置选项创建
 * SimpleColStatsCollector.Factory factory =
 *     SimpleColStatsCollector.from("TRUNCATE(32)");
 * }</pre>
 *
 * <p>最大值递增算法：
 * <pre>
 * 输入: "this is a very long string"
 * 截断为 16 个字符: "this is a very l"
 *
 * 递增过程（从后向前尝试）：
 * 1. 尝试递增最后一个字符 'l' -> 'm'
 * 2. 如果成功: "this is a very m"
 * 3. 如果失败（溢出）: 尝试前一个字符
 * </pre>
 *
 * <p>失败场景：
 * <ul>
 *   <li>所有字符都无法递增（如全是 Unicode 最大值）
 *   <li>递增会导致无效的 Unicode 码点
 *   <li>一旦失败，后续所有操作返回 null，最小值/最大值都为 null
 * </ul>
 *
 * <p>性能优势：
 * <ul>
 *   <li>减少统计信息的存储空间（特别是长字符串）
 *   <li>减少统计信息的序列化/反序列化开销
 *   <li>仍然保持统计信息的有效性（用于范围过滤）
 * </ul>
 *
 * <p>注意事项：
 * <ul>
 *   <li>仅对 {@link BinaryString} 类型进行截断
 *   <li>其他类型保持原样
 *   <li>截断长度必须大于 0
 *   <li>最大值递增可能失败，此时整个统计信息变为无效
 * </ul>
 */
public class TruncateSimpleColStatsCollector extends AbstractSimpleColStatsCollector {

    /** 截断模式的正则表达式：TRUNCATE(数字) */
    public static final Pattern TRUNCATE_PATTERN = Pattern.compile("TRUNCATE\\((\\d+)\\)");

    /** 截断长度（字符数） */
    private final int length;

    /** 是否失败（最大值递增失败） */
    private boolean failed = false;

    /**
     * 构造截断统计信息收集器。
     *
     * @param length 截断长度（必须大于 0）
     * @throws IllegalArgumentException 如果长度 ≤ 0
     */
    public TruncateSimpleColStatsCollector(int length) {
        Preconditions.checkArgument(length > 0, "Truncate length should larger than zero.");
        this.length = length;
    }

    /**
     * 获取截断长度。
     *
     * @return 截断长度
     */
    public int getLength() {
        return length;
    }

    /**
     * 收集字段的统计信息。
     *
     * <p>处理逻辑：
     * <ol>
     *   <li>如果字段为 null，增加空值计数
     *   <li>如果已经失败，快速返回
     *   <li>如果字段不是 Comparable 类型，跳过
     *   <li>更新最小值（截断）
     *   <li>更新最大值（截断并递增）
     * </ol>
     *
     * @param field 目标字段对象
     * @param fieldSerializer 字段对象的序列化器
     */
    @Override
    public void collect(Object field, Serializer<Object> fieldSerializer) {
        if (field == null) {
            nullCount++;
            return;
        }

        // 快速失败，因为结果不正确
        if (failed) {
            return;
        }

        // TODO: 对于不可比较类型使用比较器，并将此逻辑提取到工具类
        if (!(field instanceof Comparable)) {
            return;
        }

        Comparable<Object> c = (Comparable<Object>) field;
        if (minValue == null || c.compareTo(minValue) < 0) {
            minValue = fieldSerializer.copy(truncateMin(field));
        }
        if (maxValue == null || c.compareTo(maxValue) > 0) {
            Object max = truncateMax(field);
            // 可能失败
            if (max != null) {
                if (max != field) {
                    // 已在 `truncateMax` 中复制
                    maxValue = max;
                } else {
                    maxValue = fieldSerializer.copy(max);
                }
            }
        }
    }

    /**
     * 转换列统计信息。
     *
     * @param source 源列统计信息
     * @return 转换后的统计信息
     */
    @Override
    public SimpleColStats convert(SimpleColStats source) {
        Object min = truncateMin(source.min());
        Object max = truncateMax(source.max());
        if (max == null) {
            return new SimpleColStats(null, null, source.nullCount());
        }
        return new SimpleColStats(min, max, source.nullCount());
    }

    /**
     * 获取统计信息结果。
     *
     * @return 如果失败则返回无最小值/最大值的统计信息
     */
    @Override
    public SimpleColStats result() {
        if (failed) {
            return new SimpleColStats(null, null, nullCount);
        }
        return new SimpleColStats(minValue, maxValue, nullCount);
    }

    /**
     * 截断最小值。
     *
     * <p>返回一个小于或等于原始值的截断值。
     *
     * @param field 原始字段值
     * @return 截断后的最小值
     */
    private Object truncateMin(Object field) {
        if (field == null) {
            return null;
        }
        if (field instanceof BinaryString) {
            return ((BinaryString) field).substring(0, length);
        } else {
            return field;
        }
    }

    /**
     * 截断最大值。
     *
     * <p>返回一个大于或等于原始值的截断值。
     *
     * <p>算法：
     * <ol>
     *   <li>如果原始长度 ≤ 截断长度，直接返回原始值
     *   <li>截断为指定长度
     *   <li>从后向前尝试递增每个字符
     *   <li>如果成功递增，返回递增后的值
     *   <li>如果所有字符都无法递增，标记失败并返回 null
     * </ol>
     *
     * @param field 原始字段值
     * @return 截断并递增后的最大值，如果无法递增则返回 null
     */
    private Object truncateMax(Object field) {
        if (field == null) {
            return null;
        }
        if (field instanceof BinaryString) {
            BinaryString original = ((BinaryString) field);
            BinaryString truncated = original.substring(0, length);

            // 如果输入长度小于截断长度，不需要递增
            if (original.getSizeInBytes() == truncated.getSizeInBytes()) {
                return field;
            }

            StringBuilder truncatedStringBuilder = new StringBuilder(truncated.toString());

            // 从末尾开始尝试递增码点
            for (int i = length - 1; i >= 0; i--) {
                // 获取截断字符串缓冲区中 Unicode 字符数 = i 的偏移量
                int offsetByCodePoint = truncatedStringBuilder.offsetByCodePoints(0, i);
                int nextCodePoint = truncatedStringBuilder.codePointAt(offsetByCodePoint) + 1;
                // 没有溢出
                if (nextCodePoint != 0 && Character.isValidCodePoint(nextCodePoint)) {
                    truncatedStringBuilder.setLength(offsetByCodePoint);
                    // 将下一个码点附加到截断的子字符串
                    truncatedStringBuilder.appendCodePoint(nextCodePoint);
                    return BinaryString.fromString(truncatedStringBuilder.toString());
                }
            }
            failed = true;
            return null; // 无法找到有效的上界
        } else {
            return field;
        }
    }
}
