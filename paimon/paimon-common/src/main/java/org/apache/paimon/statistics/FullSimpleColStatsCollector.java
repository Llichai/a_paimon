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

import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.format.SimpleColStats;

/**
 * 完整统计信息收集器。
 *
 * <p>该收集器收集完整的列统计信息，包括：
 * <ul>
 *   <li>空值计数（null count）
 *   <li>最小值（min value）- 如果值是可比较的
 *   <li>最大值（max value）- 如果值是可比较的
 * </ul>
 *
 * <p>主要功能：
 * <ul>
 *   <li>统计空值数量
 *   <li>追踪最小值（通过比较器）
 *   <li>追踪最大值（通过比较器）
 *   <li>使用序列化器复制值以避免引用问题
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建完整统计信息收集器
 * FullSimpleColStatsCollector collector = new FullSimpleColStatsCollector();
 *
 * // 收集统计信息
 * Serializer<Object> serializer = ...;
 * collector.collect(100, serializer);
 * collector.collect(50, serializer);
 * collector.collect(200, serializer);
 * collector.collect(null, serializer);
 *
 * // 获取结果
 * SimpleColStats stats = collector.result();
 * System.out.println("Min: " + stats.min());      // 50
 * System.out.println("Max: " + stats.max());      // 200
 * System.out.println("Nulls: " + stats.nullCount()); // 1
 * }</pre>
 *
 * <p>注意事项：
 * <ul>
 *   <li>仅支持实现了 {@link Comparable} 接口的类型
 *   <li>不可比较的类型（如复杂对象）将不会更新最小值/最大值
 *   <li>值会被序列化器复制，避免后续修改影响统计信息
 * </ul>
 */
public class FullSimpleColStatsCollector extends AbstractSimpleColStatsCollector {

    /**
     * 收集字段的统计信息。
     *
     * <p>处理逻辑：
     * <ol>
     *   <li>如果字段为 null，增加空值计数
     *   <li>如果字段不是 Comparable 类型，跳过最小值/最大值更新
     *   <li>如果字段小于当前最小值，更新最小值
     *   <li>如果字段大于当前最大值，更新最大值
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

        // TODO: 对于不可比较类型使用比较器，并将此逻辑提取到工具类
        if (!(field instanceof Comparable)) {
            return;
        }
        Comparable<Object> c = (Comparable<Object>) field;
        if (minValue == null || c.compareTo(minValue) < 0) {
            minValue = fieldSerializer.copy(c);
        }
        if (maxValue == null || c.compareTo(maxValue) > 0) {
            maxValue = fieldSerializer.copy(c);
        }
    }

    /**
     * 转换列统计信息（完整模式直接返回原始统计信息）。
     *
     * @param source 源列统计信息
     * @return 原始统计信息（不做转换）
     */
    @Override
    public SimpleColStats convert(SimpleColStats source) {
        return source;
    }
}
