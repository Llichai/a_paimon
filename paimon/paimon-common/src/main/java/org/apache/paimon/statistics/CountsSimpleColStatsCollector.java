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
 * 仅计数统计信息收集器。
 *
 * <p>该收集器仅收集空值计数（null count），不收集最小值和最大值信息。
 * 这种模式适用于只需要了解数据完整性而不需要值分布信息的场景。
 *
 * <p>主要功能：
 * <ul>
 *   <li>统计空值数量
 *   <li>不收集最小值和最大值
 *   <li>转换时丢弃最小值/最大值信息
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>需要检测数据质量问题（空值比例）
 *   <li>不需要基于值范围的查询优化
 *   <li>减少统计信息的存储开销
 *   <li>提高统计信息收集的性能
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建仅计数统计信息收集器
 * CountsSimpleColStatsCollector collector = new CountsSimpleColStatsCollector();
 *
 * // 收集统计信息
 * Serializer<Object> serializer = ...;
 * collector.collect(100, serializer);
 * collector.collect(50, serializer);
 * collector.collect(null, serializer);
 * collector.collect(200, serializer);
 * collector.collect(null, serializer);
 *
 * // 获取结果
 * SimpleColStats stats = collector.result();
 * System.out.println("Min: " + stats.min());      // null (不收集)
 * System.out.println("Max: " + stats.max());      // null (不收集)
 * System.out.println("Nulls: " + stats.nullCount()); // 2
 * }</pre>
 *
 * <p>性能优势：
 * <ul>
 *   <li>不需要值比较，性能更好
 *   <li>不需要复制值，内存开销更小
 *   <li>统计信息更小，存储和传输成本更低
 * </ul>
 */
public class CountsSimpleColStatsCollector extends AbstractSimpleColStatsCollector {

    /**
     * 收集字段的统计信息。
     *
     * <p>仅统计空值数量，忽略非空值。
     *
     * @param field 目标字段对象
     * @param serializer 字段对象的序列化器（未使用）
     */
    @Override
    public void collect(Object field, Serializer<Object> serializer) {
        if (field == null) {
            nullCount++;
        }
    }

    /**
     * 转换列统计信息。
     *
     * <p>仅保留空值计数，丢弃最小值和最大值信息。
     *
     * @param source 源列统计信息
     * @return 仅包含空值计数的统计信息
     */
    @Override
    public SimpleColStats convert(SimpleColStats source) {
        return new SimpleColStats(null, null, source.nullCount());
    }
}
