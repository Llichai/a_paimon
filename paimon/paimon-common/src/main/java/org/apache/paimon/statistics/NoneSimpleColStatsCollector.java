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
 * 无统计信息收集器。
 *
 * <p>该收集器不收集任何统计信息，所有操作都是空操作。这种模式适用于不需要统计信息的场景，
 * 可以最大化提升写入性能。
 *
 * <p>主要特点：
 * <ul>
 *   <li>不收集任何统计信息
 *   <li>所有收集操作都是空操作
 *   <li>始终返回 {@link SimpleColStats#NONE}
 *   <li>零性能开销
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>不需要基于统计信息的查询优化
 *   <li>追求极致的写入性能
 *   <li>数据量很小，统计信息意义不大
 *   <li>临时表或中间表
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建无统计信息收集器
 * NoneSimpleColStatsCollector collector = new NoneSimpleColStatsCollector();
 *
 * // 收集操作（实际上是空操作）
 * Serializer<Object> serializer = ...;
 * collector.collect(100, serializer);
 * collector.collect(null, serializer);
 *
 * // 获取结果
 * SimpleColStats stats = collector.result();
 * System.out.println(stats == SimpleColStats.NONE); // true
 * }</pre>
 *
 * <p>注意事项：
 * <ul>
 *   <li>使用此模式将禁用所有基于统计信息的优化
 *   <li>查询性能可能受到影响
 *   <li>适合写多读少的场景
 * </ul>
 */
public class NoneSimpleColStatsCollector extends AbstractSimpleColStatsCollector {

    /**
     * 收集字段的统计信息（空操作）。
     *
     * @param field 目标字段对象（忽略）
     * @param fieldSerializer 字段对象的序列化器（忽略）
     */
    @Override
    public void collect(Object field, Serializer<Object> fieldSerializer) {}

    /**
     * 获取收集的统计信息。
     *
     * @return {@link SimpleColStats#NONE}
     */
    @Override
    public SimpleColStats result() {
        return SimpleColStats.NONE;
    }

    /**
     * 转换列统计信息。
     *
     * @param source 源列统计信息（忽略）
     * @return {@link SimpleColStats#NONE}
     */
    @Override
    public SimpleColStats convert(SimpleColStats source) {
        return SimpleColStats.NONE;
    }
}
