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

import java.util.Arrays;
import java.util.regex.Matcher;

import static org.apache.paimon.statistics.TruncateSimpleColStatsCollector.TRUNCATE_PATTERN;

/**
 * 简单列统计信息收集器。
 *
 * <p>该接口定义了列统计信息的收集模式和转换策略。列统计信息主要用于查询优化和数据过滤。
 *
 * <p>支持的统计信息收集模式：
 * <ul>
 *   <li><b>NONE</b>：不收集任何统计信息
 *   <li><b>FULL</b>：收集完整的统计信息（最小值、最大值、空值计数）
 *   <li><b>COUNTS</b>：仅收集空值计数
 *   <li><b>TRUNCATE(n)</b>：收集截断的统计信息（字符串截断为 n 个字符）
 * </ul>
 *
 * <p>主要功能：
 * <ul>
 *   <li>收集字段的统计信息
 *   <li>生成统计信息结果
 *   <li>转换已有的统计信息
 *   <li>通过工厂方法创建收集器
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建完整统计信息收集器
 * SimpleColStatsCollector fullCollector = SimpleColStatsCollector.from("FULL").create();
 *
 * // 收集统计信息
 * Serializer<Object> serializer = ...;
 * fullCollector.collect("value1", serializer);
 * fullCollector.collect("value2", serializer);
 * fullCollector.collect(null, serializer);
 *
 * // 获取结果
 * SimpleColStats stats = fullCollector.result();
 * System.out.println("Min: " + stats.min());
 * System.out.println("Max: " + stats.max());
 * System.out.println("Null count: " + stats.nullCount());
 *
 * // 创建截断统计信息收集器（截断字符串为 16 个字符）
 * SimpleColStatsCollector truncateCollector =
 *     SimpleColStatsCollector.from("TRUNCATE(16)").create();
 *
 * // 创建多列的统计信息收集器
 * SimpleColStatsCollector.Factory[] factories = {
 *     SimpleColStatsCollector.from("FULL"),
 *     SimpleColStatsCollector.from("COUNTS"),
 *     SimpleColStatsCollector.from("NONE")
 * };
 * SimpleColStatsCollector[] collectors = SimpleColStatsCollector.create(factories);
 * }</pre>
 *
 * <p>统计信息的使用场景：
 * <ul>
 *   <li>查询优化：基于最小值/最大值进行分区裁剪
 *   <li>索引选择：根据数据分布选择合适的索引
 *   <li>数据倾斜检测：通过统计信息识别数据倾斜
 *   <li>存储优化：根据统计信息选择压缩策略
 * </ul>
 */
public interface SimpleColStatsCollector {

    /**
     * 从字段收集统计信息。
     *
     * @param field 目标字段对象
     * @param fieldSerializer 字段对象的序列化器
     */
    void collect(Object field, Serializer<Object> fieldSerializer);

    /**
     * 获取收集的列统计信息。
     *
     * @return 收集的列统计信息
     */
    SimpleColStats result();

    /**
     * 根据策略转换列统计信息。
     *
     * @param source 源列统计信息（从文件中提取）
     * @return 转换后的列统计信息
     */
    SimpleColStats convert(SimpleColStats source);

    /**
     * 统计信息收集器工厂。
     *
     * <p>用于创建 {@link SimpleColStatsCollector} 实例。
     */
    interface Factory {
        /**
         * 创建统计信息收集器。
         *
         * @return 统计信息收集器实例
         */
        SimpleColStatsCollector create();
    }

    /**
     * 从工厂数组创建收集器数组。
     *
     * @param factories 工厂数组
     * @return 收集器数组
     */
    static SimpleColStatsCollector[] create(SimpleColStatsCollector.Factory[] factories) {
        SimpleColStatsCollector[] collectors = new SimpleColStatsCollector[factories.length];
        for (int i = 0; i < factories.length; i++) {
            collectors[i] = factories[i].create();
        }
        return collectors;
    }

    /**
     * 从配置选项创建收集器工厂。
     *
     * <p>支持的选项：
     * <ul>
     *   <li>"NONE" - 不收集统计信息
     *   <li>"FULL" - 收集完整统计信息
     *   <li>"COUNTS" - 仅收集空值计数
     *   <li>"TRUNCATE(n)" - 收集截断统计信息（n 为截断长度）
     * </ul>
     *
     * @param option 配置选项
     * @return 收集器工厂
     * @throws IllegalArgumentException 如果选项不合法
     */
    static Factory from(String option) {
        String upper = option.toUpperCase();
        switch (upper) {
            case "NONE":
                return NoneSimpleColStatsCollector::new;
            case "FULL":
                return FullSimpleColStatsCollector::new;
            case "COUNTS":
                return CountsSimpleColStatsCollector::new;
            default:
                Matcher matcher = TRUNCATE_PATTERN.matcher(upper);
                if (matcher.matches()) {
                    String length = matcher.group(1);
                    return () -> new TruncateSimpleColStatsCollector(Integer.parseInt(length));
                }
                throw new IllegalArgumentException("Unexpected option: " + option);
        }
    }

    /**
     * 创建完整统计信息工厂数组。
     *
     * @param numFields 字段数量
     * @return 完整统计信息工厂数组
     */
    static SimpleColStatsCollector.Factory[] createFullStatsFactories(int numFields) {
        SimpleColStatsCollector.Factory[] factories =
                new SimpleColStatsCollector.Factory[numFields];
        Arrays.fill(factories, (SimpleColStatsCollector.Factory) FullSimpleColStatsCollector::new);
        return factories;
    }
}
