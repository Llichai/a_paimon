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

import org.apache.paimon.format.SimpleColStats;

/**
 * 抽象简单列统计信息收集器。
 *
 * <p>该抽象类为列统计信息收集器提供基础实现，维护最小值、最大值和空值计数等核心统计信息。
 *
 * <p>主要功能：
 * <ul>
 *   <li>维护列的最小值
 *   <li>维护列的最大值
 *   <li>维护空值计数
 *   <li>提供统计信息结果生成
 * </ul>
 *
 * <p>子类需要实现：
 * <ul>
 *   <li>{@link SimpleColStatsCollector#collect(Object, org.apache.paimon.data.serializer.Serializer)} - 定义如何收集统计信息
 *   <li>{@link SimpleColStatsCollector#convert(SimpleColStats)} - 定义如何转换统计信息
 * </ul>
 *
 * <p>内置实现：
 * <ul>
 *   <li>{@link FullSimpleColStatsCollector} - 收集完整的统计信息
 *   <li>{@link CountsSimpleColStatsCollector} - 仅收集空值计数
 *   <li>{@link NoneSimpleColStatsCollector} - 不收集任何统计信息
 *   <li>{@link TruncateSimpleColStatsCollector} - 收集截断的统计信息
 * </ul>
 */
public abstract class AbstractSimpleColStatsCollector implements SimpleColStatsCollector {

    /** 最小值 */
    protected Object minValue;

    /** 最大值 */
    protected Object maxValue;

    /** 空值计数 */
    protected long nullCount;

    /**
     * 获取收集的统计信息。
     *
     * @return 包含最小值、最大值和空值计数的统计信息
     */
    @Override
    public SimpleColStats result() {
        return new SimpleColStats(minValue, maxValue, nullCount);
    }
}
