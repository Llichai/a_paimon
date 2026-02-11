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

package org.apache.paimon.metrics;

import org.apache.paimon.annotation.Public;

/**
 * 仪表盘指标接口。
 *
 * <p>仪表盘是一个 {@link Metric}，用于计算和返回某个时间点的特定值。
 *
 * <h3>核心特点：</h3>
 * <ul>
 *   <li>按需计算值，而非持续累加
 *   <li>返回瞬时状态的快照
 *   <li>支持任意类型的返回值
 * </ul>
 *
 * <h3>使用场景：</h3>
 * <ul>
 *   <li>测量当前队列大小
 *   <li>测量缓存命中率
 *   <li>测量当前内存使用量
 *   <li>测量线程池活跃线程数
 * </ul>
 *
 * <h3>与Counter的区别：</h3>
 * <ul>
 *   <li>Counter: 累计值，单调递增或递减
 *   <li>Gauge: 瞬时值，可任意变化
 * </ul>
 *
 * @param <T> 测量值的类型
 * @since 0.5.0
 */
@Public
public interface Gauge<T> extends Metric {

    /**
     * 计算并返回测量值。
     *
     * <p>每次调用都会重新计算当前值。
     *
     * @return 计算得到的值
     */
    T getValue();

    @Override
    default MetricType getMetricType() {
        return MetricType.GAUGE;
    }
}
