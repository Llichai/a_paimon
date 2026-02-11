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
 * 指标接口。
 *
 * <p>标记接口，用于指示一个类是指标。
 *
 * <h3>指标类型：</h3>
 * <ul>
 *   <li>{@link Counter}: 计数器，累计值
 *   <li>{@link Gauge}: 仪表盘，瞬时值
 *   <li>{@link Histogram}: 直方图，数值分布
 * </ul>
 *
 * <h3>使用场景：</h3>
 * <ul>
 *   <li>性能监控
 *   <li>运维指标采集
 *   <li>系统状态观测
 * </ul>
 *
 * @since 0.5.0
 */
@Public
public interface Metric {
    /**
     * 获取指标类型。
     *
     * <p>默认不支持自定义指标类型，子类应重写此方法。
     *
     * @return 指标类型
     * @throws UnsupportedOperationException 如果是自定义指标类型
     */
    default MetricType getMetricType() {
        throw new UnsupportedOperationException("Custom metric types are not supported.");
    }
}
