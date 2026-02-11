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
 * 指标类型枚举。
 *
 * <p>定义Paimon支持的指标类型。
 *
 * <h3>指标类型说明：</h3>
 * <ul>
 *   <li><b>COUNTER</b>: 计数器，用于累计值
 *     <ul>
 *       <li>单调递增或递减
 *       <li>适用于：提交次数、记录数、错误数等
 *       <li>实现：{@link Counter}, {@link SimpleCounter}
 *     </ul>
 *   </li>
 *   <li><b>GAUGE</b>: 仪表盘，用于瞬时值
 *     <ul>
 *       <li>可任意变化
 *       <li>适用于：队列大小、内存使用、线程数等
 *       <li>实现：{@link Gauge}
 *     </ul>
 *   </li>
 *   <li><b>HISTOGRAM</b>: 直方图，用于数值分布
 *     <ul>
 *       <li>记录观测值分布
 *       <li>适用于：延迟、文件大小、批大小等
 *       <li>实现：{@link Histogram}, {@link DescriptiveStatisticsHistogram}
 *     </ul>
 *   </li>
 * </ul>
 *
 * @since 0.5.0
 */
@Public
public enum MetricType {
    /** 计数器：累计值（提交次数、记录数等） */
    COUNTER,

    /** 仪表盘：瞬时值（内存使用、队列大小等） */
    GAUGE,

    /** 直方图：数值分布（延迟分布、大小分布等） */
    HISTOGRAM
}
