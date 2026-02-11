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
 * 直方图指标接口。
 *
 * <p>直方图用于记录数值分布，支持统计分位数、平均值、标准差等。
 *
 * <h3>核心功能：</h3>
 * <ul>
 *   <li>记录观测值
 *   <li>统计观测值的数量
 *   <li>生成统计快照（分位数、均值、标准差等）
 * </ul>
 *
 * <h3>使用场景：</h3>
 * <ul>
 *   <li>测量请求响应时间分布
 *   <li>测量文件大小分布
 *   <li>测量批处理记录数分布
 *   <li>测量延迟的P50、P95、P99
 * </ul>
 *
 * <h3>实现类：</h3>
 * <ul>
 *   <li>{@link DescriptiveStatisticsHistogram}: 基于滑动窗口的实现
 * </ul>
 *
 * @since 0.5.0
 */
@Public
public interface Histogram extends Metric {

    /**
     * 使用指定值更新直方图。
     *
     * @param value 观测值
     */
    void update(long value);

    /**
     * 获取已观测元素的数量。
     *
     * @return 观测元素数量
     */
    long getCount();

    /**
     * 为当前记录的元素创建统计快照。
     *
     * <p>返回的统计信息包括：
     * <ul>
     *   <li>分位数（P50、P95、P99等）
     *   <li>平均值
     *   <li>标准差
     *   <li>最大值和最小值
     * </ul>
     *
     * @return 当前元素的统计信息
     */
    HistogramStatistics getStatistics();

    @Override
    default MetricType getMetricType() {
        return MetricType.HISTOGRAM;
    }
}
