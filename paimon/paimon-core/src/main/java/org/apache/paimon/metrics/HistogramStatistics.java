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
 * 直方图统计信息抽象类。
 *
 * <p>表示直方图中记录元素的当前快照统计信息。
 *
 * <h3>提供的统计指标：</h3>
 * <ul>
 *   <li>分位数：P50（中位数）、P95、P99等
 *   <li>平均值：所有观测值的算术平均
 *   <li>标准差：观测值的离散程度
 *   <li>最大值：观测的最大值
 *   <li>最小值：观测的最小值
 *   <li>样本大小：观测值的数量
 *   <li>原始样本值：所有观测值
 * </ul>
 *
 * <h3>使用场景：</h3>
 * <ul>
 *   <li>分析延迟分布特征
 *   <li>监控系统性能指标
 *   <li>生成性能报告
 * </ul>
 *
 * <h3>实现类：</h3>
 * <ul>
 *   <li>{@link DescriptiveStatisticsHistogramStatistics}: 基于Apache Commons Math的实现
 * </ul>
 *
 * @since 0.5.0
 */
@Public
public abstract class HistogramStatistics {

    /**
     * 返回指定分位数的值。
     *
     * <p>分位数示例：
     * <ul>
     *   <li>0.5 = 中位数（P50）
     *   <li>0.95 = P95
     *   <li>0.99 = P99
     * </ul>
     *
     * @param quantile 分位数，范围 [0.0, 1.0]
     * @return 对应分位数的值
     */
    public abstract double getQuantile(double quantile);

    /**
     * 返回统计样本的所有元素。
     *
     * @return 样本元素数组
     */
    public abstract long[] getValues();

    /**
     * 返回统计样本的大小。
     *
     * @return 样本大小
     */
    public abstract int size();

    /**
     * 返回直方图值的平均值。
     *
     * @return 平均值
     */
    public abstract double getMean();

    /**
     * 返回直方图分布的标准差。
     *
     * <p>标准差衡量了观测值相对于平均值的离散程度。
     *
     * @return 标准差
     */
    public abstract double getStdDev();

    /**
     * 返回直方图的最大值。
     *
     * @return 最大值
     */
    public abstract long getMax();

    /**
     * 返回直方图的最小值。
     *
     * @return 最小值
     */
    public abstract long getMin();
}
