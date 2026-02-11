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

import org.apache.paimon.annotation.VisibleForTesting;

import org.apache.commons.math3.exception.MathIllegalArgumentException;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.UnivariateStatistic;
import org.apache.commons.math3.stat.descriptive.moment.SecondMoment;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.commons.math3.stat.ranking.NaNStrategy;

import java.io.Serializable;
import java.util.Arrays;

/**
 * 描述性统计直方图统计信息实现。
 *
 * <p>由 {@link DescriptiveStatisticsHistogram} 返回的统计实现。
 *
 * <h3>核心特性：</h3>
 * <ul>
 *   <li>时间点快照：统计信息是创建时的快照，不会随后续更新变化
 *   <li>优化的指标检索：一次计算多个指标，避免重复遍历
 *   <li>支持所有常用统计指标
 * </ul>
 *
 * <h3>实现细节：</h3>
 * <ul>
 *   <li>使用 {@link CommonMetricsSnapshot} 缓存统计结果
 *   <li>延迟初始化百分位数计算器
 *   <li>基于Apache Commons Math实现
 * </ul>
 *
 * <h3>统计指标：</h3>
 * <ul>
 *   <li>分位数：P50、P95、P99等
 *   <li>中心趋势：平均值、中位数
 *   <li>离散程度：标准差
 *   <li>极值：最大值、最小值
 *   <li>样本信息：大小、原始值
 * </ul>
 */
public class DescriptiveStatisticsHistogramStatistics extends HistogramStatistics
        implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 统计摘要，缓存计算结果 */
    private final CommonMetricsSnapshot statisticsSummary = new CommonMetricsSnapshot();

    /**
     * 从环形数组创建统计信息。
     *
     * @param histogramValues 直方图值的环形数组
     */
    public DescriptiveStatisticsHistogramStatistics(
            DescriptiveStatisticsHistogram.CircularDoubleArray histogramValues) {
        this(histogramValues.toUnsortedArray());
    }

    /**
     * 从原始值数组创建统计信息。
     *
     * @param values 原始值数组
     */
    public DescriptiveStatisticsHistogramStatistics(final double[] values) {
        statisticsSummary.evaluate(values);
    }

    /**
     * 返回指定分位数的值。
     *
     * @param quantile 分位数 [0.0, 1.0]
     * @return 对应的百分位数值
     */
    @Override
    public double getQuantile(double quantile) {
        return statisticsSummary.getPercentile(quantile * 100);
    }

    /**
     * 返回统计样本的所有值。
     *
     * @return 样本值数组
     */
    @Override
    public long[] getValues() {
        return Arrays.stream(statisticsSummary.getValues()).mapToLong(i -> (long) i).toArray();
    }

    /**
     * 返回统计样本的大小。
     *
     * @return 样本大小
     */
    @Override
    public int size() {
        return (int) statisticsSummary.getCount();
    }

    /**
     * 返回平均值。
     *
     * @return 平均值
     */
    @Override
    public double getMean() {
        return statisticsSummary.getMean();
    }

    /**
     * 返回标准差。
     *
     * @return 标准差
     */
    @Override
    public double getStdDev() {
        return statisticsSummary.getStandardDeviation();
    }

    /**
     * 返回最大值。
     *
     * @return 最大值
     */
    @Override
    public long getMax() {
        return (long) statisticsSummary.getMax();
    }

    /**
     * 返回最小值。
     *
     * @return 最小值
     */
    @Override
    public long getMin() {
        return (long) statisticsSummary.getMin();
    }

    /**
     * 优化的通用指标快照。
     *
     * <p>该类用于以优化的方式提取常用指标，即用尽可能少的数据遍历和计算。
     *
     * <h3>优化策略：</h3>
     * <ul>
     *   <li>一次遍历计算最小值、最大值、平均值和二阶矩
     *   <li>延迟初始化百分位数计算器
     *   <li>缓存计算结果避免重复计算
     * </ul>
     *
     * <p>注意：调用 {@link #evaluate} 方法不返回值，而是填充内部状态，
     * 然后可以通过其他方法检索值。
     */
    @VisibleForTesting
    static class CommonMetricsSnapshot implements UnivariateStatistic, Serializable {
        private static final long serialVersionUID = 2L;

        /** 原始数据 */
        private double[] data;

        /** 最小值 */
        private double min = Double.NaN;

        /** 最大值 */
        private double max = Double.NaN;

        /** 平均值 */
        private double mean = Double.NaN;

        /** 标准差 */
        private double stddev = Double.NaN;

        /** 百分位数计算器（延迟初始化） */
        private transient Percentile percentilesImpl;

        /**
         * 计算并填充统计信息。
         *
         * @param values 数据数组
         * @return NaN（不返回有意义的值）
         * @throws MathIllegalArgumentException 如果参数非法
         */
        @Override
        public double evaluate(final double[] values) throws MathIllegalArgumentException {
            return evaluate(values, 0, values.length);
        }

        /**
         * 计算指定范围的统计信息。
         *
         * <p>一次遍历计算：
         * <ul>
         *   <li>最小值和最大值
         *   <li>平均值（一阶矩）
         *   <li>标准差（基于二阶矩）
         * </ul>
         *
         * @param values 数据数组
         * @param begin 起始索引
         * @param length 长度
         * @return NaN（不返回有意义的值）
         * @throws MathIllegalArgumentException 如果参数非法
         */
        @Override
        public double evaluate(double[] values, int begin, int length)
                throws MathIllegalArgumentException {
            this.data = values;

            // 一次遍历计算最小值、最大值、平均值和二阶矩
            SimpleStats secondMoment = new SimpleStats();
            secondMoment.evaluate(values, begin, length);
            this.mean = secondMoment.getMean();
            this.min = secondMoment.getMin();
            this.max = secondMoment.getMax();

            // 基于二阶矩计算标准差
            this.stddev = new StandardDeviation(secondMoment).getResult();

            return Double.NaN;
        }

        /**
         * 创建副本。
         *
         * @return 统计快照副本
         */
        @Override
        public CommonMetricsSnapshot copy() {
            CommonMetricsSnapshot result = new CommonMetricsSnapshot();
            result.data = Arrays.copyOf(data, data.length);
            result.min = min;
            result.max = max;
            result.mean = mean;
            result.stddev = stddev;
            return result;
        }

        /**
         * 获取数据数量。
         *
         * @return 数据数量
         */
        long getCount() {
            return data.length;
        }

        /**
         * 获取最小值。
         *
         * @return 最小值
         */
        double getMin() {
            return min;
        }

        /**
         * 获取最大值。
         *
         * @return 最大值
         */
        double getMax() {
            return max;
        }

        /**
         * 获取平均值。
         *
         * @return 平均值
         */
        double getMean() {
            return mean;
        }

        /**
         * 获取标准差。
         *
         * @return 标准差
         */
        double getStandardDeviation() {
            return stddev;
        }

        /**
         * 获取指定百分位数的值。
         *
         * @param p 百分位数 [0, 100]
         * @return 对应的值
         */
        double getPercentile(double p) {
            maybeInitPercentile();
            return percentilesImpl.evaluate(p);
        }

        /**
         * 获取原始数据值。
         *
         * @return 数据数组
         */
        double[] getValues() {
            maybeInitPercentile();
            return percentilesImpl.getData();
        }

        /**
         * 延迟初始化百分位数计算器。
         */
        private void maybeInitPercentile() {
            if (percentilesImpl == null) {
                percentilesImpl = new Percentile().withNaNStrategy(NaNStrategy.FIXED);
            }
            if (data != null) {
                percentilesImpl.setData(data);
            }
        }
    }

    /**
     * 简单统计类。
     *
     * <p>在一次遍历中计算最小值、最大值、平均值（一阶矩）以及二阶矩。
     */
    private static class SimpleStats extends SecondMoment {
        private static final long serialVersionUID = 1L;

        /** 最小值 */
        private double min = Double.NaN;

        /** 最大值 */
        private double max = Double.NaN;

        /**
         * 递增处理每个数据点。
         *
         * <p>更新最小值、最大值，并调用父类更新矩统计。
         *
         * @param d 数据点
         */
        @Override
        public void increment(double d) {
            // 更新最小值
            if (d < min || Double.isNaN(min)) {
                min = d;
            }
            // 更新最大值
            if (d > max || Double.isNaN(max)) {
                max = d;
            }
            // 更新矩统计
            super.increment(d);
        }

        /**
         * 创建副本。
         *
         * @return 统计副本
         */
        @Override
        public SecondMoment copy() {
            SimpleStats result = new SimpleStats();
            SecondMoment.copy(this, result);
            result.min = min;
            result.max = max;
            return result;
        }

        /**
         * 获取最小值。
         *
         * @return 最小值
         */
        public double getMin() {
            return min;
        }

        /**
         * 获取最大值。
         *
         * @return 最大值
         */
        public double getMax() {
            return max;
        }

        /**
         * 获取平均值（一阶矩）。
         *
         * @return 平均值
         */
        public double getMean() {
            return m1;
        }
    }
}
