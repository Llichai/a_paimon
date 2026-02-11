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

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.Serializable;

/**
 * 基于描述性统计的直方图实现。
 *
 * <p>使用 {@link DescriptiveStatistics} 作为Paimon的 {@link Histogram} 实现。
 *
 * <h3>核心特性：</h3>
 * <ul>
 *   <li>滑动窗口：保留最近N个观测值
 *   <li>自动淘汰：超过窗口大小时自动移除最旧数据
 *   <li>实时统计：随时可获取当前窗口的统计信息
 * </ul>
 *
 * <h3>实现细节：</h3>
 * <ul>
 *   <li>使用 {@link CircularDoubleArray} 存储观测值
 *   <li>环形数组实现，内存固定
 *   <li>线程安全的addValue操作
 * </ul>
 *
 * <h3>使用示例：</h3>
 * <pre>
 * // 创建保留最近1000个值的直方图
 * Histogram histogram = new DescriptiveStatisticsHistogram(1000);
 *
 * // 记录观测值
 * histogram.update(100);
 * histogram.update(200);
 *
 * // 获取统计信息
 * HistogramStatistics stats = histogram.getStatistics();
 * double p50 = stats.getQuantile(0.5);  // 中位数
 * double p95 = stats.getQuantile(0.95); // P95
 * </pre>
 *
 * @see DescriptiveStatisticsHistogramStatistics
 */
public class DescriptiveStatisticsHistogram implements Histogram, Serializable {
    private static final long serialVersionUID = 1L;

    /** 环形数组，用于存储观测值 */
    private final CircularDoubleArray descriptiveStatistics;

    /**
     * 创建指定窗口大小的直方图。
     *
     * @param windowSize 窗口大小（保留的观测值数量）
     */
    public DescriptiveStatisticsHistogram(int windowSize) {
        this.descriptiveStatistics = new CircularDoubleArray(windowSize);
    }

    /**
     * 使用指定值更新直方图。
     *
     * @param value 观测值
     */
    @Override
    public void update(long value) {
        this.descriptiveStatistics.addValue(value);
    }

    /**
     * 获取已观测元素的总数。
     *
     * <p>注意：该值可能超过窗口大小，表示历史累计观测数。
     *
     * @return 观测元素总数
     */
    @Override
    public long getCount() {
        return this.descriptiveStatistics.getElementsSeen();
    }

    /**
     * 为当前窗口的元素创建统计快照。
     *
     * @return 统计信息快照
     */
    @Override
    public HistogramStatistics getStatistics() {
        return new DescriptiveStatisticsHistogramStatistics(this.descriptiveStatistics);
    }

    /**
     * 固定大小的环形数组，实现滑动窗口。
     *
     * <h3>特性：</h3>
     * <ul>
     *   <li>固定容量，内存可控
     *   <li>自动循环覆盖旧数据
     *   <li>动态起始位置
     *   <li>线程安全的添加操作
     * </ul>
     */
    static class CircularDoubleArray implements Serializable {
        private static final long serialVersionUID = 1L;

        /** 底层数组 */
        private final double[] backingArray;

        /** 下一个写入位置 */
        private int nextPos = 0;

        /** 是否已填满 */
        private boolean fullSize = false;

        /** 累计观测数 */
        private long elementsSeen = 0;

        /**
         * 创建指定大小的环形数组。
         *
         * @param windowSize 窗口大小
         */
        CircularDoubleArray(int windowSize) {
            this.backingArray = new double[windowSize];
        }

        /**
         * 添加值到环形数组。
         *
         * <p>线程安全，自动处理循环覆盖。
         *
         * @param value 待添加的值
         */
        synchronized void addValue(double value) {
            backingArray[nextPos] = value;
            ++elementsSeen;
            ++nextPos;
            // 达到数组末尾时循环到开头
            if (nextPos == backingArray.length) {
                nextPos = 0;
                fullSize = true;
            }
        }

        /**
         * 将当前数据转换为未排序的数组。
         *
         * @return 当前数据的副本
         */
        synchronized double[] toUnsortedArray() {
            final int size = getSize();
            double[] result = new double[size];
            System.arraycopy(backingArray, 0, result, 0, result.length);
            return result;
        }

        /**
         * 获取当前有效数据大小。
         *
         * @return 有效数据大小
         */
        private synchronized int getSize() {
            return fullSize ? backingArray.length : nextPos;
        }

        /**
         * 获取累计观测数。
         *
         * @return 累计观测数
         */
        private synchronized long getElementsSeen() {
            return elementsSeen;
        }
    }
}
