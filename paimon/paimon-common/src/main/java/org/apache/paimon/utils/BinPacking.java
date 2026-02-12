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

package org.apache.paimon.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.Function;

import static java.util.Comparator.comparingLong;

/**
 * 装箱算法实现的工具类。
 *
 * <p>提供了两种装箱算法:
 * <ul>
 *   <li>有序装箱: 按照输入顺序将项目打包到箱子中
 *   <li>固定箱数装箱: 将项目打包到固定数量的箱子中,尽量平衡每个箱子的重量
 * </ul>
 */
public class BinPacking {
    private BinPacking() {}

    /**
     * 对输入项目进行有序装箱。
     *
     * <p>按照输入顺序将项目添加到箱子中,当添加下一个项目会超过目标重量时,
     * 开始一个新箱子(前提是当前箱子不为空)。
     *
     * @param <T> 项目类型
     * @param items 要打包的项目
     * @param weightFunc 计算项目重量的函数
     * @param targetWeight 目标重量阈值
     * @return 装箱后的项目列表,每个内部列表表示一个箱子
     */
    public static <T> List<List<T>> packForOrdered(
            Iterable<T> items, Function<T, Long> weightFunc, long targetWeight) {
        List<List<T>> packed = new ArrayList<>();

        List<T> binItems = new ArrayList<>();
        long binWeight = 0L;

        for (T item : items) {
            long weight = weightFunc.apply(item);
            // 当遇到一个很大的项目或总重量足够时,检查 binItems 大小
            // 如果大于零,则打包它
            if (binWeight + weight > targetWeight && binItems.size() > 0) {
                packed.add(binItems);
                binItems = new ArrayList<>();
                binWeight = 0;
            }

            binWeight += weight;
            binItems.add(item);
        }

        if (binItems.size() > 0) {
            packed.add(binItems);
        }
        return packed;
    }

    /**
     * 固定箱数的装箱实现。
     *
     * <p>将项目分配到固定数量的箱子中,尽量使每个箱子的重量平衡。
     * 算法首先对项目按重量排序,然后总是将项目添加到当前重量最小的箱子中。
     *
     * @param <T> 项目类型
     * @param items 要打包的项目
     * @param weightFunc 计算项目重量的函数
     * @param binNumber 箱子数量
     * @return 装箱后的项目列表,每个内部列表表示一个箱子
     */
    public static <T> List<List<T>> packForFixedBinNumber(
            Iterable<T> items, Function<T, Long> weightFunc, int binNumber) {
        // 1. 首先对项目排序
        List<T> sorted = new ArrayList<>();
        items.forEach(sorted::add);
        sorted.sort(comparingLong(weightFunc::apply));

        // 2. 装箱
        PriorityQueue<FixedNumberBin<T>> bins = new PriorityQueue<>();
        for (T item : sorted) {
            long weight = weightFunc.apply(item);
            FixedNumberBin<T> bin = bins.size() < binNumber ? new FixedNumberBin<>() : bins.poll();
            bin.add(item, weight);
            bins.add(bin);
        }

        // 3. 输出结果
        List<List<T>> packed = new ArrayList<>();
        bins.forEach(bin -> packed.add(bin.items));
        return packed;
    }

    /**
     * 固定数量装箱算法使用的箱子。
     *
     * @param <T> 项目类型
     */
    private static class FixedNumberBin<T> implements Comparable<FixedNumberBin<T>> {
        private final List<T> items = new ArrayList<>();
        private long binWeight = 0L;

        /**
         * 向箱子中添加项目。
         *
         * @param item 要添加的项目
         * @param weight 项目的重量
         */
        void add(T item, long weight) {
            this.binWeight += weight;
            items.add(item);
        }

        @Override
        public int compareTo(FixedNumberBin<T> other) {
            return Long.compare(binWeight, other.binWeight);
        }
    }
}
