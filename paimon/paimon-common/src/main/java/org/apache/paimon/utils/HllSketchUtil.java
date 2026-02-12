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

import org.apache.paimon.annotation.VisibleForTesting;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;

/**
 * HyperLogLog Sketch 工具类。
 *
 * <p>提供 HyperLogLog 算法的基本操作，用于基数估计（Cardinality Estimation）。
 *
 * <p>HyperLogLog 是一种概率数据结构，用于估算数据集的基数（不重复元素个数）：
 * <ul>
 *   <li>空间效率高 - 使用很小的内存估算大规模数据集的基数
 *   <li>误差可控 - 标准误差约为 1.04/sqrt(m)，其中 m 是寄存器数量
 *   <li>可合并 - 多个 Sketch 可以合并得到总体基数估计
 * </ul>
 *
 * <p>主要功能：
 * <ul>
 *   <li>Sketch 合并 - union() 方法合并两个 Sketch
 *   <li>Sketch 构建 - sketchOf() 方法从值数组创建 Sketch
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>去重计数 - COUNT(DISTINCT) 的近似计算
 *   <li>聚合统计 - 分布式环境下的基数合并
 *   <li>数据分析 - 大规模数据集的唯一值估计
 * </ul>
 *
 * <p>注意：此实现基于 Apache DataSketches 库。
 *
 * @see org.apache.datasketches.hll.HllSketch
 */
public class HllSketchUtil {

    /**
     * 合并两个 HyperLogLog Sketch。
     *
     * @param sketchBytes1 第一个 Sketch 的字节数组
     * @param sketchBytes2 第二个 Sketch 的字节数组
     * @return 合并后的 Sketch 的紧凑字节数组
     */
    public static byte[] union(byte[] sketchBytes1, byte[] sketchBytes2) {
        HllSketch heapify = HllSketch.heapify((byte[]) sketchBytes1);
        org.apache.datasketches.hll.Union union = Union.heapify((byte[]) sketchBytes2);
        union.update(heapify);
        HllSketch result = union.getResult(TgtHllType.HLL_4);
        return result.toCompactByteArray();
    }

    /**
     * 从整数数组创建 HyperLogLog Sketch（仅用于测试）。
     *
     * @param values 整数值数组
     * @return Sketch 的紧凑字节数组
     */
    @VisibleForTesting
    public static byte[] sketchOf(int... values) {
        HllSketch hllSketch = new HllSketch();
        for (int value : values) {
            hllSketch.update(value);
        }
        return hllSketch.toCompactByteArray();
    }
}
