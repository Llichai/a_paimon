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

package org.apache.paimon.mergetree.compact.aggregate;

import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.utils.ThetaSketch;

/**
 * THETA_SKETCH 聚合器
 * 使用Theta Sketch算法进行基数估计和集合运算的聚合器
 * Theta Sketch是Apache DataSketches库提供的概率数据结构，支持并集、交集等集合操作
 */
public class FieldThetaSketchAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    /**
     * 构造 THETA_SKETCH 聚合器
     * @param name 聚合函数名称
     * @param dataType 二进制数据类型（Theta sketch序列化后的字节数组）
     */
    public FieldThetaSketchAgg(String name, VarBinaryType dataType) {
        super(name, dataType);
    }

    /**
     * 执行 THETA_SKETCH 聚合
     * 合并两个Theta sketch，得到并集
     * @param accumulator 累加器（已有的Theta sketch字节数组）
     * @param inputField 输入字段（新的Theta sketch字节数组）
     * @return 合并后的Theta sketch字节数组
     */
    @Override
    public Object agg(Object accumulator, Object inputField) {
        // 如果有一个为null，返回非null的那个
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }

        // 使用ThetaSketch工具类合并两个sketch（并集操作）
        return ThetaSketch.union((byte[]) accumulator, (byte[]) inputField);
    }
}
