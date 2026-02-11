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
import org.apache.paimon.utils.HllSketchUtil;

/**
 * HLL_SKETCH 聚合器
 * 使用HyperLogLog (HLL) 算法进行基数估计的聚合器
 * HLL是一种概率数据结构，用于高效地估计大数据集的不重复元素数量（去重计数）
 */
public class FieldHllSketchAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    /**
     * 构造 HLL_SKETCH 聚合器
     * @param name 聚合函数名称
     * @param dataType 二进制数据类型（HLL sketch序列化后的字节数组）
     */
    public FieldHllSketchAgg(String name, VarBinaryType dataType) {
        super(name, dataType);
    }

    /**
     * 执行 HLL_SKETCH 聚合
     * 合并两个HLL sketch，得到更准确的基数估计
     * @param accumulator 累加器（已有的HLL sketch字节数组）
     * @param inputField 输入字段（新的HLL sketch字节数组）
     * @return 合并后的HLL sketch字节数组
     */
    @Override
    public Object agg(Object accumulator, Object inputField) {
        // 如果有一个为null，返回非null的那个
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }

        // 使用HLL工具类合并两个sketch
        return HllSketchUtil.union((byte[]) accumulator, (byte[]) inputField);
    }
}
