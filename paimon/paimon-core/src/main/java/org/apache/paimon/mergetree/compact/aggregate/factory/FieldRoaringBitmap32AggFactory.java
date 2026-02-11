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

package org.apache.paimon.mergetree.compact.aggregate.factory;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.mergetree.compact.aggregate.FieldRoaringBitmap32Agg;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.VarBinaryType;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * FieldRoaringBitmap32Agg 聚合器工厂类
 * 用于创建 RBM32 聚合函数，使用 Roaring Bitmap 进行32位整数的位图压缩和去重
 */
public class FieldRoaringBitmap32AggFactory implements FieldAggregatorFactory {

    public static final String NAME = "rbm32"; // 聚合函数标识符

    /**
     * 创建 RBM32 聚合器实例
     * @param fieldType 字段数据类型（必须是VarBinary二进制类型）
     * @param options 核心配置选项
     * @param field 字段名称
     * @return FieldRoaringBitmap32Agg 聚合器实例
     */
    @Override
    public FieldRoaringBitmap32Agg create(DataType fieldType, CoreOptions options, String field) {
        // 校验字段类型必须是二进制类型
        checkArgument(
                fieldType instanceof VarBinaryType,
                "Data type for roaring bitmap column must be 'VarBinaryType' but was '%s'.",
                fieldType);
        // 创建并返回 RBM32 聚合器，用于32位整数的高效位图存储
        return new FieldRoaringBitmap32Agg(identifier(), (VarBinaryType) fieldType);
    }

    /**
     * 获取聚合函数标识符
     * @return "rbm32"
     */
    @Override
    public String identifier() {
        return NAME;
    }
}
