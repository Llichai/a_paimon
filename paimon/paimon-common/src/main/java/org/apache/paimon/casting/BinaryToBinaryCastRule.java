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

package org.apache.paimon.casting;

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.utils.BinaryStringUtils;

/**
 * 二进制类型到二进制类型的转换规则。
 *
 * <p>处理 {@link DataTypeFamily#BINARY_STRING} 类型族内部的转换,主要用于不同长度的 BINARY/VARBINARY 之间的转换。
 *
 * <p>转换策略:
 *
 * <ul>
 *   <li>如果目标类型长度更短,会进行截断
 *   <li>如果目标类型长度更长,保持原值(不填充)
 * </ul>
 *
 * <p>示例:
 *
 * <pre>{@code
 * // BINARY(10) → BINARY(5) (截断)
 * // VARBINARY(5) → VARBINARY(10) (保持原值)
 * }</pre>
 */
class BinaryToBinaryCastRule extends AbstractCastRule<byte[], byte[]> {

    /** 单例实例 */
    static final BinaryToBinaryCastRule INSTANCE = new BinaryToBinaryCastRule();

    /** 私有构造函数。 */
    private BinaryToBinaryCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.BINARY_STRING)
                        .target(DataTypeFamily.BINARY_STRING)
                        .build());
    }

    /**
     * 创建二进制到二进制的转换执行器。
     *
     * @param inputType 输入数据类型
     * @param targetType 目标数据类型
     * @return 转换执行器
     */
    @Override
    public CastExecutor<byte[], byte[]> create(DataType inputType, DataType targetType) {
        return value -> BinaryStringUtils.toBinaryString(value, targetType);
    }
}
