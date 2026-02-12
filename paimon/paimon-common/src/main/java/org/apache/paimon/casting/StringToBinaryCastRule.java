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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.utils.BinaryStringUtils;

/**
 * 字符串类型到二进制类型的转换规则。
 *
 * <p>将 {@link DataTypeFamily#CHARACTER_STRING} 转换为 {@link DataTypeFamily#BINARY_STRING}。
 *
 * <p>转换策略: 使用字符串的 UTF-8 字节表示作为二进制数据
 *
 * <p>示例:
 *
 * <pre>{@code
 * // VARCHAR "hello" → BINARY [0x68, 0x65, 0x6c, 0x6c, 0x6f] (UTF-8 字节)
 * }</pre>
 */
class StringToBinaryCastRule extends AbstractCastRule<BinaryString, byte[]> {

    /** 单例实例 */
    static final StringToBinaryCastRule INSTANCE = new StringToBinaryCastRule();

    /** 私有构造函数。 */
    private StringToBinaryCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.CHARACTER_STRING)
                        .target(DataTypeFamily.BINARY_STRING)
                        .build());
    }

    /**
     * 创建字符串到二进制的转换执行器。
     *
     * @param inputType 输入数据类型
     * @param targetType 目标数据类型
     * @return 转换执行器
     */
    @Override
    public CastExecutor<BinaryString, byte[]> create(DataType inputType, DataType targetType) {
        return value -> BinaryStringUtils.toBinaryString(value.toBytes(), targetType);
    }
}
