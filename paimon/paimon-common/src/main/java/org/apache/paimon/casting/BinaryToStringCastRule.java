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

/**
 * 二进制类型到字符串类型的转换规则。
 *
 * <p>将 {@link DataTypeFamily#BINARY_STRING} 转换为 {@link DataTypeFamily#CHARACTER_STRING}。
 *
 * <p>转换策略: 将字节数组解释为 UTF-8 编码的字符串
 *
 * <p>示例:
 *
 * <pre>{@code
 * // BINARY [0x68, 0x65, 0x6c, 0x6c, 0x6f] → VARCHAR "hello"
 * }</pre>
 */
public class BinaryToStringCastRule extends AbstractCastRule<byte[], BinaryString> {

    /** 单例实例 */
    static final BinaryToStringCastRule INSTANCE = new BinaryToStringCastRule();

    /** 私有构造函数。 */
    private BinaryToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.BINARY_STRING)
                        .target(DataTypeFamily.CHARACTER_STRING)
                        .build());
    }

    /**
     * 创建二进制到字符串的转换执行器。
     *
     * @param inputType 输入数据类型
     * @param targetType 目标数据类型
     * @return 转换执行器
     */
    @Override
    public CastExecutor<byte[], BinaryString> create(DataType inputType, DataType targetType) {
        return BinaryString::fromBytes;
    }
}
