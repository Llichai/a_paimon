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
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.utils.BinaryStringUtils;

/**
 * 布尔类型到字符串类型的转换规则。
 *
 * <p>将 {@link DataTypeRoot#BOOLEAN} 转换为 {@link DataTypeFamily#CHARACTER_STRING}。
 *
 * <p>转换策略: 将 true 转换为 "true", false 转换为 "false"
 *
 * <p>示例:
 *
 * <pre>{@code
 * // BOOLEAN true → VARCHAR "true"
 * // BOOLEAN false → VARCHAR "false"
 * }</pre>
 */
class BooleanToStringCastRule extends AbstractCastRule<Boolean, BinaryString> {

    /** 单例实例 */
    static final BooleanToStringCastRule INSTANCE = new BooleanToStringCastRule();

    /** 私有构造函数。 */
    private BooleanToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeRoot.BOOLEAN)
                        .target(DataTypeFamily.CHARACTER_STRING)
                        .build());
    }

    /**
     * 创建布尔值到字符串的转换执行器。
     *
     * @param inputType 输入数据类型
     * @param targetType 目标数据类型
     * @return 转换执行器
     */
    @Override
    public CastExecutor<Boolean, BinaryString> create(DataType inputType, DataType targetType) {
        return value ->
                BinaryStringUtils.toCharacterString(
                        BinaryString.fromString(value.toString()), targetType);
    }
}
