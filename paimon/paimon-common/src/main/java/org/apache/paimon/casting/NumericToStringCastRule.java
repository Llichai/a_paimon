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
 * 数值类型到字符串类型的转换规则。
 *
 * <p>将 {@link DataTypeFamily#NUMERIC} 类型族的所有类型转换为 {@link DataTypeFamily#CHARACTER_STRING}。
 *
 * <p>支持的输入类型:
 *
 * <ul>
 *   <li>整数类型: TINYINT, SMALLINT, INTEGER, BIGINT
 *   <li>浮点类型: FLOAT, DOUBLE
 *   <li>十进制类型: DECIMAL
 * </ul>
 *
 * <p>转换策略: 使用 Java 的 toString() 方法将数值转换为字符串表示
 *
 * <p>示例:
 *
 * <pre>{@code
 * // INTEGER 42 → VARCHAR "42"
 * // DOUBLE 3.14 → VARCHAR "3.14"
 * // DECIMAL(10,2) 123.45 → VARCHAR "123.45"
 * }</pre>
 */
class NumericToStringCastRule extends AbstractCastRule<Object, BinaryString> {

    /** 单例实例 */
    static final NumericToStringCastRule INSTANCE = new NumericToStringCastRule();

    /** 私有构造函数。 */
    private NumericToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.NUMERIC)
                        .target(DataTypeFamily.CHARACTER_STRING)
                        .build());
    }

    /**
     * 创建数值到字符串的转换执行器。
     *
     * @param inputType 输入数据类型
     * @param targetType 目标数据类型
     * @return 转换执行器
     */
    @Override
    public CastExecutor<Object, BinaryString> create(DataType inputType, DataType targetType) {
        return value ->
                BinaryStringUtils.toCharacterString(
                        BinaryString.fromString(value.toString()), targetType);
    }
}
