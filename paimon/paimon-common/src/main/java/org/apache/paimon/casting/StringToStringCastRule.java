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
 * 字符串类型到字符串类型的转换规则。
 *
 * <p>处理 {@link DataTypeFamily#CHARACTER_STRING} 类型族内部的转换,主要用于:
 *
 * <ul>
 *   <li>VARCHAR 到 CHAR 的转换
 *   <li>不同长度的 VARCHAR/CHAR 之间的转换
 * </ul>
 *
 * <p>转换策略:
 *
 * <ul>
 *   <li>如果目标类型长度更短,会进行截断
 *   <li>如果目标类型是 CHAR 且长度更长,会用空格填充
 * </ul>
 *
 * <p>示例:
 *
 * <pre>{@code
 * // VARCHAR(10) "hello" → CHAR(8) "hello   " (填充空格)
 * // VARCHAR(10) "helloworld" → VARCHAR(5) "hello" (截断)
 * }</pre>
 */
class StringToStringCastRule extends AbstractCastRule<BinaryString, BinaryString> {

    /** 单例实例 */
    static final StringToStringCastRule INSTANCE = new StringToStringCastRule();

    /** 私有构造函数。 */
    private StringToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.CHARACTER_STRING)
                        .target(DataTypeFamily.CHARACTER_STRING)
                        .build());
    }

    /**
     * 创建字符串到字符串的转换执行器。
     *
     * @param inputType 输入数据类型
     * @param targetType 目标数据类型
     * @return 转换执行器
     */
    @Override
    public CastExecutor<BinaryString, BinaryString> create(
            DataType inputType, DataType targetType) {
        return value -> BinaryStringUtils.toCharacterString(value, targetType);
    }
}
