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
import org.apache.paimon.utils.DateTimeUtils;

import static org.apache.paimon.types.VarCharType.STRING_TYPE;

/**
 * {@link DataTypeRoot#DATE} 到 {@link DataTypeFamily#CHARACTER_STRING} 的类型转换规则。
 *
 * <p>功能说明: 将日期值格式化为字符串表示
 *
 * <p>转换语义:
 *
 * <ul>
 *   <li>格式化规则: 使用 ISO-8601 格式 'yyyy-MM-dd'
 *   <li>日期值: 表示自 Unix 纪元(1970-01-01)以来的天数
 *   <li>输出格式: 固定为10字符的日期字符串
 * </ul>
 *
 * <p>转换示例:
 *
 * <pre>
 * DATE '2024-01-15' -> STRING '2024-01-15'
 * DATE '1970-01-01' (值为0) -> STRING '1970-01-01'
 * DATE '2024-12-31' -> STRING '2024-12-31'
 * DATE '1969-12-31' (值为-1) -> STRING '1969-12-31'
 * </pre>
 *
 * <p>NULL 值处理: 输入为 NULL 时,输出也为 NULL
 *
 * <p>SQL 标准兼容性: 符合 SQL:2016 标准中日期到字符串的显式转换规则
 */
class DateToStringCastRule extends AbstractCastRule<Integer, BinaryString> {

    static final DateToStringCastRule INSTANCE = new DateToStringCastRule();

    private DateToStringCastRule() {
        super(CastRulePredicate.builder().input(DataTypeRoot.DATE).target(STRING_TYPE).build());
    }

    @Override
    public CastExecutor<Integer, BinaryString> create(DataType inputType, DataType targetType) {
        return value -> BinaryString.fromString(DateTimeUtils.formatDate(value));
    }
}
