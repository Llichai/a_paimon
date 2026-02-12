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
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.utils.DateTimeUtils;

import static org.apache.paimon.types.VarCharType.STRING_TYPE;

/**
 * {@link DataTypeRoot#TIME_WITHOUT_TIME_ZONE} 到 {@link DataTypeFamily#CHARACTER_STRING}
 * 的类型转换规则。
 *
 * <p>功能说明: 将时间值格式化为字符串表示
 *
 * <p>转换语义:
 *
 * <ul>
 *   <li>格式化规则: 使用 ISO-8601 格式 'HH:mm:ss[.SSS...]'
 *   <li>精度处理: 根据时间类型的精度参数确定小数秒位数
 *   <li>时间值: 表示自午夜(00:00:00)以来的毫秒数
 * </ul>
 *
 * <p>转换示例:
 *
 * <pre>
 * TIME(3) '14:30:45.123' -> STRING '14:30:45.123'
 * TIME(0) '14:30:45' -> STRING '14:30:45'
 * TIME(6) '14:30:45.123456' -> STRING '14:30:45.123456'
 * TIME(9) '23:59:59.123456789' -> STRING '23:59:59.123456789'
 * TIME '00:00:00.000' -> STRING '00:00:00.000'
 * </pre>
 *
 * <p>NULL 值处理: 输入为 NULL 时,输出也为 NULL
 *
 * <p>SQL 标准兼容性: 符合 SQL:2016 标准中时间到字符串的显式转换规则
 */
class TimeToStringCastRule extends AbstractCastRule<Integer, BinaryString> {

    static final TimeToStringCastRule INSTANCE = new TimeToStringCastRule();

    private TimeToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeRoot.TIME_WITHOUT_TIME_ZONE)
                        .target(STRING_TYPE)
                        .build());
    }

    @Override
    public CastExecutor<Integer, BinaryString> create(DataType inputType, DataType targetType) {
        return value ->
                BinaryString.fromString(
                        DateTimeUtils.formatTimestampMillis(
                                value, DataTypeChecks.getPrecision(inputType)));
    }
}
