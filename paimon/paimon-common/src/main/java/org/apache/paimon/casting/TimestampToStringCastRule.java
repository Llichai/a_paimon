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
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.utils.DateTimeUtils;

import java.util.TimeZone;

import static org.apache.paimon.types.VarCharType.STRING_TYPE;

/**
 * {@link DataTypeFamily#TIMESTAMP} 到 {@link DataTypeFamily#CHARACTER_STRING} 的类型转换规则。
 *
 * <p>功能说明:
 *
 * <ul>
 *   <li>TIMESTAMP_WITHOUT_TIME_ZONE 转 STRING: 将时间戳格式化为 UTC 时区的字符串
 *   <li>TIMESTAMP_WITH_LOCAL_TIME_ZONE 转 STRING: 将时间戳格式化为本地时区的字符串
 * </ul>
 *
 * <p>转换语义:
 *
 * <ul>
 *   <li>格式化规则: 使用 ISO-8601 扩展格式 'yyyy-MM-dd HH:mm:ss[.SSS...]'
 *   <li>精度处理: 根据时间戳的精度参数确定小数秒位数
 *   <li>时区处理:
 *       <ul>
 *         <li>TIMESTAMP_WITHOUT_TIME_ZONE: 使用 UTC 时区格式化
 *         <li>TIMESTAMP_WITH_LOCAL_TIME_ZONE: 使用系统默认时区格式化
 *       </ul>
 * </ul>
 *
 * <p>转换示例:
 *
 * <pre>
 * // TIMESTAMP_WITHOUT_TIME_ZONE
 * TIMESTAMP(3) '2024-01-15 14:30:45.123' -> STRING '2024-01-15 14:30:45.123'
 * TIMESTAMP(0) '2024-01-15 14:30:45' -> STRING '2024-01-15 14:30:45'
 * TIMESTAMP(6) '2024-01-15 14:30:45.123456' -> STRING '2024-01-15 14:30:45.123456'
 *
 * // TIMESTAMP_WITH_LOCAL_TIME_ZONE (Asia/Shanghai, UTC+8)
 * TIMESTAMP_WITH_LOCAL_TIME_ZONE '2024-01-15 14:30:45.123 +08:00'
 *   -> STRING '2024-01-15 14:30:45.123'
 * </pre>
 *
 * <p>NULL 值处理: 输入为 NULL 时,输出也为 NULL
 *
 * <p>SQL 标准兼容性: 符合 SQL:2016 标准中时间戳到字符串的显式转换规则
 */
class TimestampToStringCastRule extends AbstractCastRule<Timestamp, BinaryString> {

    static final TimestampToStringCastRule INSTANCE = new TimestampToStringCastRule();

    private TimestampToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.TIMESTAMP)
                        .target(STRING_TYPE)
                        .build());
    }

    @Override
    public CastExecutor<Timestamp, BinaryString> create(DataType inputType, DataType targetType) {
        final int precision = DataTypeChecks.getPrecision(inputType);
        TimeZone timeZone =
                inputType.is(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                        ? TimeZone.getDefault()
                        : DateTimeUtils.UTC_ZONE;
        return value ->
                BinaryString.fromString(DateTimeUtils.formatTimestamp(value, timeZone, precision));
    }
}
