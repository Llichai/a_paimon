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
import org.apache.paimon.utils.BinaryStringUtils;

import java.util.TimeZone;

/**
 * {@link DataTypeFamily#CHARACTER_STRING} 到 {@link DataTypeFamily#TIMESTAMP} 的类型转换规则。
 *
 * <p>功能说明:
 *
 * <ul>
 *   <li>STRING 转 TIMESTAMP_WITHOUT_TIME_ZONE: 解析字符串为不带时区的时间戳
 *   <li>STRING 转 TIMESTAMP_WITH_LOCAL_TIME_ZONE: 解析字符串为带本地时区的时间戳
 * </ul>
 *
 * <p>转换语义:
 *
 * <ul>
 *   <li>解析格式: 支持多种 ISO-8601 格式,如:
 *       <ul>
 *         <li>'yyyy-MM-dd HH:mm:ss[.SSS...]'
 *         <li>'yyyy-MM-ddTHH:mm:ss[.SSS...]'
 *         <li>'yyyy-MM-dd'(自动补充 00:00:00)
 *       </ul>
 *   <li>精度处理: 根据目标类型的精度参数截断或填充小数秒位数
 *   <li>时区处理:
 *       <ul>
 *         <li>TIMESTAMP_WITHOUT_TIME_ZONE: 字符串被解释为 UTC 时间
 *         <li>TIMESTAMP_WITH_LOCAL_TIME_ZONE: 字符串被解释为本地时区时间
 *       </ul>
 * </ul>
 *
 * <p>转换示例:
 *
 * <pre>
 * // TIMESTAMP_WITHOUT_TIME_ZONE
 * STRING '2024-01-15 14:30:45.123' -> TIMESTAMP(3) '2024-01-15 14:30:45.123'
 * STRING '2024-01-15' -> TIMESTAMP(0) '2024-01-15 00:00:00'
 * STRING '2024-01-15T14:30:45' -> TIMESTAMP(0) '2024-01-15 14:30:45'
 *
 * // TIMESTAMP_WITH_LOCAL_TIME_ZONE (Asia/Shanghai, UTC+8)
 * STRING '2024-01-15 14:30:45.123' -> TIMESTAMP_WITH_LOCAL_TIME_ZONE '2024-01-15 14:30:45.123 +08:00'
 * </pre>
 *
 * <p>异常情况:
 *
 * <ul>
 *   <li>解析失败: 当字符串格式不符合时间戳格式时抛出异常
 *   <li>NULL 值处理: 输入为 NULL 时,输出也为 NULL
 * </ul>
 *
 * <p>SQL 标准兼容性: 符合 SQL:2016 标准中字符串到时间戳的显式转换规则
 */
class StringToTimestampCastRule extends AbstractCastRule<BinaryString, Timestamp> {

    static final StringToTimestampCastRule INSTANCE = new StringToTimestampCastRule();

    private StringToTimestampCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.CHARACTER_STRING)
                        .target(DataTypeFamily.TIMESTAMP)
                        .build());
    }

    @Override
    public CastExecutor<BinaryString, Timestamp> create(DataType inputType, DataType targetType) {
        int targetPrecision = DataTypeChecks.getPrecision(targetType);
        if (targetType.is(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
            return value -> BinaryStringUtils.toTimestamp(value, targetPrecision);
        } else if (targetType.is(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
            return value ->
                    BinaryStringUtils.toTimestamp(value, targetPrecision, TimeZone.getDefault());
        }
        return null;
    }
}
