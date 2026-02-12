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

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.utils.DateTimeUtils;

import java.util.TimeZone;
import java.util.function.Function;

/**
 * {@link DataTypeRoot#TIMESTAMP_WITHOUT_TIME_ZONE}/{@link
 * DataTypeRoot#TIMESTAMP_WITH_LOCAL_TIME_ZONE} 到 {@link
 * DataTypeRoot#TIMESTAMP_WITHOUT_TIME_ZONE}/{@link DataTypeRoot#TIMESTAMP_WITH_LOCAL_TIME_ZONE}
 * 的类型转换规则。检查并调整精度变化。
 *
 * <p>功能说明:
 *
 * <ul>
 *   <li>时间戳类型之间的转换(带时区 ↔ 不带时区)
 *   <li>时间戳精度的调整(如从微秒精度转为毫秒精度)
 * </ul>
 *
 * <p>转换语义:
 *
 * <ul>
 *   <li>时区转换:
 *       <ul>
 *         <li>TIMESTAMP_WITHOUT_TIME_ZONE -> TIMESTAMP_WITH_LOCAL_TIME_ZONE: 将 UTC 时间戳转换为本地时区
 *         <li>TIMESTAMP_WITH_LOCAL_TIME_ZONE -> TIMESTAMP_WITHOUT_TIME_ZONE: 将本地时区时间戳转换为 UTC
 *         <li>相同类型转换: 保持原值不变
 *       </ul>
 *   <li>精度调整:
 *       <ul>
 *         <li>精度相同或目标精度更高: 保持原值
 *         <li>目标精度更低: 截断(truncate)到目标精度
 *       </ul>
 * </ul>
 *
 * <p>转换示例:
 *
 * <pre>
 * // 时区转换
 * TIMESTAMP '2024-01-15 14:30:45.123' (UTC)
 *   -> TIMESTAMP_WITH_LOCAL_TIME_ZONE '2024-01-15 22:30:45.123 +08:00' (Asia/Shanghai)
 *
 * // 精度调整
 * TIMESTAMP(9) '2024-01-15 14:30:45.123456789' -> TIMESTAMP(3) '2024-01-15 14:30:45.123'
 * TIMESTAMP(3) '2024-01-15 14:30:45.123' -> TIMESTAMP(6) '2024-01-15 14:30:45.123000'
 * TIMESTAMP(6) '2024-01-15 14:30:45.123999' -> TIMESTAMP(3) '2024-01-15 14:30:45.123'
 * </pre>
 *
 * <p>精度损失: 当目标精度低于输入精度时,会发生精度截断,例如微秒级精度转为毫秒级精度
 *
 * <p>NULL 值处理: 输入为 NULL 时,输出也为 NULL
 *
 * <p>SQL 标准兼容性: 符合 SQL:2016 标准中时间戳类型转换规则
 */
class TimestampToTimestampCastRule extends AbstractCastRule<Timestamp, Timestamp> {

    static final TimestampToTimestampCastRule INSTANCE = new TimestampToTimestampCastRule();

    private TimestampToTimestampCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                        .input(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                        .target(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                        .target(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                        .build());
    }

    @Override
    public CastExecutor<Timestamp, Timestamp> create(DataType inputType, DataType targetType) {
        final int inputPrecision = DataTypeChecks.getPrecision(inputType);
        int targetPrecision = DataTypeChecks.getPrecision(targetType);

        final Function<Timestamp, Timestamp> operand;
        if (inputType.is(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                && targetType.is(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
            operand =
                    value ->
                            DateTimeUtils.timestampToTimestampWithLocalZone(
                                    value, TimeZone.getDefault());
        } else if (inputType.is(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                && targetType.is(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
            operand =
                    value ->
                            DateTimeUtils.timestampWithLocalZoneToTimestamp(
                                    value, TimeZone.getDefault());
        } else {
            operand = value -> value;
        }

        if (inputPrecision <= targetPrecision) {
            return operand::apply;
        } else {
            return value -> DateTimeUtils.truncate(operand.apply(value), targetPrecision);
        }
    }
}
