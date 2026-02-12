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
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.utils.DateTimeUtils;

import java.time.ZoneId;

/**
 * {@link DataTypeFamily#INTEGER_NUMERIC} 到 {@link DataTypeRoot#TIMESTAMP_WITHOUT_TIME_ZONE}/{@link
 * DataTypeRoot#TIMESTAMP_WITH_LOCAL_TIME_ZONE} 的类型转换规则。
 *
 * <p>功能说明:
 *
 * <ul>
 *   <li>INTEGER/BIGINT 转 TIMESTAMP_WITHOUT_TIME_ZONE: 将 Unix 时间戳(秒)转换为 UTC 时间戳
 *   <li>INTEGER/BIGINT 转 TIMESTAMP_WITH_LOCAL_TIME_ZONE: 将 Unix 时间戳(秒)转换为本地时区时间戳
 * </ul>
 *
 * <p>转换语义:
 *
 * <ul>
 *   <li>Unix 时间戳: 输入数值表示自 Unix 纪元(1970-01-01 00:00:00 UTC)以来的秒数
 *   <li>转换过程: 秒数乘以1000转换为毫秒数,然后创建时间戳对象
 *   <li>时区处理:
 *       <ul>
 *         <li>TIMESTAMP_WITHOUT_TIME_ZONE: 使用 UTC 时区
 *         <li>TIMESTAMP_WITH_LOCAL_TIME_ZONE: 使用系统默认时区
 *       </ul>
 * </ul>
 *
 * <p>转换示例:
 *
 * <pre>
 * // BIGINT -> TIMESTAMP_WITHOUT_TIME_ZONE
 * BIGINT 0 -> TIMESTAMP '1970-01-01 00:00:00.000'
 * BIGINT 1705328445 -> TIMESTAMP '2024-01-15 14:30:45.000'
 * BIGINT -1 -> TIMESTAMP '1969-12-31 23:59:59.000'
 *
 * // INTEGER -> TIMESTAMP_WITHOUT_TIME_ZONE
 * INTEGER 1705328445 -> TIMESTAMP '2024-01-15 14:30:45.000'
 *
 * // BIGINT -> TIMESTAMP_WITH_LOCAL_TIME_ZONE (Asia/Shanghai, UTC+8)
 * BIGINT 1705300245 -> TIMESTAMP_WITH_LOCAL_TIME_ZONE '2024-01-15 14:30:45.000 +08:00'
 * </pre>
 *
 * <p>精度说明:
 *
 * <ul>
 *   <li>输入精度: 秒级精度
 *   <li>输出精度: 毫秒级精度(小数秒部分为0)
 *   <li>不支持亚秒级精度: 如需微秒或纳秒精度需要使用其他转换方法
 * </ul>
 *
 * <p>溢出处理:
 *
 * <ul>
 *   <li>INTEGER 范围: -2,147,483,648 到 2,147,483,647(约 1901-12-13 到 2038-01-19)
 *   <li>BIGINT 范围: -9,223,372,036,854,775,808 到 9,223,372,036,854,775,807
 *   <li>超出范围: BIGINT 到毫秒的转换可能溢出,但在实际应用中不太可能发生
 * </ul>
 *
 * <p>NULL 值处理: 输入为 NULL 时,输出也为 NULL
 *
 * <p>SQL 标准兼容性: 符合 SQL:2016 标准中数值到时间戳的显式转换规则
 */
public class NumericPrimitiveToTimestamp extends AbstractCastRule<Number, Timestamp> {

    static final NumericPrimitiveToTimestamp INSTANCE = new NumericPrimitiveToTimestamp();

    private NumericPrimitiveToTimestamp() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.NUMERIC)
                        .target(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                        .target(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                        .build());
    }

    @Override
    public CastExecutor<Number, Timestamp> create(DataType inputType, DataType targetType) {
        ZoneId zoneId =
                targetType.is(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                        ? ZoneId.systemDefault()
                        : DateTimeUtils.UTC_ZONE.toZoneId();
        switch (inputType.getTypeRoot()) {
            case INTEGER:
            case BIGINT:
                return value ->
                        Timestamp.fromLocalDateTime(
                                DateTimeUtils.toLocalDateTime(value.longValue() * 1000, zoneId));
            default:
                return null;
        }
    }
}
