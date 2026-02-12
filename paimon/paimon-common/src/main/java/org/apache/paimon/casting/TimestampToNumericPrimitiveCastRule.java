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

/**
 * {@link DataTypeRoot#TIMESTAMP_WITHOUT_TIME_ZONE}/{@link
 * DataTypeRoot#TIMESTAMP_WITH_LOCAL_TIME_ZONE} 到 {@link DataTypeFamily#NUMERIC} 的类型转换规则。
 *
 * <p>功能说明:
 *
 * <ul>
 *   <li>TIMESTAMP 转 BIGINT: 将时间戳转换为 Unix 时间戳(秒数)
 *   <li>TIMESTAMP 转 INTEGER: 将时间戳转换为 Unix 时间戳(秒数)并截断为 int
 * </ul>
 *
 * <p>转换语义:
 *
 * <ul>
 *   <li>Unix 时间戳: 表示自 Unix 纪元(1970-01-01 00:00:00 UTC)以来的秒数
 *   <li>精度损失: 时间戳的毫秒、微秒等小数秒部分会被丢弃
 *   <li>时区处理:
 *       <ul>
 *         <li>TIMESTAMP_WITHOUT_TIME_ZONE: 直接转换毫秒为秒
 *         <li>TIMESTAMP_WITH_LOCAL_TIME_ZONE: 先转换为本地时间,再转换为秒
 *       </ul>
 *   <li>溢出处理: BIGINT 转 INTEGER 时,如果值超出 int 范围会发生溢出截断
 * </ul>
 *
 * <p>转换示例:
 *
 * <pre>
 * // TIMESTAMP_WITHOUT_TIME_ZONE -> BIGINT
 * TIMESTAMP '1970-01-01 00:00:00.000' -> BIGINT 0
 * TIMESTAMP '2024-01-15 14:30:45.123' -> BIGINT 1705328445
 * TIMESTAMP '1969-12-31 23:59:59.999' -> BIGINT -1
 *
 * // TIMESTAMP_WITHOUT_TIME_ZONE -> INTEGER
 * TIMESTAMP '2024-01-15 14:30:45.123' -> INTEGER 1705328445
 *
 * // TIMESTAMP_WITH_LOCAL_TIME_ZONE -> BIGINT (Asia/Shanghai, UTC+8)
 * TIMESTAMP_WITH_LOCAL_TIME_ZONE '2024-01-15 14:30:45.123 +08:00' -> BIGINT 1705300245
 * </pre>
 *
 * <p>精度损失: 毫秒及更小的时间单位会被丢弃
 *
 * <p>溢出风险:
 *
 * <ul>
 *   <li>INTEGER 范围: -2,147,483,648 到 2,147,483,647
 *   <li>对应时间范围: 约 1901-12-13 到 2038-01-19
 *   <li>超出范围: 会发生溢出截断,结果不可预测
 * </ul>
 *
 * <p>NULL 值处理: 输入为 NULL 时,输出也为 NULL
 *
 * <p>SQL 标准兼容性: 符合 SQL:2016 标准中时间戳到数值的显式转换规则
 */
public class TimestampToNumericPrimitiveCastRule extends AbstractCastRule<Timestamp, Number> {

    static final TimestampToNumericPrimitiveCastRule INSTANCE =
            new TimestampToNumericPrimitiveCastRule();

    private TimestampToNumericPrimitiveCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                        .input(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                        .target(DataTypeRoot.BIGINT)
                        .target(DataTypeRoot.INTEGER)
                        .build());
    }

    @Override
    public CastExecutor<Timestamp, Number> create(DataType inputType, DataType targetType) {
        if (inputType.is(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
            if (targetType.is(DataTypeRoot.BIGINT)) {
                return value -> DateTimeUtils.unixTimestamp(value.getMillisecond());
            } else if (targetType.is(DataTypeRoot.INTEGER)) {
                return value -> (int) DateTimeUtils.unixTimestamp(value.getMillisecond());
            }
        } else if (inputType.is(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
            if (targetType.is(DataTypeRoot.BIGINT)) {
                return value ->
                        DateTimeUtils.unixTimestamp(
                                Timestamp.fromLocalDateTime(value.toLocalDateTime())
                                        .getMillisecond());
            } else if (targetType.is(DataTypeRoot.INTEGER)) {
                return value ->
                        (int)
                                DateTimeUtils.unixTimestamp(
                                        Timestamp.fromLocalDateTime(value.toLocalDateTime())
                                                .getMillisecond());
            }
        }

        return null;
    }
}
