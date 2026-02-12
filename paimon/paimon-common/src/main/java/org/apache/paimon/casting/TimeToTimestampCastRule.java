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
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.utils.DateTimeUtils;

import java.util.TimeZone;

/**
 * {@link DataTypeRoot#TIME_WITHOUT_TIME_ZONE} 到 {@link
 * DataTypeRoot#TIMESTAMP_WITHOUT_TIME_ZONE}/{@link DataTypeRoot#TIMESTAMP_WITH_LOCAL_TIME_ZONE}
 * 的类型转换规则。
 *
 * <p>功能说明:
 *
 * <ul>
 *   <li>TIME 转 TIMESTAMP_WITHOUT_TIME_ZONE: 将时间值附加到 Unix 纪元日期(1970-01-01)
 *   <li>TIME 转 TIMESTAMP_WITH_LOCAL_TIME_ZONE: 将时间值附加到本地时区的 Unix 纪元日期
 * </ul>
 *
 * <p>转换语义:
 *
 * <ul>
 *   <li>时间值表示自午夜(00:00:00)以来的毫秒数
 *   <li>转换为时间戳时,日期部分设置为 1970-01-01
 *   <li>时区处理: TIMESTAMP_WITHOUT_TIME_ZONE 使用 UTC,TIMESTAMP_WITH_LOCAL_TIME_ZONE 使用系统默认时区
 * </ul>
 *
 * <p>转换示例:
 *
 * <pre>
 * TIME '14:30:45.123' -> TIMESTAMP '1970-01-01 14:30:45.123'
 * TIME '00:00:00.000' -> TIMESTAMP '1970-01-01 00:00:00.000'
 * TIME '23:59:59.999' -> TIMESTAMP '1970-01-01 23:59:59.999'
 * TIME '14:30:45.123' (Asia/Shanghai) -> TIMESTAMP_WITH_LOCAL_TIME_ZONE '1970-01-01 14:30:45.123 +08:00'
 * </pre>
 *
 * <p>NULL 值处理: 输入为 NULL 时,输出也为 NULL
 *
 * <p>SQL 标准兼容性: 符合 SQL:2016 标准中 TIME 到 TIMESTAMP 的隐式转换规则
 */
class TimeToTimestampCastRule extends AbstractCastRule<Number, Timestamp> {

    static final TimeToTimestampCastRule INSTANCE = new TimeToTimestampCastRule();

    private TimeToTimestampCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeRoot.TIME_WITHOUT_TIME_ZONE)
                        .target(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                        .target(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                        .build());
    }

    @Override
    public CastExecutor<Number, Timestamp> create(DataType inputType, DataType targetType) {
        if (targetType.is(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
            return value -> Timestamp.fromEpochMillis(value.longValue());
        } else if (targetType.is(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
            return value ->
                    DateTimeUtils.timeToTimestampWithLocalZone(
                            value.intValue(), TimeZone.getDefault());
        }
        return null;
    }
}
