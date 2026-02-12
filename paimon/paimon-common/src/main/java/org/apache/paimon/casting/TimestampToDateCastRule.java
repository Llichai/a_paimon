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
 * {@link DataTypeRoot#TIMESTAMP_WITHOUT_TIME_ZONE}/{@link
 * DataTypeRoot#TIMESTAMP_WITH_LOCAL_TIME_ZONE} 到 {@link DataTypeRoot#DATE} 的类型转换规则。
 *
 * <p>功能说明:
 *
 * <ul>
 *   <li>TIMESTAMP_WITHOUT_TIME_ZONE 转 DATE: 提取时间戳的日期部分,丢弃时间部分
 *   <li>TIMESTAMP_WITH_LOCAL_TIME_ZONE 转 DATE: 根据本地时区提取日期部分
 * </ul>
 *
 * <p>转换语义:
 *
 * <ul>
 *   <li>精度损失: 时间部分(小时、分钟、秒、毫秒等)会被丢弃
 *   <li>时区处理: TIMESTAMP_WITHOUT_TIME_ZONE 使用 UTC 解释,TIMESTAMP_WITH_LOCAL_TIME_ZONE 使用系统默认时区
 *   <li>日期计算: 时间戳毫秒数除以每天的毫秒数(86400000)得到日期值
 * </ul>
 *
 * <p>转换示例:
 *
 * <pre>
 * TIMESTAMP '2024-01-15 14:30:45.123' -> DATE '2024-01-15'
 * TIMESTAMP '1970-01-01 23:59:59.999' -> DATE '1970-01-01'
 * TIMESTAMP '2024-06-30 00:00:00.000' -> DATE '2024-06-30'
 * TIMESTAMP_WITH_LOCAL_TIME_ZONE '2024-01-15 14:30:45.123 +08:00' -> DATE '2024-01-15' (Asia/Shanghai)
 * </pre>
 *
 * <p>NULL 值处理: 输入为 NULL 时,输出也为 NULL
 *
 * <p>SQL 标准兼容性: 符合 SQL:2016 标准中 TIMESTAMP 到 DATE 的显式转换规则
 */
class TimestampToDateCastRule extends AbstractCastRule<Timestamp, Number> {

    static final TimestampToDateCastRule INSTANCE = new TimestampToDateCastRule();

    private TimestampToDateCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                        .input(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                        .target(DataTypeRoot.DATE)
                        .build());
    }

    @Override
    public CastExecutor<Timestamp, Number> create(DataType inputType, DataType targetType) {
        if (inputType.is(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
            return value -> (int) (value.getMillisecond() / DateTimeUtils.MILLIS_PER_DAY);
        } else if (inputType.is(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
            return value ->
                    DateTimeUtils.timestampWithLocalZoneToDate(value, TimeZone.getDefault());
        }
        return null;
    }
}
