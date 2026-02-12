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
import org.apache.paimon.utils.BinaryStringUtils;

/**
 * {@link DataTypeFamily#CHARACTER_STRING} 到 {@link DataTypeRoot#TIME_WITHOUT_TIME_ZONE}
 * 的类型转换规则。
 *
 * <p>功能说明: 解析字符串为时间值
 *
 * <p>转换语义:
 *
 * <ul>
 *   <li>解析格式: 支持 ISO-8601 格式 'HH:mm:ss[.SSS...]'
 *   <li>时间表示: 转换为自午夜(00:00:00)以来的毫秒数
 *   <li>字符串格式: 支持带或不带小数秒的时间格式
 * </ul>
 *
 * <p>转换示例:
 *
 * <pre>
 * STRING '14:30:45.123' -> TIME '14:30:45.123'
 * STRING '14:30:45' -> TIME '14:30:45.000'
 * STRING '00:00:00' -> TIME '00:00:00.000'
 * STRING '23:59:59.999' -> TIME '23:59:59.999'
 * STRING '14:30:45.123456' -> TIME '14:30:45.123456'
 * </pre>
 *
 * <p>异常情况:
 *
 * <ul>
 *   <li>解析失败: 当字符串格式不符合时间格式时抛出异常
 *   <li>无效时间: 如 '25:00:00' 或 '14:60:00' 等无效时间会抛出异常
 *   <li>NULL 值处理: 输入为 NULL 时,输出也为 NULL
 * </ul>
 *
 * <p>SQL 标准兼容性: 符合 SQL:2016 标准中字符串到时间的显式转换规则
 */
class StringToTimeCastRule extends AbstractCastRule<BinaryString, Integer> {

    static final StringToTimeCastRule INSTANCE = new StringToTimeCastRule();

    private StringToTimeCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.CHARACTER_STRING)
                        .target(DataTypeRoot.TIME_WITHOUT_TIME_ZONE)
                        .build());
    }

    @Override
    public CastExecutor<BinaryString, Integer> create(DataType inputType, DataType targetType) {
        return BinaryStringUtils::toTime;
    }
}
