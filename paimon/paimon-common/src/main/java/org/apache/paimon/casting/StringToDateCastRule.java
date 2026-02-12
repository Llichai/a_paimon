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
 * {@link DataTypeFamily#CHARACTER_STRING} 到 {@link DataTypeRoot#DATE} 的类型转换规则。
 *
 * <p>功能说明: 解析字符串为日期值
 *
 * <p>转换语义:
 *
 * <ul>
 *   <li>解析格式: 支持 ISO-8601 格式 'yyyy-MM-dd'
 *   <li>日期表示: 转换为自 Unix 纪元(1970-01-01)以来的天数
 *   <li>字符串格式: 必须严格遵循日期格式
 * </ul>
 *
 * <p>转换示例:
 *
 * <pre>
 * STRING '2024-01-15' -> DATE '2024-01-15'
 * STRING '1970-01-01' -> DATE '1970-01-01' (值为0)
 * STRING '1969-12-31' -> DATE '1969-12-31' (值为-1)
 * STRING '2024-12-31' -> DATE '2024-12-31'
 * </pre>
 *
 * <p>异常情况:
 *
 * <ul>
 *   <li>解析失败: 当字符串格式不符合日期格式时抛出异常
 *   <li>无效日期: 如 '2024-02-30' 等无效日期会抛出异常
 *   <li>NULL 值处理: 输入为 NULL 时,输出也为 NULL
 * </ul>
 *
 * <p>SQL 标准兼容性: 符合 SQL:2016 标准中字符串到日期的显式转换规则
 */
class StringToDateCastRule extends AbstractCastRule<BinaryString, Integer> {

    static final StringToDateCastRule INSTANCE = new StringToDateCastRule();

    private StringToDateCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.CHARACTER_STRING)
                        .target(DataTypeRoot.DATE)
                        .build());
    }

    @Override
    public CastExecutor<BinaryString, Integer> create(DataType inputType, DataType targetType) {
        return BinaryStringUtils::toDate;
    }
}
