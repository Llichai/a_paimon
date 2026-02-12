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
import org.apache.paimon.utils.BinaryStringUtils;

/**
 * 字符串类型到数值原始类型的转换规则。
 *
 * <p>支持将字符串类型({@link DataTypeFamily#CHARACTER_STRING})转换为整数类型({@link
 * DataTypeFamily#INTEGER_NUMERIC})和浮点类型({@link DataTypeFamily#APPROXIMATE_NUMERIC})。
 *
 * <p>支持的目标类型:
 *
 * <ul>
 *   <li>整数类型: TINYINT, SMALLINT, INTEGER, BIGINT
 *   <li>浮点类型: FLOAT, DOUBLE
 * </ul>
 *
 * <p>转换策略:
 *
 * <ul>
 *   <li>使用 {@link BinaryStringUtils} 中的类型转换方法
 *   <li>自动去除前导和尾随空格
 *   <li>支持正负号
 *   <li>整数类型支持十进制格式
 *   <li>浮点类型支持科学计数法
 * </ul>
 *
 * <p>支持的字符串格式:
 *
 * <ul>
 *   <li>整数: "123", "-456", "+789"
 *   <li>浮点数: "123.45", "-67.89", "+3.14"
 *   <li>科学计数法: "1.23E+10", "4.56e-5"
 *   <li>特殊值(仅浮点): "Infinity", "-Infinity", "NaN"
 * </ul>
 *
 * <p>示例:
 *
 * <pre>{@code
 * // 字符串转整数
 * "123" → INT(123)
 * "-456" → BIGINT(-456)
 * "+789" → SMALLINT(789)
 *
 * // 字符串转浮点(截断小数部分用于整数)
 * "123.45" → INT(123)        // 截断
 * "123.45" → DOUBLE(123.45)  // 保留
 *
 * // 科学计数法
 * "1.23E+2" → DOUBLE(123.0)
 * "4.56E-1" → FLOAT(0.456)
 *
 * // 特殊浮点值
 * "Infinity" → DOUBLE(Double.POSITIVE_INFINITY)
 * "NaN" → FLOAT(Float.NaN)
 *
 * // 溢出情况
 * "1000" → BYTE(-24)         // 溢出
 * "99999999999" → INT(...)   // 溢出
 *
 * // 无效格式
 * "abc" → 抛出 NumberFormatException
 * "" → 抛出异常
 * "12.34.56" → 抛出异常
 * }</pre>
 *
 * <p>注意事项:
 *
 * <ul>
 *   <li>字符串必须是有效的数值格式,否则会抛出 {@link NumberFormatException}
 *   <li>前导和尾随空格会被自动去除
 *   <li>整数转换时小数部分会被截断
 *   <li>转换为较小整数类型时可能发生溢出,不会抛出异常
 *   <li>符合 SQL:2016 标准的 CAST 语义
 * </ul>
 */
class StringToNumericPrimitiveCastRule extends AbstractCastRule<BinaryString, Number> {

    /** 单例实例 */
    static final StringToNumericPrimitiveCastRule INSTANCE = new StringToNumericPrimitiveCastRule();

    /**
     * 私有构造函数。
     *
     * <p>定义谓词:
     *
     * <ul>
     *   <li>输入类型: 字符串类型族
     *   <li>目标类型: 整数类型族或浮点类型族
     * </ul>
     */
    private StringToNumericPrimitiveCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.CHARACTER_STRING)
                        .target(DataTypeFamily.INTEGER_NUMERIC)
                        .target(DataTypeFamily.APPROXIMATE_NUMERIC)
                        .build());
    }

    /**
     * 创建字符串到数值原始类型的转换执行器。
     *
     * <p>根据目标类型返回相应的转换函数:
     *
     * <ul>
     *   <li>TINYINT: {@link BinaryStringUtils#toByte}
     *   <li>SMALLINT: {@link BinaryStringUtils#toShort}
     *   <li>INTEGER: {@link BinaryStringUtils#toInt}
     *   <li>BIGINT: {@link BinaryStringUtils#toLong}
     *   <li>FLOAT: {@link BinaryStringUtils#toFloat}
     *   <li>DOUBLE: {@link BinaryStringUtils#toDouble}
     * </ul>
     *
     * @param inputType 输入字符串类型
     * @param targetType 目标数值原始类型
     * @return 转换执行器,如果目标类型不受支持则返回 null
     */
    @Override
    public CastExecutor<BinaryString, Number> create(DataType inputType, DataType targetType) {
        switch (targetType.getTypeRoot()) {
            case TINYINT:
                return BinaryStringUtils::toByte;
            case SMALLINT:
                return BinaryStringUtils::toShort;
            case INTEGER:
                return BinaryStringUtils::toInt;
            case BIGINT:
                return BinaryStringUtils::toLong;
            case FLOAT:
                return BinaryStringUtils::toFloat;
            case DOUBLE:
                return BinaryStringUtils::toDouble;
            default:
                return null;
        }
    }
}
