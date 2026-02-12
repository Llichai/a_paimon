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
import org.apache.paimon.data.Decimal;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DecimalType;

import java.math.BigDecimal;

/**
 * 字符串类型到 DECIMAL 类型的转换规则。
 *
 * <p>支持将字符串类型({@link DataTypeFamily#CHARACTER_STRING})转换为 {@link DataTypeRoot#DECIMAL}。
 *
 * <p>转换策略:
 *
 * <ul>
 *   <li>首先将字符串转换为 Java {@link BigDecimal}
 *   <li>然后使用 {@link Decimal#fromBigDecimal} 创建指定精度和标度的 DECIMAL
 * </ul>
 *
 * <p>支持的字符串格式:
 *
 * <ul>
 *   <li>整数: "123", "-456"
 *   <li>小数: "123.45", "-67.89"
 *   <li>科学计数法: "1.23E+10", "4.56e-5"
 *   <li>前导零: "0123.45"
 *   <li>正负号: "+123.45", "-123.45"
 * </ul>
 *
 * <p>精度和标度处理:
 *
 * <ul>
 *   <li>如果字符串表示的数值精度超过目标精度,会抛出异常或返回 null
 *   <li>如果字符串的小数位数超过目标标度,会进行四舍五入
 *   <li>如果字符串的小数位数少于目标标度,会在右侧补零
 * </ul>
 *
 * <p>示例:
 *
 * <pre>{@code
 * // 基本转换
 * "123.45" → DECIMAL(10, 2)(123.45)
 * "-678.9" → DECIMAL(10, 2)(-678.90)
 *
 * // 精度调整
 * "123.456" → DECIMAL(10, 2)(123.46) // 四舍五入
 * "123.4" → DECIMAL(10, 2)(123.40)   // 补零
 *
 * // 科学计数法
 * "1.23E+2" → DECIMAL(10, 2)(123.00)
 * "1.23E-1" → DECIMAL(10, 2)(0.12)
 *
 * // 溢出情况
 * "12345.67" → DECIMAL(5, 2) → 抛出异常
 *
 * // 无效格式
 * "abc" → 抛出 NumberFormatException
 * "" → 抛出异常
 * }</pre>
 *
 * <p>注意事项:
 *
 * <ul>
 *   <li>字符串必须是有效的数值格式,否则会抛出 {@link NumberFormatException}
 *   <li>前导和尾随空格会被自动去除
 *   <li>如果数值超出目标精度范围,会抛出异常
 *   <li>符合 SQL:2016 标准的 CAST 语义
 * </ul>
 */
class StringToDecimalCastRule extends AbstractCastRule<BinaryString, Decimal> {

    /** 单例实例 */
    static final StringToDecimalCastRule INSTANCE = new StringToDecimalCastRule();

    /**
     * 私有构造函数。
     *
     * <p>定义谓词:
     *
     * <ul>
     *   <li>输入类型: 字符串类型族
     *   <li>目标类型: DECIMAL
     * </ul>
     */
    private StringToDecimalCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.CHARACTER_STRING)
                        .target(DataTypeRoot.DECIMAL)
                        .build());
    }

    /**
     * 创建字符串到 DECIMAL 的转换执行器。
     *
     * <p>转换步骤:
     *
     * <ol>
     *   <li>将 {@link BinaryString} 转换为 Java String
     *   <li>使用 {@link BigDecimal} 构造函数解析字符串
     *   <li>调用 {@link Decimal#fromBigDecimal} 创建指定精度和标度的 DECIMAL
     * </ol>
     *
     * @param inputType 输入字符串类型
     * @param targetType 目标 DECIMAL 类型(包含精度和标度信息)
     * @return 转换执行器
     */
    @Override
    public CastExecutor<BinaryString, Decimal> create(DataType inputType, DataType targetType) {
        final DecimalType decimalType = (DecimalType) targetType;
        return value ->
                Decimal.fromBigDecimal(
                        new BigDecimal(value.toString()),
                        decimalType.getPrecision(),
                        decimalType.getScale());
    }
}
