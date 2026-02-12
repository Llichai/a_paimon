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

import org.apache.paimon.data.Decimal;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.utils.DecimalUtils;

/**
 * 数值原始类型转 DECIMAL 类型的转换规则。
 *
 * <p>支持将整数类型({@link DataTypeFamily#INTEGER_NUMERIC})和浮点类型({@link
 * DataTypeFamily#APPROXIMATE_NUMERIC})转换为 {@link DataTypeRoot#DECIMAL} 类型。
 *
 * <p>支持的输入类型:
 *
 * <ul>
 *   <li>整数类型: TINYINT, SMALLINT, INTEGER, BIGINT
 *   <li>浮点类型: FLOAT, DOUBLE
 * </ul>
 *
 * <p>转换策略:
 *
 * <ul>
 *   <li>整数类型: 转换为 long,然后调用 {@link DecimalUtils#castFrom(long, int, int)}
 *   <li>浮点类型: 转换为 double,然后调用 {@link DecimalUtils#castFrom(double, int, int)}
 * </ul>
 *
 * <p>精度和标度处理:
 *
 * <ul>
 *   <li>结果会根据目标类型的精度(precision)和标度(scale)进行调整
 *   <li>如果输入值超出目标精度范围,会抛出异常或返回 null
 *   <li>小数部分会根据标度进行四舍五入
 * </ul>
 *
 * <p>示例:
 *
 * <pre>{@code
 * // INT(100) → DECIMAL(10, 2) → 100.00
 * // DOUBLE(3.14159) → DECIMAL(5, 2) → 3.14 (四舍五入)
 * // BIGINT(1234567890) → DECIMAL(10, 0) → 1234567890
 * // FLOAT(999.999) → DECIMAL(5, 1) → 1000.0 (四舍五入)
 * }</pre>
 *
 * <p>注意事项:
 *
 * <ul>
 *   <li>浮点数转 DECIMAL 可能因精度限制而产生舍入误差
 *   <li>如果整数值的位数超过目标精度,会导致溢出
 *   <li>符合 SQL:2016 标准的 CAST 语义
 * </ul>
 */
class NumericPrimitiveToDecimalCastRule extends AbstractCastRule<Number, Decimal> {

    /** 单例实例 */
    static final NumericPrimitiveToDecimalCastRule INSTANCE =
            new NumericPrimitiveToDecimalCastRule();

    /**
     * 私有构造函数。
     *
     * <p>定义谓词:
     *
     * <ul>
     *   <li>输入类型: 整数类型族或浮点类型族
     *   <li>目标类型: DECIMAL
     * </ul>
     */
    private NumericPrimitiveToDecimalCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.INTEGER_NUMERIC)
                        .input(DataTypeFamily.APPROXIMATE_NUMERIC)
                        .target(DataTypeRoot.DECIMAL)
                        .build());
    }

    /**
     * 创建数值原始类型到 DECIMAL 的转换执行器。
     *
     * <p>根据输入类型选择不同的转换策略:
     *
     * <ul>
     *   <li>整数类型: 转换为 long,然后创建 DECIMAL
     *   <li>浮点类型: 转换为 double,然后创建 DECIMAL
     * </ul>
     *
     * @param inputType 输入数据类型
     * @param targetType 目标 DECIMAL 类型(包含精度和标度信息)
     * @return 转换执行器
     */
    @Override
    public CastExecutor<Number, Decimal> create(DataType inputType, DataType targetType) {
        final DecimalType decimalType = (DecimalType) targetType;
        if (inputType.is(DataTypeFamily.INTEGER_NUMERIC)) {
            return value ->
                    DecimalUtils.castFrom(
                            value.longValue(), decimalType.getPrecision(), decimalType.getScale());
        }
        return value ->
                DecimalUtils.castFrom(
                        value.doubleValue(), decimalType.getPrecision(), decimalType.getScale());
    }
}
