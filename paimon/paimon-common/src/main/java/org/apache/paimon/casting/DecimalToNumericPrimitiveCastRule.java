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
import org.apache.paimon.utils.DecimalUtils;

/**
 * DECIMAL 类型到数值原始类型的转换规则。
 *
 * <p>支持将 {@link DataTypeRoot#DECIMAL} 转换为整数类型({@link DataTypeFamily#INTEGER_NUMERIC})
 * 和浮点类型({@link DataTypeFamily#APPROXIMATE_NUMERIC})。
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
 *   <li>转整数类型: 使用 {@link DecimalUtils#castToIntegral} 将 DECIMAL 转换为 long,然后强制转换为目标类型
 *   <li>转浮点类型: 使用 {@link DecimalUtils#doubleValue} 将 DECIMAL 转换为 double,FLOAT 类型再强制转换
 * </ul>
 *
 * <p>精度处理:
 *
 * <ul>
 *   <li>转整数时会截断小数部分(向零取整)
 *   <li>转 FLOAT 时可能因精度限制而产生舍入误差
 *   <li>转较小整数类型(如 BYTE, SHORT)时可能发生溢出
 * </ul>
 *
 * <p>示例:
 *
 * <pre>{@code
 * // 转整数(截断小数)
 * DECIMAL(5, 2)(123.45) → INT(123)
 * DECIMAL(5, 2)(123.99) → INT(123)
 * DECIMAL(5, 2)(-123.45) → INT(-123)
 *
 * // 转浮点
 * DECIMAL(10, 2)(12345.67) → DOUBLE(12345.67)
 * DECIMAL(20, 10)(123.4567890123) → FLOAT(123.456787) // 精度损失
 *
 * // 溢出情况
 * DECIMAL(5, 0)(1000) → BYTE(-24) // 溢出
 * DECIMAL(10, 0)(999999999999) → INT(1316134911) // 溢出
 * }</pre>
 *
 * <p>注意事项:
 *
 * <ul>
 *   <li>小数部分会被截断而不是四舍五入
 *   <li>转换为较小整数类型时可能发生溢出,不会抛出异常
 *   <li>转换为 FLOAT 时可能损失精度
 *   <li>符合 SQL:2016 标准的 CAST 语义
 * </ul>
 */
class DecimalToNumericPrimitiveCastRule extends AbstractCastRule<Decimal, Number> {

    /** 单例实例 */
    static final DecimalToNumericPrimitiveCastRule INSTANCE =
            new DecimalToNumericPrimitiveCastRule();

    /**
     * 私有构造函数。
     *
     * <p>定义谓词:
     *
     * <ul>
     *   <li>输入类型: DECIMAL
     *   <li>目标类型: 整数类型族或浮点类型族
     * </ul>
     */
    private DecimalToNumericPrimitiveCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeRoot.DECIMAL)
                        .target(DataTypeFamily.INTEGER_NUMERIC)
                        .target(DataTypeFamily.APPROXIMATE_NUMERIC)
                        .build());
    }

    /**
     * 创建 DECIMAL 到数值原始类型的转换执行器。
     *
     * <p>根据目标类型返回相应的转换函数:
     *
     * <ul>
     *   <li>TINYINT: 转 long 后强制转换为 byte
     *   <li>SMALLINT: 转 long 后强制转换为 short
     *   <li>INTEGER: 转 long 后强制转换为 int
     *   <li>BIGINT: 直接调用 castToIntegral 返回 long
     *   <li>FLOAT: 转 double 后强制转换为 float
     *   <li>DOUBLE: 直接调用 doubleValue 返回 double
     * </ul>
     *
     * @param inputType 输入 DECIMAL 类型
     * @param targetType 目标数值原始类型
     * @return 转换执行器,如果目标类型不受支持则返回 null
     */
    @Override
    public CastExecutor<Decimal, Number> create(DataType inputType, DataType targetType) {
        switch (targetType.getTypeRoot()) {
            case TINYINT:
                return value -> (byte) DecimalUtils.castToIntegral(value);
            case SMALLINT:
                return value -> (short) DecimalUtils.castToIntegral(value);
            case INTEGER:
                return value -> (int) DecimalUtils.castToIntegral(value);
            case BIGINT:
                return DecimalUtils::castToIntegral;
            case FLOAT:
                return value -> (float) DecimalUtils.doubleValue(value);
            case DOUBLE:
                return DecimalUtils::doubleValue;
            default:
                return null;
        }
    }
}
