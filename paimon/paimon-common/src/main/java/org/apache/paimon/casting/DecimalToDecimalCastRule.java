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
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.utils.DecimalUtils;

/**
 * DECIMAL 类型到 DECIMAL 类型的转换规则。
 *
 * <p>处理不同精度(precision)和标度(scale)的 {@link DataTypeRoot#DECIMAL} 类型之间的转换。这是 DECIMAL
 * 类型内部的精度调整转换。
 *
 * <p>转换场景:
 *
 * <ul>
 *   <li>精度扩展: DECIMAL(10, 2) → DECIMAL(15, 2)
 *   <li>标度调整: DECIMAL(10, 2) → DECIMAL(10, 4)
 *   <li>精度和标度同时变化: DECIMAL(10, 2) → DECIMAL(8, 1)
 * </ul>
 *
 * <p>转换策略:
 *
 * <ul>
 *   <li>使用 {@link DecimalUtils#castToDecimal} 执行转换
 *   <li>如果目标精度更小,可能发生溢出
 *   <li>如果目标标度更小,会进行四舍五入
 *   <li>如果目标标度更大,会在小数部分补零
 * </ul>
 *
 * <p>示例:
 *
 * <pre>{@code
 * // 精度扩展
 * DECIMAL(5, 2)(123.45) → DECIMAL(10, 2)(123.45)
 *
 * // 标度增加
 * DECIMAL(5, 2)(123.45) → DECIMAL(5, 3)(123.450)
 *
 * // 标度减少(四舍五入)
 * DECIMAL(5, 2)(123.45) → DECIMAL(5, 1)(123.5)
 * DECIMAL(5, 2)(123.44) → DECIMAL(5, 1)(123.4)
 *
 * // 精度减少(可能溢出)
 * DECIMAL(10, 2)(12345.67) → DECIMAL(5, 2) 抛出异常或返回 null
 * }</pre>
 *
 * <p>注意事项:
 *
 * <ul>
 *   <li>精度减少可能导致溢出异常
 *   <li>标度减少会进行四舍五入,可能损失精度
 *   <li>符合 SQL:2016 标准的 CAST 语义
 * </ul>
 */
class DecimalToDecimalCastRule extends AbstractCastRule<Decimal, Decimal> {

    /** 单例实例 */
    static final DecimalToDecimalCastRule INSTANCE = new DecimalToDecimalCastRule();

    /**
     * 私有构造函数。
     *
     * <p>定义谓词:
     *
     * <ul>
     *   <li>输入类型: DECIMAL
     *   <li>目标类型: DECIMAL
     * </ul>
     */
    private DecimalToDecimalCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeRoot.DECIMAL)
                        .target(DataTypeRoot.DECIMAL)
                        .build());
    }

    /**
     * 创建 DECIMAL 到 DECIMAL 的转换执行器。
     *
     * <p>使用 {@link DecimalUtils#castToDecimal} 方法,根据目标类型的精度和标度调整输入值。
     *
     * @param inputType 输入 DECIMAL 类型
     * @param targetType 目标 DECIMAL 类型(包含新的精度和标度)
     * @return 转换执行器
     */
    @Override
    public CastExecutor<Decimal, Decimal> create(DataType inputType, DataType targetType) {
        DecimalType decimalType = (DecimalType) targetType;
        return value ->
                DecimalUtils.castToDecimal(
                        value, decimalType.getPrecision(), decimalType.getScale());
    }
}
