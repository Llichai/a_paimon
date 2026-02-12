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

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;

/**
 * 数值类型之间的转换规则。
 *
 * <p>支持整数类型({@link DataTypeFamily#INTEGER_NUMERIC})和浮点类型({@link
 * DataTypeFamily#APPROXIMATE_NUMERIC})之间的所有组合转换。
 *
 * <p>支持的转换类型:
 *
 * <ul>
 *   <li>整数类型: TINYINT, SMALLINT, INTEGER, BIGINT
 *   <li>浮点类型: FLOAT, DOUBLE
 * </ul>
 *
 * <p>转换策略: 使用 Java 的 Number 类提供的标准转换方法(byteValue, shortValue 等)
 *
 * <p>注意事项:
 *
 * <ul>
 *   <li>可能发生精度损失(如 DOUBLE → FLOAT)
 *   <li>可能发生溢出(如 INT → BYTE)
 *   <li>浮点数转整数时会截断小数部分
 * </ul>
 *
 * <p>示例:
 *
 * <pre>{@code
 * // INT(100) → BYTE(100)
 * // DOUBLE(3.14) → INT(3)  // 小数被截断
 * // INT(300) → BYTE(44)     // 发生溢出
 * }</pre>
 */
class NumericPrimitiveCastRule extends AbstractCastRule<Number, Number> {

    /** 单例实例 */
    static final NumericPrimitiveCastRule INSTANCE = new NumericPrimitiveCastRule();

    /**
     * 私有构造函数。
     *
     * <p>定义谓词:
     *
     * <ul>
     *   <li>输入类型: 整数类型族或浮点类型族
     *   <li>目标类型: 整数类型族或浮点类型族
     * </ul>
     */
    private NumericPrimitiveCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.INTEGER_NUMERIC)
                        .input(DataTypeFamily.APPROXIMATE_NUMERIC)
                        .target(DataTypeFamily.INTEGER_NUMERIC)
                        .target(DataTypeFamily.APPROXIMATE_NUMERIC)
                        .build());
    }

    /**
     * 创建数值类型转换执行器。
     *
     * <p>根据目标类型返回相应的转换函数:
     *
     * <ul>
     *   <li>TINYINT: Number::byteValue
     *   <li>SMALLINT: Number::shortValue
     *   <li>INTEGER: Number::intValue
     *   <li>BIGINT: Number::longValue
     *   <li>FLOAT: Number::floatValue
     *   <li>DOUBLE: Number::doubleValue
     * </ul>
     *
     * @param inputType 输入数据类型
     * @param targetType 目标数据类型
     * @return 转换执行器,如果目标类型不受支持则返回 null
     */
    @Override
    public CastExecutor<Number, Number> create(DataType inputType, DataType targetType) {
        switch (targetType.getTypeRoot()) {
            case TINYINT:
                return Number::byteValue;
            case SMALLINT:
                return Number::shortValue;
            case INTEGER:
                return Number::intValue;
            case BIGINT:
                return Number::longValue;
            case FLOAT:
                return Number::floatValue;
            case DOUBLE:
                return Number::doubleValue;
            default:
                return null;
        }
    }
}
