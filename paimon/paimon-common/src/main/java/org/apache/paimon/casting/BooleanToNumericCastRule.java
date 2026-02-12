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
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.utils.DecimalUtils;

/**
 * BOOLEAN 类型到数值类型的转换规则。
 *
 * <p>支持将 {@link DataTypeRoot#BOOLEAN} 转换为所有数值类型({@link DataTypeFamily#NUMERIC})。
 *
 * <p>支持的目标类型:
 *
 * <ul>
 *   <li>整数类型: TINYINT, SMALLINT, INTEGER, BIGINT
 *   <li>浮点类型: FLOAT, DOUBLE
 *   <li>定点类型: DECIMAL
 * </ul>
 *
 * <p>转换规则:
 *
 * <ul>
 *   <li>true → 1
 *   <li>false → 0
 * </ul>
 *
 * <p>转换策略:
 *
 * <ul>
 *   <li>整数类型: 直接返回 1 或 0,并强制转换为目标类型
 *   <li>浮点类型: 返回 1.0 或 0.0
 *   <li>DECIMAL 类型: 使用 {@link DecimalUtils#castFrom} 创建相应精度和标度的 DECIMAL 值
 * </ul>
 *
 * <p>示例:
 *
 * <pre>{@code
 * // 转整数
 * true → TINYINT(1)
 * false → INT(0)
 * true → BIGINT(1)
 *
 * // 转浮点
 * true → FLOAT(1.0)
 * false → DOUBLE(0.0)
 *
 * // 转 DECIMAL
 * true → DECIMAL(10, 2)(1.00)
 * false → DECIMAL(5, 0)(0)
 * }</pre>
 *
 * <p>注意事项:
 *
 * <ul>
 *   <li>这是一种显式类型转换,通常不会自动发生
 *   <li>符合 SQL:2016 标准的 CAST 语义
 *   <li>转换结果总是 0 或 1,不会有其他值
 * </ul>
 */
class BooleanToNumericCastRule extends AbstractCastRule<Boolean, Object> {

    /** 单例实例 */
    static final BooleanToNumericCastRule INSTANCE = new BooleanToNumericCastRule();

    /**
     * 私有构造函数。
     *
     * <p>定义谓词:
     *
     * <ul>
     *   <li>输入类型: BOOLEAN
     *   <li>目标类型: 数值类型族
     * </ul>
     */
    private BooleanToNumericCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeRoot.BOOLEAN)
                        .target(DataTypeFamily.NUMERIC)
                        .build());
    }

    /**
     * 创建 BOOLEAN 到数值类型的转换执行器。
     *
     * <p>根据目标类型返回相应的转换函数:
     *
     * <ul>
     *   <li>TINYINT: toInt 后强制转换为 byte
     *   <li>SMALLINT: toInt 后强制转换为 short
     *   <li>INTEGER: 直接调用 toInt
     *   <li>BIGINT: toInt 后转换为 long
     *   <li>FLOAT: toInt 后转换为 float
     *   <li>DOUBLE: toInt 后转换为 double
     *   <li>DECIMAL: toInt 后使用 DecimalUtils.castFrom 创建 DECIMAL
     * </ul>
     *
     * @param inputType 输入 BOOLEAN 类型
     * @param targetType 目标数值类型
     * @return 转换执行器,如果目标类型不受支持则返回 null
     */
    @Override
    public CastExecutor<Boolean, Object> create(DataType inputType, DataType targetType) {
        switch (targetType.getTypeRoot()) {
            case TINYINT:
                return value -> (byte) toInt(value);
            case SMALLINT:
                return value -> (short) toInt(value);
            case INTEGER:
                return this::toInt;
            case BIGINT:
                return value -> (long) toInt(value);
            case FLOAT:
                return value -> (float) toInt(value);
            case DOUBLE:
                return value -> (double) toInt(value);
            case DECIMAL:
                final DecimalType decimalType = (DecimalType) targetType;
                return value ->
                        DecimalUtils.castFrom(
                                toInt(value), decimalType.getPrecision(), decimalType.getScale());
            default:
                return null;
        }
    }

    /**
     * 将 boolean 值转换为 int。
     *
     * @param bool 布尔值
     * @return true 返回 1,false 返回 0
     */
    private int toInt(boolean bool) {
        return bool ? 1 : 0;
    }
}
