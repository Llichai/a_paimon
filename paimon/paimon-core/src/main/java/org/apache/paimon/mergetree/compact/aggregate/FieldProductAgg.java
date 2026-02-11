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

package org.apache.paimon.mergetree.compact.aggregate;

import org.apache.paimon.data.Decimal;
import org.apache.paimon.types.DataType;

import java.math.BigDecimal;

import static org.apache.paimon.data.Decimal.fromBigDecimal;

/**
 * PRODUCT 聚合器
 * 对数值字段执行累乘聚合，支持撤回操作（除法）
 * 支持多种数值类型：DECIMAL, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE
 */
public class FieldProductAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    /**
     * 构造 PRODUCT 聚合器
     * @param name 聚合函数名称
     * @param dataType 字段数据类型（必须是数值类型）
     */
    public FieldProductAgg(String name, DataType dataType) {
        super(name, dataType);
    }

    /**
     * 执行 PRODUCT 聚合（乘法）
     * @param accumulator 累加器值（当前累乘积）
     * @param inputField 输入字段值
     * @return 两者的乘积，任一为null时返回非null的那个值
     */
    @Override
    public Object agg(Object accumulator, Object inputField) {
        // 如果有null值，返回非null的值
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }

        Object product;

        // 根据字段类型执行相应的乘法运算
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case DECIMAL: // Decimal类型需要保持精度和小数位
                Decimal mergeFieldDD = (Decimal) accumulator;
                Decimal inFieldDD = (Decimal) inputField;
                assert mergeFieldDD.scale() == inFieldDD.scale()
                        : "Inconsistent scale of aggregate Decimal!";
                assert mergeFieldDD.precision() == inFieldDD.precision()
                        : "Inconsistent precision of aggregate Decimal!";
                BigDecimal bigDecimal = mergeFieldDD.toBigDecimal();
                BigDecimal bigDecimal1 = inFieldDD.toBigDecimal();
                BigDecimal mul = bigDecimal.multiply(bigDecimal1);
                product = fromBigDecimal(mul, mergeFieldDD.precision(), mergeFieldDD.scale());
                break;
            case TINYINT: // 8位整数乘法
                product = (byte) ((byte) accumulator * (byte) inputField);
                break;
            case SMALLINT: // 16位整数乘法
                product = (short) ((short) accumulator * (short) inputField);
                break;
            case INTEGER: // 32位整数乘法
                product = (int) accumulator * (int) inputField;
                break;
            case BIGINT: // 64位整数乘法
                product = (long) accumulator * (long) inputField;
                break;
            case FLOAT: // 单精度浮点数乘法
                product = (float) accumulator * (float) inputField;
                break;
            case DOUBLE: // 双精度浮点数乘法
                product = (double) accumulator * (double) inputField;
                break;
            default:
                String msg =
                        String.format(
                                "type %s not support in %s",
                                fieldType.getTypeRoot().toString(), this.getClass().getName());
                throw new IllegalArgumentException(msg);
        }
        return product;
    }

    /**
     * 执行撤回操作（除法）
     * 用于处理UPDATE_BEFORE和DELETE类型的消息，从累加器中除以撤回值
     * @param accumulator 累加器值
     * @param inputField 要撤回的字段值
     * @return 除法后的结果，任一为null时返回累加器
     */
    @Override
    public Object retract(Object accumulator, Object inputField) {
        Object product;

        // 如果有null值，返回累加器
        if (accumulator == null || inputField == null) {
            product = accumulator;
        } else {
            // 根据字段类型执行相应的除法运算
            switch (fieldType.getTypeRoot()) {
                case DECIMAL: // Decimal类型的除法
                    Decimal mergeFieldDD = (Decimal) accumulator;
                    Decimal inFieldDD = (Decimal) inputField;
                    assert mergeFieldDD.scale() == inFieldDD.scale()
                            : "Inconsistent scale of aggregate Decimal!";
                    assert mergeFieldDD.precision() == inFieldDD.precision()
                            : "Inconsistent precision of aggregate Decimal!";
                    BigDecimal bigDecimal = mergeFieldDD.toBigDecimal();
                    BigDecimal bigDecimal1 = inFieldDD.toBigDecimal();
                    BigDecimal div = bigDecimal.divide(bigDecimal1);
                    product = fromBigDecimal(div, mergeFieldDD.precision(), mergeFieldDD.scale());
                    break;
                case TINYINT: // 8位整数除法
                    product = (byte) ((byte) accumulator / (byte) inputField);
                    break;
                case SMALLINT: // 16位整数除法
                    product = (short) ((short) accumulator / (short) inputField);
                    break;
                case INTEGER: // 32位整数除法
                    product = (int) accumulator / (int) inputField;
                    break;
                case BIGINT: // 64位整数除法
                    product = (long) accumulator / (long) inputField;
                    break;
                case FLOAT: // 单精度浮点数除法
                    product = (float) accumulator / (float) inputField;
                    break;
                case DOUBLE: // 双精度浮点数除法
                    product = (double) accumulator / (double) inputField;
                    break;
                default:
                    String msg =
                            String.format(
                                    "type %s not support in %s",
                                    fieldType.getTypeRoot().toString(), this.getClass().getName());
                    throw new IllegalArgumentException(msg);
            }
        }
        return product;
    }
}
