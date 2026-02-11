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
import org.apache.paimon.utils.DecimalUtils;

/**
 * SUM 聚合器
 * 对数值字段执行求和聚合，支持撤回操作（减法）
 * 支持多种数值类型：DECIMAL, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE
 */
public class FieldSumAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    /**
     * 构造 SUM 聚合器
     * @param name 聚合函数名称
     * @param dataType 字段数据类型（必须是数值类型）
     */
    public FieldSumAgg(String name, DataType dataType) {
        super(name, dataType);
    }

    /**
     * 执行 SUM 聚合（加法）
     * @param accumulator 累加器值（当前累加和）
     * @param inputField 输入字段值
     * @return 两者的和，任一为null时返回非null的那个值
     */
    @Override
    public Object agg(Object accumulator, Object inputField) {
        // 如果有null值，返回非null的值
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }
        Object sum;

        // 根据字段类型执行相应的加法运算
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case DECIMAL: // Decimal类型需要保持精度和小数位
                Decimal mergeFieldDD = (Decimal) accumulator;
                Decimal inFieldDD = (Decimal) inputField;
                assert mergeFieldDD.scale() == inFieldDD.scale()
                        : "Inconsistent scale of aggregate Decimal!";
                assert mergeFieldDD.precision() == inFieldDD.precision()
                        : "Inconsistent precision of aggregate Decimal!";
                sum =
                        DecimalUtils.add(
                                mergeFieldDD,
                                inFieldDD,
                                mergeFieldDD.precision(),
                                mergeFieldDD.scale());
                break;
            case TINYINT: // 8位整数
                sum = (byte) ((byte) accumulator + (byte) inputField);
                break;
            case SMALLINT: // 16位整数
                sum = (short) ((short) accumulator + (short) inputField);
                break;
            case INTEGER: // 32位整数
                sum = (int) accumulator + (int) inputField;
                break;
            case BIGINT: // 64位整数
                sum = (long) accumulator + (long) inputField;
                break;
            case FLOAT: // 单精度浮点数
                sum = (float) accumulator + (float) inputField;
                break;
            case DOUBLE: // 双精度浮点数
                sum = (double) accumulator + (double) inputField;
                break;
            default:
                String msg =
                        String.format(
                                "type %s not support in %s",
                                fieldType.getTypeRoot().toString(), this.getClass().getName());
                throw new IllegalArgumentException(msg);
        }
        return sum;
    }

    /**
     * 执行撤回操作（减法）
     * 用于处理UPDATE_BEFORE和DELETE类型的消息，从累加器中减去撤回值
     * @param accumulator 累加器值
     * @param inputField 要撤回的字段值
     * @return 减法后的结果，任一为null时返回累加器或输入值的负数
     */
    @Override
    public Object retract(Object accumulator, Object inputField) {

        // 如果有null值，返回累加器或输入值的负数
        if (accumulator == null || inputField == null) {
            return (accumulator == null ? negative(inputField) : accumulator);
        }
        Object sum;
        // 根据字段类型执行相应的减法运算
        switch (fieldType.getTypeRoot()) {
            case DECIMAL: // Decimal类型的减法
                Decimal mergeFieldDD = (Decimal) accumulator;
                Decimal inFieldDD = (Decimal) inputField;
                assert mergeFieldDD.scale() == inFieldDD.scale()
                        : "Inconsistent scale of aggregate Decimal!";
                assert mergeFieldDD.precision() == inFieldDD.precision()
                        : "Inconsistent precision of aggregate Decimal!";
                sum =
                        DecimalUtils.subtract(
                                mergeFieldDD,
                                inFieldDD,
                                mergeFieldDD.precision(),
                                mergeFieldDD.scale());
                break;
            case TINYINT: // 8位整数减法
                sum = (byte) ((byte) accumulator - (byte) inputField);
                break;
            case SMALLINT: // 16位整数减法
                sum = (short) ((short) accumulator - (short) inputField);
                break;
            case INTEGER: // 32位整数减法
                sum = (int) accumulator - (int) inputField;
                break;
            case BIGINT: // 64位整数减法
                sum = (long) accumulator - (long) inputField;
                break;
            case FLOAT: // 单精度浮点数减法
                sum = (float) accumulator - (float) inputField;
                break;
            case DOUBLE: // 双精度浮点数减法
                sum = (double) accumulator - (double) inputField;
                break;
            default:
                String msg =
                        String.format(
                                "type %s not support in %s",
                                fieldType.getTypeRoot().toString(), this.getClass().getName());
                throw new IllegalArgumentException(msg);
        }
        return sum;
    }

    /**
     * 计算数值的负数
     * @param value 输入值
     * @return 输入值的负数，null返回null
     */
    private Object negative(Object value) {
        if (value == null) {
            return null;
        }
        // 根据字段类型执行相应的取负运算
        switch (fieldType.getTypeRoot()) {
            case DECIMAL: // Decimal类型取负
                Decimal decimal = (Decimal) value;
                return Decimal.fromBigDecimal(
                        decimal.toBigDecimal().negate(), decimal.precision(), decimal.scale());
            case TINYINT: // 8位整数取负
                return (byte) -((byte) value);
            case SMALLINT: // 16位整数取负
                return (short) -((short) value);
            case INTEGER: // 32位整数取负
                return -((int) value);
            case BIGINT: // 64位整数取负
                return -((long) value);
            case FLOAT: // 单精度浮点数取负
                return -((float) value);
            case DOUBLE: // 双精度浮点数取负
                return -((double) value);
            default:
                String msg =
                        String.format(
                                "type %s not support in %s",
                                fieldType.getTypeRoot().toString(), this.getClass().getName());
                throw new IllegalArgumentException(msg);
        }
    }
}
