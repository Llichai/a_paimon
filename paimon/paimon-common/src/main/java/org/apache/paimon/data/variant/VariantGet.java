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

package org.apache.paimon.data.variant;

import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.variant.GenericVariantUtil.Type;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VariantType;

import java.math.BigDecimal;
import java.util.HashMap;

/**
 * Variant 值提取和类型转换工具类。
 *
 * <p>提供将 Variant 值转换为 Paimon 数据类型的功能，支持多种类型转换策略：
 * <ul>
 *   <li>基本类型转换：BOOLEAN, LONG, STRING, DOUBLE, DECIMAL 等
 *   <li>复杂类型转换：ROW, ARRAY, MAP
 *   <li>特殊类型处理：UUID, DATE, TIMESTAMP
 *   <li>错误处理：根据配置决定是抛出异常还是返回 null
 * </ul>
 *
 * <p><b>类型映射规则：</b>
 * <pre>
 * Variant 类型    → Paimon 类型
 * ─────────────────────────────
 * NULL           → null
 * BOOLEAN        → BOOLEAN
 * LONG           → BIGINT
 * STRING         → STRING
 * DOUBLE         → DOUBLE
 * DECIMAL        → DECIMAL
 * DATE           → DATE
 * TIMESTAMP      → TIMESTAMP_LTZ
 * TIMESTAMP_NTZ  → TIMESTAMP
 * FLOAT          → FLOAT
 * UUID           → STRING
 * OBJECT         → ROW / MAP&lt;STRING, *&gt;
 * ARRAY          → ARRAY&lt;*&gt;
 * </pre>
 *
 * <p><b>使用示例：</b>
 * <pre>{@code
 * GenericVariant variant = GenericVariant.fromJson("{\"age\":30}");
 * VariantCastArgs args = VariantCastArgs.defaultArgs();
 *
 * // 提取并转换为整数
 * Integer age = (Integer) VariantGet.cast(variant, DataTypes.INT(), args);
 * }</pre>
 *
 * @since 1.0
 */
public class VariantGet {

    /**
     * 将 Variant 值转换为指定的 Paimon 数据类型。
     *
     * <p><b>转换策略：</b>
     * <ol>
     *   <li>Variant 类型处理：直接复制并构造新的 Variant
     *   <li>NULL 类型处理：返回 null
     *   <li>UUID 类型处理：只能转换为 STRING
     *   <li>ROW 类型处理：从 OBJECT 提取字段并递归转换
     *   <li>MAP 类型处理：从 OBJECT 提取键值对并转换
     *   <li>ARRAY 类型处理：从 ARRAY 提取元素并递归转换
     *   <li>基本类型处理：使用 CastExecutor 进行类型转换
     * </ol>
     *
     * @param v Variant 值
     * @param dataType 目标 Paimon 数据类型
     * @param castArgs 转换参数（包含错误处理策略和时区信息）
     * @return 转换后的值，失败时根据 castArgs.failOnError() 决定抛出异常或返回 null
     */
    public static Object cast(GenericVariant v, DataType dataType, VariantCastArgs castArgs) {
        if (dataType instanceof VariantType) {
            GenericVariantBuilder builder = new GenericVariantBuilder(false);
            builder.appendVariant(v);
            GenericVariant result = builder.result();
            return new GenericVariant(result.value(), result.metadata());
        }

        Type variantType = v.getType();
        if (variantType == Type.NULL) {
            return null;
        }

        if (variantType == Type.UUID) {
            // There's no UUID type in Paimon. We only allow it to be cast to string.
            if (dataType.equals(DataTypes.STRING())) {
                return BinaryString.fromString(v.getUuid().toString());
            } else {
                return invalidCast(v, dataType, castArgs);
            }
        }

        if (dataType instanceof RowType) {
            RowType rowType = (RowType) dataType;
            if (variantType == Type.OBJECT) {
                GenericRow row = new GenericRow(rowType.getFieldCount());
                for (int i = 0; i < v.objectSize(); i++) {
                    GenericVariant.ObjectField field = v.getFieldAtIndex(i);
                    int idx = rowType.getFieldIndex(field.key);
                    if (idx != -1) {
                        row.setField(idx, cast(field.value, rowType.getTypeAt(idx), castArgs));
                    }
                }
                return row;
            } else {
                return invalidCast(v, dataType, castArgs);
            }
        } else if (dataType instanceof MapType) {
            MapType mapType = (MapType) dataType;
            DataType valueType = mapType.getValueType();
            if (mapType.getKeyType().equals(DataTypes.STRING())) {
                if (variantType == Type.OBJECT) {
                    int size = v.objectSize();
                    HashMap<BinaryString, Object> map = new HashMap<>();
                    for (int i = 0; i < size; i++) {
                        GenericVariant.ObjectField field = v.getFieldAtIndex(i);
                        map.put(
                                BinaryString.fromString(field.key),
                                cast(field.value, valueType, castArgs));
                    }
                    return new GenericMap(map);
                } else {
                    return invalidCast(v, dataType, castArgs);
                }
            } else {
                return invalidCast(v, dataType, castArgs);
            }
        } else if (dataType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) dataType;
            if (variantType == Type.ARRAY) {
                int size = v.arraySize();
                Object[] array = new Object[size];
                for (int i = 0; i < size; i++) {
                    array[i] = cast(v.getElementAtIndex(i), arrayType.getElementType(), castArgs);
                }
                return new GenericArray(array);
            } else {
                return invalidCast(v, dataType, castArgs);
            }
        } else {
            Object input;
            DataType inputType;
            switch (variantType) {
                case OBJECT:
                case ARRAY:
                    if (dataType.equals(DataTypes.STRING())) {
                        return BinaryString.fromString(v.toJson(castArgs.zoneId()));
                    } else {
                        return invalidCast(v, dataType, castArgs);
                    }
                case BOOLEAN:
                    input = v.getBoolean();
                    inputType = DataTypes.BOOLEAN();
                    break;
                case LONG:
                    input = v.getLong();
                    inputType = DataTypes.BIGINT();
                    break;
                case STRING:
                    input = BinaryString.fromString(v.getString());
                    inputType = DataTypes.STRING();
                    break;
                case DOUBLE:
                    input = v.getDouble();
                    inputType = DataTypes.DOUBLE();
                    break;
                case DECIMAL:
                    BigDecimal decimal = v.getDecimal();
                    int precision = decimal.precision();
                    int scale = decimal.scale();
                    input = Decimal.fromBigDecimal(decimal, precision, scale);
                    inputType = DataTypes.DECIMAL(precision, scale);
                    break;
                case DATE:
                    input = (int) v.getLong();
                    inputType = DataTypes.DATE();
                    break;
                case FLOAT:
                    input = v.getFloat();
                    inputType = DataTypes.FLOAT();
                    break;
                default:
                    // todo: support other types
                    throw new IllegalArgumentException("Unsupported type: " + v.getType());
            }

            if (inputType.equals(dataType)) {
                return input;
            }

            CastExecutor<Object, Object> resolve =
                    (CastExecutor<Object, Object>) CastExecutors.resolve(inputType, dataType);
            if (resolve != null) {
                try {
                    return resolve.cast(input);
                } catch (Exception e) {
                    return invalidCast(v, dataType, castArgs);
                }
            }

            return invalidCast(v, dataType, castArgs);
        }
    }

    /**
     * 处理无效的类型转换。
     *
     * <p>根据 castArgs.failOnError() 的配置：
     * <ul>
     *   <li>true: 抛出 RuntimeException，包含详细的错误信息
     *   <li>false: 返回 null
     * </ul>
     *
     * @param v Variant 值
     * @param dataType 目标数据类型
     * @param castArgs 转换参数
     * @return null（如果 failOnError 为 false）
     * @throws RuntimeException 如果 failOnError 为 true
     */
    public static Object invalidCast(Variant v, DataType dataType, VariantCastArgs castArgs) {
        if (castArgs.failOnError()) {
            throw new RuntimeException(
                    "Invalid cast " + v.toJson(castArgs.zoneId()) + " to " + dataType);
        } else {
            return null;
        }
    }
}
