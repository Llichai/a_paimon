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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;

/* This file is based on source code from the Spark Project (http://spark.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Variant切片写入器。
 *
 * <p>该类实现将Variant值切片(Shredding)为列式存储格式的功能。切片是将半结构化的Variant数据
 * 分解为结构化的列式数据,以提高查询性能和压缩率。
 *
 * <h3>核心功能:</h3>
 * <ul>
 *   <li><b>Variant切片</b> - 将Variant值按照VariantSchema分解为typed_value和value
 *   <li><b>类型转换和验证</b> - 验证Variant类型是否可以安全转换为目标Schema类型
 *   <li><b>数值类型灵活转换</b> - 支持Decimal和整数之间的数值等价转换
 *   <li><b>递归切片</b> - 递归处理嵌套的对象和数组结构
 * </ul>
 *
 * <h3>切片算法:</h3>
 * <p>切片遵循 <a href="https://github.com/apache/parquet-format/blob/master/VariantShredding.md">
 * Parquet Variant Shredding规范</a>:
 *
 * <pre>
 * 1. 标量类型切片:
 *    - 如果Variant类型与Schema类型匹配: 写入typed_value
 *    - 如果不匹配: 写入value (保持原始Variant格式)
 *    - 特殊规则: 支持数值类型的等价转换(见下文)
 *
 * 2. 数组类型切片:
 *    - 如果Variant是数组: 递归切片每个元素
 *    - 如果不是数组: 写入value
 *
 * 3. 对象类型切片:
 *    - 遍历Variant对象的每个字段:
 *      a) 字段在Schema中: 递归切片,写入typed_value
 *      b) 字段不在Schema中: 写入value (未切片字段)
 *    - Schema中缺失的字段: 创建空的子结果(所有字段为null)
 *    - 验证Variant对象不包含重复字段
 * </pre>
 *
 * <h3>数值类型转换规则:</h3>
 * <p>当allowNumericScaleChanges()为true时,支持以下数值等价转换:
 *
 * <pre>
 * Long → Integral:
 *   - Long值必须在目标类型范围内
 *   - BYTE: [-128, 127]
 *   - SHORT: [-32768, 32767]
 *   - INT: [-2147483648, 2147483647]
 *   - LONG: 任意64位整数
 *
 * Long → Decimal:
 *   - 转换为目标scale的Decimal
 *   - 精度必须足够容纳转换后的值
 *
 * Decimal → Decimal:
 *   - 如果精度和scale完全匹配: 直接使用
 *   - 如果等价转换(rescale)不丢失信息: 使用转换后的值
 *
 * Decimal → Integral:
 *   - Decimal必须是整数(无小数部分)
 *   - 整数值必须在目标类型范围内
 * </pre>
 *
 * <h3>使用示例:</h3>
 * <pre>{@code
 * // 定义切片Schema
 * VariantSchema schema = VariantSchema.of(
 *     VariantSchema.ObjectField.of("name", VariantSchema.scalarString()),
 *     VariantSchema.ObjectField.of("age", VariantSchema.scalarLong())
 * );
 *
 * // 创建Variant值
 * GenericVariant variant = GenericVariant.fromJson(
 *     "{\"name\":\"Alice\",\"age\":30,\"city\":\"NYC\"}"
 * );
 *
 * // 执行切片
 * PaimonShreddedResultBuilder builder = new PaimonShreddedResultBuilder();
 * ShreddedResult result = VariantShreddingWriter.castShredded(
 *     variant,
 *     schema,
 *     builder
 * );
 *
 * // 结果结构:
 * // typed_value: {
 * //   name: {typed_value: "Alice", value: null},
 * //   age:  {typed_value: 30, value: null}
 * // }
 * // value: {"city": "NYC"}  // 未切片字段
 * }</pre>
 *
 * <h3>切片结果结构:</h3>
 * <pre>
 * 顶层结构:
 * ┌──────────────┬─────────────┬──────────────────┐
 * │ metadata     │ value       │ typed_value      │
 * ├──────────────┼─────────────┼──────────────────┤
 * │ 元数据字典   │ 未切片数据  │ 切片后的结构化数据│
 * └──────────────┴─────────────┴──────────────────┘
 *
 * 对象字段结构:
 * 每个字段: {value: BYTES, typed_value: T}
 * - 如果类型匹配: typed_value非null, value为null
 * - 如果类型不匹配: typed_value为null, value非null
 *
 * 数组元素结构:
 * 每个元素: {value: BYTES, typed_value: T}
 * - 递归应用切片规则
 * </pre>
 *
 * <h3>数据验证:</h3>
 * <p>切片过程会进行以下验证:
 * <ul>
 *   <li>Variant对象不能包含重复字段
 *   <li>Schema中的所有必需字段都必须被处理(即使为null)
 *   <li>类型转换必须安全(不丢失精度,不溢出)
 * </ul>
 *
 * <h3>性能优化:</h3>
 * <ul>
 *   <li><b>浅拷贝优化</b> - 未切片字段使用shallowAppendVariant避免metadata复制
 *   <li><b>字段重用</b> - 对象字段数组预分配,减少内存分配
 *   <li><b>增量构建</b> - 使用GenericVariantBuilder增量构建未切片部分
 * </ul>
 *
 * <h3>线程安全:</h3>
 * 该类的方法是静态的且无状态,是线程安全的。但ShreddedResultBuilder和ShreddedResult
 * 的线程安全性由实现类决定。
 *
 * @see VariantSchema
 * @see ShreddingUtils
 * @see GenericVariantBuilder
 * @see PaimonShreddingUtils.PaimonShreddedResult
 */
public class VariantShreddingWriter {

    /**
     * 切片结果接口。
     *
     * <p>调用方应实现ShreddedResultBuilder来创建具有给定Schema的空结果。
     * castShredded方法将调用一个或多个add*方法来填充它。
     *
     * <p>实现类应提供特定存储后端(如Paimon、Spark等)的结果构建逻辑。
     *
     * @see PaimonShreddingUtils.PaimonShreddedResult
     */
    public interface ShreddedResult {

        /**
         * 创建数组结果。
         *
         * @param array 元素数组,每个元素是切片后的结果
         */
        void addArray(ShreddedResult[] array);

        /**
         * 创建对象结果。
         *
         * @param values 字段值数组,按objectSchema中的索引顺序排列,缺失字段用空结果填充
         */
        void addObject(ShreddedResult[] values);

        /**
         * 添加未切片的Variant值。
         *
         * @param result Variant二进制数据(不含metadata)
         */
        void addVariantValue(byte[] result);

        /**
         * 添加标量值到typed_value。
         *
         * <p>Object的具体类型取决于Schema中的scalarSchema:
         * <ul>
         *   <li>StringType → String
         *   <li>IntegralType → Byte/Short/Integer/Long
         *   <li>FloatType → Float
         *   <li>DoubleType → Double
         *   <li>BooleanType → Boolean
         *   <li>BinaryType → byte[]
         *   <li>UuidType → UUID
         *   <li>DecimalType → BigDecimal
         *   <li>DateType → Integer (days since epoch)
         *   <li>TimestampType → Long (microseconds since epoch)
         *   <li>TimestampNTZType → Long (microseconds since epoch, no timezone)
         * </ul>
         *
         * @param result 标量值对象
         */
        void addScalar(Object result);

        /**
         * 添加顶层metadata。
         *
         * @param result metadata二进制数据
         */
        void addMetadata(byte[] result);
    }

    /**
     * 切片结果构建器接口。
     *
     * <p>提供创建空结果和配置切片行为的方法。
     */
    public interface ShreddedResultBuilder {
        /**
         * 创建具有给定Schema的空切片结果。
         *
         * @param schema 切片Schema
         * @return 空的切片结果对象
         */
        ShreddedResult createEmpty(VariantSchema schema);

        /**
         * 是否允许数值类型的scale变化。
         *
         * <p>如果为true,我们将把Decimal切片为不同scale或整数,只要它们在数值上等价。
         * 类似地,整数也允许切片为Decimal。
         *
         * @return true表示允许数值类型灵活转换
         */
        boolean allowNumericScaleChanges();
    }

    /**
     * 将Variant值转换为切片组件。
     *
     * <p>返回切片结果,以及移除切片字段后的原始Variant。dataType必须是有效的切片Schema,
     * 如<a href="https://github.com/apache/parquet-format/blob/master/VariantShredding.md">
     * Parquet Variant Shredding规范</a>中所述。
     *
     * <h4>切片逻辑:</h4>
     * <pre>
     * 1. 顶层metadata:
     *    如果schema.topLevelMetadataIdx >= 0,添加Variant的metadata
     *
     * 2. 数组切片:
     *    - 如果variantType是ARRAY且schema有arraySchema:
     *      递归切片每个元素
     *    - 否则: 存储到untyped value
     *
     * 3. 对象切片:
     *    - 如果variantType是OBJECT且schema有objectSchema:
     *      a) 遍历Variant对象的每个字段:
     *         - 字段在schema中: 递归切片,存储到typed_value
     *         - 字段不在schema中: 存储到value (未切片字段)
     *      b) 填充schema中缺失的字段为空结果
     *      c) 验证不存在重复字段
     *    - 否则: 存储到untyped value
     *
     * 4. 标量切片:
     *    - 如果schema有scalarSchema:
     *      尝试类型转换,成功则存储到typed_value,失败则存储到value
     *    - 否则: 存储到untyped value
     * </pre>
     *
     * @param v Variant值
     * @param schema 切片Schema
     * @param builder 结果构建器
     * @return 切片后的结果
     * @throws RuntimeException 如果Variant包含重复字段或其他数据错误
     */
    public static ShreddedResult castShredded(
            GenericVariant v, VariantSchema schema, ShreddedResultBuilder builder) {
        GenericVariantUtil.Type variantType = v.getType();
        ShreddedResult result = builder.createEmpty(schema);

        if (schema.topLevelMetadataIdx >= 0) {
            result.addMetadata(v.metadata());
        }

        if (schema.arraySchema != null && variantType == GenericVariantUtil.Type.ARRAY) {
            // The array element is always a struct containing untyped and typed fields.
            VariantSchema elementSchema = schema.arraySchema;
            int size = v.arraySize();
            ShreddedResult[] array = new ShreddedResult[size];
            for (int i = 0; i < size; ++i) {
                ShreddedResult shreddedArray =
                        castShredded(v.getElementAtIndex(i), elementSchema, builder);
                array[i] = shreddedArray;
            }
            result.addArray(array);
        } else if (schema.objectSchema != null && variantType == GenericVariantUtil.Type.OBJECT) {
            VariantSchema.ObjectField[] objectSchema = schema.objectSchema;
            ShreddedResult[] shreddedValues = new ShreddedResult[objectSchema.length];

            // Create a variantBuilder for any field that exist in `v`, but not in the
            // shredding schema.
            GenericVariantBuilder variantBuilder = new GenericVariantBuilder(false);
            ArrayList<GenericVariantBuilder.FieldEntry> fieldEntries = new ArrayList<>();
            // Keep track of which schema fields we actually found in the Variant value.
            int numFieldsMatched = 0;
            int start = variantBuilder.getWritePos();
            for (int i = 0; i < v.objectSize(); ++i) {
                GenericVariant.ObjectField field = v.getFieldAtIndex(i);
                Integer fieldIdx = schema.objectSchemaMap.get(field.key);
                if (fieldIdx != null) {
                    // The field exists in the shredding schema. Recursively shred, and write the
                    // result.
                    ShreddedResult shreddedField =
                            castShredded(field.value, objectSchema[fieldIdx].schema, builder);
                    shreddedValues[fieldIdx] = shreddedField;
                    numFieldsMatched++;
                } else {
                    // The field is not shredded. Put it in the untyped_value column.
                    int id = v.getDictionaryIdAtIndex(i);
                    fieldEntries.add(
                            new GenericVariantBuilder.FieldEntry(
                                    field.key, id, variantBuilder.getWritePos() - start));
                    // shallowAppendVariant is needed for correctness, since we're relying on the
                    // metadata IDs being unchanged.
                    variantBuilder.shallowAppendVariant(field.value);
                }
            }
            if (numFieldsMatched < objectSchema.length) {
                // Set missing fields to non-null with all fields set to null.
                for (int i = 0; i < objectSchema.length; ++i) {
                    if (shreddedValues[i] == null) {
                        VariantSchema.ObjectField fieldSchema = objectSchema[i];
                        ShreddedResult emptyChild = builder.createEmpty(fieldSchema.schema);
                        shreddedValues[i] = emptyChild;
                        numFieldsMatched += 1;
                    }
                }
            }
            if (numFieldsMatched != objectSchema.length) {
                // Since we just filled in all the null entries, this can only happen if we tried to
                // write to the same field twice; i.e. the Variant contained duplicate fields, which
                // is invalid.
                throw new RuntimeException();
            }
            result.addObject(shreddedValues);
            if (variantBuilder.getWritePos() != start) {
                // We added something to the untyped value.
                variantBuilder.finishWritingObject(start, fieldEntries);
                result.addVariantValue(variantBuilder.valueWithoutMetadata());
            }
        } else if (schema.scalarSchema != null) {
            VariantSchema.ScalarType scalarType = schema.scalarSchema;
            Object typedValue = tryTypedShred(v, variantType, scalarType, builder);
            if (typedValue != null) {
                // Store the typed value.
                result.addScalar(typedValue);
            } else {
                result.addVariantValue(v.value());
            }
        } else {
            // Store in untyped.
            result.addVariantValue(v.value());
        }
        return result;
    }

    /**
     * 尝试将Variant转换为类型化值。
     *
     * <p>该方法尝试将Variant值安全地转换为目标标量类型。如果转换失败,返回null,
     * 调用方将把值存储到untyped value中。
     *
     * <h4>转换规则:</h4>
     * <pre>
     * LONG → IntegralType:
     *   - 检查值是否在目标类型范围内
     *   - BYTE: [-128, 127]
     *   - SHORT: [-32768, 32767]
     *   - INT: [-2147483648, 2147483647]
     *   - LONG: 任意64位整数
     *
     * LONG → DecimalType (allowNumericScaleChanges=true):
     *   - 设置为目标scale
     *   - 检查精度是否足够
     *
     * DECIMAL → DecimalType:
     *   - 精度和scale完全匹配: 直接使用
     *   - allowNumericScaleChanges=true: 尝试rescale,不丢失信息则使用
     *
     * DECIMAL → IntegralType (allowNumericScaleChanges=true):
     *   - 检查是否为整数(无小数部分)
     *   - 检查值是否在目标类型范围内
     *
     * 其他类型:
     *   - 精确类型匹配时转换
     *   - 不匹配则返回null
     * </pre>
     *
     * @param v Variant值
     * @param variantType Variant的类型
     * @param targetType 目标标量类型
     * @param builder 结果构建器,用于检查allowNumericScaleChanges
     * @return 标量值对象,如果转换无效则返回null
     */
    private static Object tryTypedShred(
            GenericVariant v,
            GenericVariantUtil.Type variantType,
            VariantSchema.ScalarType targetType,
            ShreddedResultBuilder builder) {
        switch (variantType) {
            case LONG:
                if (targetType instanceof VariantSchema.IntegralType) {
                    // Check that the target type can hold the actual value.
                    VariantSchema.IntegralType integralType =
                            (VariantSchema.IntegralType) targetType;
                    VariantSchema.IntegralSize size = integralType.size;
                    long value = v.getLong();
                    switch (size) {
                        case BYTE:
                            if (value == (byte) value) {
                                return (byte) value;
                            }
                            break;
                        case SHORT:
                            if (value == (short) value) {
                                return (short) value;
                            }
                            break;
                        case INT:
                            if (value == (int) value) {
                                return (int) value;
                            }
                            break;
                        case LONG:
                            return value;
                    }
                } else if (targetType instanceof VariantSchema.DecimalType
                        && builder.allowNumericScaleChanges()) {
                    VariantSchema.DecimalType decimalType = (VariantSchema.DecimalType) targetType;
                    // If the integer can fit in the given decimal precision, allow it.
                    long value = v.getLong();
                    // Set to the requested scale, and check if the precision is large enough.
                    BigDecimal decimalValue = BigDecimal.valueOf(value);
                    BigDecimal scaledValue = decimalValue.setScale(decimalType.scale);
                    // The initial value should have scale 0, so rescaling shouldn't lose
                    // information.
                    assert (decimalValue.compareTo(scaledValue) == 0);
                    if (scaledValue.precision() <= decimalType.precision) {
                        return scaledValue;
                    }
                }
                break;
            case DECIMAL:
                if (targetType instanceof VariantSchema.DecimalType) {
                    VariantSchema.DecimalType decimalType = (VariantSchema.DecimalType) targetType;
                    // Use getDecimalWithOriginalScale so that we retain scale information if
                    // allowNumericScaleChanges() is false.
                    BigDecimal value =
                            GenericVariantUtil.getDecimalWithOriginalScale(v.rawValue(), v.pos());
                    if (value.precision() <= decimalType.precision
                            && value.scale() == decimalType.scale) {
                        return value;
                    }
                    if (builder.allowNumericScaleChanges()) {
                        // Convert to the target scale, and see if it fits. Rounding mode doesn't
                        // matter, since we'll reject it if it turned out to require rounding.
                        BigDecimal scaledValue =
                                value.setScale(decimalType.scale, RoundingMode.FLOOR);
                        if (scaledValue.compareTo(value) == 0
                                && scaledValue.precision() <= decimalType.precision) {
                            return scaledValue;
                        }
                    }
                } else if (targetType instanceof VariantSchema.IntegralType
                        && builder.allowNumericScaleChanges()) {
                    VariantSchema.IntegralType integralType =
                            (VariantSchema.IntegralType) targetType;
                    // Check if the decimal happens to be an integer.
                    BigDecimal value = v.getDecimal();
                    VariantSchema.IntegralSize size = integralType.size;
                    // Try to cast to the appropriate type, and check if any information is lost.
                    switch (size) {
                        case BYTE:
                            if (value.compareTo(BigDecimal.valueOf(value.byteValue())) == 0) {
                                return value.byteValue();
                            }
                            break;
                        case SHORT:
                            if (value.compareTo(BigDecimal.valueOf(value.shortValue())) == 0) {
                                return value.shortValue();
                            }
                            break;
                        case INT:
                            if (value.compareTo(BigDecimal.valueOf(value.intValue())) == 0) {
                                return value.intValue();
                            }
                            break;
                        case LONG:
                            if (value.compareTo(BigDecimal.valueOf(value.longValue())) == 0) {
                                return value.longValue();
                            }
                    }
                }
                break;
            case BOOLEAN:
                if (targetType instanceof VariantSchema.BooleanType) {
                    return v.getBoolean();
                }
                break;
            case STRING:
                if (targetType instanceof VariantSchema.StringType) {
                    return v.getString();
                }
                break;
            case DOUBLE:
                if (targetType instanceof VariantSchema.DoubleType) {
                    return v.getDouble();
                }
                break;
            case DATE:
                if (targetType instanceof VariantSchema.DateType) {
                    return (int) v.getLong();
                }
                break;
            case TIMESTAMP:
                if (targetType instanceof VariantSchema.TimestampType) {
                    return v.getLong();
                }
                break;
            case TIMESTAMP_NTZ:
                if (targetType instanceof VariantSchema.TimestampNTZType) {
                    return v.getLong();
                }
                break;
            case FLOAT:
                if (targetType instanceof VariantSchema.FloatType) {
                    return v.getFloat();
                }
                break;
            case BINARY:
                if (targetType instanceof VariantSchema.BinaryType) {
                    return v.getBinary();
                }
                break;
            case UUID:
                if (targetType instanceof VariantSchema.UuidType) {
                    return v.getUuid();
                }
                break;
        }
        // The stored type does not match the requested shredding type. Return null, and the caller
        // will store the result in untyped_value.
        return null;
    }

    /**
     * 将Variant值添加到切片结果中。
     *
     * <p>注意: 该方法当前未使用,保留供将来扩展。
     *
     * @param variantResult Variant结果
     * @param schema 切片Schema
     * @param result 切片结果对象
     */
    private static void addVariantValueVariant(
            Variant variantResult, VariantSchema schema, ShreddedResult result) {
        result.addVariantValue(variantResult.value());
    }
}
