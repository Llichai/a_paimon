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
import java.util.ArrayList;
import java.util.UUID;

import static org.apache.paimon.data.variant.GenericVariantUtil.malformedVariant;

/* This file is based on source code from the Spark Project (http://spark.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Variant切片工具类。
 *
 * <p>该类提供通用的Variant切片(Shredding)和重建(Rebuild)功能。切片是将半结构化的Variant数据
 * 转换为列式存储格式的过程,重建则是反向过程,从切片后的列式数据恢复原始Variant值。
 *
 * <h3>核心功能:</h3>
 * <ul>
 *   <li><b>Variant重建</b> - 从切片后的列式数据重建Variant二进制格式
 *   <li><b>ShreddedRow接口</b> - 抽象的切片数据读取接口,支持多种存储后端
 *   <li><b>递归重建算法</b> - 根据VariantSchema递归重建对象、数组和标量值
 * </ul>
 *
 * <h3>重建算法 (Rebuild Algorithm):</h3>
 * <p>重建算法遵循 <a href="https://github.com/apache/parquet-format/blob/master/VariantShredding.md">
 * Parquet Variant Shredding规范</a>中定义的重建规则:
 *
 * <pre>
 * 1. 如果typed_value非null:
 *    - 标量类型: 从typed_value读取并写入Variant
 *    - 数组类型: 递归重建每个元素
 *    - 对象类型: 递归重建每个字段,然后添加value中的剩余字段
 *
 * 2. 如果typed_value为null且value非null:
 *    - 直接使用value中的Variant二进制数据
 *
 * 3. 如果typed_value和value都为null:
 *    - 抛出MALFORMED_VARIANT异常(数据损坏)
 * </pre>
 *
 * <h3>对象重建规则:</h3>
 * <pre>
 * 对于对象类型的重建:
 * 1. 遍历typed_value中的所有字段
 *    - 字段本身不能为null(必须存在)
 *    - 如果字段的typed_value或value非null,则该字段存在,递归重建
 *    - 如果字段的typed_value和value都为null,则该字段缺失(不写入结果)
 *
 * 2. 添加value中的剩余字段
 *    - value中的字段必须不与typed_value重复
 *    - 如果重复则抛出MALFORMED_VARIANT异常
 *
 * 3. 所有字段按字母顺序排序(Variant规范要求)
 * </pre>
 *
 * <h3>数组重建规则:</h3>
 * <pre>
 * 对于数组类型的重建:
 * 1. 遍历typed_value数组的每个元素
 * 2. 每个元素必须非null(数组元素不能为null)
 * 3. 递归重建每个元素
 * 4. 记录每个元素在Variant二进制中的偏移量
 * </pre>
 *
 * <h3>标量重建规则:</h3>
 * <pre>
 * 对于标量类型的重建:
 * 1. 从typed_value读取对应的Paimon类型
 * 2. 转换为Variant二进制格式:
 *    - String → Variant字符串
 *    - Integral (BYTE/SHORT/INT/LONG) → Variant长整数
 *    - Float → Variant浮点数
 *    - Double → Variant双精度浮点数
 *    - Boolean → Variant布尔值
 *    - Binary → Variant二进制数据
 *    - UUID → Variant UUID
 *    - Decimal → Variant高精度小数
 *    - Date → Variant日期
 *    - Timestamp → Variant时间戳(带时区)
 *    - TimestampNTZ → Variant时间戳(不带时区)
 * </pre>
 *
 * <h3>使用示例:</h3>
 * <pre>{@code
 * // 假设我们有切片后的数据行
 * InternalRow shreddedRow = ...;
 * VariantSchema schema = ...;
 *
 * // 重建Variant
 * Variant rebuilt = ShreddingUtils.rebuild(
 *     new PaimonShreddedRow(shreddedRow),
 *     schema
 * );
 *
 * // 使用重建的Variant
 * String json = rebuilt.toJson(ZoneId.systemDefault());
 * System.out.println(json);  // {"name": "Alice", "age": 30}
 * }</pre>
 *
 * <h3>数据完整性检查:</h3>
 * <p>重建过程会进行多项数据完整性检查:
 * <ul>
 *   <li>顶层metadata必须存在且非null
 *   <li>对象的切片字段(typed_value中的字段)必须非null
 *   <li>数组元素必须非null
 *   <li>value中的对象字段不能与typed_value重复
 *   <li>value必须是Object类型(当重建Object时)
 *   <li>如果typed_value和value都为null,数据损坏
 * </ul>
 *
 * <h3>性能考虑:</h3>
 * <ul>
 *   <li><b>零拷贝优化</b> - 对于未切片的Variant,直接返回原始二进制数据
 *   <li><b>增量构建</b> - 使用GenericVariantBuilder增量构建Variant,避免多次拷贝
 *   <li><b>递归深度</b> - 深度嵌套结构可能影响性能,建议限制maxSchemaDepth
 * </ul>
 *
 * <h3>线程安全:</h3>
 * 该类的所有方法都是静态的且无状态,是线程安全的。但传入的ShreddedRow和VariantSchema
 * 本身的线程安全性由调用方保证。
 *
 * @see VariantSchema
 * @see VariantShreddingWriter
 * @see GenericVariantBuilder
 * @see PaimonShreddingUtils.PaimonShreddedRow
 */
public class ShreddingUtils {

    /**
     * 切片数据行读取接口。
     *
     * <p>该接口提供从切片结果中读取数据的抽象,本质上与Spark的SpecializedGetters接口相同,
     * 但为了避免依赖而定义了新接口。
     *
     * <p>实现类应提供从特定存储后端(如Paimon的InternalRow、Parquet的ColumnVector等)
     * 读取切片数据的能力。
     *
     * <h4>使用场景:</h4>
     * <ul>
     *   <li>从列式存储读取切片后的Variant数据
     *   <li>支持多种存储后端的统一读取接口
     *   <li>在重建Variant时提供数据访问
     * </ul>
     *
     * @see PaimonShreddingUtils.PaimonShreddedRow
     */
    public interface ShreddedRow {

        /** 检查指定位置的值是否为null。 */
        boolean isNullAt(int ordinal);

        /** 读取Boolean值。 */
        boolean getBoolean(int ordinal);

        /** 读取Byte值。 */
        byte getByte(int ordinal);

        /** 读取Short值。 */
        short getShort(int ordinal);

        /** 读取Int值。 */
        int getInt(int ordinal);

        /** 读取Long值。 */
        long getLong(int ordinal);

        /** 读取Float值。 */
        float getFloat(int ordinal);

        /** 读取Double值。 */
        double getDouble(int ordinal);

        /**
         * 读取Decimal值。
         *
         * @param ordinal 字段位置
         * @param precision 精度
         * @param scale 小数位数
         * @return BigDecimal值
         */
        BigDecimal getDecimal(int ordinal, int precision, int scale);

        /** 读取String值。 */
        String getString(int ordinal);

        /** 读取Binary值。 */
        byte[] getBinary(int ordinal);

        /** 读取UUID值。 */
        UUID getUuid(int ordinal);

        /**
         * 读取Struct值。
         *
         * @param ordinal 字段位置
         * @param numFields 字段数量
         * @return ShreddedRow表示的结构体
         */
        ShreddedRow getStruct(int ordinal, int numFields);

        /**
         * 读取Array值。
         *
         * @param ordinal 字段位置
         * @return ShreddedRow表示的数组
         */
        ShreddedRow getArray(int ordinal);

        /**
         * 获取数组的元素数量。
         *
         * <p>只有在getArray返回的ShreddedRow上调用才有意义。
         *
         * @return 数组元素数量
         */
        int numElements();
    }

    /**
     * 从切片数据重建Variant。
     *
     * <p>该方法只应在顶层Schema上调用,内部会调用私有的递归实现处理嵌套的子Schema。
     *
     * <p>对于未切片的Variant(isUnshredded()为true),直接返回原始二进制数据,无需重建。
     *
     * @param row 切片数据行
     * @param schema 切片Schema
     * @return 重建的Variant
     * @throws RuntimeException 如果topLevelMetadataIdx < 0或metadata为null(数据损坏)
     */
    public static Variant rebuild(ShreddedRow row, VariantSchema schema) {
        if (schema.topLevelMetadataIdx < 0 || row.isNullAt(schema.topLevelMetadataIdx)) {
            throw malformedVariant();
        }
        byte[] metadata = row.getBinary(schema.topLevelMetadataIdx);
        if (schema.isUnshredded()) {
            // `rebuild` is unnecessary for unshredded variant.
            if (row.isNullAt(schema.variantIdx)) {
                throw malformedVariant();
            }
            return new GenericVariant(row.getBinary(schema.variantIdx), metadata);
        }
        GenericVariantBuilder builder = new GenericVariantBuilder(false);
        rebuild(row, metadata, schema, builder);
        return builder.result();
    }

    /**
     * 从切片数据重建Variant值的递归实现。
     *
     * <p>该方法根据重建算法(<a href="https://github.com/apache/parquet-format/blob/master/VariantShredding.md">
     * Parquet Variant Shredding规范</a>)从切片数据重建Variant,并将结果追加到builder中。
     *
     * <h4>重建逻辑:</h4>
     * <pre>
     * 1. 如果typed_value非null:
     *    a) 标量Schema: 从typed_value读取标量值,追加到builder
     *    b) 数组Schema: 递归重建每个元素,构建Variant数组
     *    c) 对象Schema:
     *       - 递归重建typed_value中的每个字段
     *       - 添加value中的剩余字段(不在typed_value中的字段)
     *       - 验证value中的字段不与typed_value重复
     *
     * 2. 如果typed_value为null且value非null:
     *    直接将value中的Variant二进制数据追加到builder
     *
     * 3. 如果typed_value和value都为null:
     *    抛出MALFORMED_VARIANT异常
     * </pre>
     *
     * @param row 切片数据行
     * @param metadata 顶层metadata(所有层级共享)
     * @param schema 当前层级的VariantSchema
     * @param builder 用于构建Variant的builder
     * @throws RuntimeException 如果数据损坏(字段缺失、重复字段、类型不匹配等)
     */
    public static void rebuild(
            ShreddedRow row, byte[] metadata, VariantSchema schema, GenericVariantBuilder builder) {
        int typedIdx = schema.typedIdx;
        int variantIdx = schema.variantIdx;
        if (typedIdx >= 0 && !row.isNullAt(typedIdx)) {
            if (schema.scalarSchema != null) {
                VariantSchema.ScalarType scalar = schema.scalarSchema;
                if (scalar instanceof VariantSchema.StringType) {
                    builder.appendString(row.getString(typedIdx));
                } else if (scalar instanceof VariantSchema.IntegralType) {
                    VariantSchema.IntegralType it = (VariantSchema.IntegralType) scalar;
                    long value = 0;
                    switch (it.size) {
                        case BYTE:
                            value = row.getByte(typedIdx);
                            break;
                        case SHORT:
                            value = row.getShort(typedIdx);
                            break;
                        case INT:
                            value = row.getInt(typedIdx);
                            break;
                        case LONG:
                            value = row.getLong(typedIdx);
                            break;
                    }
                    builder.appendLong(value);
                } else if (scalar instanceof VariantSchema.FloatType) {
                    builder.appendFloat(row.getFloat(typedIdx));
                } else if (scalar instanceof VariantSchema.DoubleType) {
                    builder.appendDouble(row.getDouble(typedIdx));
                } else if (scalar instanceof VariantSchema.BooleanType) {
                    builder.appendBoolean(row.getBoolean(typedIdx));
                } else if (scalar instanceof VariantSchema.BinaryType) {
                    builder.appendBinary(row.getBinary(typedIdx));
                } else if (scalar instanceof VariantSchema.UuidType) {
                    builder.appendUuid(row.getUuid(typedIdx));
                } else if (scalar instanceof VariantSchema.DecimalType) {
                    VariantSchema.DecimalType dt = (VariantSchema.DecimalType) scalar;
                    builder.appendDecimal(row.getDecimal(typedIdx, dt.precision, dt.scale));
                } else if (scalar instanceof VariantSchema.DateType) {
                    builder.appendDate(row.getInt(typedIdx));
                } else if (scalar instanceof VariantSchema.TimestampType) {
                    builder.appendTimestamp(row.getLong(typedIdx));
                } else {
                    assert scalar instanceof VariantSchema.TimestampNTZType;
                    builder.appendTimestampNtz(row.getLong(typedIdx));
                }
            } else if (schema.arraySchema != null) {
                VariantSchema elementSchema = schema.arraySchema;
                ShreddedRow array = row.getArray(typedIdx);
                int start = builder.getWritePos();
                ArrayList<Integer> offsets = new ArrayList<>(array.numElements());
                for (int i = 0; i < array.numElements(); i++) {
                    offsets.add(builder.getWritePos() - start);
                    rebuild(
                            array.getStruct(i, elementSchema.numFields),
                            metadata,
                            elementSchema,
                            builder);
                }
                builder.finishWritingArray(start, offsets);
            } else {
                ShreddedRow object = row.getStruct(typedIdx, schema.objectSchema.length);
                ArrayList<GenericVariantBuilder.FieldEntry> fields = new ArrayList<>();
                int start = builder.getWritePos();
                for (int fieldIdx = 0; fieldIdx < schema.objectSchema.length; ++fieldIdx) {
                    // Shredded field must not be null.
                    if (object.isNullAt(fieldIdx)) {
                        throw malformedVariant();
                    }
                    String fieldName = schema.objectSchema[fieldIdx].fieldName;
                    VariantSchema fieldSchema = schema.objectSchema[fieldIdx].schema;
                    ShreddedRow fieldValue = object.getStruct(fieldIdx, fieldSchema.numFields);
                    // If the field doesn't have non-null `typed_value` or `value`, it is missing.
                    if ((fieldSchema.typedIdx >= 0 && !fieldValue.isNullAt(fieldSchema.typedIdx))
                            || (fieldSchema.variantIdx >= 0
                                    && !fieldValue.isNullAt(fieldSchema.variantIdx))) {
                        int id = builder.addKey(fieldName);
                        fields.add(
                                new GenericVariantBuilder.FieldEntry(
                                        fieldName, id, builder.getWritePos() - start));
                        rebuild(fieldValue, metadata, fieldSchema, builder);
                    }
                }
                if (variantIdx >= 0 && !row.isNullAt(variantIdx)) {
                    // Add the leftover fields in the variant binary.
                    GenericVariant v = new GenericVariant(row.getBinary(variantIdx), metadata);
                    if (v.getType() != GenericVariantUtil.Type.OBJECT) {
                        throw malformedVariant();
                    }
                    for (int i = 0; i < v.objectSize(); ++i) {
                        GenericVariant.ObjectField field = v.getFieldAtIndex(i);
                        // `value` must not contain any shredded field.
                        if (schema.objectSchemaMap.containsKey(field.key)) {
                            throw malformedVariant();
                        }
                        int id = builder.addKey(field.key);
                        fields.add(
                                new GenericVariantBuilder.FieldEntry(
                                        field.key, id, builder.getWritePos() - start));
                        builder.appendVariant(field.value);
                    }
                }
                builder.finishWritingObject(start, fields);
            }
        } else if (variantIdx >= 0 && !row.isNullAt(variantIdx)) {
            // `typed_value` doesn't exist or is null. Read from `value`.
            builder.appendVariant(new GenericVariant(row.getBinary(variantIdx), metadata));
        } else {
            // This means the variant is missing in a context where it must present, so the input
            // data is invalid.
            throw malformedVariant();
        }
    }
}
