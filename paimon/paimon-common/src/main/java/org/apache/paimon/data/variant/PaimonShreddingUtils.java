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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.RowToColumnConverter;
import org.apache.paimon.data.columnar.heap.CastedRowColumnVector;
import org.apache.paimon.data.columnar.writable.WritableBytesVector;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.data.variant.VariantPathSegment.ArrayExtraction;
import org.apache.paimon.data.variant.VariantPathSegment.ObjectExtraction;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarBinaryType;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.paimon.data.variant.GenericVariantUtil.Type.ARRAY;
import static org.apache.paimon.data.variant.GenericVariantUtil.Type.OBJECT;
import static org.apache.paimon.data.variant.GenericVariantUtil.malformedVariant;

/**
 * Paimon专用的Variant切片工具类。
 *
 * <p>该类提供Paimon特定的Variant切片(Shredding)实现,包括切片Schema构建、数据转换、
 * 字段提取和批量处理等功能。与通用的{@link ShreddingUtils}不同,该类专门适配Paimon的
 * 数据类型系统和列式向量存储。
 *
 * <h3>核心功能:</h3>
 * <ul>
 *   <li><b>切片Schema构建</b> - 从Paimon DataType生成完整的VariantSchema
 *   <li><b>类型转换</b> - Paimon类型与Variant标量类型之间的双向转换
 *   <li><b>数据切片和重建</b> - 切片Variant为列式存储,重建列式数据为Variant
 *   <li><b>字段提取</b> - 根据路径从切片数据中提取指定字段
 *   <li><b>批量处理</b> - 批量切片和重建,适配列式向量
 * </ul>
 *
 * <h3>切片Schema结构:</h3>
 * <p>Paimon的切片Schema包含三个核心字段:
 * <pre>
 * 顶层结构:
 * ┌────────────┬─────────────┬──────────────┐
 * │ metadata   │ value       │ typed_value  │
 * ├────────────┼─────────────┼──────────────┤
 * │ BYTES      │ BYTES       │ T (根据类型) │
 * │ (非null)   │ (可为null)  │ (可为null)   │
 * └────────────┴─────────────┴──────────────┘
 *
 * metadata: Variant元数据字典(字符串、时区等)
 * value: 未切片的Variant二进制数据
 * typed_value: 切片后的结构化数据
 *
 * 对于不同类型的typed_value:
 * - 标量: 直接存储值 (INT, STRING, DECIMAL等)
 * - 数组: ARRAY<ROW<metadata: null, value: BYTES, typed_value: T>>
 * - 对象: ROW<field1: ROW<...>, field2: ROW<...>, ...>
 *   每个字段又是三元组(metadata为null, value, typed_value)
 * </pre>
 *
 * <h3>Schema构建示例:</h3>
 * <pre>{@code
 * // 输入: 简单的对象Schema
 * DataType inputType = RowType.of(
 *     new DataField(0, "name", DataTypes.STRING()),
 *     new DataField(1, "age", DataTypes.INT())
 * );
 *
 * // 输出: 完整的切片Schema
 * RowType shreddingSchema = PaimonShreddingUtils.variantShreddingSchema(inputType);
 * // 结构:
 * // ROW<
 * //   metadata: BYTES NOT NULL,
 * //   value: BYTES,
 * //   typed_value: ROW<
 * //     name: ROW<value: BYTES, typed_value: STRING> NOT NULL,
 * //     age:  ROW<value: BYTES, typed_value: INT> NOT NULL
 * //   >
 * // >
 * }</pre>
 *
 * <h3>切片和重建流程:</h3>
 * <pre>
 * 切片 (Variant → 列式存储):
 * 1. 解析Variant值的类型
 * 2. 根据VariantSchema匹配字段
 * 3. 类型匹配的字段写入typed_value
 * 4. 不匹配的字段写入value
 * 5. 递归处理嵌套结构
 *
 * 重建 (列式存储 → Variant):
 * 1. 从typed_value读取结构化数据
 * 2. 从value读取未切片数据
 * 3. 合并为完整的Variant二进制格式
 * 4. 递归处理嵌套结构
 * </pre>
 *
 * <h3>字段提取功能:</h3>
 * <p>支持从切片后的Variant中提取特定字段,形成Variant Struct:
 * <pre>{@code
 * // 原始切片数据包含完整的Variant
 * // 提取路径: ["user", "name"] 和 ["user", "age"]
 * FieldToExtract[] fields = PaimonShreddingUtils.getFieldsToExtract(
 *     variantStructType,  // 目标Struct类型
 *     variantSchema       // 切片Schema
 * );
 *
 * // 执行提取
 * InternalRow result = PaimonShreddingUtils.assembleVariantStruct(
 *     shreddedRow,
 *     variantSchema,
 *     fields
 * );
 * // 结果: ROW<name: "Alice", age: 30>
 * }</pre>
 *
 * <h3>批量处理:</h3>
 * <p>提供批量切片和重建方法,适配列式向量存储:
 * <pre>{@code
 * // 批量重建Variant
 * CastedRowColumnVector input = ...;  // 切片后的列向量
 * WritableColumnVector output = ...;  // 输出Variant列向量
 * PaimonShreddingUtils.assembleVariantBatch(
 *     input,
 *     output,
 *     variantSchema
 * );
 *
 * // 批量提取Variant Struct
 * PaimonShreddingUtils.assembleVariantStructBatch(
 *     input,
 *     output,
 *     variantSchema,
 *     fields,
 *     readType
 * );
 * }</pre>
 *
 * <h3>路径提取算法:</h3>
 * <pre>
 * 路径格式: "$.user.name" 或 "$.items[0]"
 *
 * 提取步骤:
 * 1. 解析路径为VariantPathSegment数组
 * 2. 在VariantSchema中搜索路径:
 *    - 对象路径: 在objectSchemaMap中查找字段
 *    - 数组路径: 使用arraySchema和索引
 * 3. 构建SchemaPathSegment数组(包含typedIdx等)
 * 4. 创建BaseVariantReader读取目标类型
 * 5. 执行提取:
 *    - typed_value中存在: 从typed_value读取
 *    - typed_value中不存在: 从value读取
 *    - 都不存在: 返回null
 * </pre>
 *
 * <h3>类型映射:</h3>
 * <pre>
 * Paimon Type → Variant Scalar:
 * - STRING → StringType
 * - TINYINT → IntegralType(BYTE)
 * - SMALLINT → IntegralType(SHORT)
 * - INT → IntegralType(INT)
 * - BIGINT → IntegralType(LONG)
 * - FLOAT → FloatType
 * - DOUBLE → DoubleType
 * - BOOLEAN → BooleanType
 * - BYTES → BinaryType
 * - DECIMAL(p,s) → DecimalType(p,s)
 * - DATE → DateType
 *
 * Variant Scalar → Paimon Type:
 * 反向映射,用于从切片数据读取
 * </pre>
 *
 * <h3>数据完整性保证:</h3>
 * <ul>
 *   <li>切片数据必须包含有效的metadata
 *   <li>对象字段必须非null(缺失字段用null typed_value和value表示)
 *   <li>数组元素必须非null
 *   <li>typed_value和value至少有一个非null
 *   <li>路径提取时验证数据类型匹配
 * </ul>
 *
 * <h3>性能优化:</h3>
 * <ul>
 *   <li><b>批量处理</b> - 批量切片和重建减少函数调用开销
 *   <li><b>列式存储适配</b> - 直接操作列向量,减少行列转换
 *   <li><b>增量构建</b> - 使用GenericVariantBuilder增量构建Variant
 *   <li><b>Schema缓存</b> - objectSchemaMap缓存字段查找
 *   <li><b>类型转换缓存</b> - CastExecutor缓存类型转换器
 * </ul>
 *
 * <h3>线程安全:</h3>
 * 该类的方法都是静态的且无状态,是线程安全的。但传入的参数(InternalRow、VariantSchema等)
 * 的线程安全性由调用方保证。
 *
 * @see VariantSchema
 * @see ShreddingUtils
 * @see VariantShreddingWriter
 * @see BaseVariantReader
 */
public class PaimonShreddingUtils {

    /** metadata字段名称。 */
    public static final String METADATA_FIELD_NAME = Variant.METADATA;

    /** value字段名称 (未切片的Variant二进制数据)。 */
    public static final String VARIANT_VALUE_FIELD_NAME = Variant.VALUE;

    /** typed_value字段名称 (切片后的结构化数据)。 */
    public static final String TYPED_VALUE_FIELD_NAME = "typed_value";

    /**
     * Paimon切片数据行实现。
     *
     * <p>该类将Paimon的DataGetters接口适配为{@link ShreddingUtils.ShreddedRow}接口,
     * 使得通用的切片工具可以读取Paimon的行数据。
     *
     * <p>支持的数据源:
     * <ul>
     *   <li>InternalRow - Paimon内部行格式
     *   <li>InternalArray - Paimon内部数组格式
     * </ul>
     *
     * @see ShreddingUtils.ShreddedRow
     * @see DataGetters
     */
    static class PaimonShreddedRow implements ShreddingUtils.ShreddedRow {

        /** 底层的Paimon数据获取器 (InternalRow或InternalArray)。 */
        private final DataGetters row;

        /**
         * 创建Paimon切片数据行。
         *
         * @param row Paimon数据获取器
         */
        public PaimonShreddedRow(DataGetters row) {
            this.row = row;
        }

        @Override
        public boolean isNullAt(int ordinal) {
            return row.isNullAt(ordinal);
        }

        @Override
        public boolean getBoolean(int ordinal) {
            return row.getBoolean(ordinal);
        }

        @Override
        public byte getByte(int ordinal) {
            return row.getByte(ordinal);
        }

        @Override
        public short getShort(int ordinal) {
            return row.getShort(ordinal);
        }

        @Override
        public int getInt(int ordinal) {
            return row.getInt(ordinal);
        }

        @Override
        public long getLong(int ordinal) {
            return row.getLong(ordinal);
        }

        @Override
        public float getFloat(int ordinal) {
            return row.getFloat(ordinal);
        }

        @Override
        public double getDouble(int ordinal) {
            return row.getDouble(ordinal);
        }

        @Override
        public BigDecimal getDecimal(int ordinal, int precision, int scale) {
            return row.getDecimal(ordinal, precision, scale).toBigDecimal();
        }

        @Override
        public String getString(int ordinal) {
            return row.getString(ordinal).toString();
        }

        @Override
        public byte[] getBinary(int ordinal) {
            return row.getBinary(ordinal);
        }

        @Override
        public UUID getUuid(int ordinal) {
            // Paimon currently does not shred UUID.
            throw new UnsupportedOperationException();
        }

        @Override
        public ShreddingUtils.ShreddedRow getStruct(int ordinal, int numFields) {
            return new PaimonShreddedRow(row.getRow(ordinal, numFields));
        }

        @Override
        public ShreddingUtils.ShreddedRow getArray(int ordinal) {
            return new PaimonShreddedRow(row.getArray(ordinal));
        }

        @Override
        public int numElements() {
            return ((InternalArray) row).size();
        }
    }

    /**
     * VariantPathSegment在VariantSchema中的搜索结果。
     *
     * <p>该类封装了在VariantSchema中搜索提取路径段的结果,包括:
     * <ul>
     *   <li>原始路径段 (对象字段名或数组索引)
     *   <li>路径段类型 (对象提取或数组提取)
     *   <li>typed_value索引 (如果路径在Schema中存在)
     *   <li>提取索引 (对象字段索引或数组元素索引)
     * </ul>
     *
     * <p>如果路径不存在于Schema中,typedIdx将为负数,表示需要从value中提取。
     */
    public static class SchemaPathSegment {

        /** 原始路径段 (ObjectExtraction或ArrayExtraction)。 */
        private final VariantPathSegment rawPath;

        /** 是否为对象字段提取 (true)或数组元素提取 (false)。 */
        private final boolean isObject;

        /**
         * schema.typedIdx的值,如果路径在Schema中存在。
         *
         * <p>对于对象提取,Schema应包含typed_value对象且该对象包含请求的字段;
         * 对于数组提取类似。如果路径不存在则为负数。
         */
        private final int typedIdx;

        /**
         * 提取索引。
         *
         * <p>对于对象提取: schema.objectSchema中目标字段的索引,
         * 如果字段不存在则extractionIdx和typedIdx都为负数。
         *
         * <p>对于数组提取: 数组索引。该信息已存储在rawPath中,
         * 但访问原始int应比访问Either更高效。
         */
        private final int extractionIdx;

        /**
         * 创建SchemaPathSegment。
         *
         * @param rawPath 原始路径段
         * @param isObject 是否为对象提取
         * @param typedIdx typed_value的字段索引,不存在时为负数
         * @param extractionIdx 对象字段索引或数组元素索引
         */
        public SchemaPathSegment(
                VariantPathSegment rawPath, boolean isObject, int typedIdx, int extractionIdx) {
            this.rawPath = rawPath;
            this.isObject = isObject;
            this.typedIdx = typedIdx;
            this.extractionIdx = extractionIdx;
        }

        /** 获取原始路径段。 */
        public VariantPathSegment rawPath() {
            return rawPath;
        }

        /** 是否为对象字段提取。 */
        public boolean isObject() {
            return isObject;
        }

        /** 获取typed_value的字段索引。 */
        public int typedIdx() {
            return typedIdx;
        }

        /** 获取提取索引。 */
        public int extractionIdx() {
            return extractionIdx;
        }
    }

    /**
     * Variant结构体中要提取的单个字段。
     *
     * <p>表示扫描应从Variant列中提取并生成的单个请求字段。包含:
     * <ul>
     *   <li>从根到字段的完整路径
     *   <li>用于读取和转换该字段的reader
     * </ul>
     */
    public static class FieldToExtract {

        /** 从Variant根到目标字段的路径。 */
        private final SchemaPathSegment[] path;

        /** 用于读取该字段的reader。 */
        private final BaseVariantReader reader;

        /**
         * 创建FieldToExtract。
         *
         * @param path 字段路径
         * @param reader 字段reader
         */
        public FieldToExtract(SchemaPathSegment[] path, BaseVariantReader reader) {
            this.path = path;
            this.reader = reader;
        }

        /** 获取字段路径。 */
        public SchemaPathSegment[] path() {
            return path;
        }

        /** 获取字段reader。 */
        public BaseVariantReader reader() {
            return reader;
        }
    }

    /**
     * 从Paimon DataType生成Variant切片Schema。
     *
     * <p>该方法为顶层调用,会自动添加metadata字段。
     *
     * @param dataType Paimon数据类型
     * @return 完整的切片Schema (包含metadata字段)
     */
    public static RowType variantShreddingSchema(DataType dataType) {
        return VariantMetadataUtils.addVariantMetadata(
                variantShreddingSchema(dataType, true, false));
    }

    /**
     * 从预期的Variant值Schema生成合适的切片Schema。
     *
     * <p>通过在每个层级插入适当的value/typed_value字段来创建切片Schema。
     *
     * <h4>示例:</h4>
     * <pre>
     * 输入Schema: struct&lt;a: int, b: string&gt;
     * (表示JSON: {"a": 1, "b": "hello"})
     *
     * 输出切片Schema:
     * struct&lt;
     *   metadata: binary,
     *   value: binary,
     *   typed_value: struct&lt;
     *     a: struct&lt;typed_value: int, value: binary&gt;,
     *     b: struct&lt;typed_value: string, value: binary&gt;
     *   &gt;
     * &gt;
     * </pre>
     *
     * @param dataType 数据类型
     * @param isTopLevel 是否为顶层 (顶层添加metadata字段)
     * @param isObjectField 是否为对象字段 (对象字段的value可为null)
     * @return 切片Schema
     * @throws RuntimeException 如果dataType不是有效的切片类型
     */
    private static RowType variantShreddingSchema(
            DataType dataType, boolean isTopLevel, boolean isObjectField) {
        RowType.Builder builder = RowType.builder();
        if (isTopLevel) {
            builder.field(METADATA_FIELD_NAME, DataTypes.BYTES().copy(false));
        }
        switch (dataType.getTypeRoot()) {
            case ARRAY:
                ArrayType arrayType = (ArrayType) dataType;
                ArrayType shreddedArrayType =
                        new ArrayType(
                                arrayType.isNullable(),
                                variantShreddingSchema(arrayType.getElementType(), false, false));
                builder.field(VARIANT_VALUE_FIELD_NAME, DataTypes.BYTES());
                builder.field(TYPED_VALUE_FIELD_NAME, shreddedArrayType);
                break;
            case ROW:
                // The field name level is always non-nullable: Variant null values are represented
                // in the "value" column as "00", and missing values are represented by setting both
                // "value" and "typed_value" to null.
                RowType rowType = (RowType) dataType;
                RowType shreddedRowType =
                        rowType.copy(
                                rowType.getFields().stream()
                                        .map(
                                                field ->
                                                        field.newType(
                                                                variantShreddingSchema(
                                                                                field.type(),
                                                                                false,
                                                                                true)
                                                                        .notNull()))
                                        .collect(Collectors.toList()));
                builder.field(VARIANT_VALUE_FIELD_NAME, DataTypes.BYTES());
                builder.field(TYPED_VALUE_FIELD_NAME, shreddedRowType);
                break;
            case VARIANT:
                // For Variant, we don't need a typed column. If there is no typed column, value is
                // required
                // for array elements or top-level fields, but optional for objects (where a null
                // represents
                // a missing field).
                builder.field(VARIANT_VALUE_FIELD_NAME, DataTypes.BYTES().copy(isObjectField));
                break;
            case CHAR:
            case VARCHAR:
            case BOOLEAN:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                builder.field(VARIANT_VALUE_FIELD_NAME, DataTypes.BYTES());
                builder.field(TYPED_VALUE_FIELD_NAME, dataType);
                break;
            default:
                throw invalidVariantShreddingSchema(dataType);
        }
        return builder.build();
    }

    /**
     * 从Paimon RowType构建VariantSchema。
     *
     * <p>该方法解析Paimon的切片Schema结构,构建对应的VariantSchema对象。
     *
     * @param rowType Paimon切片Schema (包含metadata/value/typed_value字段)
     * @return VariantSchema对象
     * @throws RuntimeException 如果Schema格式无效
     */
    public static VariantSchema buildVariantSchema(RowType rowType) {
        return buildVariantSchema(rowType, true);
    }

    /**
     * 从Paimon RowType构建VariantSchema的递归实现。
     *
     * <p>解析规则:
     * <ul>
     *   <li>必须包含typed_value或value字段(至少一个)
     *   <li>顶层必须包含metadata字段
     *   <li>不能有重复字段名
     *   <li>typed_value可以是: 标量类型、ROW (对象)、ARRAY (数组)
     * </ul>
     *
     * @param rowType Paimon RowType
     * @param topLevel 是否为顶层
     * @return VariantSchema对象
     * @throws RuntimeException 如果Schema格式无效
     */
    private static VariantSchema buildVariantSchema(RowType rowType, boolean topLevel) {
        int typedIdx = -1;
        int variantIdx = -1;
        int topLevelMetadataIdx = -1;
        VariantSchema.ScalarType scalarSchema = null;
        VariantSchema.ObjectField[] objectSchema = null;
        VariantSchema arraySchema = null;

        // The struct must not be empty or contain duplicate field names. The latter is enforced in
        // the loop below (`if (typedIdx != -1)` and other similar checks).
        if (rowType.getFields().isEmpty()) {
            throw invalidVariantShreddingSchema(rowType);
        }

        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            DataType dataType = field.type();
            switch (field.name()) {
                case TYPED_VALUE_FIELD_NAME:
                    if (typedIdx != -1) {
                        throw invalidVariantShreddingSchema(rowType);
                    }
                    typedIdx = i;
                    switch (field.type().getTypeRoot()) {
                        case ROW:
                            RowType r = (RowType) dataType;
                            List<DataField> rFields = r.getFields();
                            // The struct must not be empty or contain duplicate field names.
                            if (fields.isEmpty()
                                    || fields.stream().distinct().count() != fields.size()) {
                                throw invalidVariantShreddingSchema(rowType);
                            }
                            objectSchema = new VariantSchema.ObjectField[rFields.size()];
                            for (int index = 0; index < rFields.size(); index++) {
                                if (field.type() instanceof RowType) {
                                    DataField f = rFields.get(index);
                                    objectSchema[index] =
                                            new VariantSchema.ObjectField(
                                                    f.name(),
                                                    buildVariantSchema((RowType) f.type(), false));
                                } else {
                                    throw invalidVariantShreddingSchema(rowType);
                                }
                            }
                            break;
                        case ARRAY:
                            ArrayType arrayType = (ArrayType) dataType;
                            if (arrayType.getElementType() instanceof RowType) {
                                arraySchema =
                                        buildVariantSchema(
                                                (RowType) arrayType.getElementType(), false);
                            } else {
                                throw invalidVariantShreddingSchema(rowType);
                            }
                            break;
                        case BOOLEAN:
                            scalarSchema = new VariantSchema.BooleanType();
                            break;
                        case TINYINT:
                            scalarSchema =
                                    new VariantSchema.IntegralType(VariantSchema.IntegralSize.BYTE);
                            break;
                        case SMALLINT:
                            scalarSchema =
                                    new VariantSchema.IntegralType(
                                            VariantSchema.IntegralSize.SHORT);
                            break;
                        case INTEGER:
                            scalarSchema =
                                    new VariantSchema.IntegralType(VariantSchema.IntegralSize.INT);
                            break;
                        case BIGINT:
                            scalarSchema =
                                    new VariantSchema.IntegralType(VariantSchema.IntegralSize.LONG);
                            break;
                        case FLOAT:
                            scalarSchema = new VariantSchema.FloatType();
                            break;
                        case DOUBLE:
                            scalarSchema = new VariantSchema.DoubleType();
                            break;
                        case VARCHAR:
                            scalarSchema = new VariantSchema.StringType();
                            break;
                        case BINARY:
                            scalarSchema = new VariantSchema.BinaryType();
                            break;
                        case DATE:
                            scalarSchema = new VariantSchema.DateType();
                            break;
                        case DECIMAL:
                            DecimalType d = (DecimalType) dataType;
                            scalarSchema =
                                    new VariantSchema.DecimalType(d.getPrecision(), d.getScale());
                            break;
                        default:
                            throw invalidVariantShreddingSchema(rowType);
                    }
                    break;

                case VARIANT_VALUE_FIELD_NAME:
                    if (variantIdx != -1 || !(field.type() instanceof VarBinaryType)) {
                        throw invalidVariantShreddingSchema(rowType);
                    }
                    variantIdx = i;
                    break;

                case METADATA_FIELD_NAME:
                    if (topLevelMetadataIdx != -1 || !(field.type() instanceof VarBinaryType)) {
                        throw invalidVariantShreddingSchema(rowType);
                    }
                    topLevelMetadataIdx = i;
                    break;

                default:
                    throw invalidVariantShreddingSchema(rowType);
            }
        }

        if (topLevel != (topLevelMetadataIdx >= 0)) {
            throw invalidVariantShreddingSchema(rowType);
        }

        return new VariantSchema(
                typedIdx,
                variantIdx,
                topLevelMetadataIdx,
                fields.size(),
                scalarSchema,
                objectSchema,
                arraySchema);
    }

    public static DataType scalarSchemaToPaimonType(VariantSchema.ScalarType scala) {
        if (scala instanceof VariantSchema.StringType) {
            return DataTypes.STRING();
        } else if (scala instanceof VariantSchema.IntegralType) {
            VariantSchema.IntegralType it = (VariantSchema.IntegralType) scala;
            switch (it.size) {
                case BYTE:
                    return DataTypes.TINYINT();
                case SHORT:
                    return DataTypes.SMALLINT();
                case INT:
                    return DataTypes.INT();
                case LONG:
                    return DataTypes.BIGINT();
                default:
                    throw new UnsupportedOperationException();
            }
        } else if (scala instanceof VariantSchema.FloatType) {
            return DataTypes.FLOAT();
        } else if (scala instanceof VariantSchema.DoubleType) {
            return DataTypes.DOUBLE();
        } else if (scala instanceof VariantSchema.BooleanType) {
            return DataTypes.BOOLEAN();
        } else if (scala instanceof VariantSchema.BinaryType) {
            return DataTypes.BYTES();
        } else if (scala instanceof VariantSchema.DecimalType) {
            VariantSchema.DecimalType dt = (VariantSchema.DecimalType) scala;
            return DataTypes.DECIMAL(dt.precision, dt.scale);
        } else if (scala instanceof VariantSchema.DateType) {
            return DataTypes.DATE();
        } else if (scala instanceof VariantSchema.TimestampType) {
            return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE();
        } else if (scala instanceof VariantSchema.TimestampNTZType) {
            return DataTypes.TIMESTAMP();
        } else {
            throw new UnsupportedOperationException();
        }
    }

    private static RuntimeException invalidVariantShreddingSchema(DataType dataType) {
        return new RuntimeException("Invalid variant shredding schema: " + dataType);
    }

    /** Paimon shredded result. */
    public static class PaimonShreddedResult implements VariantShreddingWriter.ShreddedResult {

        private final VariantSchema schema;
        // Result is stored as an InternalRow.
        private final GenericRow row;

        public PaimonShreddedResult(VariantSchema schema) {
            this.schema = schema;
            this.row = new GenericRow(schema.numFields);
        }

        @Override
        public void addArray(VariantShreddingWriter.ShreddedResult[] array) {
            GenericArray arrayResult =
                    new GenericArray(
                            java.util.Arrays.stream(array)
                                    .map(result -> ((PaimonShreddedResult) result).row)
                                    .toArray(InternalRow[]::new));
            row.setField(schema.typedIdx, arrayResult);
        }

        @Override
        public void addObject(VariantShreddingWriter.ShreddedResult[] values) {
            GenericRow innerRow = new GenericRow(schema.objectSchema.length);
            for (int i = 0; i < values.length; i++) {
                innerRow.setField(i, ((PaimonShreddedResult) values[i]).row);
            }
            row.setField(schema.typedIdx, innerRow);
        }

        @Override
        public void addVariantValue(byte[] result) {
            row.setField(schema.variantIdx, result);
        }

        @Override
        public void addScalar(Object result) {
            Object paimonValue;
            if (schema.scalarSchema instanceof VariantSchema.StringType) {
                paimonValue = BinaryString.fromString((String) result);
            } else if (schema.scalarSchema instanceof VariantSchema.DecimalType) {
                VariantSchema.DecimalType dt = (VariantSchema.DecimalType) schema.scalarSchema;
                paimonValue = Decimal.fromBigDecimal((BigDecimal) result, dt.precision, dt.scale);
            } else {
                paimonValue = result;
            }
            row.setField(schema.typedIdx, paimonValue);
        }

        @Override
        public void addMetadata(byte[] result) {
            row.setField(schema.topLevelMetadataIdx, result);
        }
    }

    /** Paimon shredded result builder. */
    public static class PaimonShreddedResultBuilder
            implements VariantShreddingWriter.ShreddedResultBuilder {
        @Override
        public VariantShreddingWriter.ShreddedResult createEmpty(VariantSchema schema) {
            return new PaimonShreddedResult(schema);
        }

        // Consider allowing this to be set via config?
        @Override
        public boolean allowNumericScaleChanges() {
            return true;
        }
    }

    /** Converts an input variant into shredded components. Returns the shredded result. */
    public static InternalRow castShredded(GenericVariant variant, VariantSchema variantSchema) {
        return ((PaimonShreddedResult)
                        VariantShreddingWriter.castShredded(
                                variant, variantSchema, new PaimonShreddedResultBuilder()))
                .row;
    }

    /** Assemble a variant (binary format) from a variant value. */
    public static Variant assembleVariant(InternalRow row, VariantSchema schema) {
        return ShreddingUtils.rebuild(new PaimonShreddedRow(row), schema);
    }

    /** Assemble a variant struct, in which each field is extracted from the variant value. */
    public static InternalRow assembleVariantStruct(
            InternalRow inputRow, VariantSchema schema, FieldToExtract[] fields) {
        if (inputRow.isNullAt(schema.topLevelMetadataIdx)) {
            throw malformedVariant();
        }
        byte[] topLevelMetadata = inputRow.getBinary(schema.topLevelMetadataIdx);
        int numFields = fields.length;
        GenericRow resultRow = new GenericRow(numFields);
        int fieldIdx = 0;
        while (fieldIdx < numFields) {
            resultRow.setField(
                    fieldIdx,
                    extractField(
                            inputRow,
                            topLevelMetadata,
                            schema,
                            fields[fieldIdx].path(),
                            fields[fieldIdx].reader()));
            fieldIdx += 1;
        }
        return resultRow;
    }

    /**
     * Return a list of fields to extract. `targetType` must be either variant or variant struct. If
     * it is variant, return null because the target is the full variant and there is no field to
     * extract. If it is variant struct, return a list of fields matching the variant struct fields.
     */
    public static FieldToExtract[] getFieldsToExtract(
            DataType dataType, VariantSchema variantSchema) {
        if (!VariantMetadataUtils.isVariantRowType(dataType)) {
            return null;
        }

        RowType variantRowType = (RowType) dataType;
        List<DataField> fields = variantRowType.getFields();
        FieldToExtract[] fieldsToExtract = new FieldToExtract[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            String path = VariantMetadataUtils.path(field.description());
            boolean failOnError = VariantMetadataUtils.failOnError(field.description());
            ZoneId timeZoneId = VariantMetadataUtils.timeZoneId(field.description());
            VariantCastArgs castArgs = new VariantCastArgs(failOnError, timeZoneId);
            fieldsToExtract[i] = buildFieldsToExtract(field.type(), path, castArgs, variantSchema);
        }
        return fieldsToExtract;
    }

    /**
     * According to the dataType, variant extraction path and variantSchema, build the
     * FieldToExtract.
     */
    public static FieldToExtract buildFieldsToExtract(
            DataType dataType, String path, VariantCastArgs castArgs, VariantSchema inputSchema) {
        VariantPathSegment[] rawPath = VariantPathSegment.parse(path);
        SchemaPathSegment[] schemaPath = new SchemaPathSegment[rawPath.length];
        VariantSchema schema = inputSchema;
        // Search `rawPath` in `schema` to produce `schemaPath`. If a raw path segment cannot be
        // found at a certain level of the file type, then `typedIdx` will be -1 starting from
        // this position, and the final `schema` will be null.
        for (int i = 0; i < rawPath.length; i++) {
            VariantPathSegment extraction = rawPath[i];
            boolean isObject = extraction instanceof ObjectExtraction;
            int typedIdx = -1;
            int extractionIdx = -1;

            if (extraction instanceof ObjectExtraction) {
                ObjectExtraction objExtr = (ObjectExtraction) extraction;
                if (schema != null && schema.objectSchemaMap != null) {
                    Integer fieldIdx = schema.objectSchemaMap.get(objExtr.getKey());
                    if (fieldIdx != null) {
                        typedIdx = schema.typedIdx;
                        extractionIdx = fieldIdx;
                        schema = schema.objectSchema[fieldIdx].schema();
                    } else {
                        schema = null;
                    }
                } else {
                    schema = null;
                }
            } else if (extraction instanceof ArrayExtraction) {
                ArrayExtraction arrExtr = (ArrayExtraction) extraction;
                if (schema != null && schema.arraySchema != null) {
                    typedIdx = schema.typedIdx;
                    extractionIdx = arrExtr.getIndex();
                    schema = schema.arraySchema;
                } else {
                    schema = null;
                }
            } else {
                schema = null;
            }
            schemaPath[i] = new SchemaPathSegment(extraction, isObject, typedIdx, extractionIdx);
        }

        BaseVariantReader reader =
                BaseVariantReader.create(
                        schema,
                        dataType,
                        castArgs,
                        (schemaPath.length == 0) && inputSchema.isUnshredded());
        return new FieldToExtract(schemaPath, reader);
    }

    /**
     * Extract a single variant struct field from a variant value. It steps into `inputRow`
     * according to the variant extraction path, and read the extracted value as the target type.
     */
    private static Object extractField(
            InternalRow inputRow,
            byte[] topLevelMetadata,
            VariantSchema inputSchema,
            SchemaPathSegment[] pathList,
            BaseVariantReader reader) {
        int pathIdx = 0;
        int pathLen = pathList.length;
        InternalRow row = inputRow;
        VariantSchema schema = inputSchema;
        while (pathIdx < pathLen) {
            SchemaPathSegment path = pathList[pathIdx];

            if (path.typedIdx() < 0) {
                // The extraction doesn't exist in `typed_value`. Try to extract the remaining part
                // of the
                // path in `value`.
                int variantIdx = schema.variantIdx;
                if (variantIdx < 0 || row.isNullAt(variantIdx)) {
                    return null;
                }
                GenericVariant v = new GenericVariant(row.getBinary(variantIdx), topLevelMetadata);
                while (pathIdx < pathLen) {
                    VariantPathSegment rowPath = pathList[pathIdx].rawPath();
                    if (rowPath instanceof ObjectExtraction && v.getType() == OBJECT) {
                        v = v.getFieldByKey(((ObjectExtraction) rowPath).getKey());
                    } else if (rowPath instanceof ArrayExtraction && v.getType() == ARRAY) {
                        v = v.getElementAtIndex(((ArrayExtraction) rowPath).getIndex());
                    } else {
                        v = null;
                    }
                    if (v == null) {
                        return null;
                    }
                    pathIdx += 1;
                }
                return VariantGet.cast(v, reader.targetType(), reader.castArgs());
            }

            if (row.isNullAt(path.typedIdx())) {
                return null;
            }

            if (path.isObject()) {
                InternalRow obj = row.getRow(path.typedIdx(), schema.objectSchema.length);
                // Object field must not be null.
                if (obj.isNullAt(path.extractionIdx())) {
                    throw malformedVariant();
                }
                schema = schema.objectSchema[path.extractionIdx()].schema();
                row = obj.getRow(path.extractionIdx(), schema.numFields);
                // Return null if the field is missing.
                if ((schema.typedIdx < 0 || row.isNullAt(schema.typedIdx))
                        && (schema.variantIdx < 0 || row.isNullAt(schema.variantIdx))) {
                    return null;
                }
            } else {
                InternalArray arr = row.getArray(path.typedIdx());
                // Return null if the extraction index is out of bound.
                if (path.extractionIdx() >= arr.size()) {
                    return null;
                }
                // Array element must not be null.
                if (arr.isNullAt(path.extractionIdx())) {
                    throw malformedVariant();
                }
                schema = schema.arraySchema;
                row = arr.getRow(path.extractionIdx(), schema.numFields);
            }
            pathIdx += 1;
        }
        return reader.read(row, topLevelMetadata);
    }

    /** Assemble a batch of variant (binary format) from a batch of variant values. */
    public static void assembleVariantBatch(
            CastedRowColumnVector input, WritableColumnVector output, VariantSchema variantSchema) {
        int numRows = input.getElementsAppended();
        output.reset();
        output.reserve(numRows);
        WritableBytesVector valueChild = (WritableBytesVector) output.getChildren()[0];
        WritableBytesVector metadataChild = (WritableBytesVector) output.getChildren()[1];
        for (int i = 0; i < numRows; ++i) {
            if (input.isNullAt(i)) {
                output.setNullAt(i);
            } else {
                Variant v = assembleVariant(input.getRow(i), variantSchema);
                byte[] value = v.value();
                byte[] metadata = v.metadata();
                valueChild.putByteArray(i, value, 0, value.length);
                metadataChild.putByteArray(i, metadata, 0, metadata.length);
            }
        }
    }

    /** Assemble a batch of variant struct from a batch of variant values. */
    public static void assembleVariantStructBatch(
            CastedRowColumnVector input,
            WritableColumnVector output,
            VariantSchema variantSchema,
            FieldToExtract[] fields,
            DataType readType) {
        int numRows = input.getElementsAppended();
        output.reset();
        output.reserve(numRows);
        RowToColumnConverter converter =
                new RowToColumnConverter(RowType.of(new DataField(0, "placeholder", readType)));
        WritableColumnVector[] converterVectors = new WritableColumnVector[1];
        converterVectors[0] = output;
        GenericRow converterRow = new GenericRow(1);
        for (int i = 0; i < numRows; ++i) {
            if (input.isNullAt(i)) {
                converterRow.setField(0, null);
            } else {
                converterRow.setField(
                        0, assembleVariantStruct(input.getRow(i), variantSchema, fields));
            }
            converter.convert(converterRow, converterVectors);
        }
    }
}
