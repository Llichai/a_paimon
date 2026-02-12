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
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VariantType;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;

import static org.apache.paimon.data.variant.GenericVariantUtil.Type;
import static org.apache.paimon.data.variant.GenericVariantUtil.malformedVariant;

/* This file is based on source code from the Spark Project (http://spark.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Variant值读取器基类。
 *
 * <p>该类是从切片后的Variant数据读取值并转换为Paimon类型的基类。为方便起见,也允许创建
 * 基类本身的实例,作为targetType和castArgs的容器,但其功能方法不能使用。
 *
 * <h3>核心功能:</h3>
 * <ul>
 *   <li><b>类型化读取</b> - 从typed_value中读取结构化数据并转换为目标类型
 *   <li><b>回退到value</b> - 当typed_value不存在或为null时,从value中读取Variant
 *   <li><b>类型转换</b> - 将Variant值转换为Paimon的标量、数组、对象或Map类型
 *   <li><b>递归读取</b> - 递归处理嵌套的对象和数组结构
 * </ul>
 *
 * <h3>Reader类型层次结构:</h3>
 * <pre>
 * BaseVariantReader (基类)
 * ├── ScalarReader - 读取标量类型 (INT, STRING, DECIMAL等)
 * ├── RowReader - 读取Paimon RowType
 * ├── ArrayReader - 读取Paimon ArrayType
 * ├── MapReader - 读取Paimon MapType (键类型必须是STRING)
 * └── VariantReader - 读取Variant二进制格式
 * </pre>
 *
 * <h3>读取算法:</h3>
 * <pre>
 * 读取流程:
 * 1. 检查typed_value是否非null:
 *    - 如果是: 调用readFromTyped()从typed_value读取
 *    - 如果否: 转到步骤2
 *
 * 2. 检查value是否非null:
 *    - 如果是: 从value中读取Variant,然后转换为目标类型
 *    - 如果否: 抛出MALFORMED_VARIANT异常 (数据损坏)
 *
 * 3. 如果typed_value非null但类型不匹配:
 *    - 对于标量类型: 尝试类型转换
 *    - 对于复杂类型: 只有String类型可以接受任何类型(转为JSON字符串)
 *    - 其他情况: 重建Variant并尝试转换,失败则抛出异常或返回null
 * </pre>
 *
 * <h3>ScalarReader - 标量类型读取:</h3>
 * <pre>
 * 读取逻辑:
 * 1. 如果schema.scalarSchema与targetType完全匹配:
 *    直接从typed_value读取,无需转换
 *
 * 2. 如果schema.scalarSchema存在但类型不匹配:
 *    - String目标类型: 重建Variant并转为JSON字符串
 *    - 其他类型: 使用CastExecutor进行类型转换
 *                转换失败则返回invalidCast
 *
 * 3. 如果schema.scalarSchema不存在:
 *    从value中读取Variant并转换
 *
 * 支持的标量类型:
 * - STRING, TINYINT, SMALLINT, INT, BIGINT
 * - FLOAT, DOUBLE, BOOLEAN, BYTES
 * - DECIMAL, DATE, TIMESTAMP
 * </pre>
 *
 * <h3>RowReader - 对象类型读取:</h3>
 * <pre>
 * 读取逻辑:
 * 1. 如果schema.objectSchema不存在:
 *    返回invalidCast (类型不匹配)
 *
 * 2. 遍历targetType的每个字段:
 *    a) 字段在objectSchema中 (fieldInputIndices[i] >= 0):
 *       - 从typed_value的对应字段读取
 *       - 递归调用字段的reader
 *       - 如果字段缺失 (typed_value和value都为null): 该字段为null
 *
 *    b) 字段不在objectSchema中 (fieldInputIndices[i] < 0):
 *       - 从value的Variant对象中查找字段
 *       - 如果找到: 转换为目标类型
 *       - 如果未找到: 该字段为null
 *
 * 3. 构建GenericRow作为结果
 *
 * needUnshreddedObject标志:
 *   true: 至少有一个字段不在objectSchema中,需要读取value
 *   false: 所有字段都在objectSchema中,无需读取value
 * </pre>
 *
 * <h3>ArrayReader - 数组类型读取:</h3>
 * <pre>
 * 读取逻辑:
 * 1. 如果schema.arraySchema不存在:
 *    返回invalidCast (类型不匹配)
 *
 * 2. 从typed_value读取数组:
 *    - 遍历数组的每个元素
 *    - 数组元素必须非null (否则抛出MALFORMED_VARIANT)
 *    - 递归调用elementReader读取每个元素
 *
 * 3. 构建GenericArray作为结果
 * </pre>
 *
 * <h3>MapReader - Map类型读取:</h3>
 * <pre>
 * 读取逻辑:
 * 1. 如果schema.objectSchema不存在:
 *    返回invalidCast (类型不匹配)
 *
 * 2. 读取typed_value中的切片字段:
 *    - 遍历objectSchema的每个字段
 *    - 如果字段不缺失: 添加到Map
 *
 * 3. 读取value中的未切片字段:
 *    - 遍历Variant对象的每个字段
 *    - 验证字段不在objectSchema中 (否则抛出MALFORMED_VARIANT)
 *    - 添加到Map
 *
 * 4. 构建GenericMap作为结果
 *
 * 注意: Map的键类型必须是STRING
 * </pre>
 *
 * <h3>VariantReader - Variant二进制读取:</h3>
 * <pre>
 * 读取逻辑:
 * 1. 如果isTopLevelUnshredded为true (未切片):
 *    直接从value中读取Variant二进制,无需重建
 *
 * 2. 否则:
 *    调用rebuildVariant从切片数据重建Variant
 *
 * isTopLevelUnshredded优化:
 *   当Variant列未切片且提取路径为空时,可以避免重建,直接返回原始二进制
 * </pre>
 *
 * <h3>类型转换和错误处理:</h3>
 * <ul>
 *   <li><b>invalidCast</b> - 当类型转换失败时,根据castArgs.failOnError决定抛出异常或返回null
 *   <li><b>malformedVariant</b> - 当数据损坏时抛出异常(字段缺失、类型不匹配、重复字段等)
 *   <li><b>CastExecutor</b> - 使用Paimon的类型转换器进行标量类型转换
 * </ul>
 *
 * <h3>使用示例:</h3>
 * <pre>{@code
 * // 创建Reader
 * VariantSchema schema = ...;
 * DataType targetType = DataTypes.ROW(
 *     DataTypes.FIELD("name", DataTypes.STRING()),
 *     DataTypes.FIELD("age", DataTypes.INT())
 * );
 * VariantCastArgs castArgs = new VariantCastArgs(true, ZoneId.systemDefault());
 * BaseVariantReader reader = BaseVariantReader.create(
 *     schema,
 *     targetType,
 *     castArgs,
 *     false
 * );
 *
 * // 读取数据
 * InternalRow shreddedRow = ...;
 * byte[] metadata = shreddedRow.getBinary(0);
 * Object result = reader.read(shreddedRow, metadata);
 * // result: GenericRow.of("Alice", 30)
 * }</pre>
 *
 * <h3>线程安全:</h3>
 * Reader实例是不可变的且线程安全,可以被多个线程并发使用。但传入的InternalRow和metadata
 * 的线程安全性由调用方保证。
 *
 * @see VariantSchema
 * @see VariantCastArgs
 * @see PaimonShreddingUtils
 * @see VariantGet
 */
public class BaseVariantReader {

    /** Variant切片Schema,描述typed_value的结构。 */
    protected final VariantSchema schema;

    /** 目标Paimon类型,读取结果将转换为此类型。 */
    protected final DataType targetType;

    /** 类型转换参数,包含failOnError和时区信息。 */
    protected final VariantCastArgs castArgs;

    /**
     * 创建BaseVariantReader。
     *
     * @param schema Variant切片Schema
     * @param targetType 目标Paimon类型
     * @param castArgs 类型转换参数
     */
    public BaseVariantReader(VariantSchema schema, DataType targetType, VariantCastArgs castArgs) {
        this.schema = schema;
        this.targetType = targetType;
        this.castArgs = castArgs;
    }

    /** 获取VariantSchema。 */
    public VariantSchema schema() {
        return schema;
    }

    /** 获取目标类型。 */
    public DataType targetType() {
        return targetType;
    }

    /** 获取类型转换参数。 */
    public VariantCastArgs castArgs() {
        return castArgs;
    }

    /**
     * 从包含Variant值的行中读取数据并返回目标类型的值。
     *
     * <p>行的Schema由schema描述。该方法在Variant缺失时抛出MALFORMED_VARIANT异常。
     * 如果Variant可以合法缺失(唯一可能的情况是对象typed_value中的结构字段),
     * 调用方应检查并避免在Variant缺失时调用此方法。
     *
     * @param row 切片数据行 (或未切片行)
     * @param topLevelMetadata 顶层metadata字节数组
     * @return 目标类型的值
     * @throws RuntimeException 如果Variant缺失或数据损坏
     */
    public Object read(InternalRow row, byte[] topLevelMetadata) {
        if (schema.typedIdx < 0 || row.isNullAt(schema.typedIdx)) {
            if (schema.variantIdx < 0 || row.isNullAt(schema.variantIdx)) {
                // Both `typed_value` and `value` are null, meaning the variant is missing.
                throw malformedVariant();
            }
            GenericVariant variant =
                    new GenericVariant(row.getBinary(schema.variantIdx), topLevelMetadata);
            return VariantGet.cast(variant, targetType, castArgs);
        } else {
            return readFromTyped(row, topLevelMetadata);
        }
    }

    /**
     * 子类应重写此方法,当typed_value非null时生成读取结果。
     *
     * @param row 切片数据行
     * @param topLevelMetadata 顶层metadata
     * @return 读取的值
     * @throws UnsupportedOperationException 如果在BaseVariantReader基类上调用
     */
    protected Object readFromTyped(InternalRow row, byte[] topLevelMetadata) {
        throw new UnsupportedOperationException();
    }

    /**
     * 从Variant值重建Variant二进制格式的工具方法。
     *
     * @param row 切片数据行
     * @param topLevelMetadata 顶层metadata
     * @return 重建的Variant
     */
    protected Variant rebuildVariant(InternalRow row, byte[] topLevelMetadata) {
        GenericVariantBuilder builder = new GenericVariantBuilder(false);
        ShreddingUtils.rebuild(
                new PaimonShreddingUtils.PaimonShreddedRow(row), topLevelMetadata, schema, builder);
        return builder.result();
    }

    /**
     * 当无效转换发生时抛出错误或返回null的工具方法。
     *
     * @param row 切片数据行
     * @param topLevelMetadata 顶层metadata
     * @return 根据castArgs.failOnError,抛出异常或返回null
     */
    protected Object invalidCast(InternalRow row, byte[] topLevelMetadata) {
        return VariantGet.invalidCast(rebuildVariant(row, topLevelMetadata), targetType, castArgs);
    }

    /**
     * 为targetType创建Reader。
     *
     * <p>如果schema为null,表示提取路径不存在于typed_value中,返回BaseVariantReader实例。
     * 如类注释所述,此时Reader仅作为targetType和castArgs的容器。
     *
     * @param schema VariantSchema,可为null
     * @param targetType 目标Paimon类型
     * @param castArgs 类型转换参数
     * @param isTopLevelUnshredded 是否为顶层未切片 (优化标志)
     * @return 对应的Reader实例
     */
    public static BaseVariantReader create(
            @Nullable VariantSchema schema,
            DataType targetType,
            VariantCastArgs castArgs,
            boolean isTopLevelUnshredded) {
        if (schema == null) {
            return new BaseVariantReader(null, targetType, castArgs);
        }

        if (targetType instanceof RowType) {
            return new RowReader(schema, (RowType) targetType, castArgs);
        } else if (targetType instanceof ArrayType) {
            return new ArrayReader(schema, (ArrayType) targetType, castArgs);
        } else if (targetType instanceof MapType
                && ((MapType) targetType).getKeyType().equals(DataTypes.STRING())) {
            if (((MapType) targetType).getKeyType().equals(DataTypes.STRING())) {
                return new MapReader(schema, (MapType) targetType, castArgs);
            } else {
                throw new UnsupportedOperationException();
            }
        } else if (targetType instanceof VariantType) {
            return new VariantReader(
                    schema, (VariantType) targetType, castArgs, isTopLevelUnshredded);
        } else {
            return new ScalarReader(schema, targetType, castArgs);
        }
    }

    /**
     * 读取Variant值为Paimon RowType的Reader。
     *
     * <p>该Reader从对象typed_value中读取切片字段,从value中读取未切片字段。
     * value不得包含任何切片字段(根据切片规范),但此要求未强制执行。如果value确实
     * 包含切片字段,不会发生错误,typed_value中的字段将作为最终结果。
     */
    private static final class RowReader extends BaseVariantReader {

        /**
         * 对于targetType的每个字段,存储对象typed_value中同名字段的索引。
         * 如果该字段不存在于typed_value中则为-1。
         */
        private final int[] fieldInputIndices;

        /**
         * 对于targetType的每个字段,存储对象typed_value中对应字段的Reader。
         * 如果该字段不存在于typed_value中则为null。
         */
        private final BaseVariantReader[] fieldReaders;

        /**
         * 如果targetType的所有字段都可以在对象typed_value中找到,则Reader无需读取value。
         */
        private final boolean needUnshreddedObject;

        /**
         * 创建RowReader。
         *
         * @param schema VariantSchema
         * @param targetType 目标RowType
         * @param castArgs 类型转换参数
         */
        public RowReader(VariantSchema schema, RowType targetType, VariantCastArgs castArgs) {
            super(schema, targetType, castArgs);

            List<DataField> targetFields = targetType.getFields();
            this.fieldInputIndices = new int[targetFields.size()];
            for (int i = 0; i < targetFields.size(); i++) {
                fieldInputIndices[i] =
                        schema.objectSchemaMap != null
                                ? schema.objectSchemaMap.get(targetFields.get(i).name())
                                : -1;
            }

            this.fieldReaders = new BaseVariantReader[targetFields.size()];
            for (int i = 0; i < targetFields.size(); i++) {
                int inputIdx = fieldInputIndices[i];
                if (inputIdx >= 0) {
                    VariantSchema fieldSchema = schema.objectSchema[inputIdx].schema();
                    fieldReaders[i] =
                            BaseVariantReader.create(
                                    fieldSchema, targetFields.get(i).type(), castArgs, false);
                } else {
                    fieldReaders[i] = null;
                }
            }

            // Check if any field is missing (i.e., index == -1)
            boolean needsUnshredded = false;
            for (int idx : fieldInputIndices) {
                if (idx < 0) {
                    needsUnshredded = true;
                    break;
                }
            }
            this.needUnshreddedObject = needsUnshredded;
        }

        @Override
        public Object readFromTyped(InternalRow row, byte[] topLevelMetadata) {
            if (schema.objectSchema == null) {
                return invalidCast(row, topLevelMetadata);
            }

            InternalRow obj = row.getRow(schema.typedIdx, schema.objectSchema.length);
            GenericRow result = new GenericRow(fieldInputIndices.length);
            GenericVariant unshreddedObject = null;

            if (needUnshreddedObject
                    && schema.variantIdx >= 0
                    && !row.isNullAt(schema.variantIdx)) {
                unshreddedObject =
                        new GenericVariant(row.getBinary(schema.variantIdx), topLevelMetadata);
                if (unshreddedObject.getType() != Type.OBJECT) {
                    throw malformedVariant();
                }
            }

            int numFields = fieldInputIndices.length;
            int i = 0;
            while (i < numFields) {
                int inputIdx = fieldInputIndices[i];
                if (inputIdx >= 0) {
                    // Shredded field must not be null.
                    if (obj.isNullAt(inputIdx)) {
                        throw malformedVariant();
                    }

                    VariantSchema fieldSchema = schema.objectSchema[inputIdx].schema();
                    InternalRow fieldInput = obj.getRow(inputIdx, fieldSchema.numFields);
                    // Only read from the shredded field if it is not missing.
                    if ((fieldSchema.typedIdx >= 0 && !fieldInput.isNullAt(fieldSchema.typedIdx))
                            || (fieldSchema.variantIdx >= 0
                                    && !fieldInput.isNullAt(fieldSchema.variantIdx))) {
                        Object fieldValue = fieldReaders[i].read(fieldInput, topLevelMetadata);
                        result.setField(i, fieldValue);
                    }
                } else if (unshreddedObject != null) {
                    DataField field = ((RowType) targetType).getField(i);
                    String fieldName = field.name();
                    DataType fieldType = field.type();
                    GenericVariant unshreddedField = unshreddedObject.getFieldByKey(fieldName);
                    if (unshreddedField != null) {
                        Object castedValue = VariantGet.cast(unshreddedField, fieldType, castArgs);
                        result.setField(i, castedValue);
                    }
                }
                i += 1;
            }
            return result;
        }
    }

    /**
     * 读取Variant值为Paimon ArrayType的Reader。
     */
    private static final class ArrayReader extends BaseVariantReader {

        /** 数组元素的Reader。 */
        private final BaseVariantReader elementReader;

        /**
         * 创建ArrayReader。
         *
         * @param schema VariantSchema
         * @param targetType 目标ArrayType
         * @param castArgs 类型转换参数
         */
        public ArrayReader(VariantSchema schema, ArrayType targetType, VariantCastArgs castArgs) {
            super(schema, targetType, castArgs);
            if (schema.arraySchema != null) {
                this.elementReader =
                        BaseVariantReader.create(
                                schema.arraySchema, targetType.getElementType(), castArgs, false);
            } else {
                this.elementReader = null;
            }
        }

        @Override
        protected Object readFromTyped(InternalRow row, byte[] topLevelMetadata) {
            if (schema.arraySchema == null) {
                return invalidCast(row, topLevelMetadata);
            }

            int elementNumFields = schema.arraySchema.numFields;
            InternalArray arr = row.getArray(schema.typedIdx);
            int size = arr.size();
            Object[] result = new Object[size];
            int i = 0;
            while (i < size) {
                if (arr.isNullAt(i)) {
                    throw malformedVariant();
                }
                result[i] = elementReader.read(arr.getRow(i, elementNumFields), topLevelMetadata);
                i += 1;
            }
            return new GenericArray(result);
        }
    }

    /**
     * 读取Variant值为Paimon Map类型(键类型为String)的Reader。
     *
     * <p>输入必须是对象才能进行有效转换。结果Map包含对象typed_value中的切片字段
     * 和对象value中的未切片字段。
     *
     * <p>value不得包含任何切片字段(根据切片规范)。与RowReader不同,此要求在
     * MapReader中会强制执行。如果value包含切片字段,将抛出MALFORMED_VARIANT错误,
     * 目的是避免Map键重复。
     */
    private static final class MapReader extends BaseVariantReader {

        /** 对象typed_value中每个字段的值Reader数组。 */
        private final BaseVariantReader[] valueReaders;

        /** 对象typed_value中每个字段的字段名(BinaryString格式)。 */
        private final BinaryString[] shreddedFieldNames;

        /** 目标Map类型。 */
        private final MapType targetType;

        /**
         * 创建MapReader。
         *
         * @param schema VariantSchema
         * @param targetType 目标MapType (键类型必须是STRING)
         * @param castArgs 类型转换参数
         */
        public MapReader(VariantSchema schema, MapType targetType, VariantCastArgs castArgs) {
            super(schema, targetType, castArgs);
            this.targetType = targetType;

            if (schema.objectSchema != null) {
                int len = schema.objectSchema.length;
                this.valueReaders = new BaseVariantReader[len];
                this.shreddedFieldNames = new BinaryString[len];

                for (int i = 0; i < len; i++) {
                    VariantSchema.ObjectField fieldInfo = schema.objectSchema[i];
                    this.valueReaders[i] =
                            BaseVariantReader.create(
                                    fieldInfo.schema(), targetType.getValueType(), castArgs, false);
                    this.shreddedFieldNames[i] = BinaryString.fromString(fieldInfo.fieldName());
                }
            } else {
                this.valueReaders = null;
                this.shreddedFieldNames = null;
            }
        }

        @Override
        public Object readFromTyped(InternalRow row, byte[] topLevelMetadata) {
            if (schema.objectSchema == null) {
                return invalidCast(row, topLevelMetadata);
            }

            InternalRow obj = row.getRow(schema.typedIdx, schema.objectSchema.length);
            int numShreddedFields = (valueReaders != null) ? valueReaders.length : 0;

            GenericVariant unshreddedObject = null;
            if (schema.variantIdx >= 0 && !row.isNullAt(schema.variantIdx)) {
                unshreddedObject =
                        new GenericVariant(row.getBinary(schema.variantIdx), topLevelMetadata);
                if (unshreddedObject.getType() != Type.OBJECT) {
                    throw malformedVariant();
                }
            }

            int numUnshreddedFields =
                    (unshreddedObject != null) ? unshreddedObject.objectSize() : 0;
            int totalCapacity = numShreddedFields + numUnshreddedFields;

            HashMap<BinaryString, Object> map = new HashMap<>(totalCapacity);
            int i = 0;
            while (i < numShreddedFields) {
                // Shredded field must not be null.
                if (obj.isNullAt(i)) {
                    throw malformedVariant();
                }
                VariantSchema fieldSchema = schema.objectSchema[i].schema();
                InternalRow fieldInput = obj.getRow(i, fieldSchema.numFields);
                // Only add the shredded field to map if it is not missing.
                if ((fieldSchema.typedIdx >= 0 && !fieldInput.isNullAt(fieldSchema.typedIdx))
                        || (fieldSchema.variantIdx >= 0
                                && !fieldInput.isNullAt(fieldSchema.variantIdx))) {
                    map.put(
                            shreddedFieldNames[i],
                            valueReaders[i].read(fieldInput, topLevelMetadata));
                }
                i += 1;
            }
            i = 0;
            while (i < numUnshreddedFields) {
                GenericVariant.ObjectField field = unshreddedObject.getFieldAtIndex(i);
                if (schema.objectSchemaMap.containsKey(field.key)) {
                    throw malformedVariant();
                }
                map.put(
                        BinaryString.fromString(field.key),
                        VariantGet.cast(field.value, targetType.getValueType(), castArgs));
                i += 1;
            }
            return new GenericMap(map);
        }
    }

    /**
     * 读取Variant值为Paimon Variant类型(二进制格式)的Reader。
     */
    private static final class VariantReader extends BaseVariantReader {

        /**
         * 可选优化: 如果Variant列未切片且提取路径为空,用户可以设置为true。
         * 我们不需要做任何特殊处理,但可以避免重建Variant以优化性能。
         */
        private final boolean isTopLevelUnshredded;

        /**
         * 创建VariantReader。
         *
         * @param schema VariantSchema
         * @param targetType 目标VariantType
         * @param castArgs 类型转换参数
         * @param isTopLevelUnshredded 是否为顶层未切片(优化标志)
         */
        public VariantReader(
                VariantSchema schema,
                VariantType targetType,
                VariantCastArgs castArgs,
                boolean isTopLevelUnshredded) {
            super(schema, targetType, castArgs);
            this.isTopLevelUnshredded = isTopLevelUnshredded;
        }

        @Override
        public Object read(InternalRow row, byte[] topLevelMetadata) {
            if (isTopLevelUnshredded) {
                if (row.isNullAt(schema.variantIdx)) {
                    throw malformedVariant();
                }
                return new GenericVariant(row.getBinary(schema.variantIdx), topLevelMetadata);
            }
            return rebuildVariant(row, topLevelMetadata);
        }
    }

    /**
     * 读取Variant值为Paimon标量类型的Reader。
     *
     * <p>当typed_value非null但不是标量时,所有其他目标类型应返回invalidCast,
     * 但只有String目标类型仍可以从数组/对象typed_value构建字符串(转为JSON)。
     *
     * <p>对于标量typed_value,依靠ScalarCastHelper执行转换。根据切片规范,
     * 标量typed_value和value不得同时非null。此要求在Reader中未强制执行。
     * 如果两者都非null,不会发生错误,Reader将从typed_value读取。
     */
    private static final class ScalarReader extends BaseVariantReader {

        /** 标量类型 (从schema.scalarSchema转换而来)。 */
        private final DataType scalaType;

        /** 类型转换器,从scalaType转换为targetType,如果不需要转换则为null。 */
        @Nullable private final CastExecutor<Object, Object> resolve;

        /** 是否无需类型转换 (scalaType与targetType相同)。 */
        private final boolean noNeedCast;

        /**
         * 创建ScalarReader。
         *
         * @param schema VariantSchema
         * @param targetType 目标标量类型
         * @param castArgs 类型转换参数
         */
        public ScalarReader(VariantSchema schema, DataType targetType, VariantCastArgs castArgs) {
            super(schema, targetType, castArgs);
            if (schema.scalarSchema != null) {
                scalaType = PaimonShreddingUtils.scalarSchemaToPaimonType(schema.scalarSchema);
                noNeedCast = scalaType.equals(targetType);
                resolve =
                        noNeedCast
                                ? null
                                : (CastExecutor<Object, Object>)
                                        CastExecutors.resolve(scalaType, targetType);
            } else {
                scalaType = null;
                noNeedCast = false;
                resolve = null;
            }
        }

        @Override
        protected Object readFromTyped(InternalRow row, byte[] topLevelMetadata) {
            if (!noNeedCast && resolve == null) {
                if (targetType.equals(DataTypes.STRING())) {
                    return BinaryString.fromString(
                            rebuildVariant(row, topLevelMetadata).toJson(castArgs.zoneId()));
                } else {
                    return invalidCast(row, topLevelMetadata);
                }
            }

            int typedValueIdx = schema.typedIdx;

            if (row.isNullAt(typedValueIdx)) {
                return null;
            }

            Object i;
            if (scalaType.equals(DataTypes.STRING())) {
                i = row.getString(typedValueIdx);
            } else if (scalaType instanceof TinyIntType) {
                i = row.getByte(typedValueIdx);
            } else if (scalaType instanceof SmallIntType) {
                i = row.getShort(typedValueIdx);
            } else if (scalaType instanceof IntType) {
                i = row.getInt(typedValueIdx);
            } else if (scalaType instanceof BigIntType) {
                i = row.getLong(typedValueIdx);
            } else if (scalaType instanceof FloatType) {
                i = row.getFloat(typedValueIdx);
            } else if (scalaType instanceof DoubleType) {
                i = row.getDouble(typedValueIdx);
            } else if (scalaType instanceof BooleanType) {
                i = row.getBoolean(typedValueIdx);
            } else if (scalaType.equals(DataTypes.BYTES())) {
                i = row.getBinary(typedValueIdx);
            } else if (scalaType instanceof DecimalType) {
                i =
                        row.getDecimal(
                                typedValueIdx,
                                ((DecimalType) scalaType).getPrecision(),
                                ((DecimalType) scalaType).getScale());
            } else {
                throw new UnsupportedOperationException("Unsupported scalar type: " + scalaType);
            }
            if (noNeedCast) {
                return i;
            }
            try {
                return resolve.cast(i);
            } catch (Exception e) {
                return invalidCast(row, topLevelMetadata);
            }
        }
    }
}
