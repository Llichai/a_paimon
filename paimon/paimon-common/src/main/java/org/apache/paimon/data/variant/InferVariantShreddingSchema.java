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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VariantType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Variant切片模式推断器。
 *
 * <p>当切片模式(Shredding Schema)中存在Variant值时,自动推断合适的类型模式。该类通过分析多行数据,
 * 识别常见字段和数据类型,从而生成优化的列式存储模式。
 *
 * <h3>核心功能:</h3>
 * <ul>
 *   <li><b>自动类型识别</b> - 从Variant数据中自动推断标量类型、对象结构和数组元素类型
 *   <li><b>Schema合并</b> - 合并多行数据的Schema,生成统一的类型定义
 *   <li><b>频率过滤</b> - 根据字段出现频率决定是否切片(minFieldCardinalityRatio)
 *   <li><b>深度和宽度限制</b> - 控制Schema复杂度,避免过度切片
 *   <li><b>类型提升</b> - 自动处理兼容类型之间的转换(如Decimal和Long)
 * </ul>
 *
 * <h3>切片规则:</h3>
 * <ul>
 *   <li><b>顶层和Row字段中的VariantType会被切片</b>
 *   <li><b>嵌套在数组或Map中的VariantType不会被切片</b> (保持原始Variant格式)
 *   <li><b>稀有字段被丢弃</b> - 出现频率低于minFieldCardinalityRatio的字段不会被切片
 *   <li><b>深度超限变为Variant</b> - 超过maxSchemaDepth的嵌套结构保持为VariantType
 *   <li><b>宽度超限变为Variant</b> - 字段数超过maxSchemaWidth时停止切片
 * </ul>
 *
 * <h3>类型推断算法:</h3>
 * <pre>
 * 1. 标量类型识别:
 *    - Boolean → BOOLEAN
 *    - Long → DECIMAL(精度,0) 或 BIGINT (精度>18时)
 *    - String → STRING
 *    - Double → DOUBLE
 *    - Float → FLOAT
 *    - Decimal → DECIMAL(精度,小数位)
 *    - Date → DATE
 *    - Timestamp → TIMESTAMP_WITH_LOCAL_TIME_ZONE
 *    - TimestampNTZ → TIMESTAMP
 *    - Binary → BYTES
 *
 * 2. 复杂类型识别:
 *    - Object → RowType (递归推断字段类型)
 *    - Array → ArrayType (推断元素类型)
 *
 * 3. 类型合并规则:
 *    - null + T → T (null与任何类型兼容)
 *    - Decimal + Decimal → 扩大精度和小数位
 *    - Decimal + Long → BIGINT 或扩展的Decimal
 *    - RowType + RowType → 字段并集(按字母顺序)
 *    - ArrayType + ArrayType → 元素类型合并
 *    - 其他不兼容类型 → VARIANT
 * </pre>
 *
 * <h3>使用示例:</h3>
 * <pre>{@code
 * // 原始表Schema包含Variant列
 * RowType schema = RowType.of(
 *     new DataField(0, "id", DataTypes.INT()),
 *     new DataField(1, "data", DataTypes.VARIANT())
 * );
 *
 * // 创建推断器
 * InferVariantShreddingSchema inferrer = new InferVariantShreddingSchema(
 *     schema,
 *     100,    // maxSchemaWidth: 最多100个字段
 *     10,     // maxSchemaDepth: 最深10层嵌套
 *     0.1     // minFieldCardinalityRatio: 至少10%的行包含该字段
 * );
 *
 * // 收集样本数据
 * List<InternalRow> rows = Arrays.asList(
 *     GenericRow.of(1, variant1),  // {"name": "Alice", "age": 30}
 *     GenericRow.of(2, variant2),  // {"name": "Bob", "age": 25}
 *     GenericRow.of(3, variant3)   // {"name": "Carol", "city": "NYC"}
 * );
 *
 * // 推断Schema
 * RowType shreddingSchema = inferrer.inferSchema(rows);
 * // 结果:
 * // RowType<id: INT, data: ROW<
 * //   metadata: BYTES,
 * //   value: BYTES,
 * //   typed_value: ROW<
 * //     name: ROW<typed_value: STRING, value: BYTES>,
 * //     age:  ROW<typed_value: BIGINT, value: BYTES>
 * //     // city字段只出现1次,低于30%,被丢弃
 * //   >
 * // >>
 * }</pre>
 *
 * <h3>Schema宽度和深度限制:</h3>
 * <pre>
 * maxSchemaWidth: 限制总字段数
 *   - 所有层级的字段总和不超过此值
 *   - 超限时,后续字段保持为VariantType
 *   - 防止过多列导致的存储和查询性能问题
 *
 * maxSchemaDepth: 限制嵌套深度
 *   - 从顶层开始计数
 *   - 超过此深度的嵌套结构保持为VariantType
 *   - 防止深层递归导致的性能问题
 *
 * minFieldCardinalityRatio: 最小字段基数比率
 *   - 字段在非null行中的出现频率
 *   - 例如: 0.1表示至少10%的非null行包含该字段
 *   - 稀有字段不会被切片,减少列数
 * </pre>
 *
 * <h3>类型提升和兼容性:</h3>
 * <ul>
 *   <li>整数类型统一提升为BIGINT (TINYINT/SMALLINT/INT → BIGINT)
 *   <li>整数Decimal(精度≤18,小数位=0)提升为BIGINT
 *   <li>Decimal之间取更大的精度和小数位
 *   <li>Long和Decimal可以兼容转换
 *   <li>精度超过38的Decimal降级为VariantType
 * </ul>
 *
 * <h3>线程安全:</h3>
 * 该类不是线程安全的,每个Schema推断任务应使用独立的实例。
 *
 * @see VariantSchema
 * @see PaimonShreddingUtils#variantShreddingSchema
 * @see VariantShreddingWriter
 */
public class InferVariantShreddingSchema {

    /** 表的原始Schema,包含待推断的Variant字段。 */
    private final RowType schema;

    /** Variant字段在Schema中的路径列表,如[[0], [1, 2]]表示第0个字段和第1个字段的第2个子字段。 */
    private final List<List<Integer>> pathsToVariant;

    /** 最大Schema宽度,限制总字段数以控制列数。 */
    private final int maxSchemaWidth;

    /** 最大Schema深度,限制嵌套层数以避免过深的递归。 */
    private final int maxSchemaDepth;

    /**
     * 最小字段基数比率,用于过滤稀有字段。
     *
     * <p>只有在至少 minFieldCardinalityRatio * numNonNullValues 行中出现的字段才会被切片。
     * 例如0.1表示至少10%的非null行包含该字段。
     */
    private final double minFieldCardinalityRatio;

    /**
     * 创建Variant切片模式推断器。
     *
     * @param schema 表的原始Schema,包含Variant类型字段
     * @param maxSchemaWidth 最大Schema宽度,限制总字段数(所有层级)
     * @param maxSchemaDepth 最大Schema深度,限制嵌套层数
     * @param minFieldCardinalityRatio 最小字段基数比率(0.0-1.0),用于过滤稀有字段
     */
    public InferVariantShreddingSchema(
            RowType schema,
            int maxSchemaWidth,
            int maxSchemaDepth,
            double minFieldCardinalityRatio) {
        this.schema = schema;
        this.pathsToVariant = getPathsToVariant(schema);
        this.maxSchemaWidth = maxSchemaWidth;
        this.maxSchemaDepth = maxSchemaDepth;
        this.minFieldCardinalityRatio = minFieldCardinalityRatio;
    }

    /**
     * 从一批行数据中推断切片Schema。
     *
     * <p>该方法会:
     * <ol>
     *   <li>遍历所有包含Variant的字段路径
     *   <li>收集每个路径上的Variant值,推断其类型
     *   <li>合并同一路径下不同行的类型
     *   <li>根据字段出现频率过滤稀有字段
     *   <li>应用宽度和深度限制
     *   <li>将推断的Schema插入原始Schema中
     * </ol>
     *
     * @param rows 用于推断的样本行数据
     * @return 包含切片Schema的新RowType
     */
    public RowType inferSchema(List<InternalRow> rows) {
        MaxFields maxFields = new MaxFields(maxSchemaWidth);
        Map<List<Integer>, RowType> inferredSchemas = new HashMap<>();

        for (List<Integer> path : pathsToVariant) {
            int numNonNullValues = 0;
            DataType simpleSchema = null;

            for (InternalRow row : rows) {
                Variant variant = getValueAtPath(schema, row, path);
                if (variant != null) {
                    numNonNullValues++;
                    GenericVariant v = (GenericVariant) variant;
                    DataType schemaOfRow = schemaOf(v, maxSchemaDepth);
                    simpleSchema = mergeSchema(simpleSchema, schemaOfRow);
                }
            }

            // Don't infer a schema for fields that appear in less than minFieldCardinalityRatio
            int minCardinality = (int) Math.ceil(numNonNullValues * minFieldCardinalityRatio);

            DataType finalizedSchema =
                    finalizeSimpleSchema(simpleSchema, minCardinality, maxFields);
            RowType shreddingSchema = PaimonShreddingUtils.variantShreddingSchema(finalizedSchema);
            inferredSchemas.put(path, shreddingSchema);
        }

        // Insert each inferred schema into the full schema
        return updateSchema(schema, inferredSchemas, new ArrayList<>());
    }

    /**
     * 创建Schema中Variant字段的路径列表。
     *
     * <p>嵌套在数组或Map中的Variant字段不会被包含。
     *
     * <p>示例: 如果Schema是 {@code row<v: variant, row<a: int, b: int, c: variant>>},
     * 该方法将返回 [[0], [1, 2]],表示:
     * <ul>
     *   <li>[0] - 第0个字段v是Variant
     *   <li>[1, 2] - 第1个字段的第2个子字段c是Variant
     * </ul>
     *
     * @param schema 要分析的RowType
     * @return Variant字段路径列表,每个路径是字段索引的列表
     */
    private List<List<Integer>> getPathsToVariant(RowType schema) {
        List<List<Integer>> result = new ArrayList<>();
        List<DataField> fields = schema.getFields();

        for (int idx = 0; idx < fields.size(); idx++) {
            DataField field = fields.get(idx);
            DataType dataType = field.type();

            if (dataType instanceof VariantType) {
                List<Integer> path = new ArrayList<>();
                path.add(idx);
                result.add(path);
            } else if (dataType instanceof RowType) {
                // Prepend this index to each downstream path
                List<List<Integer>> innerPaths = getPathsToVariant((RowType) dataType);
                for (List<Integer> path : innerPaths) {
                    List<Integer> fullPath = new ArrayList<>();
                    fullPath.add(idx);
                    fullPath.addAll(path);
                    result.add(fullPath);
                }
            }
        }

        return result;
    }

    /**
     * 返回指定路径上的Variant值。
     *
     * <p>如果Variant值或其任何包含的行为null,则返回null。
     *
     * @param schema 行的Schema
     * @param row 要从中提取值的行
     * @param path Variant字段的路径
     * @return Variant值,如果路径上任何部分为null则返回null
     */
    private Variant getValueAtPath(RowType schema, InternalRow row, List<Integer> path) {
        return getValueAtPathHelper(schema, row, path, 0);
    }

    /**
     * 获取路径上Variant值的递归辅助方法。
     *
     * @param schema 当前层级的Schema
     * @param row 当前层级的行
     * @param path 完整路径
     * @param pathIndex 当前路径索引
     * @return Variant值或null
     */
    private Variant getValueAtPathHelper(
            RowType schema, InternalRow row, List<Integer> path, int pathIndex) {
        int idx = path.get(pathIndex);

        if (row.isNullAt(idx)) {
            return null;
        } else if (pathIndex == path.size() - 1) {
            // We've reached the Variant value
            return row.getVariant(idx);
        } else {
            // The field must be a row
            RowType childRowType = (RowType) schema.getFields().get(idx).type();
            InternalRow childRow = row.getRow(idx, childRowType.getFieldCount());
            return getValueAtPathHelper(childRowType, childRow, path, pathIndex + 1);
        }
    }

    /**
     * 为Variant值推断合适的切片Schema。
     *
     * <p>该方法与SchemaOfVariant表达式类似,但规则有所不同,因为我们希望类型与切片时允许的类型一致。
     * 例如SchemaOfVariant会将Integer和Double的公共类型视为double,但我们将其视为VariantType,
     * 因为切片不允许将这些类型写入同一个typed_value。
     *
     * <p>我们还在行字段上维护元数据以跟踪它们出现的频率。稀有字段在最终Schema中会被丢弃。
     *
     * <h4>类型映射规则:</h4>
     * <ul>
     *   <li>OBJECT → RowType (深度>0时) 或 VARIANT (深度≤0时)
     *   <li>ARRAY → ArrayType (深度>0时) 或 VARIANT (深度≤0时)
     *   <li>NULL → null (允许null出现在任何类型化Schema中)
     *   <li>BOOLEAN → BOOLEAN
     *   <li>LONG → DECIMAL(精度,0) 或 BIGINT (精度>18时)
     *   <li>STRING → STRING
     *   <li>DOUBLE → DOUBLE
     *   <li>DECIMAL → DECIMAL(精度,小数位)
     *   <li>DATE → DATE
     *   <li>TIMESTAMP → TIMESTAMP_WITH_LOCAL_TIME_ZONE
     *   <li>TIMESTAMP_NTZ → TIMESTAMP (不带时区)
     *   <li>FLOAT → FLOAT
     *   <li>BINARY → BYTES
     * </ul>
     *
     * @param v Variant值
     * @param maxDepth 最大递归深度,0表示不再递归
     * @return 推断的DataType,字段的description临时存储出现次数
     */
    private DataType schemaOf(GenericVariant v, int maxDepth) {
        GenericVariantUtil.Type type = v.getType();

        switch (type) {
            case OBJECT:
                if (maxDepth <= 0) {
                    return DataTypes.VARIANT();
                }

                int size = v.objectSize();
                List<DataField> fields = new ArrayList<>(size);

                for (int i = 0; i < size; i++) {
                    GenericVariant.ObjectField field = v.getFieldAtIndex(i);
                    DataType fieldType = schemaOf(field.value, maxDepth - 1);
                    // Store count in description temporarily (will be used in mergeRowTypes)
                    DataField dataField = new DataField(i, field.key, fieldType, "1");
                    fields.add(dataField);
                }

                // According to the variant spec, object fields must be sorted alphabetically
                for (int i = 1; i < size; i++) {
                    if (fields.get(i - 1).name().compareTo(fields.get(i).name()) >= 0) {
                        throw new RuntimeException(
                                "Variant object fields must be sorted alphabetically");
                    }
                }

                return new RowType(fields);

            case ARRAY:
                if (maxDepth <= 0) {
                    return DataTypes.VARIANT();
                }

                DataType elementType = null;
                for (int i = 0; i < v.arraySize(); i++) {
                    elementType =
                            mergeSchema(
                                    elementType, schemaOf(v.getElementAtIndex(i), maxDepth - 1));
                }
                return new ArrayType(elementType == null ? DataTypes.VARIANT() : elementType);

            case NULL:
                return null;

            case BOOLEAN:
                return DataTypes.BOOLEAN();

            case LONG:
                // Compute the smallest decimal that can contain this value
                BigDecimal d = BigDecimal.valueOf(v.getLong());
                int precision = d.precision();
                if (precision <= 18) {
                    return DataTypes.DECIMAL(precision, 0);
                } else {
                    return DataTypes.BIGINT();
                }

            case STRING:
                return DataTypes.STRING();

            case DOUBLE:
                return DataTypes.DOUBLE();

            case DECIMAL:
                BigDecimal dec = v.getDecimal();
                int decPrecision = dec.precision();
                int decScale = dec.scale();
                // Ensure precision is at least scale + 1 to be valid
                if (decPrecision < decScale) {
                    decPrecision = decScale;
                }
                // Ensure precision is at least 1
                if (decPrecision == 0) {
                    decPrecision = 1;
                }
                return DataTypes.DECIMAL(decPrecision, decScale);

            case DATE:
                return DataTypes.DATE();

            case TIMESTAMP:
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE();

            case TIMESTAMP_NTZ:
                return DataTypes.TIMESTAMP();

            case FLOAT:
                return DataTypes.FLOAT();

            case BINARY:
                return DataTypes.BYTES();

            default:
                return DataTypes.VARIANT();
        }
    }

    /**
     * 从字段的description中读取出现次数。
     *
     * <p>在Schema推断过程中,我们临时将字段出现次数存储在description字段中。
     *
     * @param field 数据字段
     * @return 该字段在样本数据中的出现次数
     * @throws IllegalStateException 如果description为null或空(不应发生)
     */
    private long getFieldCount(DataField field) {
        // Read count from description field
        String desc = field.description();
        if (desc == null || desc.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "Field '%s' is missing count in description. This should not happen during schema inference.",
                            field.name()));
        }
        return Long.parseLong(desc);
    }

    /**
     * 合并两个可能有不同小数位的Decimal类型。
     *
     * <p>合并规则:
     * <ul>
     *   <li>小数位(scale) = max(d1.scale, d2.scale)
     *   <li>整数位(range) = max(d1.precision - d1.scale, d2.precision - d2.scale)
     *   <li>精度(precision) = range + scale
     *   <li>如果精度>38,则降级为VARIANT类型
     * </ul>
     *
     * @param d1 第一个Decimal类型
     * @param d2 第二个Decimal类型
     * @return 合并后的Decimal类型或VARIANT类型
     */
    private DataType mergeDecimal(DecimalType d1, DecimalType d2) {
        int scale = Math.max(d1.getScale(), d2.getScale());
        int range = Math.max(d1.getPrecision() - d1.getScale(), d2.getPrecision() - d2.getScale());

        if (range + scale > DecimalType.MAX_PRECISION) {
            // DecimalType can't support precision > 38
            return DataTypes.VARIANT();
        } else {
            return DataTypes.DECIMAL(range + scale, scale);
        }
    }

    /**
     * 合并Decimal类型和Long类型。
     *
     * <p>合并规则:
     * <ul>
     *   <li>如果Decimal的小数位=0且精度≤18,则为整数型Decimal,合并为BIGINT
     *   <li>否则,Long视为DECIMAL(19, 0),与Decimal合并
     * </ul>
     *
     * @param d Decimal类型
     * @return 合并后的类型(BIGINT或扩展的DECIMAL)
     */
    private DataType mergeDecimalWithLong(DecimalType d) {
        if (d.getScale() == 0 && d.getPrecision() <= 18) {
            // It's an integer-like Decimal
            return DataTypes.BIGINT();
        } else {
            // Long can always fit in a Decimal(19, 0)
            return mergeDecimal(d, DataTypes.DECIMAL(19, 0));
        }
    }

    /**
     * 合并两个DataType Schema。
     *
     * <p>合并规则如下:
     * <ul>
     *   <li>null + T → T (null与任何类型兼容)
     *   <li>Decimal + Decimal → 合并精度和小数位
     *   <li>Decimal + Long → 合并为BIGINT或扩展的Decimal
     *   <li>RowType + RowType → 字段并集,按字母顺序排列
     *   <li>ArrayType + ArrayType → 元素类型合并
     *   <li>T + T → T (相同类型)
     *   <li>其他不兼容类型 → VARIANT
     * </ul>
     *
     * @param dt1 第一个DataType
     * @param dt2 第二个DataType
     * @return 合并后的DataType
     */
    private DataType mergeSchema(DataType dt1, DataType dt2) {
        // Allow null to appear in any typed schema
        if (dt1 == null) {
            return dt2;
        } else if (dt2 == null) {
            return dt1;
        } else if (dt1 instanceof DecimalType && dt2 instanceof DecimalType) {
            return mergeDecimal((DecimalType) dt1, (DecimalType) dt2);
        } else if (dt1 instanceof DecimalType
                && dt2.getTypeRoot() == org.apache.paimon.types.DataTypeRoot.BIGINT) {
            return mergeDecimalWithLong((DecimalType) dt1);
        } else if (dt1.getTypeRoot() == org.apache.paimon.types.DataTypeRoot.BIGINT
                && dt2 instanceof DecimalType) {
            return mergeDecimalWithLong((DecimalType) dt2);
        } else if (dt1 instanceof RowType && dt2 instanceof RowType) {
            return mergeRowTypes((RowType) dt1, (RowType) dt2);
        } else if (dt1 instanceof ArrayType && dt2 instanceof ArrayType) {
            ArrayType a1 = (ArrayType) dt1;
            ArrayType a2 = (ArrayType) dt2;
            return new ArrayType(mergeSchema(a1.getElementType(), a2.getElementType()));
        } else if (dt1.equals(dt2)) {
            return dt1;
        } else {
            return DataTypes.VARIANT();
        }
    }

    /**
     * 合并两个RowType Schema。
     *
     * <p>该方法执行字段的并集操作,规则如下:
     * <ol>
     *   <li>对两个RowType的字段按字母顺序排序
     *   <li>相同名称的字段:递归合并类型,累加出现次数
     *   <li>仅在s1中的字段:直接添加
     *   <li>仅在s2中的字段:直接添加
     *   <li>字段总数限制为1000个
     * </ol>
     *
     * <p>字段的出现次数临时存储在description中,用于后续的频率过滤。
     *
     * @param s1 第一个RowType
     * @param s2 第二个RowType
     * @return 合并后的RowType,包含所有字段及其累加的出现次数
     */
    private DataType mergeRowTypes(RowType s1, RowType s2) {
        List<DataField> fields1 = s1.getFields();
        List<DataField> fields2 = s2.getFields();
        List<DataField> newFields = new ArrayList<>();

        int f1Idx = 0;
        int f2Idx = 0;
        int maxRowFieldSize = 1000;
        int nextFieldId = 0;

        while (f1Idx < fields1.size()
                && f2Idx < fields2.size()
                && newFields.size() < maxRowFieldSize) {
            DataField field1 = fields1.get(f1Idx);
            DataField field2 = fields2.get(f2Idx);
            String f1Name = field1.name();
            String f2Name = field2.name();
            int comp = f1Name.compareTo(f2Name);

            if (comp == 0) {
                DataType dataType = mergeSchema(field1.type(), field2.type());
                long c1 = getFieldCount(field1);
                long c2 = getFieldCount(field2);
                // Store count in description
                DataField newField =
                        new DataField(nextFieldId++, f1Name, dataType, String.valueOf(c1 + c2));
                newFields.add(newField);
                f1Idx++;
                f2Idx++;
            } else if (comp < 0) {
                long count = getFieldCount(field1);
                DataField newField =
                        new DataField(
                                nextFieldId++, field1.name(), field1.type(), String.valueOf(count));
                newFields.add(newField);
                f1Idx++;
            } else {
                long count = getFieldCount(field2);
                DataField newField =
                        new DataField(
                                nextFieldId++, field2.name(), field2.type(), String.valueOf(count));
                newFields.add(newField);
                f2Idx++;
            }
        }

        while (f1Idx < fields1.size() && newFields.size() < maxRowFieldSize) {
            DataField field1 = fields1.get(f1Idx);
            long count = getFieldCount(field1);
            DataField newField =
                    new DataField(
                            nextFieldId++, field1.name(), field1.type(), String.valueOf(count));
            newFields.add(newField);
            f1Idx++;
        }

        while (f2Idx < fields2.size() && newFields.size() < maxRowFieldSize) {
            DataField field2 = fields2.get(f2Idx);
            long count = getFieldCount(field2);
            DataField newField =
                    new DataField(
                            nextFieldId++, field2.name(), field2.type(), String.valueOf(count));
            newFields.add(newField);
            f2Idx++;
        }

        return new RowType(newFields);
    }

    /**
     * 返回新的Schema,将每个VariantType替换为其推断的切片Schema。
     *
     * <p>该方法递归遍历Schema树,将指定路径上的Variant字段替换为推断出的切片Schema。
     *
     * @param schema 原始Schema
     * @param inferredSchemas 推断出的切片Schema映射(路径 → Schema)
     * @param path 当前路径
     * @return 更新后的RowType
     * @throws IllegalStateException 如果路径上的Variant字段没有对应的推断Schema
     */
    private RowType updateSchema(
            RowType schema, Map<List<Integer>, RowType> inferredSchemas, List<Integer> path) {

        List<DataField> fields = schema.getFields();
        List<DataField> newFields = new ArrayList<>(fields.size());

        for (int idx = 0; idx < fields.size(); idx++) {
            DataField field = fields.get(idx);
            DataType dataType = field.type();

            if (dataType instanceof VariantType) {
                List<Integer> fullPath = new ArrayList<>(path);
                fullPath.add(idx);
                if (!inferredSchemas.containsKey(fullPath)) {
                    throw new IllegalStateException(
                            String.format(
                                    "No inferred schema found for Variant field '%s' at path %s",
                                    field.name(), fullPath));
                }
                newFields.add(field.newType(inferredSchemas.get(fullPath)));
            } else if (dataType instanceof RowType) {
                List<Integer> fullPath = new ArrayList<>(path);
                fullPath.add(idx);
                RowType newType = updateSchema((RowType) dataType, inferredSchemas, fullPath);
                newFields.add(field.newType(newType));
            } else {
                newFields.add(field);
            }
        }

        return new RowType(newFields);
    }

    /**
     * 可变整数容器,用于跟踪切片字段的总数。
     *
     * <p>在finalize过程中,每次添加字段时递减remaining,当remaining≤0时停止添加更多字段。
     */
    private static class MaxFields {
        /** 剩余可用的字段数量。 */
        int remaining;

        /**
         * 创建MaxFields容器。
         *
         * @param remaining 初始可用字段数
         */
        MaxFields(int remaining) {
            this.remaining = remaining;
        }
    }

    /**
     * 最终化Variant的Schema。
     *
     * <p>该方法执行以下操作:
     * <ol>
     *   <li>将整数类型统一扩展为LongType (TINYINT/SMALLINT/INT → BIGINT)
     *   <li>将整数型Decimal(精度≤18,小数位=0)转换为LongType
     *   <li>限制Decimal精度不超过38,小数位=0的Decimal精度≤18
     *   <li>将空的RowType替换为VariantType
     *   <li>递归处理RowType,过滤出现频率低于minCardinality的字段
     *   <li>递归处理ArrayType
     *   <li>限制切片字段总数不超过maxFields.remaining
     * </ol>
     *
     * <p>标量类型占用字段计数:
     * <ul>
     *   <li>每个字段占用1个value列
     *   <li>整数类型额外占用1个typed_value列
     *   <li>其他标量类型额外占用1个typed_value列
     * </ul>
     *
     * @param dt 要最终化的DataType
     * @param minCardinality 字段的最小出现次数阈值
     * @param maxFields 剩余可用字段数的容器
     * @return 最终化的DataType
     */
    private DataType finalizeSimpleSchema(DataType dt, int minCardinality, MaxFields maxFields) {

        // Every field uses a value column
        maxFields.remaining--;
        if (maxFields.remaining <= 0) {
            return DataTypes.VARIANT();
        }

        // Handle null type first
        if (dt == null || dt instanceof VariantType) {
            return DataTypes.VARIANT();
        }

        if (dt instanceof RowType) {
            RowType rowType = (RowType) dt;
            List<DataField> newFields = new ArrayList<>();
            int fieldId = 0;

            for (DataField field : rowType.getFields()) {
                if (getFieldCount(field) >= minCardinality && maxFields.remaining > 0) {
                    DataType newType =
                            finalizeSimpleSchema(field.type(), minCardinality, maxFields);
                    // Clear description after finalizing
                    newFields.add(new DataField(fieldId++, field.name(), newType, null));
                }
            }

            if (!newFields.isEmpty()) {
                return new RowType(newFields);
            } else {
                return DataTypes.VARIANT();
            }
        } else if (dt instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) dt;
            DataType newElementType =
                    finalizeSimpleSchema(arrayType.getElementType(), minCardinality, maxFields);
            return new ArrayType(newElementType);
        } else if (dt.getTypeRoot() == org.apache.paimon.types.DataTypeRoot.TINYINT
                || dt.getTypeRoot() == org.apache.paimon.types.DataTypeRoot.SMALLINT
                || dt.getTypeRoot() == org.apache.paimon.types.DataTypeRoot.INTEGER
                || dt.getTypeRoot() == org.apache.paimon.types.DataTypeRoot.BIGINT) {
            maxFields.remaining--;
            return DataTypes.BIGINT();
        } else if (dt instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) dt;
            if (decimalType.getPrecision() <= 18 && decimalType.getScale() == 0) {
                maxFields.remaining--;
                return DataTypes.BIGINT();
            } else {
                maxFields.remaining--;
                if (decimalType.getPrecision() <= 18) {
                    return DataTypes.DECIMAL(18, decimalType.getScale());
                } else {
                    return DataTypes.DECIMAL(DecimalType.MAX_PRECISION, decimalType.getScale());
                }
            }
        } else {
            // All other scalar types use typed_value
            maxFields.remaining--;
            return dt;
        }
    }
}
