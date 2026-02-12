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

package org.apache.paimon.types;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;

import static org.apache.paimon.types.DataTypeRoot.ROW;

/**
 * 用于检查 {@link DataType} 的工具类,避免大量的类型转换和重复性工作。
 *
 * <p>这个工具类提供了一系列静态方法,用于安全地提取和检查数据类型的各种属性,
 * 如长度、精度、标度、字段信息等。通过访问者模式实现,避免了显式的类型转换。
 *
 * <h2>核心功能</h2>
 * <ul>
 *   <li>类型属性提取 - 提取长度、精度、标度等属性
 *   <li>复合类型处理 - 统一处理 ROW 等复合类型
 *   <li>类型安全 - 使用访问者模式避免类型转换
 *   <li>嵌套类型支持 - 提取嵌套类型信息
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 检查类型是否为复合类型
 * DataType type = new RowType(...);
 * boolean isComposite = DataTypeChecks.isCompositeType(type);
 *
 * // 获取 DECIMAL 类型的精度
 * DecimalType decimalType = new DecimalType(10, 2);
 * int precision = DataTypeChecks.getPrecision(decimalType); // 返回 10
 *
 * // 获取 ROW 类型的字段名
 * RowType rowType = RowType.builder()
 *     .field("id", new IntType())
 *     .field("name", new VarCharType())
 *     .build();
 * List<String> fieldNames = DataTypeChecks.getFieldNames(rowType);
 * // 返回 ["id", "name"]
 *
 * // 检查字符串类型是否定义良好
 * boolean wellDefined = DataTypeChecks.hasWellDefinedString(new VarCharType());
 * }</pre>
 */
public final class DataTypeChecks {

    /** 长度提取器,用于提取支持长度的类型的长度。 */
    private static final LengthExtractor LENGTH_EXTRACTOR = new LengthExtractor();

    /** 精度提取器,用于提取支持精度的类型的精度。 */
    private static final PrecisionExtractor PRECISION_EXTRACTOR = new PrecisionExtractor();

    /** 标度提取器,用于提取支持标度的类型的标度。 */
    private static final ScaleExtractor SCALE_EXTRACTOR = new ScaleExtractor();

    /** 字段数提取器,用于提取复合类型的字段数量。 */
    private static final FieldCountExtractor FIELD_COUNT_EXTRACTOR = new FieldCountExtractor();

    /** 字段名提取器,用于提取复合类型的字段名列表。 */
    private static final FieldNamesExtractor FIELD_NAMES_EXTRACTOR = new FieldNamesExtractor();

    /** 字段类型提取器,用于提取复合类型的字段类型列表。 */
    private static final FieldTypesExtractor FIELD_TYPES_EXTRACTOR = new FieldTypesExtractor();

    /** 嵌套类型提取器,用于提取复杂类型的嵌套类型。 */
    private static final NestedTypesExtractor NESTED_TYPES_EXTRACTOR = new NestedTypesExtractor();

    /**
     * 检查给定类型是否为复合类型。
     *
     * <p>复合类型是指包含多个字段的类型,如 ROW 类型。
     * 使用 {@link #getFieldCount(DataType)}, {@link #getFieldNames(DataType)},
     * {@link #getFieldTypes(DataType)} 可以统一处理复合类型。
     *
     * @param dataType 要检查的数据类型
     * @return 如果是复合类型返回 true
     */
    public static boolean isCompositeType(DataType dataType) {
        return dataType.getTypeRoot() == ROW;
    }

    /**
     * 获取数据类型的长度。
     *
     * <p>适用于支持长度的类型,如 CHAR, VARCHAR, BINARY, VARBINARY。
     *
     * @param dataType 数据类型
     * @return 类型的长度
     * @throws IllegalArgumentException 如果类型不支持长度
     */
    public static int getLength(DataType dataType) {
        return dataType.accept(LENGTH_EXTRACTOR);
    }

    /**
     * 检查数据类型的长度是否等于指定值。
     *
     * @param dataType 数据类型
     * @param length 期望的长度
     * @return 如果长度匹配返回 true
     */
    public static boolean hasLength(DataType dataType, int length) {
        return getLength(dataType) == length;
    }

    /**
     * 返回所有隐式或显式定义精度的类型的精度。
     *
     * <p>适用于 DECIMAL, TIMESTAMP, TIME 等类型。
     *
     * @param dataType 数据类型,可以为 null
     * @return 类型的精度,如果 dataType 为 null 则返回 null
     * @throws IllegalArgumentException 如果类型不支持精度
     */
    public static Integer getPrecision(@Nullable DataType dataType) {
        return dataType == null ? null : dataType.accept(PRECISION_EXTRACTOR);
    }

    /**
     * 检查类型的精度是否等于指定值。
     *
     * @param dataType 数据类型
     * @param precision 期望的精度
     * @return 如果精度匹配返回 true
     */
    public static boolean hasPrecision(DataType dataType, int precision) {
        return getPrecision(dataType) == precision;
    }

    /**
     * 返回所有隐式或显式定义标度的类型的标度。
     *
     * <p>适用于 DECIMAL 类型和整数类型(整数类型的标度为 0)。
     *
     * @param dataType 数据类型
     * @return 类型的标度
     * @throws IllegalArgumentException 如果类型不支持标度
     */
    public static int getScale(DataType dataType) {
        return dataType.accept(SCALE_EXTRACTOR);
    }

    /**
     * 检查类型的标度是否等于指定值。
     *
     * @param dataType 数据类型
     * @param scale 期望的标度
     * @return 如果标度匹配返回 true
     */
    public static boolean hasScale(DataType dataType, int scale) {
        return getScale(dataType) == scale;
    }

    /**
     * 返回 ROW 和结构化类型的字段数。其他类型返回 1。
     *
     * @param dataType 数据类型
     * @return 字段数量,非复合类型返回 1
     */
    public static int getFieldCount(DataType dataType) {
        return dataType.accept(FIELD_COUNT_EXTRACTOR);
    }

    /**
     * 返回 ROW 和结构化类型的字段名列表。
     *
     * @param dataType 数据类型
     * @return 字段名列表
     * @throws IllegalArgumentException 如果类型不是复合类型
     */
    public static List<String> getFieldNames(DataType dataType) {
        return dataType.accept(FIELD_NAMES_EXTRACTOR);
    }

    /**
     * 返回 ROW 和结构化类型的字段类型列表。
     *
     * @param dataType 数据类型
     * @return 字段类型列表
     * @throws IllegalArgumentException 如果类型不是复合类型
     */
    public static List<DataType> getFieldTypes(DataType dataType) {
        return dataType.accept(FIELD_TYPES_EXTRACTOR);
    }

    /**
     * 获取数据类型的嵌套类型列表。
     *
     * <p>不同类型的嵌套类型:
     * <ul>
     *   <li>ARRAY - 返回元素类型
     *   <li>MULTISET - 返回元素类型
     *   <li>MAP - 返回键类型和值类型
     *   <li>ROW - 返回所有字段类型
     * </ul>
     *
     * @param dataType 数据类型
     * @return 嵌套类型列表
     */
    public static List<DataType> getNestedTypes(DataType dataType) {
        return dataType.accept(NESTED_TYPES_EXTRACTOR);
    }

    /**
     * 检查给定的 {@link DataType} 在其内部数据结构上调用 {@link Object#toString()} 时
     * 是否具有定义良好的字符串表示。该字符串表示在 SQL 或编程语言中都是类似的。
     *
     * <p>注意:一旦我们实现了可以将任何内部数据结构转换为定义良好的字符串表示的工具,
     * 这个方法可能就不再需要了。
     *
     * @param dataType 数据类型
     * @return 如果具有定义良好的字符串表示返回 true
     */
    public static boolean hasWellDefinedString(DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return true;
            default:
                return false;
        }
    }

    /** 私有构造函数,禁止实例化。 */
    private DataTypeChecks() {
        // no instantiation
    }

    // --------------------------------------------------------------------------------------------
    // 内部提取器类 - 使用访问者模式提取类型属性
    // --------------------------------------------------------------------------------------------

    /**
     * 提取器的基类,用于提取数据类型的特定属性。
     *
     * <p>如果在不支持该属性的类型上调用,会抛出 IllegalArgumentException。
     *
     * @param <T> 提取结果的类型
     */
    private static class Extractor<T> extends DataTypeDefaultVisitor<T> {
        @Override
        protected T defaultMethod(DataType dataType) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid use of extractor %s. Called on logical type: %s",
                            this.getClass().getName(), dataType));
        }
    }

    /**
     * 长度提取器,用于提取 CHAR, VARCHAR, BINARY, VARBINARY 类型的长度。
     */
    private static class LengthExtractor extends Extractor<Integer> {
        @Override
        protected Integer defaultMethod(DataType dataType) {
            OptionalInt length = DataTypes.getLength(dataType);
            if (length.isPresent()) {
                return length.getAsInt();
            }
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid use of extractor %s. Called on logical type: %s",
                            this.getClass().getName(), dataType));
        }
    }

    /**
     * 精度提取器,用于提取 DECIMAL, TIMESTAMP, TIME 等类型的精度。
     */
    private static class PrecisionExtractor extends Extractor<Integer> {

        @Override
        protected Integer defaultMethod(DataType dataType) {
            OptionalInt precision = DataTypes.getPrecision(dataType);
            if (precision.isPresent()) {
                return precision.getAsInt();
            }
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid use of extractor %s. Called on logical type: %s",
                            this.getClass().getName(), dataType));
        }
    }

    /**
     * 标度提取器,用于提取 DECIMAL 和整数类型的标度。
     *
     * <p>整数类型的标度固定为 0。
     */
    private static class ScaleExtractor extends Extractor<Integer> {

        @Override
        public Integer visit(DecimalType decimalType) {
            return decimalType.getScale();
        }

        @Override
        public Integer visit(TinyIntType tinyIntType) {
            return 0;
        }

        @Override
        public Integer visit(SmallIntType smallIntType) {
            return 0;
        }

        @Override
        public Integer visit(IntType intType) {
            return 0;
        }

        @Override
        public Integer visit(BigIntType bigIntType) {
            return 0;
        }
    }

    /**
     * 字段数提取器,用于提取 ROW 类型的字段数量。
     *
     * <p>对于非复合类型,默认返回 1。
     */
    private static class FieldCountExtractor extends Extractor<Integer> {

        @Override
        public Integer visit(RowType rowType) {
            return rowType.getFieldCount();
        }

        @Override
        protected Integer defaultMethod(DataType dataType) {
            return 1;
        }
    }

    /**
     * 字段名提取器,用于提取 ROW 类型的字段名列表。
     */
    private static class FieldNamesExtractor extends Extractor<List<String>> {

        @Override
        public List<String> visit(RowType rowType) {
            return rowType.getFieldNames();
        }
    }

    /**
     * 字段类型提取器,用于提取 ROW 类型的字段类型列表。
     */
    private static class FieldTypesExtractor extends Extractor<List<DataType>> {

        @Override
        public List<DataType> visit(RowType rowType) {
            return rowType.getFieldTypes();
        }
    }

    /**
     * 嵌套类型提取器,用于提取复杂类型的嵌套类型。
     *
     * <p>不同类型返回不同的嵌套类型:
     * <ul>
     *   <li>ARRAY - 元素类型
     *   <li>MULTISET - 元素类型
     *   <li>MAP - 键类型和值类型
     *   <li>ROW - 所有字段类型
     * </ul>
     */
    private static class NestedTypesExtractor extends Extractor<List<DataType>> {

        @Override
        public List<DataType> visit(ArrayType arrayType) {
            return Collections.singletonList(arrayType.getElementType());
        }

        @Override
        public List<DataType> visit(MultisetType multisetType) {
            return Collections.singletonList(multisetType.getElementType());
        }

        @Override
        public List<DataType> visit(MapType mapType) {
            return Arrays.asList(mapType.getKeyType(), mapType.getValueType());
        }

        @Override
        public List<DataType> visit(RowType rowType) {
            return rowType.getFieldTypes();
        }
    }
}
