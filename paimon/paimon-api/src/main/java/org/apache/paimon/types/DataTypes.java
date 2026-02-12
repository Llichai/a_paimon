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

import org.apache.paimon.annotation.Public;

import java.util.Arrays;
import java.util.OptionalInt;

/**
 * 用于创建 {@link DataType} 的工具类。
 *
 * <p>DataTypes 是创建各种数据类型实例的工厂类,提供了便捷的静态方法来构造所有支持的数据类型。
 * 这是创建 Paimon 数据类型的推荐方式,相比直接使用构造函数更加简洁和类型安全。
 *
 * <p>设计特点:
 * <ul>
 *     <li><b>流式 API</b>: 支持链式调用,可以方便地设置类型属性</li>
 *     <li><b>类型安全</b>: 所有方法都经过精心设计,避免类型错误</li>
 *     <li><b>默认值</b>: 为带参数的类型提供合理的默认值</li>
 *     <li><b>SQL 兼容</b>: 方法名称和参数与 SQL 类型定义保持一致</li>
 * </ul>
 *
 * <p>支持的数据类型分类:
 * <ul>
 *     <li><b>整数类型</b>: TINYINT, SMALLINT, INT, BIGINT</li>
 *     <li><b>浮点类型</b>: FLOAT, DOUBLE</li>
 *     <li><b>定点类型</b>: DECIMAL(precision, scale)</li>
 *     <li><b>布尔类型</b>: BOOLEAN</li>
 *     <li><b>字符串类型</b>: CHAR(length), VARCHAR(length), STRING</li>
 *     <li><b>二进制类型</b>: BINARY(length), VARBINARY(length), BYTES, BLOB</li>
 *     <li><b>日期时间类型</b>: DATE, TIME, TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE</li>
 *     <li><b>复杂类型</b>: ARRAY, MAP, ROW, MULTISET</li>
 *     <li><b>特殊类型</b>: VARIANT (半结构化数据)</li>
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 基本类型
 * DataType intType = DataTypes.INT();
 * DataType stringType = DataTypes.STRING();
 *
 * // 带参数的类型
 * DataType decimalType = DataTypes.DECIMAL(10, 2);  // DECIMAL(10, 2)
 * DataType varcharType = DataTypes.VARCHAR(100);    // VARCHAR(100)
 *
 * // 复杂类型
 * DataType arrayType = DataTypes.ARRAY(DataTypes.INT());
 * DataType mapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());
 *
 * // 行类型(方式1: 使用 DataField)
 * DataType rowType1 = DataTypes.ROW(
 *     DataTypes.FIELD(0, "id", DataTypes.INT()),
 *     DataTypes.FIELD(1, "name", DataTypes.STRING())
 * );
 *
 * // 行类型(方式2: 只指定类型,字段名自动生成为 f0, f1, ...)
 * DataType rowType2 = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());
 *
 * // 设置可空性
 * DataType notNullInt = DataTypes.INT().notNull();
 * DataType nullableString = DataTypes.STRING().nullable();
 * }</pre>
 *
 * @see DataType 数据类型基类
 * @since 0.4.0
 */
@Public
public class DataTypes {

    /** 创建一个 4 字节有符号整数类型。范围: -2,147,483,648 到 2,147,483,647。 */
    public static IntType INT() {
        return new IntType();
    }

    /** 创建一个 1 字节有符号整数类型。范围: -128 到 127。 */
    public static TinyIntType TINYINT() {
        return new TinyIntType();
    }

    /** 创建一个 2 字节有符号整数类型。范围: -32,768 到 32,767。 */
    public static SmallIntType SMALLINT() {
        return new SmallIntType();
    }

    /** 创建一个 8 字节有符号整数类型。范围: -9,223,372,036,854,775,808 到 9,223,372,036,854,775,807。 */
    public static BigIntType BIGINT() {
        return new BigIntType();
    }

    /**
     * 创建一个无长度限制的变长字符串类型。
     *
     * <p>这是最常用的字符串类型,等价于 VARCHAR(2147483647)。
     */
    public static VarCharType STRING() {
        return VarCharType.STRING_TYPE;
    }

    /** 创建一个 8 字节双精度浮点数类型(IEEE 754)。 */
    public static DoubleType DOUBLE() {
        return new DoubleType();
    }

    /**
     * 创建一个数组类型。
     *
     * @param element 数组元素的数据类型
     */
    public static ArrayType ARRAY(DataType element) {
        return new ArrayType(element);
    }

    /**
     * 创建一个定长字符类型。
     *
     * @param length 字符长度(字符数,非字节数)
     */
    public static CharType CHAR(int length) {
        return new CharType(length);
    }

    /**
     * 创建一个变长字符串类型。
     *
     * @param length 最大字符长度(字符数,非字节数)
     */
    public static VarCharType VARCHAR(int length) {
        return new VarCharType(length);
    }

    /** 创建一个布尔类型,取值为 true 或 false。 */
    public static BooleanType BOOLEAN() {
        return new BooleanType();
    }

    /** 创建一个日期类型,表示年-月-日。 */
    public static DateType DATE() {
        return new DateType();
    }

    /** 创建一个时间类型,使用默认精度 0(秒级)。 */
    public static TimeType TIME() {
        return new TimeType();
    }

    /**
     * 创建一个时间类型,指定精度。
     *
     * @param precision 小数秒精度,范围 0-9,默认为 0
     */
    public static TimeType TIME(int precision) {
        return new TimeType(precision);
    }

    /** 创建一个时间戳类型(不带时区),使用默认精度 6(微秒级)。 */
    public static TimestampType TIMESTAMP() {
        return new TimestampType();
    }

    /**
     * 创建一个时间戳类型(不带时区),精度为毫秒级(3位小数)。
     *
     * <p>这是常用的毫秒级时间戳,与 Java 的 System.currentTimeMillis() 对应。
     */
    public static TimestampType TIMESTAMP_MILLIS() {
        return new TimestampType(3);
    }

    /**
     * 创建一个时间戳类型(不带时区),指定精度。
     *
     * @param precision 小数秒精度,范围 0-9,默认为 6
     */
    public static TimestampType TIMESTAMP(int precision) {
        return new TimestampType(precision);
    }

    /** 创建一个带本地时区的时间戳类型,使用默认精度 6(微秒级)。 */
    public static LocalZonedTimestampType TIMESTAMP_WITH_LOCAL_TIME_ZONE() {
        return new LocalZonedTimestampType();
    }

    /**
     * 创建一个带本地时区的时间戳类型,指定精度。
     *
     * @param precision 小数秒精度,范围 0-9,默认为 6
     */
    public static LocalZonedTimestampType TIMESTAMP_WITH_LOCAL_TIME_ZONE(int precision) {
        return new LocalZonedTimestampType(precision);
    }

    /**
     * 创建一个带本地时区的时间戳类型,精度为毫秒级(3位小数)。
     *
     * <p>缩写方法名,等价于 TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)。
     */
    public static LocalZonedTimestampType TIMESTAMP_LTZ_MILLIS() {
        return new LocalZonedTimestampType(3);
    }

    /**
     * 创建一个定点数类型(DECIMAL)。
     *
     * <p>DECIMAL 类型用于精确存储小数,常用于金融计算等需要精确计算的场景。
     *
     * @param precision 总位数(包括小数部分),范围 1-38
     * @param scale 小数位数,范围 0-precision
     */
    public static DecimalType DECIMAL(int precision, int scale) {
        return new DecimalType(precision, scale);
    }

    /**
     * 创建一个无长度限制的变长二进制类型。
     *
     * <p>这是最常用的二进制类型,等价于 VARBINARY(2147483647)。
     */
    public static VarBinaryType BYTES() {
        return new VarBinaryType(VarBinaryType.MAX_LENGTH);
    }

    /** 创建一个 4 字节单精度浮点数类型(IEEE 754)。 */
    public static FloatType FLOAT() {
        return new FloatType();
    }

    /**
     * 创建一个映射(Map)类型。
     *
     * @param keyType 键的数据类型
     * @param valueType 值的数据类型
     */
    public static MapType MAP(DataType keyType, DataType valueType) {
        return new MapType(keyType, valueType);
    }

    /**
     * 创建一个数据字段,不包含描述信息。
     *
     * @param id 字段 ID
     * @param name 字段名称
     * @param type 字段类型
     */
    public static DataField FIELD(int id, String name, DataType type) {
        return new DataField(id, name, type);
    }

    /**
     * 创建一个数据字段,包含描述信息。
     *
     * @param id 字段 ID
     * @param name 字段名称
     * @param type 字段类型
     * @param description 字段描述(注释)
     */
    public static DataField FIELD(int id, String name, DataType type, String description) {
        return new DataField(id, name, type, description);
    }

    /**
     * 创建一个行类型,由多个数据字段组成。
     *
     * @param fields 字段数组
     */
    public static RowType ROW(DataField... fields) {
        return new RowType(Arrays.asList(fields));
    }

    /**
     * 创建一个行类型,只指定字段类型。
     *
     * <p>字段名称会自动生成为 f0, f1, f2, ...,字段 ID 也会自动分配。
     *
     * @param fieldTypes 字段类型数组
     */
    public static RowType ROW(DataType... fieldTypes) {
        return RowType.builder().fields(fieldTypes).build();
    }

    /**
     * 创建一个定长二进制类型。
     *
     * @param length 字节长度
     */
    public static BinaryType BINARY(int length) {
        return new BinaryType(length);
    }

    /**
     * 创建一个变长二进制类型。
     *
     * @param length 最大字节长度
     */
    public static VarBinaryType VARBINARY(int length) {
        return new VarBinaryType(length);
    }

    /**
     * 创建一个多重集(Multiset)类型。
     *
     * <p>多重集是允许重复元素的集合类型。
     *
     * @param elementType 元素的数据类型
     */
    public static MultisetType MULTISET(DataType elementType) {
        return new MultisetType(elementType);
    }

    /**
     * 创建一个半结构化数据类型(Variant)。
     *
     * <p>Variant 类型可以存储 JSON 等半结构化数据,支持嵌套和动态模式。
     */
    public static VariantType VARIANT() {
        return new VariantType();
    }

    /**
     * 创建一个二进制大对象(BLOB)类型。
     *
     * <p>用于存储大型二进制数据,如图片、视频等。
     */
    public static BlobType BLOB() {
        return new BlobType();
    }

    /**
     * 获取数据类型的精度(如果适用)。
     *
     * <p>精度适用于以下类型:
     * <ul>
     *     <li>DECIMAL: 总位数</li>
     *     <li>TIME: 小数秒位数</li>
     *     <li>TIMESTAMP: 小数秒位数</li>
     *     <li>TIMESTAMP_WITH_LOCAL_TIME_ZONE: 小数秒位数</li>
     * </ul>
     *
     * @param dataType 数据类型
     * @return 精度值,如果类型不支持精度则返回空
     */
    public static OptionalInt getPrecision(DataType dataType) {
        return dataType.accept(PRECISION_EXTRACTOR);
    }

    /**
     * 获取数据类型的长度(如果适用)。
     *
     * <p>长度适用于以下类型:
     * <ul>
     *     <li>CHAR: 字符长度</li>
     *     <li>VARCHAR: 最大字符长度</li>
     *     <li>BINARY: 字节长度</li>
     *     <li>VARBINARY: 最大字节长度</li>
     * </ul>
     *
     * @param dataType 数据类型
     * @return 长度值,如果类型不支持长度则返回空
     */
    public static OptionalInt getLength(DataType dataType) {
        return dataType.accept(LENGTH_EXTRACTOR);
    }

    /** 精度提取器单例,用于提取类型的精度信息 */
    private static final PrecisionExtractor PRECISION_EXTRACTOR = new PrecisionExtractor();

    /** 长度提取器单例,用于提取类型的长度信息 */
    private static final LengthExtractor LENGTH_EXTRACTOR = new LengthExtractor();

    /**
     * 精度提取访问者,用于从数据类型中提取精度信息。
     *
     * <p>通过访问者模式遍历类型,对于支持精度的类型返回精度值,
     * 对于不支持精度的类型返回空。
     */
    private static class PrecisionExtractor extends DataTypeDefaultVisitor<OptionalInt> {

        @Override
        public OptionalInt visit(DecimalType decimalType) {
            return OptionalInt.of(decimalType.getPrecision());
        }

        @Override
        public OptionalInt visit(TimeType timeType) {
            return OptionalInt.of(timeType.getPrecision());
        }

        @Override
        public OptionalInt visit(TimestampType timestampType) {
            return OptionalInt.of(timestampType.getPrecision());
        }

        @Override
        public OptionalInt visit(LocalZonedTimestampType localZonedTimestampType) {
            return OptionalInt.of(localZonedTimestampType.getPrecision());
        }

        @Override
        protected OptionalInt defaultMethod(DataType dataType) {
            return OptionalInt.empty();
        }
    }

    /**
     * 长度提取访问者,用于从数据类型中提取长度信息。
     *
     * <p>通过访问者模式遍历类型,对于支持长度的类型返回长度值,
     * 对于不支持长度的类型返回空。
     */
    private static class LengthExtractor extends DataTypeDefaultVisitor<OptionalInt> {

        @Override
        public OptionalInt visit(CharType charType) {
            return OptionalInt.of(charType.getLength());
        }

        @Override
        public OptionalInt visit(VarCharType varCharType) {
            return OptionalInt.of(varCharType.getLength());
        }

        @Override
        public OptionalInt visit(BinaryType binaryType) {
            return OptionalInt.of(binaryType.getLength());
        }

        @Override
        public OptionalInt visit(VarBinaryType varBinaryType) {
            return OptionalInt.of(varBinaryType.getLength());
        }

        @Override
        protected OptionalInt defaultMethod(DataType dataType) {
            return OptionalInt.empty();
        }
    }
}
