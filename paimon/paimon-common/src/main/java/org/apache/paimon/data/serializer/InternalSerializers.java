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

package org.apache.paimon.data.serializer;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;

import static org.apache.paimon.types.DataTypeChecks.getFieldTypes;
import static org.apache.paimon.types.DataTypeChecks.getPrecision;
import static org.apache.paimon.types.DataTypeChecks.getScale;

/**
 * 内部数据结构序列化器工厂 - 根据 {@link DataType} 创建相应的 {@link Serializer}。
 *
 * <p>这是一个工厂类,为 Paimon 的内部数据结构提供统一的序列化器创建入口。
 * 支持所有 Paimon 数据类型,并自动选择最合适的序列化器实现。
 *
 * <p>支持的数据类型:
 * <ul>
 *   <li>字符串类型: CHAR, VARCHAR -> BinaryStringSerializer
 *   <li>基本类型: BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE
 *   <li>二进制类型: BINARY, VARBINARY -> BinarySerializer
 *   <li>数值类型: DECIMAL -> DecimalSerializer(带精度和小数位)
 *   <li>时间类型: DATE, TIME, TIMESTAMP -> IntSerializer 或 TimestampSerializer
 *   <li>复杂类型: ARRAY, MAP, MULTISET, ROW
 *   <li>特殊类型: VARIANT, BLOB
 * </ul>
 *
 * <p>设计特点:
 * <ul>
 *   <li>类型安全: 使用泛型保证类型匹配
 *   <li>自动选择: 根据数据类型自动选择最优序列化器
 *   <li>参数适配: 自动提取精度、小数位等类型参数
 *   <li>单一入口: 统一的序列化器创建接口
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建字符串序列化器
 * Serializer<BinaryString> stringSerializer =
 *     InternalSerializers.create(new VarCharType());
 *
 * // 创建 Decimal 序列化器(自动获取精度和小数位)
 * Serializer<Decimal> decimalSerializer =
 *     InternalSerializers.create(new DecimalType(10, 2));
 *
 * // 创建 Row 序列化器
 * InternalRowSerializer rowSerializer =
 *     InternalSerializers.create(
 *         RowType.of(new IntType(), new VarCharType()));
 * }</pre>
 */
public final class InternalSerializers {

    /**
     * 为给定数据类型的内部数据结构创建序列化器。
     *
     * <p>这是一个通用方法,返回类型安全的序列化器。
     *
     * @param type 数据类型
     * @param <T> 序列化对象的类型
     * @return 对应的序列化器
     * @throws UnsupportedOperationException 如果数据类型不支持
     */
    @SuppressWarnings("unchecked")
    public static <T> Serializer<T> create(DataType type) {
        return (Serializer<T>) createInternal(type);
    }

    /**
     * 为给定 RowType 的内部数据结构创建行序列化器。
     *
     * <p>这是一个特化方法,专门用于创建 InternalRowSerializer。
     *
     * @param type 行类型
     * @return InternalRowSerializer 实例
     */
    public static InternalRowSerializer create(RowType type) {
        return (InternalRowSerializer) createInternal(type);
    }

    /**
     * 内部实现: 根据数据类型创建对应的序列化器。
     *
     * <p>这个方法按照 TypeRoot 的定义顺序处理各种数据类型。
     *
     * @param type 数据类型
     * @return 对应的序列化器
     * @throws UnsupportedOperationException 如果数据类型不支持
     */
    private static Serializer<?> createInternal(DataType type) {
        // 按照 type root 定义的顺序处理
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                // 字符串类型使用 BinaryStringSerializer
                return BinaryStringSerializer.INSTANCE;
            case BOOLEAN:
                // 布尔类型
                return BooleanSerializer.INSTANCE;
            case BINARY:
            case VARBINARY:
                // 二进制类型使用 BinarySerializer
                return BinarySerializer.INSTANCE;
            case DECIMAL:
                // Decimal 类型,需要精度和小数位参数
                return new DecimalSerializer(getPrecision(type), getScale(type));
            case TINYINT:
                // 1 字节整数
                return ByteSerializer.INSTANCE;
            case SMALLINT:
                // 2 字节整数
                return ShortSerializer.INSTANCE;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                // 4 字节整数,DATE 和 TIME 内部表示为 int
                return IntSerializer.INSTANCE;
            case BIGINT:
                // 8 字节整数
                return LongSerializer.INSTANCE;
            case FLOAT:
                // 单精度浮点数
                return FloatSerializer.INSTANCE;
            case DOUBLE:
                // 双精度浮点数
                return DoubleSerializer.INSTANCE;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // 时间戳类型,需要精度参数
                return new TimestampSerializer(getPrecision(type));
            case ARRAY:
                // 数组类型,递归创建元素序列化器
                return new InternalArraySerializer(((ArrayType) type).getElementType());
            case MULTISET:
                // Multiset 类型,内部实现为 Map<Element, Integer>
                return new InternalMapSerializer(
                        ((MultisetType) type).getElementType(), new IntType(false));
            case MAP:
                // Map 类型,需要 key 和 value 的序列化器
                MapType mapType = (MapType) type;
                return new InternalMapSerializer(mapType.getKeyType(), mapType.getValueType());
            case ROW:
                // Row 类型,需要所有字段的类型
                return new InternalRowSerializer(getFieldTypes(type).toArray(new DataType[0]));
            case VARIANT:
                // Variant 半结构化类型
                return VariantSerializer.INSTANCE;
            case BLOB:
                // Blob 二进制大对象类型
                return BlobSerializer.INSTANCE;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type '" + type + "' to get internal serializer");
        }
    }

    /** 私有构造函数,禁止实例化。 */
    private InternalSerializers() {
        // no instantiation
    }
}
