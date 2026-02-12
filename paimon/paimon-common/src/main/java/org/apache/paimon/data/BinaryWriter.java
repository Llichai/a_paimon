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

package org.apache.paimon.data;

import org.apache.paimon.data.serializer.InternalArraySerializer;
import org.apache.paimon.data.serializer.InternalMapSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;

import java.io.Serializable;

import static org.apache.paimon.types.DataTypeChecks.getPrecision;

/**
 * 二进制写入器接口。
 *
 * <p>用于将数据写入复合数据格式（如 row、array）的二进制表示。该接口定义了写入各种数据类型的方法。
 *
 * <p>使用流程：
 * <ol>
 *   <li>调用 {@link #reset()} 重置写入器
 *   <li>通过 writeXX 方法或 setNullAt 写入每个字段（同一字段不能重复写入）
 *   <li>调用 {@link #complete()} 完成写入
 * </ol>
 *
 * <p>设计要点：
 * <ul>
 *   <li>支持定长和变长类型：定长类型直接写入固定位置，变长类型写入可变长度部分
 *   <li>位置索引：通过 pos 参数指定字段在复合结构中的位置
 *   <li>NULL 值处理：通过 setNullAt 方法设置 NULL 标记
 *   <li>类型安全：为每种数据类型提供专门的写入方法
 * </ul>
 *
 * <p>内存布局：
 * <ul>
 *   <li>固定长度部分：存储小数据（≤7字节）和大数据的偏移量+长度
 *   <li>可变长度部分：存储大数据（>7字节）的实际内容
 * </ul>
 */
public interface BinaryWriter {

    /**
     * 重置写入器以准备下一次写入。
     *
     * <p>清除之前的状态，使写入器可以被重用。
     */
    void reset();

    /**
     * 将指定位置的字段设置为 NULL。
     *
     * @param pos 字段位置索引
     */
    void setNullAt(int pos);

    /**
     * 写入布尔值。
     *
     * @param pos 字段位置索引
     * @param value 布尔值（占用 1 字节）
     */
    void writeBoolean(int pos, boolean value);

    /**
     * 写入字节值。
     *
     * @param pos 字段位置索引
     * @param value 字节值
     */
    void writeByte(int pos, byte value);

    /**
     * 写入短整型值。
     *
     * @param pos 字段位置索引
     * @param value 短整型值（占用 2 字节）
     */
    void writeShort(int pos, short value);

    /**
     * 写入整型值。
     *
     * @param pos 字段位置索引
     * @param value 整型值（占用 4 字节）
     */
    void writeInt(int pos, int value);

    /**
     * 写入长整型值。
     *
     * @param pos 字段位置索引
     * @param value 长整型值（占用 8 字节）
     */
    void writeLong(int pos, long value);

    /**
     * 写入浮点数值。
     *
     * @param pos 字段位置索引
     * @param value 浮点数值（占用 4 字节）
     */
    void writeFloat(int pos, float value);

    /**
     * 写入双精度浮点数值。
     *
     * @param pos 字段位置索引
     * @param value 双精度浮点数值（占用 8 字节）
     */
    void writeDouble(int pos, double value);

    /**
     * 写入二进制字符串。
     *
     * @param pos 字段位置索引
     * @param value 二进制字符串对象
     */
    void writeString(int pos, BinaryString value);

    /**
     * 写入二进制数据。
     *
     * @param pos 字段位置索引
     * @param bytes 字节数组
     * @param offset 数组起始偏移量
     * @param length 数据长度
     */
    void writeBinary(int pos, byte[] bytes, int offset, int length);

    /**
     * 写入十进制数。
     *
     * @param pos 字段位置索引
     * @param value 十进制数对象
     * @param precision 精度（总位数）
     */
    void writeDecimal(int pos, Decimal value, int precision);

    /**
     * 写入时间戳。
     *
     * @param pos 字段位置索引
     * @param value 时间戳对象
     * @param precision 精度（时间单位）
     */
    void writeTimestamp(int pos, Timestamp value, int precision);

    /**
     * 写入变体类型数据。
     *
     * @param pos 字段位置索引
     * @param variant 变体对象
     */
    void writeVariant(int pos, Variant variant);

    /**
     * 写入 Blob 大对象。
     *
     * @param pos 字段位置索引
     * @param blob Blob 对象
     */
    void writeBlob(int pos, Blob blob);

    /**
     * 写入数组。
     *
     * @param pos 字段位置索引
     * @param value 内部数组对象
     * @param serializer 数组序列化器
     */
    void writeArray(int pos, InternalArray value, InternalArraySerializer serializer);

    /**
     * 写入 Map。
     *
     * @param pos 字段位置索引
     * @param value 内部 Map 对象
     * @param serializer Map 序列化器
     */
    void writeMap(int pos, InternalMap value, InternalMapSerializer serializer);

    /**
     * 写入行数据。
     *
     * @param pos 字段位置索引
     * @param value 内部行对象
     * @param serializer 行序列化器
     */
    void writeRow(int pos, InternalRow value, InternalRowSerializer serializer);

    /**
     * 完成写入操作。
     *
     * <p>设置二进制数据的实际大小，完成最终的写入过程。
     */
    void complete();

    // --------------------------------------------------------------------------------------------

    /**
     * 通用写入方法（已废弃）。
     *
     * <p>根据数据类型将对象写入 BinaryWriter。
     *
     * @param writer 二进制写入器
     * @param pos 字段位置索引
     * @param o 要写入的对象
     * @param type 数据类型
     * @param serializer 序列化器（用于复杂类型）
     * @deprecated 使用 {@code #createValueSetter(DataType)} 以避免运行时的逻辑类型开销
     */
    @Deprecated
    static void write(
            BinaryWriter writer, int pos, Object o, DataType type, Serializer<?> serializer) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                writer.writeBoolean(pos, (boolean) o);
                break;
            case TINYINT:
                writer.writeByte(pos, (byte) o);
                break;
            case SMALLINT:
                writer.writeShort(pos, (short) o);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                writer.writeInt(pos, (int) o);
                break;
            case BIGINT:
                writer.writeLong(pos, (long) o);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) type;
                writer.writeTimestamp(pos, (Timestamp) o, timestampType.getPrecision());
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType lzTs = (LocalZonedTimestampType) type;
                writer.writeTimestamp(pos, (Timestamp) o, lzTs.getPrecision());
                break;
            case FLOAT:
                writer.writeFloat(pos, (float) o);
                break;
            case DOUBLE:
                writer.writeDouble(pos, (double) o);
                break;
            case CHAR:
            case VARCHAR:
                writer.writeString(pos, (BinaryString) o);
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                writer.writeDecimal(pos, (Decimal) o, decimalType.getPrecision());
                break;
            case ARRAY:
                writer.writeArray(pos, (InternalArray) o, (InternalArraySerializer) serializer);
                break;
            case MAP:
            case MULTISET:
                writer.writeMap(pos, (InternalMap) o, (InternalMapSerializer) serializer);
                break;
            case ROW:
                writer.writeRow(pos, (InternalRow) o, (InternalRowSerializer) serializer);
                break;
            case BINARY:
            case VARBINARY:
                byte[] bytes = (byte[]) o;
                writer.writeBinary(pos, bytes, 0, bytes.length);
                break;
            case VARIANT:
                writer.writeVariant(pos, (Variant) o);
                break;
            case BLOB:
                writer.writeBlob(pos, (Blob) o);
                break;
            default:
                throw new UnsupportedOperationException("Not support type: " + type);
        }
    }

    /**
     * 创建运行时值设置器。
     *
     * <p>为指定的数据类型创建一个访问器，用于在运行时设置二进制写入器的元素。
     * 这种方式避免了运行时的类型检查，提高了性能。
     *
     * @param elementType 元素的数据类型
     * @return 值设置器函数接口
     */
    static ValueSetter createValueSetter(DataType elementType) {
        return createValueSetter(elementType, null);
    }

    /**
     * 创建运行时值设置器（带序列化器）。
     *
     * <p>根据数据类型的 TypeRoot 创建对应的值设置器。该方法按照类型根定义的顺序进行匹配。
     *
     * @param elementType 元素的数据类型
     * @param serializer 序列化器（用于复杂类型，如果为 null 则自动创建）
     * @return 值设置器函数接口
     */
    static ValueSetter createValueSetter(DataType elementType, Serializer<?> serializer) {
        // 按照类型根定义的顺序处理
        switch (elementType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return (writer, pos, value) -> writer.writeString(pos, (BinaryString) value);
            case BOOLEAN:
                return (writer, pos, value) -> writer.writeBoolean(pos, (boolean) value);
            case BINARY:
            case VARBINARY:
                return (writer, pos, value) -> {
                    byte[] bytes = (byte[]) value;
                    writer.writeBinary(pos, bytes, 0, bytes.length);
                };
            case DECIMAL:
                final int decimalPrecision = getPrecision(elementType);
                return (writer, pos, value) ->
                        writer.writeDecimal(pos, (Decimal) value, decimalPrecision);
            case TINYINT:
                return (writer, pos, value) -> writer.writeByte(pos, (byte) value);
            case SMALLINT:
                return (writer, pos, value) -> writer.writeShort(pos, (short) value);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return (writer, pos, value) -> writer.writeInt(pos, (int) value);
            case BIGINT:
                return (writer, pos, value) -> writer.writeLong(pos, (long) value);
            case FLOAT:
                return (writer, pos, value) -> writer.writeFloat(pos, (float) value);
            case DOUBLE:
                return (writer, pos, value) -> writer.writeDouble(pos, (double) value);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(elementType);
                return (writer, pos, value) ->
                        writer.writeTimestamp(pos, (Timestamp) value, timestampPrecision);
            case ARRAY:
                final Serializer<?> arraySerializer =
                        serializer == null ? InternalSerializers.create(elementType) : serializer;
                return (writer, pos, value) ->
                        writer.writeArray(
                                pos,
                                (InternalArray) value,
                                (InternalArraySerializer) arraySerializer);
            case MULTISET:
            case MAP:
                final Serializer<?> mapSerializer =
                        serializer == null ? InternalSerializers.create(elementType) : serializer;
                return (writer, pos, value) ->
                        writer.writeMap(
                                pos, (InternalMap) value, (InternalMapSerializer) mapSerializer);
            case ROW:
                final Serializer<?> rowSerializer =
                        serializer == null ? InternalSerializers.create(elementType) : serializer;
                return (writer, pos, value) ->
                        writer.writeRow(
                                pos, (InternalRow) value, (InternalRowSerializer) rowSerializer);
            case VARIANT:
                return (writer, pos, value) -> writer.writeVariant(pos, (Variant) value);
            case BLOB:
                return (writer, pos, value) -> writer.writeBlob(pos, (Blob) value);
            default:
                String msg =
                        String.format(
                                "type %s not support in %s",
                                elementType.getTypeRoot().toString(), BinaryArray.class.getName());
                throw new IllegalArgumentException(msg);
        }
    }

    /**
     * 值设置器接口。
     *
     * <p>用于在运行时设置二进制写入器元素的访问器。该接口采用函数式编程风格，
     * 为不同的数据类型提供统一的设置接口。
     *
     * <p>使用场景：
     * <ul>
     *   <li>避免运行时类型检查的性能开销
     *   <li>提供类型安全的值设置操作
     *   <li>支持序列化和反序列化过程
     * </ul>
     */
    interface ValueSetter extends Serializable {
        /**
         * 设置值到二进制写入器。
         *
         * @param writer 二进制写入器
         * @param pos 字段位置索引
         * @param value 要设置的值对象
         */
        void setValue(BinaryWriter writer, int pos, Object value);
    }
}
