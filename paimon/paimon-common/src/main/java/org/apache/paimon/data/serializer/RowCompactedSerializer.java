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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryMap;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalRow.FieldGetter;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.VarLengthIntUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

import static org.apache.paimon.data.BinaryRow.HEADER_SIZE_IN_BITS;
import static org.apache.paimon.memory.MemorySegmentUtils.bitGet;
import static org.apache.paimon.memory.MemorySegmentUtils.bitSet;
import static org.apache.paimon.types.DataTypeChecks.getPrecision;
import static org.apache.paimon.types.DataTypeChecks.getScale;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.VarLengthIntUtils.MAX_VAR_INT_SIZE;

/**
 * {@link InternalRow} 的紧凑二进制序列化器。
 *
 * <p>该序列化器使用紧凑的二进制格式来序列化行数据,相比 {@link BinaryRowSerializer},提供了更加紧凑的存储格式。
 * 主要特点是使用变长整数编码和优化的字段布局,减少存储空间占用。
 *
 * <h2>紧凑二进制格式</h2>
 *
 * <p>序列化格式包括三个部分:
 *
 * <pre>
 * +------------------+
 * |  Header          | 头部区域
 * |  - RowKind (1B)  | 行类型 (INSERT, UPDATE_BEFORE 等)
 * |  - Null bits     | null位图 ((字段数 + 7 + HEADER_SIZE_IN_BITS) / 8 bytes)
 * +------------------+
 * |  Field Data      | 字段数据区域 (变长编码)
 * |  - 基本类型      | 直接存储 (boolean, byte, short, int, long, float, double)
 * |  - String        | [变长长度] + [UTF-8 字节]
 * |  - Binary        | [变长长度] + [字节数组]
 * |  - Decimal       | 紧凑: long; 非紧凑: [变长长度] + [字节数组]
 * |  - Timestamp     | 紧凑: long; 非紧凑: long + [变长纳秒]
 * |  - Array/Map     | [变长长度] + [BinaryArray/BinaryMap 数据]
 * |  - Row           | [变长长度] + [嵌套行数据]
 * +------------------+
 * </pre>
 *
 * <h2>变长整数编码</h2>
 *
 * <p>使用 VarLengthIntUtils 进行变长编码,小整数占用更少的字节:
 *
 * <ul>
 *   <li>0-127: 1 字节
 *   <li>128-16383: 2 字节
 *   <li>16384-2097151: 3 字节
 *   <li>更大的值: 最多 5 字节
 * </ul>
 *
 * <h2>主要特点</h2>
 *
 * <ul>
 *   <li><b>紧凑存储</b>: 变长编码减少空间占用,特别适合小整数和短字符串
 *   <li><b>对象重用</b>: 维护可重用的 RowWriter 和 RowReader,减少 GC 压力
 *   <li><b>类型安全</b>: 根据 RowType 生成类型化的 FieldGetter/Writer/Reader
 *   <li><b>支持嵌套</b>: 支持嵌套的 Array、Map 和 Row 类型
 *   <li><b>支持比较</b>: 提供基于 MemorySlice 的比较器,用于排序等场景
 * </ul>
 *
 * <h2>使用示例</h2>
 *
 * <pre>{@code
 * // 创建序列化器
 * RowType rowType = RowType.of(
 *     DataTypes.INT(),
 *     DataTypes.STRING(),
 *     DataTypes.DOUBLE()
 * );
 * RowCompactedSerializer serializer = new RowCompactedSerializer(rowType);
 *
 * // 序列化行
 * GenericRow row = GenericRow.of(42, BinaryString.fromString("hello"), 3.14);
 * byte[] bytes = serializer.serializeToBytes(row);
 * System.out.println("紧凑格式大小: " + bytes.length + " bytes");
 *
 * // 反序列化行
 * InternalRow deserializedRow = serializer.deserialize(bytes);
 *
 * // 流式序列化/反序列化
 * DataOutputView output = ...;
 * serializer.serialize(row, output);
 *
 * DataInputView input = ...;
 * InternalRow readRow = serializer.deserialize(input);
 *
 * // 创建比较器用于排序
 * Comparator<MemorySlice> comparator = serializer.createSliceComparator();
 * MemorySlice slice1 = MemorySlice.wrap(bytes1);
 * MemorySlice slice2 = MemorySlice.wrap(bytes2);
 * int cmp = comparator.compare(slice1, slice2);
 * }</pre>
 *
 * <h2>性能考虑</h2>
 *
 * <ul>
 *   <li><b>紧凑性</b>: 比 BinaryRow 更紧凑,但需要额外的编码/解码开销
 *   <li><b>对象重用</b>: 重用 RowWriter/RowReader 减少内存分配
 *   <li><b>动态扩容</b>: RowWriter 使用 2 倍增长策略,避免频繁扩容
 *   <li><b>适用场景</b>: 适合存储和传输,不适合频繁的随机访问
 * </ul>
 *
 * <h2>线程安全性</h2>
 *
 * <p>该类不是线程安全的。每个序列化器实例维护可重用的 RowWriter 和 RowReader,
 * 不能被多个线程并发使用。如果需要在多线程环境中使用,应该为每个线程创建独立的序列化器实例(通过 {@link #duplicate()} 方法)。
 *
 * @see BinaryRowSerializer BinaryRow 序列化器,支持零拷贝和随机访问
 * @see InternalRowSerializer 通用行序列化器,委托给 BinaryRowSerializer
 * @see VarLengthIntUtils 变长整数编码工具
 * @since 1.0
 */
public class RowCompactedSerializer implements Serializer<InternalRow> {

    private static final long serialVersionUID = 1L;

    /** 字段值获取器数组,用于从 InternalRow 中读取字段值。 */
    private final FieldGetter[] getters;

    /** 字段写入器数组,用于将字段值写入 RowWriter。 */
    private final FieldWriter[] writers;

    /** 字段读取器数组,用于从 RowReader 中读取字段值。 */
    private final FieldReader[] readers;

    /** 行类型定义,包含字段数量和每个字段的数据类型。 */
    private final RowType rowType;

    /** 可重用的行写入器,用于序列化时减少对象创建。每次序列化时重置状态。 */
    @Nullable private RowWriter rowWriter;

    /** 可重用的行读取器,用于反序列化时减少对象创建。每次反序列化时重新定位。 */
    @Nullable private RowReader rowReader;

    /**
     * 计算给定字段数量所需的位集字节数。
     *
     * <p>位集包括 RowKind 头部和 null 位图:
     *
     * <ul>
     *   <li>HEADER_SIZE_IN_BITS (通常为 8): RowKind 占用的位数
     *   <li>arity: null 位图占用的位数,每个字段一位
     *   <li>总位数向上取整到字节边界
     * </ul>
     *
     * @param arity 字段数量
     * @return 位集占用的字节数
     */
    public static int calculateBitSetInBytes(int arity) {
        return (arity + 7 + HEADER_SIZE_IN_BITS) / 8;
    }

    /**
     * 构造一个紧凑行序列化器。
     *
     * <p>根据 RowType 创建类型化的字段获取器、写入器和读取器,用于高效的序列化和反序列化。
     *
     * @param rowType 行类型定义,包含字段数量和每个字段的数据类型
     */
    public RowCompactedSerializer(RowType rowType) {
        this.getters = new FieldGetter[rowType.getFieldCount()];
        this.writers = new FieldWriter[rowType.getFieldCount()];
        this.readers = new FieldReader[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            DataType type = rowType.getTypeAt(i);
            getters[i] = InternalRow.createFieldGetter(type, i);
            writers[i] = createFieldWriter(type);
            readers[i] = createFieldReader(type);
        }
        this.rowType = rowType;
    }

    /**
     * 获取行类型定义(用于测试)。
     *
     * @return 行类型
     */
    @VisibleForTesting
    RowType rowType() {
        return rowType;
    }

    /**
     * 创建该序列化器的副本。
     *
     * <p>新的序列化器实例共享相同的 RowType 定义,但有独立的状态(RowWriter 和 RowReader)。
     * 适用于多线程环境,每个线程使用独立的序列化器实例。
     *
     * @return 新的序列化器实例
     */
    @Override
    public Serializer<InternalRow> duplicate() {
        return new RowCompactedSerializer(rowType);
    }

    /**
     * 深拷贝一个行对象。
     *
     * <p>通过序列化和反序列化实现深拷贝,确保原对象和副本完全独立。
     *
     * @param from 源行对象
     * @return 深拷贝的行对象
     */
    @Override
    public InternalRow copy(InternalRow from) {
        return deserialize(serializeToBytes(from));
    }

    /**
     * 将行对象序列化到输出流。
     *
     * <p>序列化格式:
     *
     * <pre>
     * [变长整数: 字节数组长度]
     * [紧凑二进制数据]
     * </pre>
     *
     * @param record 待序列化的行对象
     * @param target 输出流
     * @throws IOException 如果写入失败
     */
    @Override
    public void serialize(InternalRow record, DataOutputView target) throws IOException {
        byte[] bytes = serializeToBytes(record);
        VarLengthIntUtils.encodeInt(target, bytes.length);
        target.write(bytes);
    }

    /**
     * 从输入流反序列化行对象。
     *
     * <p>反序列化格式:
     *
     * <pre>
     * [变长整数: 字节数组长度]
     * [紧凑二进制数据]
     * </pre>
     *
     * @param source 输入流
     * @return 反序列化的行对象
     * @throws IOException 如果读取失败
     */
    @Override
    public InternalRow deserialize(DataInputView source) throws IOException {
        int len = VarLengthIntUtils.decodeInt(source);
        byte[] bytes = new byte[len];
        source.readFully(bytes);
        return deserialize(bytes);
    }

    /**
     * 判断两个序列化器是否相等。
     *
     * <p>两个序列化器相等当且仅当它们的 RowType 相同。
     *
     * @param o 另一个对象
     * @return 如果相等返回 true
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowCompactedSerializer that = (RowCompactedSerializer) o;
        return Objects.equals(rowType, that.rowType);
    }

    /**
     * 返回哈希码。
     *
     * @return 哈希码,基于 RowType
     */
    @Override
    public int hashCode() {
        return Objects.hash(rowType);
    }

    /**
     * 将行对象序列化为字节数组。
     *
     * <p>这是核心序列化方法,遍历所有字段并使用对应的 FieldWriter 写入数据。
     * 使用可重用的 RowWriter 减少对象创建。
     *
     * <p>序列化步骤:
     *
     * <ol>
     *   <li>重置 RowWriter 状态
     *   <li>写入 RowKind (INSERT, UPDATE_BEFORE 等)
     *   <li>遍历所有字段:
     *       <ul>
     *         <li>如果字段为 null,设置 null 位
     *         <li>否则调用对应的 FieldWriter 写入字段值
     *       </ul>
     *   <li>返回字节数组副本
     * </ol>
     *
     * @param record 待序列化的行对象
     * @return 紧凑二进制格式的字节数组
     */
    public byte[] serializeToBytes(InternalRow record) {
        if (rowWriter == null) {
            rowWriter = new RowWriter(calculateBitSetInBytes(getters.length));
        }
        rowWriter.reset();
        rowWriter.writeRowKind(record.getRowKind());
        for (int i = 0; i < getters.length; i++) {
            Object field = getters[i].getFieldOrNull(record);
            if (field == null) {
                rowWriter.setNullAt(i);
            } else {
                writers[i].writeField(rowWriter, i, field);
            }
        }
        return rowWriter.copyBuffer();
    }

    /**
     * 从字节数组反序列化行对象。
     *
     * <p>这是核心反序列化方法,从字节数组中读取所有字段并构造 GenericRow。
     * 使用可重用的 RowReader 减少对象创建。
     *
     * <p>反序列化步骤:
     *
     * <ol>
     *   <li>让 RowReader 指向字节数组
     *   <li>创建 GenericRow
     *   <li>读取 RowKind
     *   <li>遍历所有字段:
     *       <ul>
     *         <li>如果 null 位被设置,字段值为 null
     *         <li>否则调用对应的 FieldReader 读取字段值
     *       </ul>
     *   <li>返回 GenericRow
     * </ol>
     *
     * @param bytes 紧凑二进制格式的字节数组
     * @return 反序列化的行对象
     */
    public InternalRow deserialize(byte[] bytes) {
        if (rowReader == null) {
            rowReader = new RowReader(calculateBitSetInBytes(getters.length));
        }
        rowReader.pointTo(bytes);
        GenericRow row = new GenericRow(readers.length);
        row.setRowKind(rowReader.readRowKind());
        for (int i = 0; i < readers.length; i++) {
            row.setField(i, rowReader.isNullAt(i) ? null : readers[i].readField(rowReader, i));
        }
        return row;
    }

    /**
     * 创建基于 MemorySlice 的比较器。
     *
     * <p>该比较器可以直接比较序列化后的字节数据,无需反序列化整行。
     * 比较规则:
     *
     * <ul>
     *   <li>按字段顺序逐个比较
     *   <li>null 值小于非 null 值
     *   <li>使用字段值的自然顺序 (Comparable)
     *   <li>遇到不相等的字段立即返回比较结果
     * </ul>
     *
     * @return MemorySlice 比较器,用于排序等场景
     */
    public Comparator<MemorySlice> createSliceComparator() {
        return new SliceComparator(rowType);
    }

    private static FieldWriter createFieldWriter(DataType fieldType) {
        final FieldWriter fieldWriter;
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                fieldWriter = (writer, pos, value) -> writer.writeString((BinaryString) value);
                break;
            case BOOLEAN:
                fieldWriter = (writer, pos, value) -> writer.writeBoolean((boolean) value);
                break;
            case BINARY:
            case VARBINARY:
                fieldWriter = (writer, pos, value) -> writer.writeBinary((byte[]) value);
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                fieldWriter =
                        (writer, pos, value) ->
                                writer.writeDecimal((Decimal) value, decimalPrecision);
                break;
            case TINYINT:
                fieldWriter = (writer, pos, value) -> writer.writeByte((byte) value);
                break;
            case SMALLINT:
                fieldWriter = (writer, pos, value) -> writer.writeShort((short) value);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                fieldWriter = (writer, pos, value) -> writer.writeInt((int) value);
                break;
            case BIGINT:
                fieldWriter = (writer, pos, value) -> writer.writeLong((long) value);
                break;
            case FLOAT:
                fieldWriter = (writer, pos, value) -> writer.writeFloat((float) value);
                break;
            case DOUBLE:
                fieldWriter = (writer, pos, value) -> writer.writeDouble((double) value);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(fieldType);
                fieldWriter =
                        (writer, pos, value) ->
                                writer.writeTimestamp((Timestamp) value, timestampPrecision);
                break;
            case VARIANT:
                fieldWriter =
                        (writer, pos, value) -> {
                            Variant variant = (Variant) value;
                            byte[] bytes;
                            try {
                                bytes = VariantSerializer.INSTANCE.serializeToBytes(variant);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                            writer.writeBinary(bytes);
                        };
                break;
            case ARRAY:
                Serializer<InternalArray> arraySerializer = InternalSerializers.create(fieldType);
                fieldWriter =
                        (writer, pos, value) ->
                                writer.writeArray(
                                        (InternalArray) value,
                                        (InternalArraySerializer) arraySerializer);
                break;
            case MULTISET:
            case MAP:
                Serializer<InternalMap> mapSerializer = InternalSerializers.create(fieldType);
                fieldWriter =
                        (writer, pos, value) ->
                                writer.writeMap(
                                        (InternalMap) value, (InternalMapSerializer) mapSerializer);
                break;
            case ROW:
                RowCompactedSerializer rowSerializer =
                        new RowCompactedSerializer((RowType) fieldType);
                fieldWriter =
                        (writer, pos, value) -> writer.writeRow((InternalRow) value, rowSerializer);
                break;
            default:
                String msg =
                        String.format(
                                "type %s not support in %s",
                                fieldType.getTypeRoot().toString(),
                                RowCompactedSerializer.class.getName());
                throw new IllegalArgumentException(msg);
        }

        if (!fieldType.isNullable()) {
            return fieldWriter;
        }
        return (writer, pos, value) -> {
            if (value == null) {
                writer.setNullAt(pos);
            } else {
                fieldWriter.writeField(writer, pos, value);
            }
        };
    }

    private static FieldReader createFieldReader(DataType fieldType) {
        final FieldReader fieldReader;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                fieldReader = (reader, pos) -> reader.readString();
                break;
            case BOOLEAN:
                fieldReader = (reader, pos) -> reader.readBoolean();
                break;
            case BINARY:
            case VARBINARY:
                fieldReader = (reader, pos) -> reader.readBinary();
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                fieldReader = (reader, pos) -> reader.readDecimal(decimalPrecision, decimalScale);
                break;
            case TINYINT:
                fieldReader = (reader, pos) -> reader.readByte();
                break;
            case SMALLINT:
                fieldReader = (reader, pos) -> reader.readShort();
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                fieldReader = (reader, pos) -> reader.readInt();
                break;
            case BIGINT:
                fieldReader = (reader, pos) -> reader.readLong();
                break;
            case FLOAT:
                fieldReader = (reader, pos) -> reader.readFloat();
                break;
            case DOUBLE:
                fieldReader = (reader, pos) -> reader.readDouble();
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(fieldType);
                fieldReader = (reader, pos) -> reader.readTimestamp(timestampPrecision);
                break;
            case VARIANT:
                fieldReader =
                        (reader, pos) -> {
                            byte[] bytes = reader.readBinary();
                            try {
                                return VariantSerializer.INSTANCE.deserializeFromBytes(bytes);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        };
                break;
            case ARRAY:
                fieldReader = (reader, pos) -> reader.readArray();
                break;
            case MULTISET:
            case MAP:
                fieldReader = (reader, pos) -> reader.readMap();
                break;
            case ROW:
                RowCompactedSerializer serializer = new RowCompactedSerializer((RowType) fieldType);
                fieldReader = (reader, pos) -> reader.readRow(serializer);
                break;
            default:
                String msg =
                        String.format(
                                "type %s not support in %s",
                                fieldType.getTypeRoot().toString(),
                                RowCompactedSerializer.class.getName());
                throw new IllegalArgumentException(msg);
        }
        if (!fieldType.isNullable()) {
            return fieldReader;
        }
        return (reader, pos) -> {
            if (reader.isNullAt(pos)) {
                return null;
            }
            return fieldReader.readField(reader, pos);
        };
    }

    /**
     * 字段写入器接口。
     *
     * <p>将特定类型的字段值写入 RowWriter。每个数据类型都有对应的 FieldWriter 实现。
     */
    private interface FieldWriter extends Serializable {
        /**
         * 写入字段值。
         *
         * @param writer RowWriter 实例
         * @param pos 字段位置(用于设置 null 位)
         * @param value 字段值(不为 null)
         */
        void writeField(RowWriter writer, int pos, Object value);
    }

    /**
     * 字段读取器接口。
     *
     * <p>从 RowReader 中读取特定类型的字段值。每个数据类型都有对应的 FieldReader 实现。
     */
    private interface FieldReader extends Serializable {
        /**
         * 读取字段值。
         *
         * @param reader RowReader 实例
         * @param pos 字段位置(用于检查 null 位)
         * @return 字段值,可能为 null
         */
        Object readField(RowReader reader, int pos);
    }

    /**
     * 行写入器 - 负责将行数据写入字节缓冲区。
     *
     * <p>主要特点:
     *
     * <ul>
     *   <li><b>紧凑格式</b>: 使用变长整数编码,减少存储空间
     *   <li><b>动态扩容</b>: 缓冲区不足时自动扩容(2倍增长)
     *   <li><b>对象重用</b>: 可以重置状态后重复使用,减少 GC
     *   <li><b>类型化写入</b>: 针对每种数据类型提供专门的写入方法
     * </ul>
     *
     * <p>缓冲区布局:
     *
     * <pre>
     * [RowKind | Null Bits | Field1 | Field2 | ...]
     * </pre>
     */
    private static class RowWriter {

        /** 头部大小(字节),包括 RowKind 和 null 位图。 */
        private final int headerSizeInBytes;

        /** 字节缓冲区,存储序列化后的数据。 */
        private byte[] buffer;

        /** 缓冲区的内存段包装,用于高效的读写操作。 */
        private MemorySegment segment;

        /** 当前写入位置(字节偏移)。 */
        private int position;

        /**
         * 构造行写入器。
         *
         * @param headerSizeInBytes 头部大小(字节)
         */
        private RowWriter(int headerSizeInBytes) {
            this.headerSizeInBytes = headerSizeInBytes;
            setBuffer(new byte[Math.max(64, headerSizeInBytes)]);
            this.position = headerSizeInBytes;
        }

        /**
         * 重置写入器状态,准备写入新的行。
         *
         * <p>重置步骤:
         *
         * <ul>
         *   <li>将写入位置重置到头部之后
         *   <li>清空头部区域(RowKind 和 null 位图)
         * </ul>
         */
        private void reset() {
            this.position = headerSizeInBytes;
            for (int i = 0; i < headerSizeInBytes; i++) {
                buffer[i] = 0;
            }
        }

        /**
         * 写入行类型。
         *
         * @param kind 行类型 (INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE)
         */
        private void writeRowKind(RowKind kind) {
            this.buffer[0] = kind.toByteValue();
        }

        /**
         * 设置字段为 null。
         *
         * @param pos 字段位置
         */
        private void setNullAt(int pos) {
            bitSet(segment, 0, pos + HEADER_SIZE_IN_BITS);
        }

        /**
         * 写入布尔值。
         *
         * @param value 布尔值
         */
        private void writeBoolean(boolean value) {
            ensureCapacity(1);
            segment.putBoolean(position++, value);
        }

        /**
         * 写入字节值。
         *
         * @param value 字节值
         */
        private void writeByte(byte value) {
            ensureCapacity(1);
            segment.put(position++, value);
        }

        /**
         * 写入短整数值。
         *
         * @param value 短整数值
         */
        private void writeShort(short value) {
            ensureCapacity(2);
            segment.putShort(position, value);
            position += 2;
        }

        /**
         * 写入整数值。
         *
         * @param value 整数值
         */
        private void writeInt(int value) {
            ensureCapacity(4);
            segment.putInt(position, value);
            position += 4;
        }

        /**
         * 写入长整数值。
         *
         * @param value 长整数值
         */
        private void writeLong(long value) {
            ensureCapacity(8);
            segment.putLong(position, value);
            position += 8;
        }

        /**
         * 写入单精度浮点值。
         *
         * @param value 单精度浮点值
         */
        private void writeFloat(float value) {
            ensureCapacity(4);
            segment.putFloat(position, value);
            position += 4;
        }

        /**
         * 写入双精度浮点值。
         *
         * @param value 双精度浮点值
         */
        private void writeDouble(double value) {
            ensureCapacity(8);
            segment.putDouble(position, value);
            position += 8;
        }

        /**
         * 写入字符串。
         *
         * <p>格式: [变长长度] + [UTF-8 字节]
         *
         * @param value 二进制字符串
         */
        private void writeString(BinaryString value) {
            writeSegments(value.getSegments(), value.getOffset(), value.getSizeInBytes());
        }

        /**
         * 写入 Decimal 值。
         *
         * <p>格式:
         *
         * <ul>
         *   <li>紧凑 (precision <= 18): 8 字节 long
         *   <li>非紧凑: [变长长度] + [字节数组]
         * </ul>
         *
         * @param value Decimal 值
         * @param precision 精度
         */
        private void writeDecimal(Decimal value, int precision) {
            if (Decimal.isCompact(precision)) {
                writeLong(value.toUnscaledLong());
            } else {
                writeBinary(value.toUnscaledBytes());
            }
        }

        /**
         * 写入时间戳。
         *
         * <p>格式:
         *
         * <ul>
         *   <li>紧凑 (precision <= 3): 8 字节 long (毫秒)
         *   <li>非紧凑: 8 字节 long (毫秒) + 变长 int (纳秒)
         * </ul>
         *
         * @param value 时间戳
         * @param precision 精度 (0-9)
         */
        private void writeTimestamp(Timestamp value, int precision) {
            if (Timestamp.isCompact(precision)) {
                writeLong(value.getMillisecond());
            } else {
                writeLong(value.getMillisecond());
                writeUnsignedInt(value.getNanoOfMillisecond());
            }
        }

        /**
         * 写入无符号整数(变长编码)。
         *
         * @param value 无符号整数 (>= 0)
         */
        private void writeUnsignedInt(int value) {
            checkArgument(value >= 0);
            ensureCapacity(MAX_VAR_INT_SIZE);
            int len = VarLengthIntUtils.encodeInt(buffer, position, value);
            position += len;
        }

        /**
         * 写入数组。
         *
         * <p>格式: [变长长度] + [BinaryArray 数据]
         *
         * @param value 内部数组
         * @param serializer 数组序列化器
         */
        private void writeArray(InternalArray value, InternalArraySerializer serializer) {
            BinaryArray binary = serializer.toBinaryArray(value);
            writeSegments(binary.getSegments(), binary.getOffset(), binary.getSizeInBytes());
        }

        /**
         * 写入 Map。
         *
         * <p>格式: [变长长度] + [BinaryMap 数据]
         *
         * @param value 内部 Map
         * @param serializer Map 序列化器
         */
        private void writeMap(InternalMap value, InternalMapSerializer serializer) {
            BinaryMap binary = serializer.toBinaryMap(value);
            writeSegments(binary.getSegments(), binary.getOffset(), binary.getSizeInBytes());
        }

        /**
         * 写入嵌套行。
         *
         * <p>格式: [变长长度] + [嵌套行数据]
         *
         * @param value 内部行
         * @param serializer 紧凑行序列化器
         */
        private void writeRow(InternalRow value, RowCompactedSerializer serializer) {
            writeBinary(serializer.serializeToBytes(value));
        }

        /**
         * 复制缓冲区内容。
         *
         * @return 字节数组副本,长度为当前写入位置
         */
        private byte[] copyBuffer() {
            return Arrays.copyOf(buffer, position);
        }

        /**
         * 设置新的缓冲区并包装为内存段。
         *
         * @param buffer 新的字节缓冲区
         */
        private void setBuffer(byte[] buffer) {
            this.buffer = buffer;
            this.segment = MemorySegment.wrap(buffer);
        }

        /**
         * 确保缓冲区有足够的容量。
         *
         * @param size 需要的额外字节数
         */
        private void ensureCapacity(int size) {
            if (buffer.length - position < size) {
                grow(size);
            }
        }

        /**
         * 扩容缓冲区。
         *
         * <p>扩容策略: 取两者中较大的值:
         *
         * <ul>
         *   <li>当前大小的 2 倍
         *   <li>当前大小 + 需要的最小容量
         * </ul>
         *
         * @param minCapacityAdd 需要的最小额外容量
         */
        private void grow(int minCapacityAdd) {
            int newLen = Math.max(this.buffer.length * 2, this.buffer.length + minCapacityAdd);
            setBuffer(Arrays.copyOf(this.buffer, newLen));
        }

        /**
         * 写入字节数组。
         *
         * <p>格式: [变长长度] + [字节数据]
         *
         * @param value 字节数组
         */
        private void writeBinary(byte[] value) {
            writeUnsignedInt(value.length);
            ensureCapacity(value.length);
            System.arraycopy(value, 0, buffer, position, value.length);
            position += value.length;
        }

        /**
         * 从单个内存段写入数据。
         *
         * @param segment 内存段
         * @param off 起始偏移
         * @param len 长度
         */
        private void write(MemorySegment segment, int off, int len) {
            ensureCapacity(len);
            segment.get(off, this.buffer, this.position, len);
            this.position += len;
        }

        /**
         * 写入内存段数组的数据(优化版本)。
         *
         * <p>格式: [变长长度] + [数据]
         *
         * <p>如果数据在第一个段内,使用单段优化路径,否则使用跨段复制。
         *
         * @param segments 内存段数组
         * @param off 起始偏移
         * @param len 长度
         */
        private void writeSegments(MemorySegment[] segments, int off, int len) {
            writeUnsignedInt(len);
            if (len + off <= segments[0].size()) {
                write(segments[0], off, len);
            } else {
                write(segments, off, len);
            }
        }

        /**
         * 从多个内存段写入数据(跨段复制)。
         *
         * <p>遍历所有段,按顺序复制数据,直到复制完指定长度。
         *
         * @param segments 内存段数组
         * @param off 起始偏移
         * @param len 长度
         */
        private void write(MemorySegment[] segments, int off, int len) {
            ensureCapacity(len);
            int toWrite = len;
            int fromOffset = off;
            int toOffset = this.position;
            for (MemorySegment sourceSegment : segments) {
                int remain = sourceSegment.size() - fromOffset;
                if (remain > 0) {
                    int localToWrite = Math.min(remain, toWrite);
                    sourceSegment.get(fromOffset, buffer, toOffset, localToWrite);
                    toWrite -= localToWrite;
                    toOffset += localToWrite;
                    fromOffset = 0;
                } else {
                    fromOffset -= sourceSegment.size();
                }
            }
            this.position += len;
        }
    }

    /**
     * 行读取器 - 负责从字节缓冲区中读取行数据。
     *
     * <p>主要特点:
     *
     * <ul>
     *   <li><b>零拷贝</b>: 直接从内存段读取,不需要复制数据
     *   <li><b>对象重用</b>: 可以重新定位后重复使用,减少 GC
     *   <li><b>类型化读取</b>: 针对每种数据类型提供专门的读取方法
     *   <li><b>变长解码</b>: 支持变长整数解码
     * </ul>
     *
     * <p>缓冲区布局:
     *
     * <pre>
     * [RowKind | Null Bits | Field1 | Field2 | ...]
     * </pre>
     */
    private static class RowReader {

        /** 头部大小(字节),包括 RowKind 和 null 位图。 */
        private final int headerSizeInBytes;

        /** 当前内存段。 */
        private MemorySegment segment;

        /** 内存段数组(用于跨段访问)。 */
        private MemorySegment[] segments;

        /** 起始偏移(字节)。 */
        private int offset;

        /** 当前读取位置(字节偏移)。 */
        private int position;

        /**
         * 构造行读取器。
         *
         * @param headerSizeInBytes 头部大小(字节)
         */
        private RowReader(int headerSizeInBytes) {
            this.headerSizeInBytes = headerSizeInBytes;
        }

        /**
         * 让读取器指向字节数组。
         *
         * @param bytes 字节数组
         */
        private void pointTo(byte[] bytes) {
            pointTo(MemorySegment.wrap(bytes), 0);
        }

        /**
         * 让读取器指向内存段。
         *
         * @param segment 内存段
         * @param offset 起始偏移
         */
        private void pointTo(MemorySegment segment, int offset) {
            this.segment = segment;
            this.segments = new MemorySegment[] {segment};
            this.offset = offset;
            this.position = offset + headerSizeInBytes;
        }

        /**
         * 读取行类型。
         *
         * @return 行类型 (INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE)
         */
        private RowKind readRowKind() {
            return RowKind.fromByteValue(segment.get(offset));
        }

        /**
         * 检查字段是否为 null。
         *
         * @param pos 字段位置
         * @return 如果字段为 null 返回 true
         */
        private boolean isNullAt(int pos) {
            return bitGet(segment, offset, pos + HEADER_SIZE_IN_BITS);
        }

        /**
         * 读取布尔值。
         *
         * @return 布尔值
         */
        private boolean readBoolean() {
            return segment.getBoolean(position++);
        }

        /**
         * 读取字节值。
         *
         * @return 字节值
         */
        private byte readByte() {
            return segment.get(position++);
        }

        /**
         * 读取短整数值。
         *
         * @return 短整数值
         */
        private short readShort() {
            short value = segment.getShort(position);
            position += 2;
            return value;
        }

        /**
         * 读取整数值。
         *
         * @return 整数值
         */
        private int readInt() {
            int value = segment.getInt(position);
            position += 4;
            return value;
        }

        /**
         * 读取长整数值。
         *
         * @return 长整数值
         */
        private long readLong() {
            long value = segment.getLong(position);
            position += 8;
            return value;
        }

        /**
         * 读取单精度浮点值。
         *
         * @return 单精度浮点值
         */
        private float readFloat() {
            float value = segment.getFloat(position);
            position += 4;
            return value;
        }

        /**
         * 读取双精度浮点值。
         *
         * @return 双精度浮点值
         */
        private double readDouble() {
            double value = segment.getDouble(position);
            position += 8;
            return value;
        }

        /**
         * 读取字符串。
         *
         * <p>格式: [变长长度] + [UTF-8 字节]
         *
         * @return 二进制字符串
         */
        private BinaryString readString() {
            int length = readUnsignedInt();
            BinaryString string = BinaryString.fromAddress(segments, position, length);
            position += length;
            return string;
        }

        /**
         * 读取无符号整数(变长解码)。
         *
         * <p>使用变长整数编码,每个字节的最高位表示是否还有后续字节。
         *
         * @return 无符号整数
         * @throws Error 如果格式错误
         */
        private int readUnsignedInt() {
            for (int offset = 0, result = 0; offset < 32; offset += 7) {
                int b = readByte();
                result |= (b & 0x7F) << offset;
                if ((b & 0x80) == 0) {
                    return result;
                }
            }
            throw new Error("Malformed integer.");
        }

        /**
         * 读取 Decimal 值。
         *
         * <p>格式:
         *
         * <ul>
         *   <li>紧凑 (precision <= 18): 8 字节 long
         *   <li>非紧凑: [变长长度] + [字节数组]
         * </ul>
         *
         * @param precision 精度
         * @param scale 小数位数
         * @return Decimal 值
         */
        private Decimal readDecimal(int precision, int scale) {
            return Decimal.isCompact(precision)
                    ? Decimal.fromUnscaledLong(readLong(), precision, scale)
                    : Decimal.fromUnscaledBytes(readBinary(), precision, scale);
        }

        /**
         * 读取时间戳。
         *
         * <p>格式:
         *
         * <ul>
         *   <li>紧凑 (precision <= 3): 8 字节 long (毫秒)
         *   <li>非紧凑: 8 字节 long (毫秒) + 变长 int (纳秒)
         * </ul>
         *
         * @param precision 精度 (0-9)
         * @return 时间戳
         */
        private Timestamp readTimestamp(int precision) {
            if (Timestamp.isCompact(precision)) {
                return Timestamp.fromEpochMillis(readLong());
            }
            long milliseconds = readLong();
            int nanosOfMillisecond = readUnsignedInt();
            return Timestamp.fromEpochMillis(milliseconds, nanosOfMillisecond);
        }

        /**
         * 读取字节数组。
         *
         * <p>格式: [变长长度] + [字节数据]
         *
         * @return 字节数组
         */
        private byte[] readBinary() {
            int length = readUnsignedInt();
            byte[] bytes = new byte[length];
            segment.get(position, bytes, 0, length);
            position += length;
            return bytes;
        }

        /**
         * 读取数组。
         *
         * <p>格式: [变长长度] + [BinaryArray 数据]
         *
         * @return 内部数组
         */
        private InternalArray readArray() {
            BinaryArray value = new BinaryArray();
            int length = readUnsignedInt();
            value.pointTo(segments, position, length);
            position += length;
            return value;
        }

        /**
         * 读取 Map。
         *
         * <p>格式: [变长长度] + [BinaryMap 数据]
         *
         * @return 内部 Map
         */
        private InternalMap readMap() {
            BinaryMap value = new BinaryMap();
            int length = readUnsignedInt();
            value.pointTo(segments, position, length);
            position += length;
            return value;
        }

        /**
         * 读取嵌套行。
         *
         * <p>格式: [变长长度] + [嵌套行数据]
         *
         * @param serializer 紧凑行序列化器
         * @return 内部行
         */
        private InternalRow readRow(RowCompactedSerializer serializer) {
            byte[] bytes = readBinary();
            return serializer.deserialize(bytes);
        }
    }

    /**
     * 内存切片比较器 - 直接比较序列化后的行数据。
     *
     * <p>该比较器可以在不反序列化整行的情况下进行比较,提高排序等操作的性能。
     *
     * <p>比较规则:
     *
     * <ul>
     *   <li>按字段顺序逐个比较
     *   <li>null 值小于非 null 值
     *   <li>如果两个值都不为 null,使用字段值的自然顺序 (Comparable)
     *   <li>遇到第一个不相等的字段立即返回比较结果
     *   <li>如果所有字段都相等,返回 0
     * </ul>
     *
     * <p>性能特点:
     *
     * <ul>
     *   <li><b>延迟反序列化</b>: 只反序列化需要比较的字段
     *   <li><b>对象重用</b>: 重用两个 RowReader 实例
     *   <li><b>短路求值</b>: 遇到不相等的字段立即返回
     * </ul>
     */
    private static class SliceComparator implements Comparator<MemorySlice> {

        /** 第一个切片的行读取器。 */
        private final RowReader reader1;

        /** 第二个切片的行读取器。 */
        private final RowReader reader2;

        /** 字段读取器数组,用于读取字段值。 */
        private final FieldReader[] fieldReaders;

        /**
         * 构造切片比较器。
         *
         * @param rowType 行类型定义
         */
        public SliceComparator(RowType rowType) {
            int bitSetInBytes = calculateBitSetInBytes(rowType.getFieldCount());
            this.reader1 = new RowReader(bitSetInBytes);
            this.reader2 = new RowReader(bitSetInBytes);
            this.fieldReaders = new FieldReader[rowType.getFieldCount()];
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                fieldReaders[i] = createFieldReader(rowType.getTypeAt(i));
            }
        }

        /**
         * 比较两个内存切片。
         *
         * <p>比较逻辑:
         *
         * <ol>
         *   <li>让两个 RowReader 分别指向两个切片
         *   <li>按字段顺序逐个比较:
         *       <ul>
         *         <li>如果两个字段都为 null,继续比较下一个字段
         *         <li>如果第一个字段为 null,返回 -1
         *         <li>如果第二个字段为 null,返回 1
         *         <li>否则读取字段值并使用 Comparable 进行比较
         *         <li>如果字段值不相等,返回比较结果
         *       </ul>
         *   <li>如果所有字段都相等,返回 0
         * </ol>
         *
         * @param slice1 第一个切片
         * @param slice2 第二个切片
         * @return 比较结果: 负数表示 slice1 < slice2, 0 表示相等, 正数表示 slice1 > slice2
         */
        @Override
        public int compare(MemorySlice slice1, MemorySlice slice2) {
            reader1.pointTo(slice1.segment(), slice1.offset());
            reader2.pointTo(slice2.segment(), slice2.offset());
            for (int i = 0; i < fieldReaders.length; i++) {
                boolean isNull1 = reader1.isNullAt(i);
                boolean isNull2 = reader2.isNullAt(i);
                if (!isNull1 || !isNull2) {
                    if (isNull1) {
                        return -1;
                    } else if (isNull2) {
                        return 1;
                    } else {
                        FieldReader fieldReader = fieldReaders[i];
                        Object o1 = fieldReader.readField(reader1, i);
                        Object o2 = fieldReader.readField(reader2, i);
                        @SuppressWarnings({"unchecked", "rawtypes"})
                        int comp = ((Comparable) o1).compareTo(o2);
                        if (comp != 0) {
                            return comp;
                        }
                    }
                }
            }
            return 0;
        }
    }
}
