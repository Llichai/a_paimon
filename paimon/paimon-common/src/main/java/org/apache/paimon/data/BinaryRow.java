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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentUtils;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.TimestampType;

import javax.annotation.Nullable;

import java.nio.ByteOrder;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * {@link InternalRow} 的高性能实现,基于 {@link MemorySegment} 而非 Java 对象。
 * 可以显著减少 Java 对象的序列化/反序列化开销。
 *
 * <p>内存布局说明:
 * 一个 Row 由两部分组成:固定长度部分和可变长度部分。
 *
 * <p><b>固定长度部分</b>包含:
 * <ol>
 *   <li>1 字节的头部(header):存储 RowKind 信息</li>
 *   <li>空值位集(null bit set):用于跟踪哪些字段为空,按 8 字节对齐</li>
 *   <li>字段值(field values):存储固定长度的基本类型和能放入 8 字节的可变长度值。
 *       对于无法放入 8 字节的可变长度字段,存储其在可变长度部分的长度和偏移量</li>
 * </ol>
 *
 * <p>固定长度部分的内存布局:
 * <pre>
 * +-------+-------------------+------------------+------------------+-----+
 * | byte0 | null bit set      | field-0 (8 byte) | field-1 (8 byte) | ... |
 * +-------+-------------------+------------------+------------------+-----+
 * | header| (aligned to 8)    | fixed-len part   | fixed-len part   | ... |
 * +-------+-------------------+------------------+------------------+-----+
 * </pre>
 *
 * <p>字段值部分的存储策略:
 * <ul>
 *   <li>固定长度类型(boolean, byte, short, int, long, float, double):直接存储值</li>
 *   <li>紧凑格式的特殊类型(compact decimal, compact timestamp):存储为 long</li>
 *   <li>短字符串/二进制数据(≤8字节):直接内联存储</li>
 *   <li>长字符串/二进制数据(>8字节):存储 [offset(4字节) | length(4字节)]</li>
 * </ul>
 *
 * <p><b>可变长度部分</b>:
 * 存储无法内联到固定长度部分的数据(如长字符串、数组、Map、Row等复杂类型)。
 * 可变长度部分可能跨越多个 MemorySegment。
 *
 * <p>性能优化特性:
 * <ul>
 *   <li>零拷贝:数据直接在内存段中读写,避免对象创建</li>
 *   <li>缓存友好:固定长度部分保证在单个 MemorySegment 中,提高访问速度</li>
 *   <li>空间高效:紧凑的内存布局,减少内存占用</li>
 *   <li>序列化高效:内存布局即序列化格式,无需额外转换</li>
 * </ul>
 *
 * <p>使用限制:
 * 固定长度部分必须完全落在一个 MemorySegment 中。在写入阶段,如果目标内存段的剩余空间
 * 小于固定长度部分大小,将跳过该空间。因此,单个 Row 的字段数量不能超过单个 MemorySegment
 * 的容量。如果字段过多,建议增大 MemorySegment 的 pageSize。
 *
 * <p>使用场景:
 * <ul>
 *   <li>高性能数据处理:需要频繁序列化/反序列化的场景</li>
 *   <li>大数据集:需要减少内存占用和 GC 压力</li>
 *   <li>网络传输:内存布局可直接作为传输格式</li>
 *   <li>持久化存储:可高效地写入和读取</li>
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public final class BinaryRow extends BinarySection implements InternalRow, DataSetters {

    private static final long serialVersionUID = 1L;

    public static final boolean LITTLE_ENDIAN =
            (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);
    /** 用于清除长整型第一个字节的掩码,根据字节序选择不同的掩码。 */
    private static final long FIRST_BYTE_ZERO = LITTLE_ENDIAN ? ~0xFFL : ~(0xFFL << 56L);
    /** 头部大小(以位为单位),用于存储 RowKind。 */
    public static final int HEADER_SIZE_IN_BITS = 8;

    /** 空行单例,表示没有任何字段的行。 */
    public static final BinaryRow EMPTY_ROW = new BinaryRow(0);

    static {
        int size = EMPTY_ROW.getFixedLengthPartSize();
        byte[] bytes = new byte[size];
        EMPTY_ROW.pointTo(MemorySegment.wrap(bytes), 0, size);
    }

    /**
     * 计算空值位集(null bit set)所需的字节数。
     *
     * <p>空值位集用于跟踪哪些字段为空。每个字段占用 1 位,加上 8 位的头部。
     * 为了内存对齐,结果向上取整到 8 字节的倍数。
     *
     * <p>计算公式: ((arity + 63 + 8) / 64) * 8
     * 这确保了位集按 64 位(8字节)对齐。
     *
     * @param arity 字段数量
     * @return 空值位集所需的字节数
     */
    public static int calculateBitSetWidthInBytes(int arity) {
        return ((arity + 63 + HEADER_SIZE_IN_BITS) / 64) * 8;
    }

    /**
     * 计算固定长度部分所需的字节数。
     *
     * <p>固定长度部分 = 空值位集 + (字段数量 * 8字节)
     * 每个字段在固定长度部分占用 8 字节,用于存储值或偏移量/长度。
     *
     * @param arity 字段数量
     * @return 固定长度部分所需的字节数
     */
    public static int calculateFixPartSizeInBytes(int arity) {
        return calculateBitSetWidthInBytes(arity) + 8 * arity;
    }

    /** 此行的字段数量(arity)。 */
    private final int arity;
    /** 空值位集的大小(以字节为单位)。 */
    private final int nullBitsSizeInBytes;

    /**
     * 创建具有指定字段数量的 BinaryRow。
     *
     * @param arity 字段数量,必须 >= 0
     * @throws IllegalArgumentException 如果 arity < 0
     */
    public BinaryRow(int arity) {
        checkArgument(arity >= 0);
        this.arity = arity;
        this.nullBitsSizeInBytes = calculateBitSetWidthInBytes(arity);
    }

    /**
     * 获取指定位置字段在内存中的偏移量。
     *
     * @param pos 字段位置
     * @return 字段的绝对偏移量
     */
    private int getFieldOffset(int pos) {
        return offset + nullBitsSizeInBytes + pos * 8;
    }

    /**
     * 断言索引有效性。
     *
     * @param index 要验证的索引
     */
    private void assertIndexIsValid(int index) {
        assert index >= 0 : "index (" + index + ") should >= 0";
        assert index < arity : "index (" + index + ") should < " + arity;
    }

    /**
     * 获取固定长度部分的大小。
     *
     * @return 固定长度部分的字节数
     */
    public int getFixedLengthPartSize() {
        return nullBitsSizeInBytes + 8 * arity;
    }

    @Override
    public int getFieldCount() {
        return arity;
    }

    @Override
    public RowKind getRowKind() {
        byte kindValue = segments[0].get(offset);
        return RowKind.fromByteValue(kindValue);
    }

    @Override
    public void setRowKind(RowKind kind) {
        segments[0].put(offset, kind.toByteValue());
    }

    public void setTotalSize(int sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
    }

    @Override
    public boolean isNullAt(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.bitGet(segments[0], offset, pos + HEADER_SIZE_IN_BITS);
    }

    /**
     * 将指定位置的字段标记为非空。
     *
     * @param i 字段位置
     */
    private void setNotNullAt(int i) {
        assertIndexIsValid(i);
        MemorySegmentUtils.bitUnSet(segments[0], offset, i + HEADER_SIZE_IN_BITS);
    }

    @Override
    public void setNullAt(int i) {
        assertIndexIsValid(i);
        MemorySegmentUtils.bitSet(segments[0], offset, i + HEADER_SIZE_IN_BITS);
        // 必须将固定长度部分置零。
        // 1. 只有 int/long/boolean 等固定长度类型会调用此 setNullAt。
        // 2. 置零是为了 equals 和 hash 操作的字节计算。
        segments[0].putLong(getFieldOffset(i), 0);
    }

    @Override
    public void setInt(int pos, int value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        segments[0].putInt(getFieldOffset(pos), value);
    }

    @Override
    public void setLong(int pos, long value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        segments[0].putLong(getFieldOffset(pos), value);
    }

    @Override
    public void setDouble(int pos, double value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        segments[0].putDouble(getFieldOffset(pos), value);
    }

    @Override
    public void setDecimal(int pos, Decimal value, int precision) {
        assertIndexIsValid(pos);

        if (Decimal.isCompact(precision)) {
            // 紧凑格式:精度较小的 Decimal 可以用 long 表示
            setLong(pos, value.toUnscaledLong());
        } else {
            // 非紧凑格式:需要使用可变长度部分
            int fieldOffset = getFieldOffset(pos);
            int cursor = (int) (segments[0].getLong(fieldOffset) >>> 32);
            assert cursor > 0 : "invalid cursor " + cursor;
            // 将字节置零
            MemorySegmentUtils.setLong(segments, offset + cursor, 0L);
            MemorySegmentUtils.setLong(segments, offset + cursor + 8, 0L);

            if (value == null) {
                setNullAt(pos);
                // 保留偏移量以供将来更新
                segments[0].putLong(fieldOffset, ((long) cursor) << 32);
            } else {

                byte[] bytes = value.toUnscaledBytes();
                assert bytes.length <= 16;

                // 将字节写入可变长度部分
                MemorySegmentUtils.copyFromBytes(segments, offset + cursor, bytes, 0, bytes.length);
                setLong(pos, ((long) cursor << 32) | ((long) bytes.length));
            }
        }
    }

    @Override
    public void setTimestamp(int pos, Timestamp value, int precision) {
        assertIndexIsValid(pos);

        if (Timestamp.isCompact(precision)) {
            // 紧凑格式:精度 <= 3 时可以只用 long 表示毫秒数
            setLong(pos, value.getMillisecond());
        } else {
            // 非紧凑格式:需要额外存储纳秒部分
            int fieldOffset = getFieldOffset(pos);
            int cursor = (int) (segments[0].getLong(fieldOffset) >>> 32);
            assert cursor > 0 : "invalid cursor " + cursor;

            if (value == null) {
                setNullAt(pos);
                // 将字节置零
                MemorySegmentUtils.setLong(segments, offset + cursor, 0L);
                // 保留偏移量以供将来更新
                segments[0].putLong(fieldOffset, ((long) cursor) << 32);
            } else {
                // 将毫秒数写入可变长度部分
                MemorySegmentUtils.setLong(segments, offset + cursor, value.getMillisecond());
                // 将毫秒内的纳秒数写入固定长度部分
                setLong(pos, ((long) cursor << 32) | (long) value.getNanoOfMillisecond());
            }
        }
    }

    @Override
    public void setBoolean(int pos, boolean value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        segments[0].putBoolean(getFieldOffset(pos), value);
    }

    @Override
    public void setShort(int pos, short value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        segments[0].putShort(getFieldOffset(pos), value);
    }

    @Override
    public void setByte(int pos, byte value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        segments[0].put(getFieldOffset(pos), value);
    }

    @Override
    public void setFloat(int pos, float value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        segments[0].putFloat(getFieldOffset(pos), value);
    }

    @Override
    public boolean getBoolean(int pos) {
        assertIndexIsValid(pos);
        return segments[0].getBoolean(getFieldOffset(pos));
    }

    @Override
    public byte getByte(int pos) {
        assertIndexIsValid(pos);
        return segments[0].get(getFieldOffset(pos));
    }

    @Override
    public short getShort(int pos) {
        assertIndexIsValid(pos);
        return segments[0].getShort(getFieldOffset(pos));
    }

    @Override
    public int getInt(int pos) {
        assertIndexIsValid(pos);
        return segments[0].getInt(getFieldOffset(pos));
    }

    @Override
    public long getLong(int pos) {
        assertIndexIsValid(pos);
        return segments[0].getLong(getFieldOffset(pos));
    }

    @Override
    public float getFloat(int pos) {
        assertIndexIsValid(pos);
        return segments[0].getFloat(getFieldOffset(pos));
    }

    @Override
    public double getDouble(int pos) {
        assertIndexIsValid(pos);
        return segments[0].getDouble(getFieldOffset(pos));
    }

    @Override
    public BinaryString getString(int pos) {
        assertIndexIsValid(pos);
        int fieldOffset = getFieldOffset(pos);
        final long offsetAndLen = segments[0].getLong(fieldOffset);
        return MemorySegmentUtils.readBinaryString(segments, offset, fieldOffset, offsetAndLen);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        assertIndexIsValid(pos);

        if (Decimal.isCompact(precision)) {
            return Decimal.fromUnscaledLong(
                    segments[0].getLong(getFieldOffset(pos)), precision, scale);
        }

        int fieldOffset = getFieldOffset(pos);
        final long offsetAndSize = segments[0].getLong(fieldOffset);
        return MemorySegmentUtils.readDecimal(segments, offset, offsetAndSize, precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        assertIndexIsValid(pos);

        if (Timestamp.isCompact(precision)) {
            return Timestamp.fromEpochMillis(segments[0].getLong(getFieldOffset(pos)));
        }

        int fieldOffset = getFieldOffset(pos);
        final long offsetAndNanoOfMilli = segments[0].getLong(fieldOffset);
        return MemorySegmentUtils.readTimestampData(segments, offset, offsetAndNanoOfMilli);
    }

    @Override
    public byte[] getBinary(int pos) {
        assertIndexIsValid(pos);
        int fieldOffset = getFieldOffset(pos);
        final long offsetAndLen = segments[0].getLong(fieldOffset);
        return MemorySegmentUtils.readBinary(segments, offset, fieldOffset, offsetAndLen);
    }

    @Override
    public Variant getVariant(int pos) {
        assertIndexIsValid(pos);
        int fieldOffset = getFieldOffset(pos);
        final long offsetAndLen = segments[0].getLong(fieldOffset);
        return MemorySegmentUtils.readVariant(segments, offset, offsetAndLen);
    }

    @Override
    public Blob getBlob(int pos) {
        return new BlobData(getBinary(pos));
    }

    @Override
    public InternalArray getArray(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.readArrayData(segments, offset, getLong(pos));
    }

    @Override
    public InternalMap getMap(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.readMapData(segments, offset, getLong(pos));
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.readRowData(segments, numFields, offset, getLong(pos));
    }

    /** The bit is 1 when the field is null. Default is 0. */
    public boolean anyNull() {
        // Skip the header.
        if ((segments[0].getLong(0) & FIRST_BYTE_ZERO) != 0) {
            return true;
        }
        for (int i = 8; i < nullBitsSizeInBytes; i += 8) {
            if (segments[0].getLong(i) != 0) {
                return true;
            }
        }
        return false;
    }

    public boolean anyNull(int[] fields) {
        for (int field : fields) {
            if (isNullAt(field)) {
                return true;
            }
        }
        return false;
    }

    public BinaryRow copy() {
        return copy(new BinaryRow(arity));
    }

    public BinaryRow copy(BinaryRow reuse) {
        return copyInternal(reuse);
    }

    private BinaryRow copyInternal(BinaryRow reuse) {
        byte[] bytes = MemorySegmentUtils.copyToBytes(segments, offset, sizeInBytes);
        reuse.pointTo(MemorySegment.wrap(bytes), 0, sizeInBytes);
        return reuse;
    }

    public void clear() {
        segments = null;
        offset = 0;
        sizeInBytes = 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        // both BinaryRow and NestedRow have the same memory format
        if (!(o instanceof BinaryRow || o instanceof NestedRow)) {
            return false;
        }
        final BinarySection that = (BinarySection) o;
        return sizeInBytes == that.sizeInBytes
                && MemorySegmentUtils.equals(
                        segments, offset, that.segments, that.offset, sizeInBytes);
    }

    @Override
    public int hashCode() {
        return MemorySegmentUtils.hashByWords(segments, offset, sizeInBytes);
    }

    public static BinaryRow singleColumn(@Nullable Integer i) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.reset();
        if (i == null) {
            writer.setNullAt(0);
        } else {
            writer.writeInt(0, i);
        }
        writer.complete();
        return row;
    }

    public static BinaryRow singleColumn(@Nullable String string) {
        BinaryString binaryString = string == null ? null : BinaryString.fromString(string);
        return singleColumn(binaryString);
    }

    public static BinaryRow singleColumn(@Nullable BinaryString string) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.reset();
        if (string == null) {
            writer.setNullAt(0);
        } else {
            writer.writeString(0, string);
        }
        writer.complete();
        return row;
    }

    /**
     * If it is a fixed-length field, we can call this BinaryRowData's setXX method for in-place
     * updates. If it is variable-length field, can't use this method, because the underlying data
     * is stored continuously.
     */
    public static boolean isInFixedLengthPart(DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return true;
            case DECIMAL:
                return Decimal.isCompact(((DecimalType) type).getPrecision());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return Timestamp.isCompact(((TimestampType) type).getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Timestamp.isCompact(((LocalZonedTimestampType) type).getPrecision());
            default:
                return false;
        }
    }
}
