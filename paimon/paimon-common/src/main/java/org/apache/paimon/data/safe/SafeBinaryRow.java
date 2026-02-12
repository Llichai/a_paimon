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

package org.apache.paimon.data.safe;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.memory.BytesUtils;
import org.apache.paimon.types.RowKind;

import static org.apache.paimon.data.BinaryRow.HEADER_SIZE_IN_BITS;
import static org.apache.paimon.data.BinaryRow.calculateBitSetWidthInBytes;
import static org.apache.paimon.memory.MemorySegmentUtils.BIT_BYTE_INDEX_MASK;
import static org.apache.paimon.memory.MemorySegmentUtils.byteIndex;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 安全的二进制行实现，避免核心转储问题。
 *
 * <p>这是 {@link BinaryRow} 的安全版本，使用纯 Java 字节数组操作而不是直接内存访问，
 * 从而避免在某些环境下可能出现的核心转储（core dump）问题。
 *
 * <p><b>设计特点：</b>
 * <ul>
 *   <li>使用 byte[] 数组代替 MemorySegment 进行数据存储
 *   <li>所有数据访问都经过边界检查，更加安全
 *   <li>适用于需要高稳定性但可以接受轻微性能损失的场景
 *   <li>与 BinaryRow 保持相同的二进制格式，方便互操作
 * </ul>
 *
 * <p><b>内存布局：</b>
 * <pre>
 * [RowKind (1 byte)] [Null Bits] [Field Values (fixed 8 bytes per field)] [Variable Length Data]
 * </pre>
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li>在沙箱环境中运行，禁止直接内存访问
 *   <li>需要跨越 JNI 边界传递数据
 *   <li>调试和测试阶段，需要更好的错误诊断
 * </ul>
 *
 * @see BinaryRow
 * @since 1.0
 */
public final class SafeBinaryRow implements InternalRow {

    /** 字段数量（列数）。 */
    private final int arity;

    /** null 位数组占用的字节数。 */
    private final int nullBitsSizeInBytes;

    /** 底层字节数组，存储完整的行数据。 */
    private final byte[] bytes;

    /** 当前行数据在 bytes 数组中的起始偏移量。 */
    private final int offset;

    /**
     * 构造安全二进制行。
     *
     * @param arity 字段数量
     * @param bytes 字节数组
     * @param offset 数据起始偏移量
     */
    public SafeBinaryRow(int arity, byte[] bytes, int offset) {
        checkArgument(arity >= 0);
        this.arity = arity;
        this.nullBitsSizeInBytes = calculateBitSetWidthInBytes(arity);
        this.bytes = bytes;
        this.offset = offset;
    }

    /**
     * 获取指定字段的偏移量。
     *
     * <p>每个字段占用固定 8 字节空间。
     *
     * @param pos 字段位置
     * @return 字段在 bytes 数组中的偏移量
     */
    private int getFieldOffset(int pos) {
        return offset + nullBitsSizeInBytes + pos * 8;
    }

    @Override
    public int getFieldCount() {
        return arity;
    }

    @Override
    public RowKind getRowKind() {
        byte kindValue = bytes[offset];
        return RowKind.fromByteValue(kindValue);
    }

    @Override
    public void setRowKind(RowKind kind) {
        bytes[offset] = kind.toByteValue();
    }

    @Override
    public boolean isNullAt(int pos) {
        int index = pos + HEADER_SIZE_IN_BITS;
        int offset = this.offset + byteIndex(index);
        byte current = bytes[offset];
        return (current & (1 << (index & BIT_BYTE_INDEX_MASK))) != 0;
    }

    @Override
    public boolean getBoolean(int pos) {
        return bytes[getFieldOffset(pos)] != 0;
    }

    @Override
    public byte getByte(int pos) {
        return bytes[getFieldOffset(pos)];
    }

    @Override
    public short getShort(int pos) {
        return BytesUtils.getShort(bytes, getFieldOffset(pos));
    }

    @Override
    public int getInt(int pos) {
        return BytesUtils.getInt(bytes, getFieldOffset(pos));
    }

    @Override
    public long getLong(int pos) {
        return BytesUtils.getLong(bytes, getFieldOffset(pos));
    }

    @Override
    public float getFloat(int pos) {
        return Float.intBitsToFloat(getInt(pos));
    }

    @Override
    public double getDouble(int pos) {
        return Double.longBitsToDouble(getLong(pos));
    }

    @Override
    public BinaryString getString(int pos) {
        return BinaryString.fromBytes(getBinary(pos));
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        long longValue = getLong(pos);
        if (Decimal.isCompact(precision)) {
            return Decimal.fromUnscaledLong(longValue, precision, scale);
        }

        final int size = ((int) longValue);
        int subOffset = (int) (longValue >> 32);
        byte[] decimalBytes = new byte[size];
        System.arraycopy(bytes, offset + subOffset, decimalBytes, 0, size);
        return Decimal.fromUnscaledBytes(decimalBytes, precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        long longValue = getLong(pos);
        if (Timestamp.isCompact(precision)) {
            return Timestamp.fromEpochMillis(longValue);
        }

        final int nanoOfMillisecond = (int) longValue;
        final int subOffset = (int) (longValue >> 32);

        checkArgument(bytes.length >= offset + subOffset + 8);
        final long millisecond = BytesUtils.getLong(bytes, offset + subOffset);
        return Timestamp.fromEpochMillis(millisecond, nanoOfMillisecond);
    }

    @Override
    public byte[] getBinary(int pos) {
        return BytesUtils.readBinary(bytes, offset, getFieldOffset(pos), getLong(pos));
    }

    @Override
    public Variant getVariant(int pos) {
        return BytesUtils.readVariant(bytes, offset, getLong(pos));
    }

    @Override
    public Blob getBlob(int pos) {
        return new BlobData(getBinary(pos));
    }

    @Override
    public InternalArray getArray(int pos) {
        return readArrayData(bytes, offset, getLong(pos));
    }

    private static InternalArray readArrayData(byte[] bytes, int baseOffset, long offsetAndSize) {
        int offset = (int) (offsetAndSize >> 32);
        return new SafeBinaryArray(bytes, offset + baseOffset);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return readNestedRow(bytes, numFields, offset, getLong(pos));
    }

    private static InternalRow readNestedRow(
            byte[] bytes, int numFields, int baseOffset, long offsetAndSize) {
        int offset = (int) (offsetAndSize >> 32);
        return new SafeBinaryRow(numFields, bytes, offset + baseOffset);
    }

    @Override
    public InternalMap getMap(int pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException();
    }
}
