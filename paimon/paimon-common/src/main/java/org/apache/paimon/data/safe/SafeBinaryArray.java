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

import static org.apache.paimon.data.BinaryArray.calculateHeaderInBytes;
import static org.apache.paimon.memory.MemorySegmentUtils.BIT_BYTE_INDEX_MASK;
import static org.apache.paimon.memory.MemorySegmentUtils.byteIndex;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 安全的二进制数组实现，避免核心转储问题。
 *
 * <p>这是 {@link BinaryArray} 的安全版本，使用纯 Java 字节数组操作而不是直接内存访问。
 *
 * <p><b>设计特点：</b>
 * <ul>
 *   <li>使用 byte[] 数组代替 MemorySegment 进行数据存储
 *   <li>所有数据访问都经过边界检查，避免内存访问错误
 *   <li>与 BinaryArray 保持相同的二进制格式
 *   <li>适用于需要高稳定性的场景
 * </ul>
 *
 * <p><b>内存布局：</b>
 * <pre>
 * [Size (4 bytes)] [Null Bits] [Element Data (fixed size per element for primitives)]
 * </pre>
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li>在限制直接内存访问的环境中运行
 *   <li>跨越 JNI 边界传递数组数据
 *   <li>调试和测试，需要更好的错误诊断
 * </ul>
 *
 * @see BinaryArray
 * @since 1.0
 */
public final class SafeBinaryArray implements InternalArray {

    /** 数组元素数量。 */
    private final int size;

    /** 底层字节数组，存储完整的数组数据。 */
    private final byte[] bytes;

    /** 数组数据在 bytes 数组中的起始偏移量。 */
    private final int offset;

    /** 数组元素数据区的起始偏移量。 */
    private final int elementOffset;

    /**
     * 构造安全二进制数组。
     *
     * <p>从 bytes 数组中读取 size，并计算元素数据区的起始偏移量。
     *
     * @param bytes 字节数组
     * @param offset 数组数据起始偏移量
     */
    public SafeBinaryArray(byte[] bytes, int offset) {
        checkArgument(bytes.length > offset + 4);
        final int size = BytesUtils.getInt(bytes, offset);
        assert size >= 0 : "size (" + size + ") should >= 0";

        this.size = size;
        this.bytes = bytes;
        this.offset = offset;
        this.elementOffset = offset + calculateHeaderInBytes(this.size);
    }

    @Override
    public int size() {
        return size;
    }

    /**
     * 获取指定位置元素的偏移量。
     *
     * @param ordinal 元素索引
     * @param elementSize 元素大小（字节数）
     * @return 元素在 bytes 数组中的偏移量
     */
    private int getElementOffset(int ordinal, int elementSize) {
        return elementOffset + ordinal * elementSize;
    }

    @Override
    public boolean isNullAt(int pos) {
        byte current = bytes[offset + 4 + byteIndex(pos)];
        return (current & (1 << (pos & BIT_BYTE_INDEX_MASK))) != 0;
    }

    @Override
    public boolean getBoolean(int pos) {
        return bytes[getElementOffset(pos, 1)] != 0;
    }

    @Override
    public byte getByte(int pos) {
        return bytes[getElementOffset(pos, 1)];
    }

    @Override
    public short getShort(int pos) {
        int fieldOffset = getElementOffset(pos, 2);
        checkArgument(bytes.length >= fieldOffset + 2);
        return BytesUtils.getShort(bytes, fieldOffset);
    }

    @Override
    public int getInt(int pos) {
        int fieldOffset = getElementOffset(pos, 4);
        checkArgument(bytes.length >= fieldOffset + 4);
        return BytesUtils.getInt(bytes, fieldOffset);
    }

    @Override
    public long getLong(int pos) {
        int fieldOffset = getElementOffset(pos, 8);
        checkArgument(bytes.length >= fieldOffset + 8);
        return BytesUtils.getLong(bytes, fieldOffset);
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
        return BytesUtils.readBinary(bytes, offset, getElementOffset(pos, 8), getLong(pos));
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
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalMap getMap(int pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
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

    @Override
    public boolean[] toBooleanArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] toByteArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public short[] toShortArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int[] toIntArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long[] toLongArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public float[] toFloatArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public double[] toDoubleArray() {
        throw new UnsupportedOperationException();
    }
}
