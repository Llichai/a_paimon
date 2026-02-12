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
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentUtils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.apache.paimon.data.BinarySection.MAX_FIX_PART_DATA_SIZE;

/**
 * 抽象二进制写入器。
 *
 * <p>使用特殊格式将数据写入 {@link MemorySegment}（其容量可自动增长）。
 * 该类是 BinaryRowWriter 和 BinaryArrayWriter 的基础类。
 *
 * <p>写入二进制格式的流程：
 * <ol>
 *   <li>创建一个新的写入器
 *   <li>通过 writeXX 或 setNullAt 写入每个字段（变长字段不能重复写入）
 *   <li>调用 {@link #complete()} 完成写入
 * </ol>
 *
 * <p>如果要重用此写入器，请先调用 {@link #reset()}。
 *
 * <p>设计要点：
 * <ul>
 *   <li>自动扩容：当内存不足时自动增长 MemorySegment 容量
 *   <li>混合布局：小数据存储在固定长度部分，大数据存储在可变长度部分
 *   <li>字节对齐：数据按 8 字节边界对齐，提高访问性能
 *   <li>高效序列化：避免对象创建，直接操作内存
 * </ul>
 */
abstract class AbstractBinaryWriter implements BinaryWriter {

    /** 当前使用的内存段 */
    protected MemorySegment segment;

    /** 可变长度部分的写入游标 */
    protected int cursor;

    /**
     * 设置偏移量和大小到固定长度部分。
     *
     * @param pos 字段位置索引
     * @param offset 数据在可变长度部分的偏移量
     * @param size 数据大小
     */
    protected abstract void setOffsetAndSize(int pos, int offset, long size);

    /**
     * 获取字段在固定长度部分的偏移量。
     *
     * @param pos 字段位置索引
     * @return 字段偏移量
     */
    protected abstract int getFieldOffset(int pos);

    /**
     * 内存段增长后的回调。
     *
     * <p>当 segment 被替换为更大的内存段时，子类需要更新其引用。
     */
    protected abstract void afterGrow();

    /**
     * 设置 NULL 位标记。
     *
     * @param ordinal 字段序号
     */
    protected abstract void setNullBit(int ordinal);

    /**
     * 写入字符串。
     *
     * <p>根据字符串长度决定存储位置：
     * <ul>
     *   <li>≤7 字节：存储在固定长度部分
     *   <li>>7 字节：存储在可变长度部分
     * </ul>
     *
     * <p>详见 {@link MemorySegmentUtils#readBinaryString(MemorySegment[], int, int, long)}。
     *
     * @param pos 字段位置索引
     * @param input 二进制字符串
     */
    @Override
    public void writeString(int pos, BinaryString input) {
        if (input.getSegments() == null) {
            String javaObject = input.toString();
            writeBytes(pos, javaObject.getBytes(StandardCharsets.UTF_8));
        } else {
            int len = input.getSizeInBytes();
            if (len <= 7) {
                byte[] bytes = MemorySegmentUtils.allocateReuseBytes(len);
                MemorySegmentUtils.copyToBytes(
                        input.getSegments(), input.getOffset(), bytes, 0, len);
                writeBytesToFixLenPart(segment, getFieldOffset(pos), bytes, 0, len);
            } else {
                writeSegmentsToVarLenPart(pos, input.getSegments(), input.getOffset(), len);
            }
        }
    }

    /**
     * 写入字节数组。
     *
     * <p>根据长度决定存储位置：
     * <ul>
     *   <li>≤7 字节：存储在固定长度部分
     *   <li>>7 字节：存储在可变长度部分
     * </ul>
     *
     * @param pos 字段位置索引
     * @param bytes 字节数组
     */
    private void writeBytes(int pos, byte[] bytes) {
        int len = bytes.length;
        if (len <= MAX_FIX_PART_DATA_SIZE) {
            writeBytesToFixLenPart(segment, getFieldOffset(pos), bytes, 0, len);
        } else {
            writeBytesToVarLenPart(pos, bytes, 0, len);
        }
    }

    /**
     * 写入数组。
     *
     * <p>将 InternalArray 序列化为 BinaryArray 后写入可变长度部分。
     *
     * @param pos 字段位置索引
     * @param input 内部数组
     * @param serializer 数组序列化器
     */
    @Override
    public void writeArray(int pos, InternalArray input, InternalArraySerializer serializer) {
        BinaryArray binary = serializer.toBinaryArray(input);
        writeSegmentsToVarLenPart(
                pos, binary.getSegments(), binary.getOffset(), binary.getSizeInBytes());
    }

    /**
     * 写入 Map。
     *
     * <p>将 InternalMap 序列化为 BinaryMap 后写入可变长度部分。
     *
     * @param pos 字段位置索引
     * @param input 内部 Map
     * @param serializer Map 序列化器
     */
    @Override
    public void writeMap(int pos, InternalMap input, InternalMapSerializer serializer) {
        BinaryMap binary = serializer.toBinaryMap(input);
        writeSegmentsToVarLenPart(
                pos, binary.getSegments(), binary.getOffset(), binary.getSizeInBytes());
    }

    /**
     * 写入行数据。
     *
     * <p>如果输入已经是 BinarySection，直接写入；否则先序列化为 BinaryRow。
     *
     * @param pos 字段位置索引
     * @param input 内部行
     * @param serializer 行序列化器
     */
    @Override
    public void writeRow(int pos, InternalRow input, InternalRowSerializer serializer) {
        if (input instanceof BinarySection) {
            BinarySection row = (BinarySection) input;
            writeSegmentsToVarLenPart(
                    pos, row.getSegments(), row.getOffset(), row.getSizeInBytes());
        } else {
            BinaryRow row = serializer.toBinaryRow(input);
            writeSegmentsToVarLenPart(
                    pos, row.getSegments(), row.getOffset(), row.getSizeInBytes());
        }
    }

    /**
     * 写入二进制数据。
     *
     * <p>根据长度决定存储位置。
     *
     * @param pos 字段位置索引
     * @param bytes 字节数组
     * @param offset 数组起始偏移量
     * @param len 数据长度
     */
    @Override
    public void writeBinary(int pos, byte[] bytes, int offset, int len) {
        if (len <= BinarySection.MAX_FIX_PART_DATA_SIZE) {
            writeBytesToFixLenPart(segment, getFieldOffset(pos), bytes, offset, len);
        } else {
            writeBytesToVarLenPart(pos, bytes, offset, len);
        }
    }

    /**
     * 写入十进制数。
     *
     * <p>根据精度选择存储方式：
     * <ul>
     *   <li>紧凑型（precision ≤ 18）：直接存储为 long 值
     *   <li>非紧凑型（precision > 18）：存储为字节数组（最多 16 字节）
     * </ul>
     *
     * @param pos 字段位置索引
     * @param value 十进制数值
     * @param precision 精度
     */
    @Override
    public void writeDecimal(int pos, Decimal value, int precision) {
        assert value == null || (value.precision() == precision);

        if (Decimal.isCompact(precision)) {
            assert value != null;
            writeLong(pos, value.toUnscaledLong());
        } else {
            // grow the global buffer before writing data.
            ensureCapacity(16);

            // zero-out the bytes
            segment.putLong(cursor, 0L);
            segment.putLong(cursor + 8, 0L);

            // Make sure Decimal object has the same scale as DecimalType.
            // Note that we may pass in null Decimal object to set null for it.
            if (value == null) {
                setNullBit(pos);
                // keep the offset for future update
                setOffsetAndSize(pos, cursor, 0);
            } else {
                final byte[] bytes = value.toUnscaledBytes();
                assert bytes.length <= 16;

                // Write the bytes to the variable length portion.
                segment.put(cursor, bytes, 0, bytes.length);
                setOffsetAndSize(pos, cursor, bytes.length);
            }

            // move the cursor forward.
            cursor += 16;
        }
    }

    /**
     * 写入时间戳。
     *
     * <p>根据精度选择存储方式：
     * <ul>
     *   <li>紧凑型（precision ≤ 3）：直接存储毫秒值
     *   <li>非紧凑型（precision > 3）：存储毫秒值和纳秒值
     * </ul>
     *
     * @param pos 字段位置索引
     * @param value 时间戳值
     * @param precision 精度
     */
    @Override
    public void writeTimestamp(int pos, Timestamp value, int precision) {
        if (Timestamp.isCompact(precision)) {
            writeLong(pos, value.getMillisecond());
        } else {
            // store the nanoOfMillisecond in fixed-length part as offset and nanoOfMillisecond
            ensureCapacity(8);

            if (value == null) {
                setNullBit(pos);
                // zero-out the bytes
                segment.putLong(cursor, 0L);
                setOffsetAndSize(pos, cursor, 0);
            } else {
                segment.putLong(cursor, value.getMillisecond());
                setOffsetAndSize(pos, cursor, value.getNanoOfMillisecond());
            }

            cursor += 8;
        }
    }

    /**
     * 写入变体类型数据。
     *
     * <p>变体数据包含值和元数据两部分，格式为：
     * <ul>
     *   <li>4 字节：值的长度
     *   <li>N 字节：值数据
     *   <li>M 字节：元数据
     * </ul>
     *
     * @param pos 字段位置索引
     * @param variant 变体对象
     */
    @Override
    public void writeVariant(int pos, Variant variant) {
        byte[] value = variant.value();
        byte[] metadata = variant.metadata();
        int totalSize = 4 + value.length + metadata.length;
        final int roundedSize = roundNumberOfBytesToNearestWord(totalSize);
        ensureCapacity(roundedSize);
        zeroOutPaddingBytes(totalSize);

        segment.putInt(cursor, value.length);
        segment.put(cursor + 4, value, 0, value.length);
        segment.put(cursor + 4 + value.length, metadata, 0, metadata.length);

        setOffsetAndSize(pos, cursor, totalSize);
        cursor += roundedSize;
    }

    /**
     * 写入 Blob 大对象。
     *
     * <p>将 Blob 转换为字节数组后写入。
     *
     * @param pos 字段位置索引
     * @param blob Blob 对象
     */
    @Override
    public void writeBlob(int pos, Blob blob) {
        byte[] bytes = blob.toData();
        writeBinary(pos, bytes, 0, bytes.length);
    }

    /**
     * 将填充字节清零。
     *
     * <p>为了保持 8 字节对齐，需要将填充字节设置为 0。
     *
     * @param numBytes 实际数据字节数
     */
    protected void zeroOutPaddingBytes(int numBytes) {
        if ((numBytes & 0x07) > 0) {
            segment.putLong(cursor + ((numBytes >> 3) << 3), 0L);
        }
    }

    /**
     * 确保有足够的容量。
     *
     * <p>如果当前内存段容量不足，自动扩容。
     *
     * @param neededSize 需要的额外空间大小
     */
    protected void ensureCapacity(int neededSize) {
        final int length = cursor + neededSize;
        if (segment.size() < length) {
            grow(length);
        }
    }

    /**
     * 将内存段数据写入可变长度部分。
     *
     * <p>支持跨多个 MemorySegment 的数据写入。
     *
     * @param pos 字段位置索引
     * @param segments 源内存段数组
     * @param offset 源数据起始偏移量
     * @param size 数据大小
     */
    private void writeSegmentsToVarLenPart(
            int pos, MemorySegment[] segments, int offset, int size) {
        final int roundedSize = roundNumberOfBytesToNearestWord(size);

        // grow the global buffer before writing data.
        ensureCapacity(roundedSize);

        zeroOutPaddingBytes(size);

        if (segments.length == 1) {
            segments[0].copyTo(offset, segment, cursor, size);
        } else {
            writeMultiSegmentsToVarLenPart(segments, offset, size);
        }

        setOffsetAndSize(pos, cursor, size);

        // move the cursor forward.
        cursor += roundedSize;
    }

    /**
     * 将多个内存段的数据写入可变长度部分。
     *
     * <p>逐段复制数据，处理跨段的情况。
     *
     * @param segments 源内存段数组
     * @param offset 源数据起始偏移量
     * @param size 数据大小
     */
    private void writeMultiSegmentsToVarLenPart(MemorySegment[] segments, int offset, int size) {
        // Write the bytes to the variable length portion.
        int needCopy = size;
        int fromOffset = offset;
        int toOffset = cursor;
        for (MemorySegment sourceSegment : segments) {
            int remain = sourceSegment.size() - fromOffset;
            if (remain > 0) {
                int copySize = Math.min(remain, needCopy);
                sourceSegment.copyTo(fromOffset, segment, toOffset, copySize);
                needCopy -= copySize;
                toOffset += copySize;
                fromOffset = 0;
            } else {
                fromOffset -= sourceSegment.size();
            }
        }
    }

    /**
     * 将字节数组写入可变长度部分。
     *
     * @param pos 字段位置索引
     * @param bytes 字节数组
     * @param offset 数组起始偏移量
     * @param len 数据长度
     */
    private void writeBytesToVarLenPart(int pos, byte[] bytes, int offset, int len) {
        final int roundedSize = roundNumberOfBytesToNearestWord(len);

        // grow the global buffer before writing data.
        ensureCapacity(roundedSize);

        zeroOutPaddingBytes(len);

        // Write the bytes to the variable length portion.
        segment.put(cursor, bytes, offset, len);

        setOffsetAndSize(pos, cursor, len);

        // move the cursor forward.
        cursor += roundedSize;
    }

    /**
     * 扩容内存段。
     *
     * <p>采用 1.5 倍扩容策略，确保有足够空间。
     *
     * @param minCapacity 最小所需容量
     */
    private void grow(int minCapacity) {
        int oldCapacity = segment.size();
        int newCapacity = oldCapacity + (oldCapacity >> 1);
        if (newCapacity - minCapacity < 0) {
            newCapacity = minCapacity;
        }
        segment = MemorySegment.wrap(Arrays.copyOf(segment.getArray(), newCapacity));
        afterGrow();
    }

    /**
     * 将字节数向上取整到最近的字（8 字节）边界。
     *
     * <p>用于确保数据按 8 字节对齐，提高访问性能。
     *
     * @param numBytes 原始字节数
     * @return 对齐后的字节数
     */
    protected static int roundNumberOfBytesToNearestWord(int numBytes) {
        int remainder = numBytes & 0x07;
        if (remainder == 0) {
            return numBytes;
        } else {
            return numBytes + (8 - remainder);
        }
    }

    /**
     * 将字节数组写入固定长度部分。
     *
     * <p>数据格式：
     * <ul>
     *   <li>最高位 = 1（标记数据在固定长度部分）
     *   <li>接下来 7 位 = 数据长度
     *   <li>剩余 7 字节 = 实际数据
     * </ul>
     *
     * @param segment 目标内存段
     * @param fieldOffset 字段偏移量
     * @param bytes 字节数组
     * @param offset 数组起始偏移量
     * @param len 数据长度（≤7）
     */
    private static void writeBytesToFixLenPart(
            MemorySegment segment, int fieldOffset, byte[] bytes, int offset, int len) {
        long firstByte = len | 0x80; // first bit is 1, other bits is len
        long sevenBytes = 0L; // real data
        if (BinaryRow.LITTLE_ENDIAN) {
            for (int i = 0; i < len; i++) {
                sevenBytes |= ((0x00000000000000FFL & bytes[offset + i]) << (i * 8L));
            }
        } else {
            for (int i = 0; i < len; i++) {
                sevenBytes |= ((0x00000000000000FFL & bytes[offset + i]) << ((6 - i) * 8L));
            }
        }

        final long offsetAndSize = (firstByte << 56) | sevenBytes;

        segment.putLong(fieldOffset, offsetAndSize);
    }

    /**
     * 获取内存段。
     *
     * @return 当前使用的内存段
     */
    public MemorySegment getSegments() {
        return segment;
    }
}
