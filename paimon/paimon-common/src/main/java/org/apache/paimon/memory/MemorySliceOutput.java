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

package org.apache.paimon.memory;

/**
 * 内存段输出流。
 *
 * <p>提供向 {@link MemorySegment} 写入数据的能力,支持:
 *
 * <ul>
 *   <li>写入基本类型:byte、int、short、long等
 *   <li>写入变长编码的整数:writeVarLenInt、writeVarLenLong
 *   <li>写入字节数组
 *   <li>动态扩容:当空间不足时自动扩展
 * </ul>
 *
 * <h2>动态扩容</h2>
 *
 * <p>初始分配指定大小的内存,写入时如果空间不足会自动扩容(每次翻倍),避免频繁的内存分配。
 *
 * <h2>使用方式</h2>
 *
 * <pre>{@code
 * MemorySliceOutput output = new MemorySliceOutput(256);
 * output.writeInt(42);
 * output.writeBytes(data);
 * MemorySlice slice = output.toSlice();
 * }</pre>
 *
 * <h2>线程安全性</h2>
 *
 * <p>该类不是线程安全的。
 */
public class MemorySliceOutput {

    /** 底层内存段,可能会因扩容而更换。 */
    private MemorySegment segment;

    /** 当前已写入的数据大小。 */
    private int size;

    /**
     * 构造内存段输出流。
     *
     * @param estimatedSize 预估大小(字节),用于初始内存分配
     */
    public MemorySliceOutput(int estimatedSize) {
        this.segment = MemorySegment.wrap(new byte[estimatedSize]);
    }

    /**
     * 获取当前已写入的数据大小。
     *
     * @return 数据大小(字节)
     */
    public int size() {
        return size;
    }

    /**
     * 将写入的数据转换为内存切片。
     *
     * @return 包含所有已写入数据的切片
     */
    public MemorySlice toSlice() {
        return new MemorySlice(segment, 0, size);
    }

    /**
     * 重置输出流,清空所有数据。
     *
     * <p>不释放底层内存,可以继续写入。
     */
    public void reset() {
        size = 0;
    }

    /**
     * 写入一个字节。
     *
     * @param value 字节值(只使用低8位)
     */
    public void writeByte(int value) {
        ensureSize(size + 1);
        segment.put(size++, (byte) value);
    }

    /**
     * 写入一个 int 值。
     *
     * @param value int 值
     */
    public void writeInt(int value) {
        ensureSize(size + 4);
        segment.putInt(size, value);
        size += 4;
    }

    /**
     * 写入一个 short 值。
     *
     * @param value short 值
     */
    public void writeShort(int value) {
        ensureSize(size + 2);
        segment.putShort(size, (short) value);
        size += 2;
    }

    /**
     * 写入变长编码的 int 值。
     *
     * <p>使用每字节7位存储数据,最高位作为延续标志,适合存储小整数。
     *
     * @param value 要写入的非负整数
     * @throws IllegalArgumentException 如果 value 为负数
     */
    public void writeVarLenInt(int value) {
        if (value < 0) {
            throw new IllegalArgumentException("negative value: v=" + value);
        }

        while ((value & ~0x7F) != 0) {
            writeByte(((value & 0x7F) | 0x80));
            value >>>= 7;
        }

        writeByte((byte) value);
    }

    /**
     * 写入一个 long 值。
     *
     * @param value long 值
     */
    public void writeLong(long value) {
        ensureSize(size + 8);
        segment.putLong(size, value);
        size += 8;
    }

    /**
     * 写入变长编码的 long 值。
     *
     * @param value 要写入的非负长整数
     * @throws IllegalArgumentException 如果 value 为负数
     */
    public void writeVarLenLong(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("negative value: v=" + value);
        }

        while ((value & ~0x7FL) != 0) {
            writeByte((((int) value & 0x7F) | 0x80));
            value >>>= 7;
        }
        writeByte((byte) value);
    }

    /**
     * 写入整个字节数组。
     *
     * @param source 源字节数组
     */
    public void writeBytes(byte[] source) {
        writeBytes(source, 0, source.length);
    }

    /**
     * 写入字节数组的指定部分。
     *
     * @param source 源字节数组
     * @param sourceIndex 起始索引
     * @param length 写入长度
     */
    public void writeBytes(byte[] source, int sourceIndex, int length) {
        ensureSize(size + length);
        segment.put(size, source, sourceIndex, length);
        size += length;
    }

    /**
     * 确保有足够的空间容纳指定字节数。
     *
     * <p>如果空间不足,分配新的内存段(容量翻倍)并复制现有数据。
     *
     * @param minWritableBytes 需要的最小可写字节数
     */
    private void ensureSize(int minWritableBytes) {
        if (minWritableBytes <= segment.size()) {
            return;
        }

        int newCapacity = segment.size();
        int minNewCapacity = segment.size() + minWritableBytes;
        while (newCapacity < minNewCapacity) {
            newCapacity <<= 1;
        }

        MemorySegment newSegment = MemorySegment.wrap(new byte[newCapacity]);
        segment.copyTo(0, newSegment, 0, segment.size());
        segment = newSegment;
    }
}
