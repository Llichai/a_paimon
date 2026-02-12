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

import org.apache.paimon.utils.MurmurHashUtils;

/**
 * 内存段的切片视图。
 *
 * <p>MemorySlice 是对 {@link MemorySegment} 的轻量级包装,表示内存段的一个连续区域。
 *
 * <h2>核心特性</h2>
 *
 * <ul>
 *   <li>零拷贝视图:不复制数据,只记录偏移量和长度
 *   <li>支持子切片:可以从现有切片创建更小的子切片
 *   <li>实现 Comparable:支持按字节序比较
 *   <li>提供便捷的读取方法:readByte、readInt、readLong等
 * </ul>
 *
 * <h2>使用场景</h2>
 *
 * <ul>
 *   <li>表示序列化数据的一部分,如字符串、二进制数据
 *   <li>在不同组件间传递内存区域引用,避免数据复制
 *   <li>作为哈希表的键,利用其 hashCode 和 equals 实现
 * </ul>
 *
 * <h2>线程安全性</h2>
 *
 * <p>该类不是线程安全的。多线程访问需要外部同步。
 *
 * @see MemorySegment
 */
public final class MemorySlice implements Comparable<MemorySlice> {

    /** 底层的内存段。 */
    private final MemorySegment segment;

    /** 切片在内存段中的起始偏移量。 */
    private final int offset;

    /** 切片的长度(字节数)。 */
    private final int length;

    /**
     * 构造内存切片。
     *
     * @param segment 底层内存段
     * @param offset 起始偏移量
     * @param length 长度
     */
    public MemorySlice(MemorySegment segment, int offset, int length) {
        this.segment = segment;
        this.offset = offset;
        this.length = length;
    }

    /** 获取底层内存段。 */
    public MemorySegment segment() {
        return segment;
    }

    /** 获取起始偏移量。 */
    public int offset() {
        return offset;
    }

    /** 获取长度。 */
    public int length() {
        return length;
    }

    /**
     * 创建子切片。
     *
     * <p>如果index为0且length相同,返回当前切片本身(不创建新对象)。
     *
     * @param index 相对于当前切片的起始位置
     * @param length 子切片长度
     * @return 子切片
     */
    public MemorySlice slice(int index, int length) {
        if (index == 0 && length == this.length) {
            return this;
        }

        return new MemorySlice(segment, offset + index, length);
    }

    /** 读取指定位置的byte值。 */
    public byte readByte(int position) {
        return segment.get(offset + position);
    }

    /** 读取指定位置的int值。 */
    public int readInt(int position) {
        return segment.getInt(offset + position);
    }

    /** 读取指定位置的short值。 */
    public short readShort(int position) {
        return segment.getShort(offset + position);
    }

    /** 读取指定位置的long值。 */
    public long readLong(int position) {
        return segment.getLong(offset + position);
    }

    /** 获取底层堆内存数组(如果是堆内存)。 */
    public byte[] getHeapMemory() {
        return segment.getHeapMemory();
    }

    /**
     * 复制切片数据到新的字节数组。
     *
     * @return 包含切片所有数据的新字节数组
     */
    public byte[] copyBytes() {
        byte[] bytes = new byte[length];
        segment.get(offset, bytes, 0, length);
        return bytes;
    }

    /**
     * 将切片转换为输入流。
     *
     * @return 可用于读取切片数据的输入流
     */
    public MemorySliceInput toInput() {
        return new MemorySliceInput(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MemorySlice slice = (MemorySlice) o;

        // do lengths match
        if (length != slice.length) {
            return false;
        }

        // if arrays have same base offset, some optimizations can be taken...
        if (offset == slice.offset && segment == slice.segment) {
            return true;
        }
        return segment.equalTo(slice.segment, offset, slice.offset, length);
    }

    @Override
    public int hashCode() {
        return MurmurHashUtils.hashBytes(segment, offset, length);
    }

    /**
     * 将字节数组包装为切片。
     *
     * @param bytes 要包装的字节数组
     * @return 新的内存切片
     */
    public static MemorySlice wrap(byte[] bytes) {
        return new MemorySlice(MemorySegment.wrap(bytes), 0, bytes.length);
    }

    /**
     * 将整个内存段包装为切片。
     *
     * @param segment 要包装的内存段
     * @return 新的内存切片
     */
    public static MemorySlice wrap(MemorySegment segment) {
        return new MemorySlice(segment, 0, segment.size());
    }

    /**
     * 比较两个切片(字典序)。
     *
     * @param other 要比较的另一个切片
     * @return 比较结果:负数、0或正数
     */
    @Override
    public int compareTo(MemorySlice other) {
        int len = Math.min(length, other.length);
        for (int i = 0; i < len; i++) {
            int res =
                    (segment.get(offset + i) & 0xFF) - (other.segment.get(other.offset + i) & 0xFF);
            if (res != 0) {
                return res;
            }
        }
        return length - other.length;
    }
}
