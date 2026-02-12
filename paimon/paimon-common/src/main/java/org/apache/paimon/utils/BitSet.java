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

package org.apache.paimon.utils;

import org.apache.paimon.memory.MemorySegment;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 基于 {@link MemorySegment} 的位集合。
 *
 * <p>提供高效的位操作,支持设置、获取和清除位。
 * 内部使用内存段来存储位数据,适合大规模位操作场景。
 */
public class BitSet {

    /** 字节索引掩码,用于获取字节内的位索引 */
    private static final int BYTE_INDEX_MASK = 0x00000007;

    /** 内存段,用于存储位数据 */
    private MemorySegment memorySegment;

    /** 内存段字节数组偏移量 */
    private int offset;

    /** BitSet 的字节大小 */
    private final int byteLength;

    /** BitSet 的位大小 */
    private final int bitLength;

    /**
     * 构造一个指定字节大小的位集合。
     *
     * @param byteSize 字节大小,必须大于 0
     */
    public BitSet(int byteSize) {
        checkArgument(byteSize > 0, "bits size should be greater than 0.");
        this.byteLength = byteSize;
        this.bitLength = byteSize << 3;
    }

    /**
     * 设置位集合使用的内存段。
     *
     * @param memorySegment 内存段,不能为 null
     * @param offset 偏移量,必须为非负整数
     */
    public void setMemorySegment(MemorySegment memorySegment, int offset) {
        checkArgument(memorySegment != null, "MemorySegment can not be null.");
        checkArgument(offset >= 0, "Offset should be positive integer.");
        checkArgument(
                offset + byteLength <= memorySegment.size(),
                "Could not set MemorySegment, the remain buffers is not enough.");
        this.memorySegment = memorySegment;
        this.offset = offset;
    }

    /**
     * 取消设置内存段。
     */
    public void unsetMemorySegment() {
        this.memorySegment = null;
    }

    /**
     * 获取当前使用的内存段。
     *
     * @return 内存段
     */
    public MemorySegment getMemorySegment() {
        return this.memorySegment;
    }

    /**
     * 设置指定索引处的位。
     *
     * @param index 位置索引,必须在 [0, bitLength) 范围内
     */
    public void set(int index) {
        checkArgument(index < bitLength && index >= 0);

        int byteIndex = index >>> 3;
        byte current = memorySegment.get(offset + byteIndex);
        current |= (1 << (index & BYTE_INDEX_MASK));
        memorySegment.put(offset + byteIndex, current);
    }

    /**
     * 获取指定索引处的位是否被设置。
     *
     * @param index 位置索引,必须在 [0, bitLength) 范围内
     * @return 如果位被设置则返回 true,否则返回 false
     */
    public boolean get(int index) {
        checkArgument(index < bitLength && index >= 0);

        int byteIndex = index >>> 3;
        byte current = memorySegment.get(offset + byteIndex);
        return (current & (1 << (index & BYTE_INDEX_MASK))) != 0;
    }

    /**
     * 获取位的数量。
     *
     * @return 位数量
     */
    public int bitSize() {
        return bitLength;
    }

    /**
     * 清除位集合,将所有位设置为 0。
     */
    public void clear() {
        int index = 0;
        while (index + 8 <= byteLength) {
            memorySegment.putLong(offset + index, 0L);
            index += 8;
        }
        while (index < byteLength) {
            memorySegment.put(offset + index, (byte) 0);
            index += 1;
        }
    }

    @Override
    public String toString() {
        return "BitSet:\n"
                + "\tMemorySegment:"
                + memorySegment.size()
                + "\n"
                + "\tOffset:"
                + offset
                + "\n"
                + "\tLength:"
                + byteLength
                + "\n";
    }
}
