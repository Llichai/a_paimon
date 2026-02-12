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

import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentUtils;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Preconditions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * 二进制内存段描述符。
 *
 * <p>描述一段内存区域，用于存储二进制格式的数据。该类是 BinaryRow、BinaryArray、BinaryMap 等
 * 二进制数据结构的基础类。
 *
 * <p>内存布局设计：
 * <ul>
 *   <li>采用固定长度部分（FixLenPart）+ 可变长度部分（VarLenPart）的混合布局
 *   <li>小数据（≤7字节）直接存储在固定长度部分，节省空间
 *   <li>大数据（≥8字节）存储在可变长度部分，固定长度部分存储偏移量和长度
 * </ul>
 *
 * <p>数据格式：
 * <ul>
 *   <li>小数据格式（len ≤ 7）：
 *       <ul>
 *         <li>1 位标记位 = 1（表示存储在固定长度部分）
 *         <li>7 位长度信息
 *         <li>7 字节数据内容
 *       </ul>
 *   <li>大数据格式（len ≥ 8）：
 *       <ul>
 *         <li>1 位标记位 = 0（表示存储在可变长度部分）
 *         <li>31 位偏移量信息
 *         <li>4 字节长度信息
 *       </ul>
 * </ul>
 *
 * <p>设计优势：
 * <ul>
 *   <li>空间效率：小数据避免了指针和长度的额外开销
 *   <li>缓存友好：固定长度部分的数据局部性好，提高缓存命中率
 *   <li>灵活性：支持跨多个 MemorySegment 的数据存储
 *   <li>序列化支持：实现了 Java 序列化接口
 * </ul>
 */
public abstract class BinarySection implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 固定长度部分可存储的最大数据大小（7 字节）。
     *
     * <p>如果数据长度小于 8 字节，其二进制格式为：
     * <ul>
     *   <li>1 位标记位 = 1
     *   <li>7 位长度
     *   <li>7 字节数据
     * </ul>
     * 数据存储在固定长度部分。
     *
     * <p>如果数据长度大于或等于 8 字节，其二进制格式为：
     * <ul>
     *   <li>1 位标记位 = 0
     *   <li>31 位偏移量
     *   <li>4 字节长度
     * </ul>
     * 数据存储在可变长度部分。
     */
    public static final int MAX_FIX_PART_DATA_SIZE = 7;

    /**
     * 最高位标记位掩码。
     *
     * <p>格式：10000000 00000000 ... (8 字节)
     *
     * <p>用于判断数据是存储在固定长度部分还是可变长度部分。
     * 详见 {@link #MAX_FIX_PART_DATA_SIZE}。
     */
    public static final long HIGHEST_FIRST_BIT = 0x80L << 56;

    /**
     * 第 2-8 位长度信息掩码。
     *
     * <p>格式：01111111 00000000 ... (8 字节)
     *
     * <p>用于获取存储在 long 值中的数据长度。
     * 详见 {@link #MAX_FIX_PART_DATA_SIZE}。
     */
    public static final long HIGHEST_SECOND_TO_EIGHTH_BIT = 0x7FL << 56;

    /**
     * 内存段数组（支持跨段存储）。
     *
     * <p>标记为 transient，因为序列化时会转换为字节数组。
     */
    protected transient MemorySegment[] segments;

    /**
     * 数据在内存段中的起始偏移量。
     */
    protected transient int offset;

    /**
     * 数据的总字节大小。
     */
    protected transient int sizeInBytes;

    /**
     * 默认构造函数。
     */
    public BinarySection() {}

    /**
     * 构造函数。
     *
     * @param segments 内存段数组
     * @param offset 起始偏移量
     * @param sizeInBytes 数据大小（字节）
     */
    public BinarySection(MemorySegment[] segments, int offset, int sizeInBytes) {
        Preconditions.checkArgument(segments != null);
        this.segments = segments;
        this.offset = offset;
        this.sizeInBytes = sizeInBytes;
    }

    /**
     * 指向单个内存段的数据。
     *
     * @param segment 内存段
     * @param offset 起始偏移量
     * @param sizeInBytes 数据大小（字节）
     */
    public final void pointTo(MemorySegment segment, int offset, int sizeInBytes) {
        pointTo(new MemorySegment[] {segment}, offset, sizeInBytes);
    }

    /**
     * 指向多个内存段的数据。
     *
     * @param segments 内存段数组
     * @param offset 起始偏移量
     * @param sizeInBytes 数据大小（字节）
     */
    public void pointTo(MemorySegment[] segments, int offset, int sizeInBytes) {
        Preconditions.checkArgument(segments != null);
        this.segments = segments;
        this.offset = offset;
        this.sizeInBytes = sizeInBytes;
    }

    /**
     * 获取内存段数组。
     *
     * @return 内存段数组
     */
    public MemorySegment[] getSegments() {
        return segments;
    }

    /**
     * 获取起始偏移量。
     *
     * @return 起始偏移量
     */
    public int getOffset() {
        return offset;
    }

    /**
     * 获取数据大小。
     *
     * @return 数据大小（字节）
     */
    public int getSizeInBytes() {
        return sizeInBytes;
    }

    /**
     * 将数据转换为字节数组。
     *
     * @return 包含数据的字节数组
     */
    public byte[] toBytes() {
        return MemorySegmentUtils.getBytes(segments, offset, sizeInBytes);
    }

    /**
     * 序列化写入。
     *
     * <p>将内存段中的数据转换为字节数组并写入输出流。
     *
     * @param out 对象输出流
     * @throws IOException 如果写入失败
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        byte[] bytes = toBytes();
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    /**
     * 反序列化读取。
     *
     * <p>从输入流读取字节数组并包装为内存段。
     *
     * @param in 对象输入流
     * @throws IOException 如果读取失败
     * @throws ClassNotFoundException 如果类未找到
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        byte[] bytes = new byte[in.readInt()];
        IOUtils.readFully(in, bytes);
        pointTo(MemorySegment.wrap(bytes), 0, bytes.length);
    }

    /**
     * 判断两个 BinarySection 是否相等。
     *
     * <p>比较内存段中的实际数据内容。
     *
     * @param o 待比较的对象
     * @return 如果数据内容相同则返回 true
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final BinarySection that = (BinarySection) o;
        return sizeInBytes == that.sizeInBytes
                && MemorySegmentUtils.equals(
                        segments, offset, that.segments, that.offset, sizeInBytes);
    }

    /**
     * 计算哈希码。
     *
     * <p>基于内存段中的实际数据内容计算哈希值。
     *
     * @return 哈希码
     */
    @Override
    public int hashCode() {
        return MemorySegmentUtils.hash(segments, offset, sizeInBytes);
    }
}
