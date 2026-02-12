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
 * 内存切片输入流。
 *
 * <p>提供对 {@link MemorySlice} 的顺序读取能力,支持:
 *
 * <ul>
 *   <li>读取基本类型:byte、int、long等
 *   <li>读取变长编码的整数:readVarLenInt、readVarLenLong
 *   <li>位置管理:position、setPosition、available
 *   <li>子切片读取:readSlice
 * </ul>
 *
 * <h2>变长编码</h2>
 *
 * <p>变长编码使用每字节的最高位作为延续标志,剩余7位存储数据,适合存储小整数。
 *
 * <h2>线程安全性</h2>
 *
 * <p>该类不是线程安全的。
 */
public class MemorySliceInput {

    /** 底层的内存切片。 */
    private final MemorySlice slice;

    /** 当前读取位置。 */
    private int position;

    /**
     * 构造内存切片输入流。
     *
     * @param slice 底层内存切片
     */
    public MemorySliceInput(MemorySlice slice) {
        this.slice = slice;
    }

    /**
     * 获取当前读取位置。
     *
     * @return 当前位置
     */
    public int position() {
        return position;
    }

    /**
     * 设置读取位置。
     *
     * @param position 新的位置
     * @throws IndexOutOfBoundsException 如果位置超出范围
     */
    public void setPosition(int position) {
        if (position < 0 || position > slice.length()) {
            throw new IndexOutOfBoundsException();
        }
        this.position = position;
    }

    /**
     * 检查是否还有可读数据。
     *
     * @return 如果还有数据返回 true
     */
    public boolean isReadable() {
        return available() > 0;
    }

    /**
     * 获取剩余可读字节数。
     *
     * @return 剩余字节数
     */
    public int available() {
        return slice.length() - position;
    }

    /**
     * 读取一个字节。
     *
     * @return byte 值
     * @throws IndexOutOfBoundsException 如果已到达末尾
     */
    public byte readByte() {
        if (position == slice.length()) {
            throw new IndexOutOfBoundsException();
        }
        return slice.readByte(position++);
    }

    /**
     * 读取一个无符号字节。
     *
     * @return 0-255 的整数值
     */
    public int readUnsignedByte() {
        return (short) (readByte() & 0xFF);
    }

    /**
     * 读取一个 int 值。
     *
     * @return int 值
     */
    public int readInt() {
        int v = slice.readInt(position);
        position += 4;
        return v;
    }

    /**
     * 读取变长编码的 int 值。
     *
     * <p>变长编码:每字节最高位为延续标志,0表示结束,1表示后续还有字节。
     *
     * @return 解码后的 int 值
     * @throws Error 如果数据格式错误
     */
    public int readVarLenInt() {
        for (int offset = 0, result = 0; offset < 32; offset += 7) {
            int b = readUnsignedByte();
            result |= (b & 0x7F) << offset;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new Error("Malformed integer.");
    }

    /**
     * 读取一个 long 值。
     *
     * @return long 值
     */
    public long readLong() {
        long v = slice.readLong(position);
        position += 8;
        return v;
    }

    /**
     * 读取变长编码的 long 值。
     *
     * @return 解码后的 long 值
     * @throws Error 如果数据格式错误
     */
    public long readVarLenLong() {
        long result = 0;
        for (int offset = 0; offset < 64; offset += 7) {
            long b = readUnsignedByte();
            result |= (b & 0x7F) << offset;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new Error("Malformed long.");
    }

    /**
     * 读取指定长度的子切片。
     *
     * @param length 子切片长度
     * @return 子切片
     */
    public MemorySlice readSlice(int length) {
        MemorySlice newSlice = slice.slice(position, length);
        position += length;
        return newSlice;
    }
}
