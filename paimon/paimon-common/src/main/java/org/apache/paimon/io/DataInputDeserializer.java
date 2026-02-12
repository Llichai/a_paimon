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

package org.apache.paimon.io;

import org.apache.paimon.memory.MemoryUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * {@link java.io.DataInput} 接口的简单高效反序列化器。
 *
 * <p>DataInputDeserializer 提供了从字节数组高效读取 Java 基本类型和字符串的能力。
 * 它是一个轻量级的、可重用的反序列化工具,避免了不必要的对象创建和缓冲开销。
 *
 * <h3>核心特性</h3>
 * <ul>
 *   <li><b>零拷贝读取</b>: 直接操作字节数组,无需额外缓冲</li>
 *   <li><b>可重用性</b>: 支持通过 setBuffer 方法重置和重用实例</li>
 *   <li><b>Unsafe 优化</b>: 使用 sun.misc.Unsafe 加速整数和长整型读取</li>
 *   <li><b>灵活初始化</b>: 支持从字节数组或 ByteBuffer 初始化</li>
 *   <li><b>位置控制</b>: 支持跳过字节和查询剩余可读字节</li>
 * </ul>
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li>网络消息的快速反序列化</li>
 *   <li>内存数据结构的序列化传输</li>
 *   <li>缓存数据的读取</li>
 *   <li>RPC 调用参数的反序列化</li>
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 创建反序列化器
 * byte[] data = ...;
 * DataInputDeserializer deserializer = new DataInputDeserializer(data);
 *
 * // 读取基本类型
 * int value1 = deserializer.readInt();
 * long value2 = deserializer.readLong();
 * String str = deserializer.readUTF();
 *
 * // 重用反序列化器
 * byte[] newData = ...;
 * deserializer.setBuffer(newData);
 * int newValue = deserializer.readInt();
 *
 * // 检查剩余可读字节
 * int remaining = deserializer.available();
 * }</pre>
 *
 * <h3>性能优化</h3>
 * <ul>
 *   <li><b>Unsafe 访问</b>: 使用 Unsafe.getInt/getLong 实现快速读取(4x-8x 提升)</li>
 *   <li><b>字节序处理</b>: 在小端系统上自动进行字节翻转</li>
 *   <li><b>无锁设计</b>: 不涉及同步操作,适合单线程场景</li>
 *   <li><b>缓冲复用</b>: 避免频繁的缓冲区分配</li>
 * </ul>
 *
 * <h3>线程安全性</h3>
 * <p>该类<b>不是线程安全的</b>。如果多个线程需要共享反序列化器,必须外部同步。
 * 推荐每个线程使用独立的实例。
 *
 * <h3>与 DataInputStream 的区别</h3>
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>DataInputDeserializer</th>
 *     <th>DataInputStream</th>
 *   </tr>
 *   <tr>
 *     <td>性能</td>
 *     <td>高(Unsafe + 零拷贝)</td>
 *     <td>中(标准 I/O)</td>
 *   </tr>
 *   <tr>
 *     <td>可重用性</td>
 *     <td>支持 setBuffer 重用</td>
 *     <td>需要重新创建</td>
 *   </tr>
 *   <tr>
 *     <td>内存开销</td>
 *     <td>低(直接操作数组)</td>
 *     <td>高(额外缓冲)</td>
 *   </tr>
 *   <tr>
 *     <td>数据源</td>
 *     <td>仅字节数组/ByteBuffer</td>
 *     <td>任意 InputStream</td>
 *   </tr>
 * </table>
 *
 * @see DataInputView
 * @see DataOutputSerializer
 * @see java.io.DataInput
 */
public class DataInputDeserializer implements DataInputView, java.io.Serializable {

    private static final byte[] EMPTY = new byte[0];
    private static final long serialVersionUID = 1L;

    // ------------------------------------------------------------------------

    /** 存储待反序列化数据的字节数组。 */
    private byte[] buffer;

    /** 有效数据的结束位置(不包含)。 */
    private int end;

    /** 当前读取位置。 */
    private int position;

    // ------------------------------------------------------------------------

    /**
     * 创建一个空的反序列化器。
     * 需要调用 {@link #setBuffer(byte[])} 设置数据后才能使用。
     */
    public DataInputDeserializer() {
        setBuffer(EMPTY);
    }

    /**
     * 使用给定的字节数组创建反序列化器。
     *
     * @param buffer 包含待反序列化数据的字节数组
     */
    public DataInputDeserializer(@Nonnull byte[] buffer) {
        setBufferInternal(buffer, 0, buffer.length);
    }

    /**
     * 使用字节数组的指定范围创建反序列化器。
     *
     * @param buffer 包含待反序列化数据的字节数组
     * @param start 数据起始位置
     * @param len 数据长度
     */
    public DataInputDeserializer(@Nonnull byte[] buffer, int start, int len) {
        setBuffer(buffer, start, len);
    }

    /**
     * 使用 ByteBuffer 创建反序列化器。
     *
     * <p>如果 ByteBuffer 有底层数组,则直接使用该数组;
     * 否则会复制数据到新数组。
     *
     * @param buffer 包含待反序列化数据的 ByteBuffer
     */
    public DataInputDeserializer(@Nonnull ByteBuffer buffer) {
        setBuffer(buffer);
    }

    // ------------------------------------------------------------------------
    //  Changing buffers
    // ------------------------------------------------------------------------

    /**
     * 设置新的 ByteBuffer 作为数据源。
     *
     * <p>支持三种类型的 ByteBuffer:
     * <ul>
     *   <li><b>Array-backed heap ByteBuffer</b>: 直接使用底层数组,零拷贝</li>
     *   <li><b>Direct ByteBuffer</b>: 复制数据到堆数组</li>
     *   <li><b>Read-only ByteBuffer</b>: 复制数据到堆数组</li>
     * </ul>
     *
     * @param buffer 新的数据源
     * @throws IllegalArgumentException 如果 ByteBuffer 类型不支持
     */
    public void setBuffer(@Nonnull ByteBuffer buffer) {
        if (buffer.hasArray()) {
            this.buffer = buffer.array();
            this.position = buffer.arrayOffset() + buffer.position();
            this.end = this.position + buffer.remaining();
        } else if (buffer.isDirect() || buffer.isReadOnly()) {
            this.buffer = new byte[buffer.remaining()];
            this.position = 0;
            this.end = this.buffer.length;

            buffer.get(this.buffer);
        } else {
            throw new IllegalArgumentException(
                    "The given buffer is neither an array-backed heap ByteBuffer, nor a direct ByteBuffer.");
        }
    }

    /**
     * 设置字节数组的指定范围作为数据源。
     *
     * @param buffer 字节数组
     * @param start 起始位置
     * @param len 长度
     * @throws IllegalArgumentException 如果范围无效
     */
    public void setBuffer(@Nonnull byte[] buffer, int start, int len) {

        if (start < 0 || len < 0 || start + len > buffer.length) {
            throw new IllegalArgumentException("Invalid bounds.");
        }

        setBufferInternal(buffer, start, len);
    }

    /**
     * 设置整个字节数组作为数据源。
     *
     * @param buffer 字节数组
     */
    public void setBuffer(@Nonnull byte[] buffer) {
        setBufferInternal(buffer, 0, buffer.length);
    }

    /**
     * 内部方法:设置缓冲区而不进行边界检查。
     */
    private void setBufferInternal(@Nonnull byte[] buffer, int start, int len) {
        this.buffer = buffer;
        this.position = start;
        this.end = start + len;
    }

    /**
     * 释放对字节数组的引用,帮助 GC 回收内存。
     * 调用后需要重新 setBuffer 才能继续使用。
     */
    public void releaseArrays() {
        this.buffer = null;
    }

    // ----------------------------------------------------------------------------------------
    //                               Data Input
    // ----------------------------------------------------------------------------------------

    /**
     * 获取剩余可读字节数。
     *
     * @return 剩余可读字节数,如果已到末尾则返回 0
     */
    public int available() {
        if (position < end) {
            return end - position;
        } else {
            return 0;
        }
    }

    @Override
    public boolean readBoolean() throws IOException {
        if (this.position < this.end) {
            return this.buffer[this.position++] != 0;
        } else {
            throw new EOFException();
        }
    }

    @Override
    public byte readByte() throws IOException {
        if (this.position < this.end) {
            return this.buffer[this.position++];
        } else {
            throw new EOFException();
        }
    }

    @Override
    public char readChar() throws IOException {
        if (this.position < this.end - 1) {
            return (char)
                    (((this.buffer[this.position++] & 0xff) << 8)
                            | (this.buffer[this.position++] & 0xff));
        } else {
            throw new EOFException();
        }
    }

    @Override
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public void readFully(@Nonnull byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(@Nonnull byte[] b, int off, int len) throws IOException {
        if (len >= 0) {
            if (off <= b.length - len) {
                if (this.position <= this.end - len) {
                    System.arraycopy(this.buffer, position, b, off, len);
                    position += len;
                } else {
                    throw new EOFException();
                }
            } else {
                throw new ArrayIndexOutOfBoundsException();
            }
        } else {
            throw new IllegalArgumentException("Length may not be negative.");
        }
    }

    @Override
    public int readInt() throws IOException {
        if (this.position >= 0 && this.position < this.end - 3) {
            @SuppressWarnings("restriction")
            int value = UNSAFE.getInt(this.buffer, BASE_OFFSET + this.position);
            if (LITTLE_ENDIAN) {
                value = Integer.reverseBytes(value);
            }

            this.position += 4;
            return value;
        } else {
            throw new EOFException();
        }
    }

    @Nullable
    @Override
    public String readLine() throws IOException {
        if (this.position < this.end) {
            // read until a newline is found
            StringBuilder bld = new StringBuilder();
            char curr = (char) readUnsignedByte();
            while (position < this.end && curr != '\n') {
                bld.append(curr);
                curr = (char) readUnsignedByte();
            }
            // trim a trailing carriage return
            int len = bld.length();
            if (len > 0 && bld.charAt(len - 1) == '\r') {
                bld.setLength(len - 1);
            }
            String s = bld.toString();
            bld.setLength(0);
            return s;
        } else {
            return null;
        }
    }

    @Override
    public long readLong() throws IOException {
        if (position >= 0 && position < this.end - 7) {
            @SuppressWarnings("restriction")
            long value = UNSAFE.getLong(this.buffer, BASE_OFFSET + this.position);
            if (LITTLE_ENDIAN) {
                value = Long.reverseBytes(value);
            }
            this.position += 8;
            return value;
        } else {
            throw new EOFException();
        }
    }

    @Override
    public short readShort() throws IOException {
        if (position >= 0 && position < this.end - 1) {
            return (short)
                    ((((this.buffer[position++]) & 0xff) << 8)
                            | ((this.buffer[position++]) & 0xff));
        } else {
            throw new EOFException();
        }
    }

    @Nonnull
    @Override
    public String readUTF() throws IOException {
        int utflen = readUnsignedShort();
        byte[] bytearr = new byte[utflen];
        char[] chararr = new char[utflen];

        int c, char2, char3;
        int count = 0;
        int chararrCount = 0;

        readFully(bytearr, 0, utflen);

        while (count < utflen) {
            c = (int) bytearr[count] & 0xff;
            if (c > 127) {
                break;
            }
            count++;
            chararr[chararrCount++] = (char) c;
        }

        while (count < utflen) {
            c = (int) bytearr[count] & 0xff;
            switch (c >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    /* 0xxxxxxx */
                    count++;
                    chararr[chararrCount++] = (char) c;
                    break;
                case 12:
                case 13:
                    /* 110x xxxx 10xx xxxx */
                    count += 2;
                    if (count > utflen) {
                        throw new UTFDataFormatException(
                                "malformed input: partial character at end");
                    }
                    char2 = (int) bytearr[count - 1];
                    if ((char2 & 0xC0) != 0x80) {
                        throw new UTFDataFormatException("malformed input around byte " + count);
                    }
                    chararr[chararrCount++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
                    break;
                case 14:
                    /* 1110 xxxx 10xx xxxx 10xx xxxx */
                    count += 3;
                    if (count > utflen) {
                        throw new UTFDataFormatException(
                                "malformed input: partial character at end");
                    }
                    char2 = (int) bytearr[count - 2];
                    char3 = (int) bytearr[count - 1];
                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) {
                        throw new UTFDataFormatException(
                                "malformed input around byte " + (count - 1));
                    }
                    chararr[chararrCount++] =
                            (char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | (char3 & 0x3F));
                    break;
                default:
                    /* 10xx xxxx, 1111 xxxx */
                    throw new UTFDataFormatException("malformed input around byte " + count);
            }
        }
        // The number of chars produced may be less than utflen
        return new String(chararr, 0, chararrCount);
    }

    @Override
    public int readUnsignedByte() throws IOException {
        if (this.position < this.end) {
            return (this.buffer[this.position++] & 0xff);
        } else {
            throw new EOFException();
        }
    }

    @Override
    public int readUnsignedShort() throws IOException {
        if (this.position < this.end - 1) {
            return ((this.buffer[this.position++] & 0xff) << 8)
                    | (this.buffer[this.position++] & 0xff);
        } else {
            throw new EOFException();
        }
    }

    @Override
    public int skipBytes(int n) {
        if (this.position <= this.end - n) {
            this.position += n;
            return n;
        } else {
            n = this.end - this.position;
            this.position = this.end;
            return n;
        }
    }

    @Override
    public void skipBytesToRead(int numBytes) throws IOException {
        int skippedBytes = skipBytes(numBytes);

        if (skippedBytes < numBytes) {
            throw new EOFException("Could not skip " + numBytes + " bytes.");
        }
    }

    @Override
    public int read(@Nonnull byte[] b, int off, int len) throws IOException {

        if (off < 0) {
            throw new IndexOutOfBoundsException("Offset cannot be negative.");
        }

        if (len < 0) {
            throw new IndexOutOfBoundsException("Length cannot be negative.");
        }

        if (b.length - off < len) {
            throw new IndexOutOfBoundsException(
                    "Byte array does not provide enough space to store requested data" + ".");
        }

        if (this.position >= this.end) {
            return -1;
        } else {
            int toRead = Math.min(this.end - this.position, len);
            System.arraycopy(this.buffer, this.position, b, off, toRead);
            this.position += toRead;

            return toRead;
        }
    }

    @Override
    public int read(@Nonnull byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    public int getPosition() {
        return position;
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @SuppressWarnings("restriction")
    private static final sun.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;

    @SuppressWarnings("restriction")
    private static final long BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    private static final boolean LITTLE_ENDIAN =
            (MemoryUtils.NATIVE_BYTE_ORDER == ByteOrder.LITTLE_ENDIAN);
}
