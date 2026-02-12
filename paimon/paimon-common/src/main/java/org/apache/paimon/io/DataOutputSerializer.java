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

import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentWritable;
import org.apache.paimon.memory.MemoryUtils;
import org.apache.paimon.utils.Preconditions;

import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * {@link java.io.DataOutput} 接口的简单高效序列化器。
 *
 * <p>DataOutputSerializer 提供了将 Java 基本类型和字符串高效写入字节数组的能力。
 * 它是一个轻量级的、可重用的序列化工具,支持自动扩容和零拷贝操作。
 *
 * <h3>核心特性</h3>
 * <ul>
 *   <li><b>自动扩容</b>: 当缓冲区不足时自动扩展(2倍增长策略)</li>
 *   <li><b>零拷贝访问</b>: getSharedBuffer() 直接返回内部缓冲区引用</li>
 *   <li><b>Unsafe 优化</b>: 使用 sun.misc.Unsafe 加速整数和长整型写入</li>
 *   <li><b>可重用性</b>: 支持通过 clear() 方法重置和重用实例</li>
 *   <li><b>内存段写入</b>: 实现 MemorySegmentWritable,支持高效的段到段拷贝</li>
 * </ul>
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li>网络消息的快速序列化</li>
 *   <li>内存数据结构的序列化存储</li>
 *   <li>缓存数据的写入</li>
 *   <li>RPC 调用结果的序列化</li>
 *   <li>记录批量写入前的缓冲</li>
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 创建序列化器,初始缓冲区 1KB
 * DataOutputSerializer serializer = new DataOutputSerializer(1024);
 *
 * // 写入基本类型
 * serializer.writeInt(42);
 * serializer.writeLong(123456789L);
 * serializer.writeUTF("Hello, Paimon!");
 *
 * // 获取序列化后的数据
 * byte[] data = serializer.getCopyOfBuffer();
 *
 * // 重用序列化器
 * serializer.clear();
 * serializer.writeInt(100);
 *
 * // 零拷贝访问(注意:共享缓冲区)
 * byte[] sharedBuffer = serializer.getSharedBuffer();
 * int length = serializer.length(); // 有效数据长度
 * }</pre>
 *
 * <h3>性能优化</h3>
 * <ul>
 *   <li><b>Unsafe 访问</b>: writeInt/writeLong 使用 Unsafe 实现快速写入(4x-8x 提升)</li>
 *   <li><b>字节序处理</b>: 在小端系统上自动进行字节翻转</li>
 *   <li><b>扩容策略</b>: 2倍增长 + 最小需求,减少扩容次数</li>
 *   <li><b>无锁设计</b>: 不涉及同步操作,适合单线程场景</li>
 *   <li><b>UTF-8 优化</b>: writeUTF 使用优化的编码算法</li>
 * </ul>
 *
 * <h3>缓冲区管理</h3>
 * <table border="1">
 *   <tr>
 *     <th>方法</th>
 *     <th>说明</th>
 *     <th>性能</th>
 *   </tr>
 *   <tr>
 *     <td>getSharedBuffer()</td>
 *     <td>返回内部缓冲区引用(可能被覆盖)</td>
 *     <td>O(1),零拷贝</td>
 *   </tr>
 *   <tr>
 *     <td>getCopyOfBuffer()</td>
 *     <td>返回精确长度的拷贝(安全独立)</td>
 *     <td>O(n),数据拷贝</td>
 *   </tr>
 *   <tr>
 *     <td>length()</td>
 *     <td>返回有效数据长度</td>
 *     <td>O(1)</td>
 *   </tr>
 *   <tr>
 *     <td>clear()</td>
 *     <td>重置位置,复用缓冲区</td>
 *     <td>O(1)</td>
 *   </tr>
 * </table>
 *
 * <h3>扩容策略</h3>
 * <p>当缓冲区空间不足时,按以下规则扩容:
 * <ol>
 *   <li>尝试扩容到 {@code max(currentSize * 2, currentSize + required)}</li>
 *   <li>如果 OOM,降级到 {@code currentSize + required}</li>
 *   <li>如果仍然 OOM,抛出 IOException 并报告所需大小</li>
 *   <li>最大支持 2GB(Java 数组最大寻址范围)</li>
 * </ol>
 *
 * <h3>线程安全性</h3>
 * <p>该类<b>不是线程安全的</b>。如果多个线程需要共享序列化器,必须外部同步。
 * 推荐每个线程使用独立的实例。
 *
 * <h3>与 DataOutputStream 的区别</h3>
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>DataOutputSerializer</th>
 *     <th>DataOutputStream</th>
 *   </tr>
 *   <tr>
 *     <td>性能</td>
 *     <td>高(Unsafe + 零拷贝)</td>
 *     <td>中(标准 I/O)</td>
 *   </tr>
 *   <tr>
 *     <td>自动扩容</td>
 *     <td>支持</td>
 *     <td>不支持(依赖底层流)</td>
 *   </tr>
 *   <tr>
 *     <td>可重用性</td>
 *     <td>支持 clear() 重用</td>
 *     <td>需要重新创建</td>
 *   </tr>
 *   <tr>
 *     <td>目标</td>
 *     <td>内存缓冲区</td>
 *     <td>任意 OutputStream</td>
 *   </tr>
 * </table>
 *
 * @see DataOutputView
 * @see DataInputDeserializer
 * @see MemorySegmentWritable
 * @see java.io.DataOutput
 */
public class DataOutputSerializer implements DataOutputView, MemorySegmentWritable {

    /** 存储序列化数据的字节数组缓冲区。 */
    private byte[] buffer;

    /** 当前写入位置。 */
    private int position;

    // ------------------------------------------------------------------------

    /**
     * 创建一个指定初始大小的序列化器。
     *
     * @param startSize 初始缓冲区大小(字节),必须 >= 1
     * @throws IllegalArgumentException 如果 startSize < 1
     */
    public DataOutputSerializer(int startSize) {
        if (startSize < 1) {
            throw new IllegalArgumentException();
        }

        this.buffer = new byte[startSize];
    }

    /** @deprecated Replaced by {@link #getSharedBuffer()} for a better, safer name. */
    @Deprecated
    public byte[] getByteArray() {
        return getSharedBuffer();
    }

    /**
     * Gets a reference to the internal byte buffer. This buffer may be larger than the actual
     * serialized data. Only the bytes from zero to {@link #length()} are valid. The buffer will
     * also be overwritten with the next write calls.
     *
     * <p>This method is useful when trying to avid byte copies, but should be used carefully.
     *
     * @return A reference to the internal shared and reused buffer.
     */
    public byte[] getSharedBuffer() {
        return buffer;
    }

    /**
     * Gets a copy of the buffer that has the right length for the data serialized so far. The
     * returned buffer is an exclusive copy and can be safely used without being overwritten by
     * future write calls to this serializer.
     *
     * <p>This method is equivalent to {@code Arrays.copyOf(getSharedBuffer(), length());}
     *
     * @return A non-shared copy of the serialization buffer.
     */
    public byte[] getCopyOfBuffer() {
        return Arrays.copyOf(buffer, position);
    }

    /**
     * 清空序列化器,重置写入位置为 0。
     * 缓冲区被保留,可以复用,避免重新分配内存。
     */
    public void clear() {
        this.position = 0;
    }

    /**
     * 获取已序列化数据的长度(字节数)。
     *
     * @return 有效数据长度
     */
    public int length() {
        return this.position;
    }

    @Override
    public String toString() {
        return String.format("[pos=%d cap=%d]", this.position, this.buffer.length);
    }

    // ----------------------------------------------------------------------------------------
    //                               Data Output
    // ----------------------------------------------------------------------------------------

    @Override
    public void write(int b) throws IOException {
        if (this.position >= this.buffer.length) {
            resize(1);
        }
        this.buffer[this.position++] = (byte) (b & 0xff);
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (len < 0 || off > b.length - len) {
            throw new ArrayIndexOutOfBoundsException();
        }
        if (this.position > this.buffer.length - len) {
            resize(len);
        }
        System.arraycopy(b, off, this.buffer, this.position, len);
        this.position += len;
    }

    @Override
    public void write(MemorySegment segment, int off, int len) throws IOException {
        if (len < 0 || off < 0 || off > segment.size() - len) {
            throw new IndexOutOfBoundsException(
                    String.format("offset: %d, length: %d, size: %d", off, len, segment.size()));
        }
        if (this.position > this.buffer.length - len) {
            resize(len);
        }
        segment.get(off, this.buffer, this.position, len);
        this.position += len;
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        write(v ? 1 : 0);
    }

    @Override
    public void writeByte(int v) throws IOException {
        write(v);
    }

    @Override
    public void writeBytes(String s) throws IOException {
        final int sLen = s.length();
        if (this.position >= this.buffer.length - sLen) {
            resize(sLen);
        }

        for (int i = 0; i < sLen; i++) {
            writeByte(s.charAt(i));
        }
        this.position += sLen;
    }

    @Override
    public void writeChar(int v) throws IOException {
        if (this.position >= this.buffer.length - 1) {
            resize(2);
        }
        this.buffer[this.position++] = (byte) (v >> 8);
        this.buffer[this.position++] = (byte) v;
    }

    @Override
    public void writeChars(String s) throws IOException {
        final int sLen = s.length();
        if (this.position >= this.buffer.length - 2 * sLen) {
            resize(2 * sLen);
        }
        for (int i = 0; i < sLen; i++) {
            writeChar(s.charAt(i));
        }
    }

    @Override
    public void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    @Override
    public void writeFloat(float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    @SuppressWarnings("restriction")
    @Override
    public void writeInt(int v) throws IOException {
        if (this.position >= this.buffer.length - 3) {
            resize(4);
        }
        if (LITTLE_ENDIAN) {
            v = Integer.reverseBytes(v);
        }
        UNSAFE.putInt(this.buffer, BASE_OFFSET + this.position, v);
        this.position += 4;
    }

    public void writeIntUnsafe(int v, int pos) throws IOException {
        if (LITTLE_ENDIAN) {
            v = Integer.reverseBytes(v);
        }
        UNSAFE.putInt(this.buffer, BASE_OFFSET + pos, v);
    }

    @SuppressWarnings("restriction")
    @Override
    public void writeLong(long v) throws IOException {
        if (this.position >= this.buffer.length - 7) {
            resize(8);
        }
        if (LITTLE_ENDIAN) {
            v = Long.reverseBytes(v);
        }
        UNSAFE.putLong(this.buffer, BASE_OFFSET + this.position, v);
        this.position += 8;
    }

    @Override
    public void writeShort(int v) throws IOException {
        if (this.position >= this.buffer.length - 1) {
            resize(2);
        }
        this.buffer[this.position++] = (byte) ((v >>> 8) & 0xff);
        this.buffer[this.position++] = (byte) (v & 0xff);
    }

    @Override
    public void writeUTF(String str) throws IOException {
        int strlen = str.length();
        int utflen = 0;
        int c;

        /* use charAt instead of copying String to char array */
        for (int i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                utflen++;
            } else if (c > 0x07FF) {
                utflen += 3;
            } else {
                utflen += 2;
            }
        }

        if (utflen > 65535) {
            throw new UTFDataFormatException("Encoded string is too long: " + utflen);
        } else if (this.position > this.buffer.length - utflen - 2) {
            resize(utflen + 2);
        }

        byte[] bytearr = this.buffer;
        int count = this.position;

        bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
        bytearr[count++] = (byte) (utflen & 0xFF);

        int i;
        for (i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if (!((c >= 0x0001) && (c <= 0x007F))) {
                break;
            }
            bytearr[count++] = (byte) c;
        }

        for (; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                bytearr[count++] = (byte) c;

            } else if (c > 0x07FF) {
                bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                bytearr[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                bytearr[count++] = (byte) (0x80 | (c & 0x3F));
            } else {
                bytearr[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                bytearr[count++] = (byte) (0x80 | (c & 0x3F));
            }
        }

        this.position = count;
    }

    private void resize(int minCapacityAdd) throws IOException {
        int newLen = Math.max(this.buffer.length * 2, this.buffer.length + minCapacityAdd);
        byte[] nb;
        try {
            nb = new byte[newLen];
        } catch (NegativeArraySizeException e) {
            throw new IOException(
                    "Serialization failed because the record length would exceed 2GB (max addressable array size in Java).");
        } catch (OutOfMemoryError e) {
            // this was too large to allocate, try the smaller size (if possible)
            if (newLen > this.buffer.length + minCapacityAdd) {
                newLen = this.buffer.length + minCapacityAdd;
                try {
                    nb = new byte[newLen];
                } catch (OutOfMemoryError ee) {
                    // still not possible. give an informative exception message that reports the
                    // size
                    throw new IOException(
                            "Failed to serialize element. Serialized size (> "
                                    + newLen
                                    + " bytes) exceeds JVM heap space",
                            ee);
                }
            } else {
                throw new IOException(
                        "Failed to serialize element. Serialized size (> "
                                + newLen
                                + " bytes) exceeds JVM heap space",
                        e);
            }
        }

        System.arraycopy(this.buffer, 0, nb, 0, this.position);
        this.buffer = nb;
    }

    @Override
    public void skipBytesToWrite(int numBytes) throws IOException {
        if (buffer.length - this.position < numBytes) {
            throw new EOFException("Could not skip " + numBytes + " bytes.");
        }

        this.position += numBytes;
    }

    @Override
    public void write(DataInputView source, int numBytes) throws IOException {
        if (buffer.length - this.position < numBytes) {
            throw new EOFException("Could not write " + numBytes + " bytes. Buffer overflow.");
        }

        source.readFully(this.buffer, this.position, numBytes);
        this.position += numBytes;
    }

    public void setPosition(int position) {
        Preconditions.checkArgument(
                position >= 0 && position <= this.position, "Position out of bounds.");
        this.position = position;
    }

    public void setPositionUnsafe(int position) {
        this.position = position;
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
