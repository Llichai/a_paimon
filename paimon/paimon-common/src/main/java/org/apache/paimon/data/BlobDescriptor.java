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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Blob 描述符,用于描述 Blob 引用。
 *
 * <h2>设计目的</h2>
 * <p>BlobDescriptor 用于存储大型二进制对象（Blob）的元数据信息，包括数据的位置、偏移量和长度。
 * 主要应用场景：
 * <ul>
 *   <li>引用外部存储的大对象（如文件、图片等）</li>
 *   <li>避免将大对象直接嵌入数据记录中</li>
 *   <li>支持跨存储系统的数据引用</li>
 *   <li>实现零拷贝的数据访问</li>
 * </ul>
 *
 * <h2>内存布局</h2>
 * <p>所有多字节数值（int/long）使用小端字节序（Little Endian）存储。
 * <pre>
 * +--------+-----------+------+------------+--------------------------------------------------+
 * | 偏移量 | 字段名    | 类型 | 大小(字节) | 说明                                             |
 * +--------+-----------+------+------------+--------------------------------------------------+
 * | 0      | version   | byte | 1          | 序列化结构版本号                                 |
 * | 1      | uriLength | int  | 4          | URI 字符串的 UTF-8 字节长度 (N)                  |
 * | 5      | uriBytes  | byte | N          | URI 字符串的 UTF-8 编码字节                      |
 * | 5+N    | offset    | long | 8          | Blob 在 URI 资源中的起始偏移量                   |
 * | 13+N   | length    | long | 8          | Blob 数据的长度                                  |
 * +--------+-----------+------+------------+--------------------------------------------------+
 * 总大小 = 1 + 4 + N + 8 + 8 = 21 + N 字节
 * </pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 创建 Blob 描述符
 * BlobDescriptor descriptor = new BlobDescriptor(
 *     "hdfs://cluster/data/blob001.bin",  // URI
 *     1024,                                 // 起始偏移量
 *     4096                                  // 数据长度
 * );
 *
 * // 2. 序列化
 * byte[] serialized = descriptor.serialize();
 *
 * // 3. 反序列化
 * BlobDescriptor restored = BlobDescriptor.deserialize(serialized);
 *
 * // 4. 访问信息
 * String uri = restored.uri();      // "hdfs://cluster/data/blob001.bin"
 * long offset = restored.offset();  // 1024
 * long length = restored.length();  // 4096
 * }</pre>
 *
 * <h2>序列化格式</h2>
 * <p>序列化示例（URI = "file:///data", offset = 100, length = 200）:
 * <pre>
 * [01]                   // version = 1
 * [0B 00 00 00]         // uriLength = 11 (小端)
 * [66 69 6C 65 3A 2F 2F 2F 64 61 74 61]  // "file:///data" UTF-8
 * [64 00 00 00 00 00 00 00]  // offset = 100 (小端)
 * [C8 00 00 00 00 00 00 00]  // length = 200 (小端)
 * </pre>
 *
 * <h2>版本控制</h2>
 * <p>当前版本为 1。如果反序列化时遇到不同的版本号，会抛出 UnsupportedOperationException。
 * 这确保了向后兼容性和数据完整性。
 *
 * <h2>线程安全性</h2>
 * <p>BlobDescriptor 是不可变对象，所有字段都是 final 的，因此是线程安全的。
 *
 * @since 0.9.0
 */
public class BlobDescriptor implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 当前序列化版本号 */
    private static final byte CURRENT_VERSION = 1;

    /** 序列化结构版本 */
    private final byte version;

    /** Blob 所在资源的 URI */
    private final String uri;

    /** Blob 在资源中的起始偏移量（字节） */
    private final long offset;

    /** Blob 数据的长度（字节） */
    private final long length;

    /**
     * 使用当前版本创建 BlobDescriptor。
     *
     * @param uri Blob 所在资源的 URI
     * @param offset Blob 在资源中的起始偏移量
     * @param length Blob 数据的长度
     */
    public BlobDescriptor(String uri, long offset, long length) {
        this(CURRENT_VERSION, uri, offset, length);
    }

    /**
     * 使用指定版本创建 BlobDescriptor（内部使用）。
     *
     * @param version 序列化版本号
     * @param uri Blob 所在资源的 URI
     * @param offset Blob 在资源中的起始偏移量
     * @param length Blob 数据的长度
     */
    private BlobDescriptor(byte version, String uri, long offset, long length) {
        this.version = version;
        this.uri = uri;
        this.offset = offset;
        this.length = length;
    }

    /**
     * 返回 Blob 所在资源的 URI。
     *
     * @return URI 字符串
     */
    public String uri() {
        return uri;
    }

    /**
     * 返回 Blob 在资源中的起始偏移量。
     *
     * @return 起始偏移量（字节）
     */
    public long offset() {
        return offset;
    }

    /**
     * 返回 Blob 数据的长度。
     *
     * @return 数据长度（字节）
     */
    public long length() {
        return length;
    }

    /**
     * 比较此 BlobDescriptor 与另一个对象是否相等。
     *
     * <p>相等条件：类型相同且所有字段值相等。
     *
     * @param o 要比较的对象
     * @return 如果相等返回 true
     */
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BlobDescriptor that = (BlobDescriptor) o;
        return version == that.version
                && offset == that.offset
                && length == that.length
                && Objects.equals(uri, that.uri);
    }

    /**
     * 计算此 BlobDescriptor 的哈希码。
     *
     * @return 哈希码
     */
    @Override
    public int hashCode() {
        return Objects.hash(version, uri, offset, length);
    }

    /**
     * 返回此 BlobDescriptor 的字符串表示。
     *
     * @return 字符串表示
     */
    @Override
    public String toString() {
        return "BlobDescriptor{"
                + "version="
                + version
                + '\''
                + ", uri='"
                + uri
                + '\''
                + ", offset="
                + offset
                + ", length="
                + length
                + '}';
    }

    /**
     * 将 BlobDescriptor 序列化为字节数组。
     *
     * <p>序列化步骤：
     * <ol>
     *   <li>计算总大小：1(version) + 4(uriLength) + N(uri) + 8(offset) + 8(length)</li>
     *   <li>分配 ByteBuffer 并设置为小端字节序</li>
     *   <li>依次写入 version、uriLength、uriBytes、offset、length</li>
     *   <li>返回字节数组</li>
     * </ol>
     *
     * @return 序列化后的字节数组
     */
    public byte[] serialize() {
        byte[] uriBytes = uri.getBytes(UTF_8);
        int uriLength = uriBytes.length;

        int totalSize = 1 + 4 + uriLength + 8 + 8;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        buffer.put(version);
        buffer.putInt(uriLength);
        buffer.put(uriBytes);

        buffer.putLong(offset);
        buffer.putLong(length);

        return buffer.array();
    }

    /**
     * 从字节数组反序列化 BlobDescriptor。
     *
     * <p>反序列化步骤：
     * <ol>
     *   <li>设置 ByteBuffer 为小端字节序</li>
     *   <li>读取并验证版本号</li>
     *   <li>读取 URI 长度和内容</li>
     *   <li>读取 offset 和 length</li>
     *   <li>创建并返回 BlobDescriptor 实例</li>
     * </ol>
     *
     * @param bytes 序列化的字节数组
     * @return 反序列化后的 BlobDescriptor
     * @throws UnsupportedOperationException 如果版本号不匹配
     */
    public static BlobDescriptor deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        byte version = buffer.get();
        if (version != CURRENT_VERSION) {
            throw new UnsupportedOperationException(
                    "Expecting BlobDescriptor version to be "
                            + CURRENT_VERSION
                            + ", but found "
                            + version
                            + ".");
        }
        int uriLength = buffer.getInt();
        byte[] uriBytes = new byte[uriLength];
        buffer.get(uriBytes);
        String uri = new String(uriBytes, StandardCharsets.UTF_8);

        long offset = buffer.getLong();
        long length = buffer.getLong();
        return new BlobDescriptor(version, uri, offset, length);
    }
}
