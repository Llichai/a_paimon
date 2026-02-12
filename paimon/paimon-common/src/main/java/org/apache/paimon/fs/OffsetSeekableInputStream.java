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

package org.apache.paimon.fs;

import java.io.IOException;

/**
 * 带偏移量的可查找输入流 - 提供逻辑视图的流切片.
 *
 * <p>这个类包装另一个 {@link SeekableInputStream},提供一个逻辑上的子流视图。
 * 它通过偏移量和长度限制,将底层流的一部分作为独立的流暴露出来。
 *
 * <h2>主要特性</h2>
 * <ul>
 *   <li><b>逻辑切片</b>: 从底层流的指定偏移量开始,提供指定长度的视图</li>
 *   <li><b>位置转换</b>: 自动转换逻辑位置和物理位置</li>
 *   <li><b>边界检查</b>: 防止读取超出指定长度的数据</li>
 *   <li><b>零拷贝</b>: 直接读取底层流,无额外缓冲</li>
 * </ul>
 *
 * <h2>工作原理</h2>
 * <pre>
 * 底层流视图:
 * ┌───────────────────────────────────────────┐
 * │ 0    offset          offset+length    end │
 * │ │──────│──────────────│───────────────│   │
 * │        ↑              ↑                    │
 * │     逻辑位置0      逻辑位置length          │
 * └───────────────────────────────────────────┘
 *
 * 位置映射:
 * - 逻辑位置 0     → 物理位置 offset
 * - 逻辑位置 100   → 物理位置 offset + 100
 * - getPos() = 物理位置 - offset
 * - seek(n)  = wrapped.seek(offset + n)
 * </pre>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>分块读取</b>: 将大文件划分为多个逻辑块,并行读取</li>
 *   <li><b>数据隔离</b>: 限制某个组件只能访问文件的特定部分</li>
 *   <li><b>格式解析</b>: 读取容器格式中的嵌入文件(如 ZIP、TAR)</li>
 *   <li><b>分区文件</b>: 访问分区文件的特定分区</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 分块读取大文件
 * SeekableInputStream fullFile = fileIO.newInputStream(path);
 * long fileSize = fileIO.getFileStatus(path).getLen();
 * long chunkSize = 64 * 1024 * 1024; // 64MB
 *
 * List<SeekableInputStream> chunks = new ArrayList<>();
 * for (long offset = 0; offset < fileSize; offset += chunkSize) {
 *     long length = Math.min(chunkSize, fileSize - offset);
 *     chunks.add(new OffsetSeekableInputStream(fullFile, offset, length));
 * }
 *
 * // 并行处理每个块
 * chunks.parallelStream().forEach(chunk -> {
 *     try {
 *         processChunk(chunk); // chunk 的位置从 0 开始
 *     } catch (IOException e) {
 *         throw new UncheckedIOException(e);
 *     }
 * });
 *
 * fullFile.close();
 *
 * // 2. 读取容器格式中的嵌入文件
 * // 假设容器文件包含多个嵌入文件的元数据
 * record FileEntry(long offset, long length, String name) {}
 *
 * SeekableInputStream container = fileIO.newInputStream(containerPath);
 * List<FileEntry> entries = readTOC(container); // 读取目录
 *
 * for (FileEntry entry : entries) {
 *     // 为每个嵌入文件创建独立的流
 *     try (SeekableInputStream embeddedFile =
 *             new OffsetSeekableInputStream(container, entry.offset(), entry.length())) {
 *         // 读取嵌入文件,位置从 0 开始
 *         byte[] content = IOUtils.readFully(embeddedFile, entry.length());
 *         saveFile(entry.name(), content);
 *     }
 * }
 *
 * container.close();
 *
 * // 3. 无长度限制的偏移流(读取到文件末尾)
 * SeekableInputStream file = fileIO.newInputStream(path);
 * long skipHeader = 1024; // 跳过 1KB 的文件头
 *
 * // length = -1 表示读取到文件末尾
 * SeekableInputStream dataOnly = new OffsetSeekableInputStream(file, skipHeader, -1);
 * processData(dataOnly); // 只处理数据部分,不包含文件头
 * }</pre>
 *
 * <h2>长度限制</h2>
 * <ul>
 *   <li><b>length > 0</b>: 严格限制读取长度,到达长度限制时返回 EOF</li>
 *   <li><b>length = -1</b>: 不限制长度,读取到底层流的 EOF</li>
 *   <li><b>length = 0</b>: 空流,任何读取都返回 EOF</li>
 * </ul>
 *
 * <h2>边界检查示例</h2>
 * <pre>{@code
 * // 假设底层流长度为 1000 字节
 * SeekableInputStream base = ...;
 *
 * // 创建偏移流: 从位置 100 开始,长度 50
 * OffsetSeekableInputStream offset = new OffsetSeekableInputStream(base, 100, 50);
 *
 * offset.seek(0);          // 物理位置 = 100
 * offset.getPos();         // 返回 0(逻辑位置)
 *
 * byte[] buf = new byte[100];
 * int n = offset.read(buf); // 只读取 50 字节,即使缓冲区更大
 * // n = 50
 *
 * offset.seek(40);         // 物理位置 = 140
 * n = offset.read(buf);    // 只读取 10 字节(50 - 40 = 10)
 * // n = 10
 *
 * n = offset.read(buf);    // 到达逻辑 EOF
 * // n = -1
 * }</pre>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li><b>共享底层流</b>: 多个 OffsetSeekableInputStream 可以共享同一个底层流</li>
 *   <li><b>线程安全</b>: 不是线程安全的,并发访问需要外部同步</li>
 *   <li><b>关闭语义</b>: close() 会关闭底层流,共享时需要注意</li>
 *   <li><b>位置独立</b>: 每个包装流有自己的逻辑位置,但影响底层流的物理位置</li>
 * </ul>
 *
 * <h2>性能特性</h2>
 * <ul>
 *   <li>零拷贝: 直接读取底层流,无额外缓冲</li>
 *   <li>无状态转换: 位置转换为简单的加减法,开销极低</li>
 *   <li>适合: 顺序读取和少量随机访问</li>
 * </ul>
 *
 * @see SeekableInputStream
 * @see SeekableInputStreamWrapper
 * @since 1.0
 */
public class OffsetSeekableInputStream extends SeekableInputStream {

    /** 被包装的底层可查找输入流. */
    private final SeekableInputStream wrapped;

    /** 逻辑位置 0 对应的物理偏移量. */
    private final long offset;

    /** 逻辑流的长度限制, -1 表示无限制(读取到底层流的 EOF). */
    private final long length;

    /**
     * 构造带偏移量和长度的可查找输入流.
     *
     * <p>如果 offset != 0,会立即 seek 到指定的偏移量。
     *
     * @param wrapped 底层可查找输入流,不能为 null
     * @param offset 起始偏移量(字节),必须 >= 0
     * @param length 长度限制(字节),必须 >= -1。-1 表示读取到底层流的 EOF
     * @throws IOException 如果初始 seek 失败
     * @throws IllegalArgumentException 如果 offset < 0 或 length < -1
     */
    public OffsetSeekableInputStream(SeekableInputStream wrapped, long offset, long length)
            throws IOException {
        this.wrapped = wrapped;
        this.offset = offset;
        this.length = length;
        if (offset != 0) {
            wrapped.seek(offset);
        }
    }

    /**
     * 查找到逻辑流中的指定位置.
     *
     * <p>将逻辑位置转换为物理位置: {@code physicalPos = offset + desired}
     *
     * @param desired 目标逻辑位置(字节偏移量),必须 >= 0
     * @throws IOException 如果查找失败
     * @throws IllegalArgumentException 如果 desired < 0
     */
    @Override
    public void seek(long desired) throws IOException {
        wrapped.seek(offset + desired);
    }

    /**
     * 获取逻辑流中的当前读取位置.
     *
     * <p>将物理位置转换为逻辑位置: {@code logicalPos = physicalPos - offset}
     *
     * @return 当前逻辑位置(字节偏移量),从 0 开始
     * @throws IOException 如果获取位置失败
     */
    @Override
    public long getPos() throws IOException {
        return wrapped.getPos() - offset;
    }

    /**
     * 读取单个字节.
     *
     * <p>如果设置了长度限制,在到达逻辑流末尾时返回 -1,即使底层流还有数据。
     *
     * @return 读取的字节(0-255),如果到达逻辑流末尾则返回 -1
     * @throws IOException 如果读取失败
     */
    @Override
    public int read() throws IOException {
        if (length != -1) {
            if (getPos() >= length) {
                return -1;
            }
        }
        return wrapped.read();
    }

    /**
     * 读取数据到字节数组的一部分.
     *
     * <p>如果设置了长度限制,会自动调整读取长度以不超过逻辑流的末尾:
     * <ul>
     *   <li>如果剩余字节 < len,只读取剩余字节</li>
     *   <li>如果剩余字节 = 0,返回 -1(EOF)</li>
     *   <li>如果 length = -1,不限制读取长度</li>
     * </ul>
     *
     * @param b 用于接收数据的字节数组,不能为 null
     * @param off 数据在数组中的起始偏移量
     * @param len 最多读取的字节数
     * @return 实际读取的字节数,如果到达逻辑流末尾则返回 -1
     * @throws IOException 如果读取失败
     * @throws IndexOutOfBoundsException 如果 off 或 len 无效
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (length != -1) {
            len = (int) Math.min(len, length - getPos());
            if (len == 0) {
                return -1;
            }
        }
        return wrapped.read(b, off, len);
    }

    /**
     * 关闭输入流.
     *
     * <p><b>注意</b>: 这会关闭底层流。如果多个 OffsetSeekableInputStream 共享同一个底层流,
     * 需要注意关闭顺序,或者使用 CloseShieldInputStream 保护底层流。
     *
     * @throws IOException 如果关闭失败
     */
    @Override
    public void close() throws IOException {
        wrapped.close();
    }
}
