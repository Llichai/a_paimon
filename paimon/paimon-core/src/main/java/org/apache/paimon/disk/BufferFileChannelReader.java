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

package org.apache.paimon.disk;

import org.apache.paimon.memory.Buffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 缓冲文件通道读取器
 *
 * <p>用于从文件通道读取 {@link Buffer} 数据块的辅助类。
 *
 * <p>核心功能：
 * <ul>
 *   <li>块格式读取：先读取 4 字节头部（数据大小），再读取数据内容
 *   <li>EOF 检测：检测是否已读取到文件末尾
 *   <li>缓冲区验证：确保缓冲区足够大以容纳数据
 * </ul>
 *
 * <p>数据格式（与 {@link BufferFileWriterImpl} 对应）：
 * <pre>
 * +----------------+-------------------+
 * | Header (4字节) |  Data (N字节)     |
 * +----------------+-------------------+
 * | 数据大小 (int) |  实际数据内容     |
 * +----------------+-------------------+
 * </pre>
 *
 * <p>使用场景：
 * <ul>
 *   <li>内部工具类：被 {@link BufferFileReaderImpl} 使用
 *   <li>块读取：从文件读取压缩或未压缩的数据块
 * </ul>
 *
 * @see BufferFileWriterImpl
 * @see BufferFileReaderImpl
 */
public class BufferFileChannelReader {
    /** 4 字节头部缓冲区（存储数据大小） */
    private final ByteBuffer header = ByteBuffer.allocateDirect(4);
    /** 文件通道 */
    private final FileChannel fileChannel;

    /**
     * 构造缓冲文件通道读取器
     *
     * @param fileChannel 文件通道
     */
    BufferFileChannelReader(FileChannel fileChannel) {
        this.fileChannel = fileChannel;
    }

    /**
     * 从文件通道读取数据到缓冲区
     *
     * <p>读取流程：
     * <ol>
     *   <li>读取 4 字节头部，获取数据大小
     *   <li>验证缓冲区是否足够大
     *   <li>读取实际数据到缓冲区
     *   <li>检测是否到达文件末尾
     * </ol>
     *
     * @param buffer 目标缓冲区
     * @return true 表示已到达文件末尾，false 表示还有更多数据
     * @throws IOException IO 异常
     * @throws IllegalStateException 缓冲区太小无法容纳数据
     */
    public boolean readBufferFromFileChannel(Buffer buffer) throws IOException {
        checkArgument(fileChannel.size() - fileChannel.position() > 0);

        // 读取 4 字节头部
        header.clear();
        fileChannel.read(header);
        header.flip();

        // 解析数据大小
        int size = header.getInt();
        if (size > buffer.getMaxCapacity()) {
            throw new IllegalStateException(
                    "Buffer is too small for data: "
                            + buffer.getMaxCapacity()
                            + " bytes available, but "
                            + size
                            + " needed. This is most likely due to an serialized event, which is larger than the buffer size.");
        }
        checkArgument(buffer.getSize() == 0, "Buffer not empty");

        // 读取实际数据
        fileChannel.read(buffer.getNioBuffer(0, size));
        buffer.setSize(size);
        // 检查是否到达文件末尾
        return fileChannel.size() - fileChannel.position() == 0;
    }
}
