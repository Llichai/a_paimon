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
import org.apache.paimon.utils.FileIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 缓冲文件写入器实现类
 *
 * <p>同步的 {@link BufferFileWriter} 实现，用于将缓冲区块写入文件。
 *
 * <p>核心功能：
 * <ul>
 *   <li>块格式写入：先写入 4 字节头部（数据大小），再写入数据内容
 *   <li>同步写入：每次调用都直接写入文件（非异步）
 *   <li>完整写入：确保所有数据都写入文件（处理部分写入）
 * </ul>
 *
 * <p>数据格式：
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
 *   <li>溢写数据：将内存缓冲区溢写到磁盘
 *   <li>临时文件：写入临时数据文件供后续读取
 * </ul>
 *
 * <p>设计思路：
 * <ul>
 *   <li>继承 {@link AbstractFileIOChannel}：复用通道管理逻辑
 *   <li>块头信息：记录块大小，方便读取时解析
 *   <li>完整写入：使用 {@link FileIOUtils#writeCompletely} 处理部分写入问题
 * </ul>
 *
 * @see BufferFileChannelReader
 * @see ChannelWriterOutputView
 */
public class BufferFileWriterImpl extends AbstractFileIOChannel implements BufferFileWriter {

    /**
     * 构造缓冲文件写入器
     *
     * @param channelID 通道 ID
     * @throws IOException 文件打开失败
     */
    protected BufferFileWriterImpl(ID channelID) throws IOException {
        super(channelID, true);
    }

    /**
     * 写入缓冲区块
     *
     * <p>写入流程：
     * <ol>
     *   <li>创建 4 字节头部，写入数据大小
     *   <li>完整写入头部到文件
     *   <li>完整写入数据内容到文件
     * </ol>
     *
     * @param buffer 缓冲区块
     * @throws IOException IO 异常
     */
    @Override
    public void writeBlock(Buffer buffer) throws IOException {
        // 包装缓冲区为只读 ByteBuffer
        ByteBuffer nioBufferReadable = buffer.getMemorySegment().wrap(0, buffer.getSize()).slice();
        // 创建 4 字节头部，写入数据大小
        ByteBuffer header = ByteBuffer.allocateDirect(4);
        header.putInt(nioBufferReadable.remaining());
        header.flip();

        // 完整写入头部和数据
        FileIOUtils.writeCompletely(fileChannel, header);
        FileIOUtils.writeCompletely(fileChannel, nioBufferReadable);
    }
}
