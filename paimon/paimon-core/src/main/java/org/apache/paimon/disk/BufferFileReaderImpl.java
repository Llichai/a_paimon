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

/**
 * 缓冲文件读取器实现类
 *
 * <p>同步的 {@link BufferFileReader} 实现，用于从文件读取缓冲区块。
 *
 * <p>核心功能：
 * <ul>
 *   <li>块格式读取：先读取头部获取大小，再读取数据内容
 *   <li>同步读取：每次调用都直接从文件读取（非异步）
 *   <li>EOF 检测：跟踪是否已读取到文件末尾
 * </ul>
 *
 * <p>工作流程：
 * <ol>
 *   <li>打开文件：根据通道 ID 打开只读文件通道
 *   <li>顺序读取：使用 {@link BufferFileChannelReader} 按块读取
 *   <li>EOF 标记：记录是否到达文件末尾
 * </ol>
 *
 * <p>使用场景：
 * <ul>
 *   <li>读取溢写数据：从磁盘读取之前溢写的数据
 *   <li>临时文件读取：读取临时文件中的块数据
 * </ul>
 *
 * <p>设计思路：
 * <ul>
 *   <li>继承 {@link AbstractFileIOChannel}：复用通道管理逻辑
 *   <li>委托模式：将实际读取委托给 {@link BufferFileChannelReader}
 *   <li>状态跟踪：维护 EOF 状态供调用者查询
 * </ul>
 *
 * @see BufferFileWriterImpl
 * @see BufferFileChannelReader
 */
public class BufferFileReaderImpl extends AbstractFileIOChannel implements BufferFileReader {

    /** 缓冲文件通道读取器 */
    private final BufferFileChannelReader reader;

    /** 是否已到达文件末尾 */
    private boolean hasReachedEndOfFile;

    /**
     * 构造缓冲文件读取器
     *
     * @param channelID 通道 ID
     * @throws IOException 文件打开失败
     */
    public BufferFileReaderImpl(ID channelID) throws IOException {
        super(channelID, false);
        this.reader = new BufferFileChannelReader(fileChannel);
    }

    /**
     * 从文件读取数据到缓冲区
     *
     * <p>委托给 {@link BufferFileChannelReader} 执行实际读取，并更新 EOF 状态。
     *
     * @param buffer 目标缓冲区
     * @throws IOException IO 异常
     */
    @Override
    public void readInto(Buffer buffer) throws IOException {
        hasReachedEndOfFile = reader.readBufferFromFileChannel(buffer);
    }

    /**
     * 检查是否已到达文件末尾
     *
     * @return true 表示已到达文件末尾，false 表示还有更多数据
     */
    @Override
    public boolean hasReachedEndOfFile() {
        return hasReachedEndOfFile;
    }
}
