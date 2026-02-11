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

import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * 文件 I/O 通道抽象类
 *
 * <p>提供 {@link FileIOChannel} 接口的通用实现，供具体通道子类继承。
 *
 * <p>核心功能：
 * <ul>
 *   <li>封装 NIO 文件通道（FileChannel）操作
 *   <li>统一通道的创建、关闭、删除逻辑
 *   <li>提供通道 ID 和文件大小管理
 * </ul>
 *
 * <p>设计思路：
 * <ul>
 *   <li>共享实现：将通道公共行为提取到抽象类中
 *   <li>模板方法：定义通道操作流程，子类专注业务逻辑
 *   <li>资源管理：确保文件通道的正确打开、关闭和清理
 * </ul>
 *
 * <p>子类实现：
 * <ul>
 *   <li>{@link BufferFileWriterImpl}：缓冲文件写入器
 *   <li>{@link BufferFileReaderImpl}：缓冲文件读取器
 * </ul>
 */
public abstract class AbstractFileIOChannel implements FileIOChannel {

    /** 日志记录器 */
    protected static final Logger LOG = LoggerFactory.getLogger(FileIOChannel.class);

    /** 通道 ID */
    protected final FileIOChannel.ID id;

    /** NIO 文件通道 */
    protected final FileChannel fileChannel;

    /**
     * 构造文件 I/O 通道
     *
     * <p>根据通道 ID 指定的路径创建文件通道。
     *
     * @param channelID 通道 ID，描述文件路径
     * @param writeEnabled 是否启用写入模式（true：读写模式，false：只读模式）
     * @throws IOException 通道打开失败
     */
    protected AbstractFileIOChannel(FileIOChannel.ID channelID, boolean writeEnabled)
            throws IOException {
        this.id = Preconditions.checkNotNull(channelID);

        try {
            // 创建随机访问文件和对应的文件通道
            @SuppressWarnings("resource")
            RandomAccessFile file = new RandomAccessFile(id.getPath(), writeEnabled ? "rw" : "r");
            this.fileChannel = file.getChannel();
        } catch (IOException e) {
            throw new IOException(
                    "Channel to path '" + channelID.getPath() + "' could not be opened.", e);
        }
    }

    // --------------------------------------------------------------------------------------------

    /**
     * 获取通道 ID
     *
     * @return 通道 ID
     */
    @Override
    public final FileIOChannel.ID getChannelID() {
        return this.id;
    }

    /**
     * 获取文件大小
     *
     * @return 文件大小（字节数）
     * @throws IOException IO 异常
     */
    @Override
    public long getSize() throws IOException {
        FileChannel channel = fileChannel;
        return channel == null ? 0 : channel.size();
    }

    /**
     * 检查通道是否已关闭
     *
     * @return true 表示已关闭，false 表示未关闭
     */
    @Override
    public boolean isClosed() {
        return !this.fileChannel.isOpen();
    }

    /**
     * 关闭文件通道
     *
     * @throws IOException IO 异常
     */
    @Override
    public void close() throws IOException {
        if (this.fileChannel.isOpen()) {
            this.fileChannel.close();
        }
    }

    /**
     * 删除文件通道对应的文件
     *
     * <p>仅在通道已关闭时才能删除文件。
     *
     * @throws IllegalStateException 通道仍处于打开状态
     */
    @Override
    public void deleteChannel() {
        if (!isClosed() || this.fileChannel.isOpen()) {
            throw new IllegalStateException("Cannot delete a channel that is open.");
        }

        // 尽力删除文件，忽略异常
        try {
            File f = new File(this.id.getPath());
            if (f.exists()) {
                f.delete();
            }
        } catch (Throwable ignored) {
        }
    }

    /**
     * 关闭并删除文件通道
     *
     * <p>先关闭通道，然后删除底层文件。
     *
     * @throws IOException IO 异常
     */
    @Override
    public void closeAndDelete() throws IOException {
        try {
            close();
        } finally {
            deleteChannel();
        }
    }

    /**
     * 获取 NIO 文件通道
     *
     * @return NIO 文件通道
     */
    @Override
    public FileChannel getNioFileChannel() {
        return fileChannel;
    }
}
