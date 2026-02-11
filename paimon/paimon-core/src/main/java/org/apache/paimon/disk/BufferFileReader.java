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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.memory.Buffer;

import java.io.IOException;

/**
 * 缓冲文件读取器
 *
 * <p>从文件读取 {@link Buffer} 的接口。
 *
 * <p>核心功能：
 * <ul>
 *   <li>块读取：从磁盘读取数据到内存缓冲区
 *   <li>顺序读取：按写入顺序读取数据块
 *   <li>EOF 检测：检测是否已到达文件末尾
 * </ul>
 *
 * <p>实现类：
 * <ul>
 *   <li>{@link BufferFileReaderImpl}：基于 NIO FileChannel 的实现
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>溢写读取：从磁盘读取溢写的数据
 *   <li>通道输入：{@link ChannelReaderInputView} 的底层读取器
 *   <li>批量读取：一次读取一个完整的内存段
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public interface BufferFileReader extends FileIOChannel {

    /**
     * 读取数据到缓冲区
     *
     * <p>从文件读取数据填充给定的缓冲区
     *
     * @param buffer 要填充的缓冲区
     * @throws IOException 读取器遇到 I/O 错误时抛出
     */
    void readInto(Buffer buffer) throws IOException;

    /**
     * 检查是否已到达文件末尾
     *
     * @return true 表示已到达文件末尾，false 表示还有更多数据
     */
    boolean hasReachedEndOfFile();
}
