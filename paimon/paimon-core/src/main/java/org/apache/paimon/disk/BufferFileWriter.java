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
import org.apache.paimon.memory.MemorySegment;

import java.io.IOException;

/**
 * 缓冲文件写入器
 *
 * <p>将 {@link MemorySegment} 写入文件的接口。
 *
 * <p>核心功能：
 * <ul>
 *   <li>块写入：将内存缓冲区写入磁盘
 *   <li>异步/同步：根据实现可能同步或异步执行
 *   <li>通道管理：继承 {@link FileIOChannel} 的通道管理功能
 * </ul>
 *
 * <p>实现类：
 * <ul>
 *   <li>{@link BufferFileWriterImpl}：基于 NIO FileChannel 的实现
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>溢写写入：将内存段溢写到磁盘
 *   <li>通道输出：{@link ChannelWriterOutputView} 的底层写入器
 *   <li>批量写入：一次写入一个完整的内存段
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public interface BufferFileWriter extends FileIOChannel {

    /**
     * 写入缓冲区块
     *
     * <p>根据实现，请求可能同步或异步执行。
     *
     * @param buffer 要写入的缓冲区
     * @throws IOException 写入器遇到 I/O 错误时抛出
     */
    void writeBlock(Buffer buffer) throws IOException;
}
