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
import org.apache.paimon.disk.FileIOChannel.Enumerator;
import org.apache.paimon.disk.FileIOChannel.ID;

import java.io.IOException;

/**
 * IO 管理器接口
 *
 * <p>提供磁盘 I/O 服务的外观接口，负责管理临时文件和缓冲区读写。
 *
 * <p>核心功能：
 * <ul>
 *   <li>创建文件通道：分配临时文件用于数据溢写
 *   <li>管理临时目录：支持多个临时目录（提高 I/O 并行度）
 *   <li>创建读写器：创建缓冲文件读写器
 *   <li>通道枚举器：顺序分配多个通道到不同目录
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>排序溢写：{@link org.apache.paimon.mergetree.MergeSorter}
 *   <li>外部缓冲区：{@link ExternalBuffer}
 *   <li>哈希表溢写：BytesHashMap 的磁盘溢写
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public interface IOManager extends AutoCloseable {

    /**
     * 创建文件通道
     *
     * <p>在临时目录中创建一个唯一的文件通道 ID
     *
     * @return 文件通道 ID
     */
    ID createChannel();

    /**
     * 创建带前缀的文件通道
     *
     * <p>创建具有指定前缀的文件通道，便于区分不同用途的临时文件
     *
     * @param prefix 文件名前缀
     * @return 文件通道 ID
     */
    ID createChannel(String prefix);

    /**
     * 获取所有临时目录
     *
     * @return 临时目录路径数组
     */
    String[] tempDirs();

    /**
     * 创建通道枚举器
     *
     * <p>枚举器按轮询方式将通道分配到不同的临时目录，实现负载均衡
     *
     * @return 通道枚举器
     */
    Enumerator createChannelEnumerator();

    /**
     * 创建缓冲文件写入器
     *
     * @param channelID 文件通道 ID
     * @return 缓冲文件写入器
     * @throws IOException IO 异常
     */
    BufferFileWriter createBufferFileWriter(ID channelID) throws IOException;

    /**
     * 创建缓冲文件读取器
     *
     * @param channelID 文件通道 ID
     * @return 缓冲文件读取器
     * @throws IOException IO 异常
     */
    BufferFileReader createBufferFileReader(ID channelID) throws IOException;

    /**
     * 创建 IO 管理器（单个临时目录）
     *
     * @param tempDir 临时目录路径
     * @return IO 管理器实例
     */
    static IOManager create(String tempDir) {
        return create(new String[] {tempDir});
    }

    /**
     * 创建 IO 管理器（多个临时目录）
     *
     * <p>支持多个临时目录可以提高 I/O 并行度和吞吐量
     *
     * @param tempDirs 临时目录路径数组
     * @return IO 管理器实例
     */
    static IOManager create(String[] tempDirs) {
        return new IOManagerImpl(tempDirs);
    }
}
