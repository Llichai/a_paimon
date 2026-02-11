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

import java.io.File;

/**
 * 文件通道管理器接口
 *
 * <p>基于配置的临时目录创建和管理文件 I/O 通道。
 *
 * <p>核心功能：
 * <ul>
 *   <li>创建文件通道：为临时文件分配唯一的通道 ID
 *   <li>通道枚举器：顺序分配通道到不同的临时目录（负载均衡）
 *   <li>路径管理：获取所有临时目录路径
 * </ul>
 *
 * <p>设计目的：
 * <ul>
 *   <li>多目录支持：支持多个临时目录提高 I/O 并行度
 *   <li>负载均衡：轮询方式分配通道到不同目录
 *   <li>资源清理：自动清理临时文件和目录
 * </ul>
 *
 * <p>实现类：
 * <ul>
 *   <li>{@link FileChannelManagerImpl}：默认实现，支持多临时目录
 * </ul>
 *
 * @see FileIOChannel
 * @see IOManager
 */
public interface FileChannelManager extends AutoCloseable {

    /**
     * 创建文件通道 ID
     *
     * <p>在其中一个临时目录中创建唯一的文件通道 ID。
     *
     * @return 文件通道 ID
     */
    FileIOChannel.ID createChannel();

    /**
     * 创建带前缀的文件通道 ID
     *
     * <p>在其中一个临时目录中创建具有指定前缀的文件通道 ID，便于区分不同用途的临时文件。
     *
     * @param prefix 文件名前缀
     * @return 文件通道 ID
     */
    FileIOChannel.ID createChannel(String prefix);

    /**
     * 创建通道枚举器
     *
     * <p>枚举器按轮询方式将通道分配到不同的临时目录，实现负载均衡。
     *
     * @return 通道枚举器
     */
    FileIOChannel.Enumerator createChannelEnumerator();

    /**
     * 获取所有临时目录路径
     *
     * @return 临时目录文件数组
     */
    File[] getPaths();
}
