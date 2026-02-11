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

/**
 * 带元数据的通道
 *
 * <p>封装文件通道及其元数据（块数量和字节数）。
 *
 * <p>元数据包括：
 * <ul>
 *   <li>channel：文件通道 ID
 *   <li>blockCount：压缩块数量
 *   <li>numBytes：总字节数
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>溢写文件管理：记录溢写文件的元信息
 *   <li>压缩文件读取：提供块数量用于解压缩
 *   <li>磁盘配额：跟踪磁盘使用量
 * </ul>
 */
public class ChannelWithMeta {

    /** 文件通道 ID */
    private final FileIOChannel.ID channel;
    /** 压缩块数量 */
    private final int blockCount;
    /** 总字节数 */
    private final long numBytes;

    /**
     * 构造带元数据的通道
     *
     * @param channel 文件通道 ID
     * @param blockCount 压缩块数量
     * @param numEstimatedBytes 总字节数（估计值）
     */
    public ChannelWithMeta(FileIOChannel.ID channel, int blockCount, long numEstimatedBytes) {
        this.channel = channel;
        this.blockCount = blockCount;
        this.numBytes = numEstimatedBytes;
    }

    /**
     * 获取文件通道 ID
     *
     * @return 通道 ID
     */
    public FileIOChannel.ID getChannel() {
        return channel;
    }

    /**
     * 获取压缩块数量
     *
     * @return 块数量
     */
    public int getBlockCount() {
        return blockCount;
    }

    /**
     * 获取总字节数
     *
     * @return 字节数
     */
    public long getNumBytes() {
        return numBytes;
    }
}
