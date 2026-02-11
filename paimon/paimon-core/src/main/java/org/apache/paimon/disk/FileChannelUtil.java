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

import org.apache.paimon.compression.BlockCompressionFactory;

import java.io.IOException;
import java.util.List;

/**
 * 文件通道工具类
 *
 * <p>提供创建通道输入输出视图的工厂方法。
 *
 * <p>核心功能：
 * <ul>
 *   <li>创建输入视图：{@link ChannelReaderInputView}（支持压缩）
 *   <li>创建输出视图：{@link ChannelWriterOutputView}（支持压缩）
 *   <li>自动管理通道：跟踪打开的通道以便清理
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>溢写排序：{@link org.apache.paimon.mergetree.MergeSorter}
 *   <li>外部缓冲区：{@link ExternalBuffer}
 *   <li>哈希表溢写：BytesHashMap 的磁盘分区
 * </ul>
 */
public class FileChannelUtil {

    /**
     * 创建通道输入视图
     *
     * <p>从溢写文件读取数据，支持块压缩解压
     *
     * @param ioManager IO 管理器
     * @param channel 通道元数据（包含块数量）
     * @param channels 通道列表（用于跟踪打开的通道）
     * @param compressionCodecFactory 压缩编解码器工厂
     * @param compressionBlockSize 压缩块大小
     * @return 通道输入视图
     * @throws IOException IO 异常
     */
    public static ChannelReaderInputView createInputView(
            IOManager ioManager,
            ChannelWithMeta channel,
            List<FileIOChannel> channels,
            BlockCompressionFactory compressionCodecFactory,
            int compressionBlockSize)
            throws IOException {
        // 创建通道读取器输入视图
        ChannelReaderInputView in =
                new ChannelReaderInputView(
                        channel.getChannel(),
                        ioManager,
                        compressionCodecFactory,
                        compressionBlockSize,
                        channel.getBlockCount());
        // 跟踪打开的通道
        channels.add(in.getChannel());
        return in;
    }

    /**
     * 创建通道输出视图
     *
     * <p>向溢写文件写入数据，支持块压缩
     *
     * @param ioManager IO 管理器
     * @param channel 文件通道 ID
     * @param compressionCodecFactory 压缩编解码器工厂
     * @param compressionBlockSize 压缩块大小
     * @return 通道输出视图
     * @throws IOException IO 异常
     */
    public static ChannelWriterOutputView createOutputView(
            IOManager ioManager,
            FileIOChannel.ID channel,
            BlockCompressionFactory compressionCodecFactory,
            int compressionBlockSize)
            throws IOException {
        // 创建缓冲文件写入器
        BufferFileWriter bufferWriter = ioManager.createBufferFileWriter(channel);
        // 创建通道写入器输出视图
        return new ChannelWriterOutputView(
                bufferWriter, compressionCodecFactory, compressionBlockSize);
    }
}
