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
import org.apache.paimon.compression.BlockDecompressor;
import org.apache.paimon.data.AbstractPagedInputView;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.memory.Buffer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.utils.MutableObjectIterator;

import java.io.EOFException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * 通道读取器输入视图
 *
 * <p>基于 {@link BufferFileReader} 的 {@link DataInputView} 实现，作为数据输入流使用。
 * 该视图从底层通道按块读取数据，并在返回给调用者之前解压缩。
 *
 * <p>核心功能：
 * <ul>
 *   <li>分块读取：按压缩块从文件读取数据
 *   <li>自动解压：使用块解压缩器解压数据
 *   <li>分页视图：继承 {@link AbstractPagedInputView}，支持跨段读取
 *   <li>二进制行迭代：提供高效的 BinaryRow 迭代器
 * </ul>
 *
 * <p>工作流程：
 * <ol>
 *   <li>读取块：从文件读取一个压缩块到 compressedBuffer
 *   <li>解压缩：将压缩数据解压到 uncompressedBuffer
 *   <li>提供数据：通过 AbstractPagedInputView 提供解压后的数据
 *   <li>重复循环：直到读取完所有块
 * </ol>
 *
 * <p>存储结构：
 * <ul>
 *   <li>compressedBuffer：存储从文件读取的压缩数据
 *   <li>uncompressedBuffer：存储解压后的数据（提供给调用者）
 *   <li>numBlocksRemaining：剩余块数（由写入器提供）
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>溢写读取：{@link ExternalBuffer} 读取溢写文件
 *   <li>外部排序：读取排序过程中溢写的数据
 * </ul>
 *
 * <p>数据格式约束：
 * <ul>
 *   <li>只能读取由 {@link ChannelWriterOutputView} 写入的数据
 *   <li>块格式必须匹配（头部+压缩数据）
 * </ul>
 *
 * @see ChannelWriterOutputView
 * @see AbstractPagedInputView
 */
public class ChannelReaderInputView extends AbstractPagedInputView {

    /** 块解压缩器 */
    private final BlockDecompressor decompressor;
    /** 缓冲文件读取器 */
    private final BufferFileReader reader;
    /** 未压缩数据缓冲区 */
    private final MemorySegment uncompressedBuffer;

    /** 压缩数据缓冲区 */
    private final MemorySegment compressedBuffer;

    /** 剩余块数 */
    private int numBlocksRemaining;
    /** 当前段的有效数据长度 */
    private int currentSegmentLimit;

    /**
     * 构造通道读取器输入视图
     *
     * @param id 文件通道 ID
     * @param ioManager IO 管理器
     * @param compressionCodecFactory 块压缩工厂
     * @param compressionBlockSize 压缩块大小
     * @param numBlocks 总块数
     * @throws IOException IO 异常
     */
    public ChannelReaderInputView(
            FileIOChannel.ID id,
            IOManager ioManager,
            BlockCompressionFactory compressionCodecFactory,
            int compressionBlockSize,
            int numBlocks)
            throws IOException {
        this.numBlocksRemaining = numBlocks;
        this.reader = ioManager.createBufferFileReader(id);
        // 创建未压缩数据缓冲区
        uncompressedBuffer = MemorySegment.wrap(new byte[compressionBlockSize]);
        decompressor = compressionCodecFactory.getDecompressor();
        // 创建压缩数据缓冲区（考虑最大压缩大小）
        compressedBuffer =
                MemorySegment.wrap(
                        new byte
                                [compressionCodecFactory
                                        .getCompressor()
                                        .getMaxCompressedSize(compressionBlockSize)]);
    }

    /**
     * 获取下一个内存段
     *
     * <p>从文件读取下一个压缩块，解压后返回。
     *
     * @param current 当前内存段（未使用）
     * @return 下一个内存段（解压后的数据）
     * @throws IOException IO 异常或 EOF
     */
    @Override
    protected MemorySegment nextSegment(MemorySegment current) throws IOException {
        // 检查是否已读取完所有块
        if (this.numBlocksRemaining <= 0) {
            this.reader.close();
            throw new EOFException();
        }

        // 读取一个压缩块
        Buffer buffer = Buffer.create(compressedBuffer);
        reader.readInto(buffer);
        // 解压缩到未压缩缓冲区
        this.currentSegmentLimit =
                decompressor.decompress(
                        buffer.getMemorySegment().getArray(),
                        0,
                        buffer.getSize(),
                        uncompressedBuffer.getArray(),
                        0);
        this.numBlocksRemaining--;
        return uncompressedBuffer;
    }

    /**
     * 获取段的有效数据长度
     *
     * <p>返回解压后的实际数据长度（可能小于段大小）。
     *
     * @param segment 内存段
     * @return 有效数据长度
     */
    @Override
    protected int getLimitForSegment(MemorySegment segment) {
        return currentSegmentLimit;
    }

    /**
     * 关闭输入视图
     *
     * @return 空列表（无内存段需要释放）
     * @throws IOException IO 异常
     */
    public List<MemorySegment> close() throws IOException {
        reader.close();
        return Collections.emptyList();
    }

    /**
     * 获取底层文件通道
     *
     * @return 文件 I/O 通道
     */
    public FileIOChannel getChannel() {
        return reader;
    }

    /**
     * 创建二进制行迭代器
     *
     * <p>创建高效的 BinaryRow 迭代器，用于反序列化行数据。
     *
     * @param serializer 二进制行序列化器
     * @return BinaryRow 迭代器
     */
    public MutableObjectIterator<BinaryRow> createBinaryRowIterator(
            BinaryRowSerializer serializer) {
        return new BinaryRowChannelInputViewIterator(serializer);
    }

    /**
     * 二进制行通道输入视图迭代器
     *
     * <p>内部迭代器类，用于从输入视图反序列化 BinaryRow。
     */
    private class BinaryRowChannelInputViewIterator implements MutableObjectIterator<BinaryRow> {

        /** 二进制行序列化器 */
        protected final BinaryRowSerializer serializer;

        /**
         * 构造迭代器
         *
         * @param serializer 二进制行序列化器
         */
        public BinaryRowChannelInputViewIterator(BinaryRowSerializer serializer) {
            this.serializer = serializer;
        }

        /**
         * 读取下一行（复用对象）
         *
         * @param reuse 复用的行对象
         * @return 下一行，如果到达末尾则返回 null
         * @throws IOException IO 异常
         */
        @Override
        public BinaryRow next(BinaryRow reuse) throws IOException {
            try {
                // 从输入视图反序列化行
                return this.serializer.deserializeFromPages(reuse, ChannelReaderInputView.this);
            } catch (EOFException e) {
                // 到达文件末尾，关闭并返回 null
                close();
                return null;
            }
        }

        /**
         * 读取下一行（创建新对象）
         *
         * <p>此方法已禁用以避免性能问题。
         *
         * @return 不支持
         * @throws UnsupportedOperationException 总是抛出
         */
        @Override
        public BinaryRow next() throws IOException {
            throw new UnsupportedOperationException(
                    "This method is disabled due to performance issue!");
        }
    }
}
