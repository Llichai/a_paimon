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
import org.apache.paimon.compression.BlockCompressor;
import org.apache.paimon.data.AbstractPagedOutputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.memory.Buffer;
import org.apache.paimon.memory.MemorySegment;

import java.io.Closeable;
import java.io.IOException;

/**
 * 通道写入器输出视图
 *
 * <p>基于 {@link FileIOChannel} 的 {@link DataOutputView} 实现，作为数据输出流使用。
 * 该视图在将数据写入底层通道之前会对数据进行压缩。
 *
 * <p>核心功能：
 * <ul>
 *   <li>分块写入：将数据按压缩块写入文件
 *   <li>自动压缩：使用块压缩器压缩数据
 *   <li>分页视图：继承 {@link AbstractPagedOutputView}，支持跨段写入
 *   <li>统计信息：记录原始大小、压缩大小、块数等
 * </ul>
 *
 * <p>工作流程：
 * <ol>
 *   <li>接收数据：调用者写入数据到输出视图
 *   <li>填充缓冲：数据填充到 currentSegment（未压缩缓冲区）
 *   <li>压缩写入：缓冲区满时，压缩数据并写入文件
 *   <li>重复循环：直到所有数据写入完成
 *   <li>关闭刷新：关闭时写入最后一个未满的块
 * </ol>
 *
 * <p>存储结构：
 * <ul>
 *   <li>currentSegment：未压缩数据缓冲区（填充中）
 *   <li>compressedBuffer：压缩数据缓冲区（写入文件前）
 *   <li>统计信息：原始字节数、压缩字节数、实际写入字节数、块数
 * </ul>
 *
 * <p>数据格式（写入文件）：
 * <pre>
 * 块1: [4字节大小][压缩数据]
 * 块2: [4字节大小][压缩数据]
 * ...
 * </pre>
 *
 * <p>使用场景：
 * <ul>
 *   <li>溢写数据：{@link ExternalBuffer} 溢写内存数据到磁盘
 *   <li>外部排序：排序过程中溢写数据到临时文件
 * </ul>
 *
 * <p>配套读取：
 * <ul>
 *   <li>必须使用 {@link ChannelReaderInputView} 读取
 *   <li>块格式必须匹配
 * </ul>
 *
 * @see ChannelReaderInputView
 * @see AbstractPagedOutputView
 */
public final class ChannelWriterOutputView extends AbstractPagedOutputView implements Closeable {

    /** 压缩数据缓冲区 */
    private final MemorySegment compressedBuffer;
    /** 块压缩器 */
    private final BlockCompressor compressor;
    /** 缓冲文件写入器 */
    private final BufferFileWriter writer;

    /** 已写入块数 */
    private int blockCount;

    /** 原始字节数（压缩前） */
    private long numBytes;
    /** 压缩字节数（压缩后） */
    private long numCompressedBytes;
    /** 实际写入字节数（包括头部） */
    private long writeBytes;

    /**
     * 构造通道写入器输出视图
     *
     * @param writer 缓冲文件写入器
     * @param compressionCodecFactory 块压缩工厂
     * @param compressionBlockSize 压缩块大小
     */
    public ChannelWriterOutputView(
            BufferFileWriter writer,
            BlockCompressionFactory compressionCodecFactory,
            int compressionBlockSize) {
        super(MemorySegment.wrap(new byte[compressionBlockSize]), compressionBlockSize);

        compressor = compressionCodecFactory.getCompressor();
        // 创建压缩缓冲区（考虑最大压缩大小）
        compressedBuffer =
                MemorySegment.wrap(new byte[compressor.getMaxCompressedSize(compressionBlockSize)]);
        this.writer = writer;
    }

    /**
     * 获取底层文件通道
     *
     * @return 文件 I/O 通道
     */
    public FileIOChannel getChannel() {
        return writer;
    }

    /**
     * 关闭输出视图
     *
     * <p>刷新最后一个未满的块，清理资源，记录统计信息。
     *
     * @throws IOException IO 异常
     */
    @Override
    public void close() throws IOException {
        if (!writer.isClosed()) {
            // 写入最后一个未满的块
            int currentPositionInSegment = getCurrentPositionInSegment();
            writeCompressed(currentSegment, currentPositionInSegment);
            clear();
            // 记录实际写入字节数
            this.writeBytes = writer.getSize();
            this.writer.close();
        }
    }

    /**
     * 关闭并删除输出视图
     *
     * <p>先关闭输出视图，然后删除底层文件。
     *
     * @throws IOException IO 异常
     */
    public void closeAndDelete() throws IOException {
        try {
            close();
        } finally {
            writer.deleteChannel();
        }
    }

    /**
     * 获取下一个内存段
     *
     * <p>当前段满时调用，将当前段压缩并写入文件，然后返回同一个段继续使用。
     *
     * @param current 当前内存段
     * @param positionInCurrent 当前位置（段中的有效数据长度）
     * @return 下一个内存段（实际返回同一个段）
     * @throws IOException IO 异常
     */
    @Override
    protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent)
            throws IOException {
        // 压缩并写入当前段
        writeCompressed(current, positionInCurrent);
        return current;
    }

    /**
     * 压缩并写入数据
     *
     * <p>将未压缩数据压缩后写入文件，并更新统计信息。
     *
     * @param current 当前内存段（未压缩数据）
     * @param size 有效数据大小
     * @throws IOException IO 异常
     */
    private void writeCompressed(MemorySegment current, int size) throws IOException {
        // 压缩数据
        int compressedLen =
                compressor.compress(current.getArray(), 0, size, compressedBuffer.getArray(), 0);
        // 写入压缩块
        writer.writeBlock(Buffer.create(compressedBuffer, compressedLen));
        // 更新统计信息
        blockCount++;
        numBytes += size;
        numCompressedBytes += compressedLen;
    }

    /**
     * 获取原始字节数（压缩前）
     *
     * @return 原始字节数
     */
    public long getNumBytes() {
        return numBytes;
    }

    /**
     * 获取压缩字节数（压缩后）
     *
     * @return 压缩字节数
     */
    public long getNumCompressedBytes() {
        return numCompressedBytes;
    }

    /**
     * 获取实际写入字节数（包括头部）
     *
     * @return 实际写入字节数
     */
    public long getWriteBytes() {
        return writeBytes;
    }

    /**
     * 获取已写入块数
     *
     * @return 块数
     */
    public int getBlockCount() {
        return blockCount;
    }
}
