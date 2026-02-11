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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.AbstractRowDataSerializer;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 外部缓冲区
 *
 * <p>用于存储行的外部缓冲区，当内存满时会将数据溢写到磁盘。
 *
 * <p>核心特性：
 * <ul>
 *   <li>内存+磁盘：内存不足时自动溢写到磁盘
 *   <li>块压缩：溢写的数据使用块压缩减少磁盘占用
 *   <li>零拷贝溢写：直接将内存段写入磁盘，无需序列化
 *   <li>磁盘配额：限制最大磁盘使用量
 * </ul>
 *
 * <p>工作流程：
 * <ol>
 *   <li>写入：先写入内存缓冲区（{@link InMemoryBuffer}）
 *   <li>溢写：内存满时，将内存数据零拷贝写入磁盘
 *   <li>重置：清空内存，准备接收新数据
 *   <li>读取：先读取所有溢写文件，最后读取内存数据
 * </ol>
 *
 * <p>存储结构：
 * <ul>
 *   <li>inMemoryBuffer：内存缓冲区
 *   <li>spilledChannelIDs：溢写文件通道列表
 *   <li>压缩块：使用块压缩减少磁盘占用
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>大数据量缓存：内存不足时自动溢写
 *   <li>追加写入：AppendOnlyWriter 的大缓冲
 *   <li>排序缓冲：外部排序的输入缓冲
 * </ul>
 */
public class ExternalBuffer implements RowBuffer {

    private static final Logger LOG = LoggerFactory.getLogger(ExternalBuffer.class);

    /** IO 管理器 */
    private final IOManager ioManager;
    /** 内存池 */
    private final MemorySegmentPool pool;
    /** 二进制行序列化器 */
    private final BinaryRowSerializer binaryRowSerializer;
    /** 内存缓冲区 */
    private final InMemoryBuffer inMemoryBuffer;
    /** 最大磁盘使用量 */
    private final MemorySize maxDiskSize;
    /** 块压缩工厂 */
    private final BlockCompressionFactory compactionFactory;

    /** 内存段大小 */
    private final int segmentSize;

    /** 溢写通道列表 */
    private final List<ChannelWithMeta> spilledChannelIDs;
    /** 行数 */
    private int numRows;

    /**
     * 构造外部缓冲区
     *
     * @param ioManager IO 管理器
     * @param pool 内存池
     * @param serializer 行序列化器
     * @param maxDiskSize 最大磁盘使用量
     * @param compression 压缩选项
     */
    public ExternalBuffer(
            IOManager ioManager,
            MemorySegmentPool pool,
            AbstractRowDataSerializer<?> serializer,
            MemorySize maxDiskSize,
            CompressOptions compression) {
        this.ioManager = ioManager;
        this.pool = pool;
        this.maxDiskSize = maxDiskSize;

        // 创建块压缩工厂
        this.compactionFactory = BlockCompressionFactory.create(compression);

        // 创建或复制二进制行序列化器
        this.binaryRowSerializer =
                serializer instanceof BinaryRowSerializer
                        ? (BinaryRowSerializer) serializer.duplicate()
                        : new BinaryRowSerializer(serializer.getArity());

        this.segmentSize = pool.pageSize();

        this.spilledChannelIDs = new ArrayList<>();

        this.numRows = 0;

        // 创建内存缓冲区
        //noinspection unchecked
        this.inMemoryBuffer =
                new InMemoryBuffer(pool, (AbstractRowDataSerializer<InternalRow>) serializer);
    }

    /**
     * 重置缓冲区
     *
     * <p>清空所有数据：删除溢写文件，清空内存缓冲区
     */
    @Override
    public void reset() {
        clearChannels();
        inMemoryBuffer.reset();
        numRows = 0;
    }

    /**
     * 刷新内存到磁盘
     *
     * <p>将内存数据溢写到磁盘（如果未达到磁盘配额）
     *
     * @return true 表示刷新成功，false 表示磁盘已满
     * @throws IOException IO 异常
     */
    @Override
    public boolean flushMemory() throws IOException {
        // 检查磁盘配额
        boolean isFull = getDiskUsage() >= maxDiskSize.getBytes();
        if (isFull) {
            return false;
        } else {
            spill();
            return true;
        }
    }

    /**
     * 获取磁盘使用量（测试可见）
     *
     * @return 磁盘使用字节数
     */
    @VisibleForTesting
    public long getDiskUsage() {
        long bytes = 0;

        for (ChannelWithMeta spillChannelID : spilledChannelIDs) {
            bytes += spillChannelID.getNumBytes();
        }
        return bytes;
    }

    /**
     * 写入一行数据
     *
     * <p>写入流程：
     * <ol>
     *   <li>尝试写入内存缓冲区
     *   <li>如果内存满，溢写到磁盘
     *   <li>重试写入内存缓冲区
     *   <li>如果仍失败，说明单条记录太大，抛出异常
     * </ol>
     *
     * @param row 行数据
     * @return true（总是成功，失败抛异常）
     * @throws IOException IO 异常（记录太大或磁盘写入失败）
     */
    @Override
    public boolean put(InternalRow row) throws IOException {
        if (!inMemoryBuffer.put(row)) {
            // 检查记录是否太大
            if (inMemoryBuffer.getCurrentDataBufferOffset() == 0) {
                throwTooBigException(row);
            }
            // 溢写到磁盘
            spill();
            // 重试写入
            if (!inMemoryBuffer.put(row)) {
                throwTooBigException(row);
            }
        }

        numRows++;
        return true;
    }

    /**
     * 创建迭代器
     *
     * @return 缓冲区迭代器
     */
    @Override
    public RowBufferIterator newIterator() {
        return new BufferIterator();
    }

    /**
     * 抛出"记录太大"异常
     *
     * @param row 太大的记录
     * @throws IOException 记录太大异常
     */
    private void throwTooBigException(InternalRow row) throws IOException {
        int rowSize = inMemoryBuffer.getSerializer().toBinaryRow(row).toBytes().length;
        throw new IOException(
                "Record is too big, it can't be added to a empty InMemoryBuffer! "
                        + "Record size: "
                        + rowSize
                        + ", Buffer: "
                        + memorySize());
    }

    /**
     * 溢写内存数据到磁盘
     *
     * <p>采用零拷贝策略：直接将内存段写入磁盘，无需反序列化
     *
     * @throws IOException IO 异常
     */
    private void spill() throws IOException {
        // 创建磁盘通道
        FileIOChannel.ID channel = ioManager.createChannel();

        // 创建压缩输出视图
        ChannelWriterOutputView channelWriterOutputView =
                new ChannelWriterOutputView(
                        ioManager.createBufferFileWriter(channel), compactionFactory, segmentSize);
        int numRecordBuffers = inMemoryBuffer.getNumRecordBuffers();
        ArrayList<MemorySegment> segments = inMemoryBuffer.getRecordBufferSegments();
        try {
            // 零拷贝溢写：直接写入内存段
            for (int i = 0; i < numRecordBuffers; i++) {
                MemorySegment segment = segments.get(i);
                int bufferSize =
                        i == numRecordBuffers - 1
                                ? inMemoryBuffer.getNumBytesInLastBuffer()
                                : segment.size();
                channelWriterOutputView.write(segment, 0, bufferSize);
            }
            channelWriterOutputView.close();
            LOG.info(
                    "here spill the reset buffer data with {} records {} bytes",
                    inMemoryBuffer.size(),
                    channelWriterOutputView.getWriteBytes());
        } catch (IOException e) {
            channelWriterOutputView.closeAndDelete();
            throw e;
        }

        // 记录溢写通道元数据
        spilledChannelIDs.add(
                new ChannelWithMeta(
                        channel,
                        inMemoryBuffer.getNumRecordBuffers(),
                        channelWriterOutputView.getWriteBytes()));

        // 重置内存缓冲区
        inMemoryBuffer.reset();
    }

    /**
     * 获取行数
     *
     * @return 行数
     */
    @Override
    public int size() {
        return numRows;
    }

    /**
     * 获取内存占用量
     *
     * @return 内存占用字节数
     */
    @Override
    public long memoryOccupancy() {
        return inMemoryBuffer.memoryOccupancy();
    }

    /**
     * 获取可用内存大小
     *
     * @return 可用内存字节数
     */
    private int memorySize() {
        return pool.freePages() * segmentSize;
    }

    /**
     * 清空溢写通道
     *
     * <p>删除所有溢写文件
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void clearChannels() {
        for (ChannelWithMeta meta : spilledChannelIDs) {
            final File f = new File(meta.getChannel().getPath());
            if (f.exists()) {
                f.delete();
            }
        }
        spilledChannelIDs.clear();
    }

    /**
     * 缓冲区迭代器
     *
     * <p>按顺序读取溢写文件和内存数据。
     *
     * <p>读取顺序：
     * <ol>
     *   <li>溢写文件 0
     *   <li>溢写文件 1
     *   <li>...
     *   <li>内存缓冲区
     * </ol>
     */
    public class BufferIterator implements RowBufferIterator {

        /** 当前迭代器 */
        private MutableObjectIterator<BinaryRow> currentIterator;
        /** 复用的行对象 */
        private final BinaryRow reuse = binaryRowSerializer.createInstance();

        /** 当前通道 ID */
        private int currentChannelID = -1;
        /** 当前行 */
        private BinaryRow row;
        /** 是否已关闭 */
        private boolean closed;
        /** 通道读取器 */
        private ChannelReaderInputView channelReader;

        /**
         * 构造迭代器
         */
        private BufferIterator() {
            this.closed = false;
        }

        /**
         * 检查有效性
         *
         * @throws RuntimeException 如果迭代器已关闭
         */
        private void checkValidity() {
            if (closed) {
                throw new RuntimeException("This iterator is closed!");
            }
        }

        /**
         * 关闭迭代器
         */
        @Override
        public void close() {
            if (closed) {
                return;
            }

            try {
                closeCurrentFileReader();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            closed = true;
        }

        /**
         * 前进到下一行
         *
         * <p>从当前迭代器获取，如果为空则切换到下一个迭代器
         *
         * @return true 表示有下一行，false 表示已到末尾
         */
        @Override
        public boolean advanceNext() {
            checkValidity();

            try {
                // 从当前迭代器或新迭代器获取
                while (true) {
                    if (currentIterator != null && (row = currentIterator.next(reuse)) != null) {
                        return true;
                    } else {
                        if (!nextIterator()) {
                            return false;
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * 切换到下一个迭代器
         *
         * <p>迭代顺序：溢写文件 → 内存缓冲区
         *
         * @return true 表示有下一个迭代器，false 表示已全部遍历
         * @throws IOException IO 异常
         */
        private boolean nextIterator() throws IOException {
            if (currentChannelID == Integer.MAX_VALUE || numRows == 0) {
                return false;
            } else if (currentChannelID < spilledChannelIDs.size() - 1) {
                // 下一个溢写文件
                nextSpilledIterator();
            } else {
                // 最后：内存缓冲区
                newMemoryIterator();
            }
            return true;
        }

        /**
         * 获取当前行
         *
         * @return 当前行
         */
        @Override
        public BinaryRow getRow() {
            return row;
        }

        /**
         * 关闭当前文件读取器
         *
         * @throws IOException IO 异常
         */
        private void closeCurrentFileReader() throws IOException {
            if (channelReader != null) {
                channelReader.close();
                channelReader = null;
            }
        }

        /**
         * 切换到下一个溢写文件迭代器
         *
         * @throws IOException IO 异常
         */
        private void nextSpilledIterator() throws IOException {
            ChannelWithMeta channel = spilledChannelIDs.get(currentChannelID + 1);
            currentChannelID++;

            // 先关闭当前读取器
            closeCurrentFileReader();

            // 创建新读取器
            this.channelReader =
                    new ChannelReaderInputView(
                            channel.getChannel(),
                            ioManager,
                            compactionFactory,
                            segmentSize,
                            channel.getBlockCount());

            // 创建二进制行迭代器
            this.currentIterator = channelReader.createBinaryRowIterator(binaryRowSerializer);
        }

        /**
         * 切换到内存缓冲区迭代器
         */
        private void newMemoryIterator() {
            this.currentChannelID = Integer.MAX_VALUE;
            this.currentIterator = inMemoryBuffer.newIterator();
        }
    }

    /**
     * 获取溢写通道列表（测试可见）
     *
     * @return 溢写通道列表
     */
    @VisibleForTesting
    public List<ChannelWithMeta> getSpillChannels() {
        return spilledChannelIDs;
    }
}
