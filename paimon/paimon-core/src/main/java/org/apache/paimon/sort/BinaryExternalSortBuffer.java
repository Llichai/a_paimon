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

package org.apache.paimon.sort;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.ChannelWithMeta;
import org.apache.paimon.disk.ChannelWriterOutputView;
import org.apache.paimon.disk.FileChannelUtil;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.MutableObjectIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.codegen.CodeGenUtils.newNormalizedKeyComputer;
import static org.apache.paimon.codegen.CodeGenUtils.newRecordComparator;

/**
 * 可溢出的 {@link SortBuffer}。
 *
 * <p>当内存不足时,可以将数据溢出到磁盘,支持外部排序。
 */
public class BinaryExternalSortBuffer implements SortBuffer {

    /** 二进制行序列化器 */
    private final BinaryRowSerializer serializer;
    /** 内存排序缓冲区 */
    private final BinaryInMemorySortBuffer inMemorySortBuffer;
    /** IO管理器 */
    private final IOManager ioManager;
    /** 溢出通道管理器 */
    private final SpillChannelManager channelManager;
    /** 最大文件句柄数 */
    private final int maxNumFileHandles;
    /** 压缩编解码工厂 */
    private final BlockCompressionFactory compressionCodecFactory;
    /** 压缩块大小 */
    private final int compressionBlockSize;
    /** 二进制外部合并器 */
    private final BinaryExternalMerger merger;

    /** 文件通道枚举器 */
    private final FileIOChannel.Enumerator enumerator;
    /** 溢出通道ID列表 */
    private final List<ChannelWithMeta> spillChannelIDs;
    /** 最大磁盘大小 */
    private final MemorySize maxDiskSize;

    /** 记录数 */
    private int numRecords = 0;

    /**
     * 构造二进制外部排序缓冲区。
     *
     * @param serializer 二进制行序列化器
     * @param comparator 记录比较器
     * @param pageSize 页大小
     * @param inMemorySortBuffer 内存排序缓冲区
     * @param ioManager IO管理器
     * @param maxNumFileHandles 最大文件句柄数
     * @param compression 压缩选项
     * @param maxDiskSize 最大磁盘大小
     */
    public BinaryExternalSortBuffer(
            BinaryRowSerializer serializer,
            RecordComparator comparator,
            int pageSize,
            BinaryInMemorySortBuffer inMemorySortBuffer,
            IOManager ioManager,
            int maxNumFileHandles,
            CompressOptions compression,
            MemorySize maxDiskSize) {
        this.serializer = serializer;
        this.inMemorySortBuffer = inMemorySortBuffer;
        this.ioManager = ioManager;
        this.channelManager = new SpillChannelManager();
        this.maxNumFileHandles = maxNumFileHandles;
        this.compressionCodecFactory = BlockCompressionFactory.create(compression);
        this.compressionBlockSize = (int) MemorySize.parse("64 kb").getBytes();
        this.maxDiskSize = maxDiskSize;
        this.merger =
                new BinaryExternalMerger(
                        ioManager,
                        pageSize,
                        maxNumFileHandles,
                        channelManager,
                        serializer.duplicate(),
                        comparator,
                        compressionCodecFactory,
                        compressionBlockSize);
        this.enumerator = ioManager.createChannelEnumerator();
        this.spillChannelIDs = new ArrayList<>();
    }

    /**
     * 创建二进制外部排序缓冲区。
     *
     * @param ioManager IO管理器
     * @param rowType 行类型
     * @param keyFields 键字段索引数组
     * @param bufferSize 缓冲区大小
     * @param pageSize 页大小
     * @param maxNumFileHandles 最大文件句柄数
     * @param compression 压缩选项
     * @param maxDiskSize 最大磁盘大小
     * @param sequenceOrder 是否顺序排序
     * @return 二进制外部排序缓冲区
     */
    public static BinaryExternalSortBuffer create(
            IOManager ioManager,
            RowType rowType,
            int[] keyFields,
            long bufferSize,
            int pageSize,
            int maxNumFileHandles,
            CompressOptions compression,
            MemorySize maxDiskSize,
            boolean sequenceOrder) {
        return create(
                ioManager,
                rowType,
                keyFields,
                new HeapMemorySegmentPool(bufferSize, pageSize),
                maxNumFileHandles,
                compression,
                maxDiskSize,
                sequenceOrder);
    }

    /**
     * 创建二进制外部排序缓冲区。
     *
     * @param ioManager IO管理器
     * @param rowType 行类型
     * @param keyFields 键字段索引数组
     * @param pool 内存段池
     * @param maxNumFileHandles 最大文件句柄数
     * @param compression 压缩选项
     * @param maxDiskSize 最大磁盘大小
     * @param sequenceOrder 是否顺序排序
     * @return 二进制外部排序缓冲区
     */
    public static BinaryExternalSortBuffer create(
            IOManager ioManager,
            RowType rowType,
            int[] keyFields,
            MemorySegmentPool pool,
            int maxNumFileHandles,
            CompressOptions compression,
            MemorySize maxDiskSize,
            boolean sequenceOrder) {
        RecordComparator comparator =
                newRecordComparator(rowType.getFieldTypes(), keyFields, sequenceOrder);
        BinaryInMemorySortBuffer sortBuffer =
                BinaryInMemorySortBuffer.createBuffer(
                        newNormalizedKeyComputer(rowType.getFieldTypes(), keyFields),
                        new InternalRowSerializer(rowType),
                        comparator,
                        pool);
        return new BinaryExternalSortBuffer(
                new BinaryRowSerializer(rowType.getFieldCount()),
                comparator,
                pool.pageSize(),
                sortBuffer,
                ioManager,
                maxNumFileHandles,
                compression,
                maxDiskSize);
    }

    @Override
    public int size() {
        return numRecords;
    }

    @Override
    public void clear() {
        this.numRecords = 0;
        // release memory
        inMemorySortBuffer.clear();
        spillChannelIDs.clear();
        // delete files
        channelManager.reset();
    }

    @Override
    public long getOccupancy() {
        return inMemorySortBuffer.getOccupancy();
    }

    /**
     * 刷新内存数据到磁盘。
     *
     * @return 刷新成功返回true,磁盘已满返回false
     * @throws IOException 如果遇到IO问题
     */
    @Override
    public boolean flushMemory() throws IOException {
        boolean isFull = getDiskUsage() >= maxDiskSize.getBytes();
        if (isFull) {
            return false;
        } else {
            spill();
            return true;
        }
    }

    /**
     * 获取磁盘使用量。
     *
     * @return 磁盘使用字节数
     */
    private long getDiskUsage() {
        long bytes = 0;

        for (ChannelWithMeta spillChannelID : spillChannelIDs) {
            bytes += spillChannelID.getNumBytes();
        }
        return bytes;
    }

    /**
     * 写入迭代器中的记录(用于测试)。
     *
     * @param iterator 二进制行迭代器
     * @throws IOException 如果遇到IO问题
     */
    @VisibleForTesting
    public void write(MutableObjectIterator<BinaryRow> iterator) throws IOException {
        BinaryRow row = serializer.createInstance();
        while ((row = iterator.next(row)) != null) {
            write(row);
        }
    }

    @Override
    public boolean write(InternalRow record) throws IOException {
        while (true) {
            boolean success = inMemorySortBuffer.write(record);
            if (success) {
                this.numRecords++;
                return true;
            }
            if (inMemorySortBuffer.isEmpty()) {
                // did not fit in a fresh buffer, must be large...
                throw new IOException("The record exceeds the maximum size of a sort buffer.");
            } else {
                spill();

                if (spillChannelIDs.size() >= maxNumFileHandles) {
                    List<ChannelWithMeta> merged = merger.mergeChannelList(spillChannelIDs);
                    spillChannelIDs.clear();
                    spillChannelIDs.addAll(merged);
                }
            }
        }
    }

    /**
     * 获取已排序的迭代器。
     *
     * @return 已排序的二进制行迭代器
     * @throws IOException 如果遇到IO问题
     */
    @Override
    public final MutableObjectIterator<BinaryRow> sortedIterator() throws IOException {
        if (spillChannelIDs.isEmpty()) {
            return inMemorySortBuffer.sortedIterator();
        }
        return spilledIterator();
    }

    /**
     * 获取溢出数据的迭代器。
     *
     * @return 溢出数据迭代器
     * @throws IOException 如果遇到IO问题
     */
    private MutableObjectIterator<BinaryRow> spilledIterator() throws IOException {
        spill();

        List<FileIOChannel> openChannels = new ArrayList<>();
        BinaryMergeIterator<BinaryRow> iterator =
                merger.getMergingIterator(spillChannelIDs, openChannels);
        channelManager.addOpenChannels(openChannels);

        return new MutableObjectIterator<BinaryRow>() {
            @Override
            public BinaryRow next(BinaryRow reuse) throws IOException {
                // BinaryMergeIterator ignore reuse object argument, use its own reusing object
                return next();
            }

            @Override
            public BinaryRow next() throws IOException {
                BinaryRow row = iterator.next();
                // BinaryMergeIterator reuse object anyway, here we need to copy it to do compaction
                return row == null ? null : row.copy();
            }
        };
    }

    /**
     * 将内存数据溢出到磁盘。
     *
     * @throws IOException 如果遇到IO问题
     */
    private void spill() throws IOException {
        if (inMemorySortBuffer.isEmpty()) {
            return;
        }

        // open next channel
        FileIOChannel.ID channel = enumerator.next();
        channelManager.addChannel(channel);

        ChannelWriterOutputView output = null;
        int blockCount;

        try {
            output =
                    FileChannelUtil.createOutputView(
                            ioManager, channel, compressionCodecFactory, compressionBlockSize);
            new QuickSort().sort(inMemorySortBuffer);
            inMemorySortBuffer.writeToOutput(output);
            output.close();
            blockCount = output.getBlockCount();
        } catch (IOException e) {
            if (output != null) {
                output.close();
                output.getChannel().deleteChannel();
            }
            throw e;
        }

        spillChannelIDs.add(new ChannelWithMeta(channel, blockCount, output.getWriteBytes()));
        inMemorySortBuffer.clear();
    }
}
