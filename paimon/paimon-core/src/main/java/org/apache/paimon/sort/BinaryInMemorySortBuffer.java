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

import org.apache.paimon.codegen.NormalizedKeyComputer;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.SimpleCollectingOutputView;
import org.apache.paimon.data.serializer.AbstractRowDataSerializer;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.utils.MutableObjectIterator;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 二进制行的内存排序缓冲区。
 *
 * <p>在内存中对数据进行排序,支持以下操作:
 * <ul>
 *   <li>{@link #clear}: 清理所有内存
 *   <li>{@link #tryInitialize}: 在缓冲区写入和读取前初始化内存
 * </ul>
 */
public class BinaryInMemorySortBuffer extends BinaryIndexedSortable implements SortBuffer {

    /** 最小所需缓冲区数量 */
    private static final int MIN_REQUIRED_BUFFERS = 3;

    /** 输入序列化器 */
    private final AbstractRowDataSerializer<InternalRow> inputSerializer;
    /** 记录缓冲区段列表 */
    private final ArrayList<MemorySegment> recordBufferSegments;
    /** 记录收集器 */
    private final SimpleCollectingOutputView recordCollector;

    /** 当前数据缓冲区偏移量 */
    private long currentDataBufferOffset;
    /** 排序索引字节数 */
    private long sortIndexBytes;
    /** 是否已初始化 */
    private boolean isInitialized;

    /**
     * 创建内存排序器(插入方式)。
     *
     * @param normalizedKeyComputer 归一化键计算器
     * @param serializer 序列化器
     * @param comparator 比较器
     * @param memoryPool 内存池
     * @return 内存排序缓冲区
     */
    public static BinaryInMemorySortBuffer createBuffer(
            NormalizedKeyComputer normalizedKeyComputer,
            AbstractRowDataSerializer<InternalRow> serializer,
            RecordComparator comparator,
            MemorySegmentPool memoryPool) {
        checkArgument(memoryPool.freePages() >= MIN_REQUIRED_BUFFERS);
        ArrayList<MemorySegment> recordBufferSegments = new ArrayList<>(16);
        return new BinaryInMemorySortBuffer(
                normalizedKeyComputer,
                serializer,
                comparator,
                recordBufferSegments,
                new SimpleCollectingOutputView(
                        recordBufferSegments, memoryPool, memoryPool.pageSize()),
                memoryPool);
    }

    private BinaryInMemorySortBuffer(
            NormalizedKeyComputer normalizedKeyComputer,
            AbstractRowDataSerializer<InternalRow> inputSerializer,
            RecordComparator comparator,
            ArrayList<MemorySegment> recordBufferSegments,
            SimpleCollectingOutputView recordCollector,
            MemorySegmentPool pool) {
        super(
                normalizedKeyComputer,
                new BinaryRowSerializer(inputSerializer.getArity()),
                comparator,
                recordBufferSegments,
                pool);
        this.inputSerializer = inputSerializer;
        this.recordBufferSegments = recordBufferSegments;
        this.recordCollector = recordCollector;
        // The memory will be initialized in super()
        this.isInitialized = true;
        this.clear();
    }

    // -------------------------------------------------------------------------
    // Memory Segment
    // -------------------------------------------------------------------------

    /**
     * 将内存段归还到内存池。
     */
    private void returnToSegmentPool() {
        // return all memory
        this.memorySegmentPool.returnAll(this.sortIndex);
        this.memorySegmentPool.returnAll(this.recordBufferSegments);
        this.sortIndex.clear();
        this.recordBufferSegments.clear();
    }

    /**
     * 获取缓冲区段数量。
     *
     * @return 缓冲区段数量
     */
    public int getBufferSegmentCount() {
        return this.recordBufferSegments.size();
    }

    /**
     * 尝试初始化排序缓冲区(如果所有包含的数据已被丢弃)。
     */
    private void tryInitialize() {
        if (!isInitialized) {
            // grab first buffer
            this.currentSortIndexSegment = nextMemorySegment();
            this.sortIndex.add(this.currentSortIndexSegment);
            // grab second buffer
            this.recordCollector.reset();
            this.isInitialized = true;
        }
    }

    @Override
    public void clear() {
        if (this.isInitialized) {
            // reset all offsets
            this.numRecords = 0;
            this.currentSortIndexOffset = 0;
            this.currentDataBufferOffset = 0;
            this.sortIndexBytes = 0;

            // return all memory
            returnToSegmentPool();
            this.currentSortIndexSegment = null;
            this.isInitialized = false;
        }
    }

    @Override
    public long getOccupancy() {
        return this.currentDataBufferOffset + this.sortIndexBytes;
    }

    @Override
    public boolean flushMemory() {
        return false;
    }

    /**
     * 判断缓冲区是否为空。
     *
     * @return 空返回true,否则返回false
     */
    boolean isEmpty() {
        return this.numRecords == 0;
    }

    /**
     * 将给定记录写入此排序缓冲区。
     *
     * <p>写入的记录将被追加并占据最后的逻辑位置。
     *
     * @param record 要写入的记录
     * @return 成功写入返回true,缓冲区满返回false
     * @throws IOException 如果将记录序列化到缓冲区时发生错误
     */
    @Override
    public boolean write(InternalRow record) throws IOException {
        tryInitialize();

        // check whether we need a new memory segment for the sort index
        if (!checkNextIndexOffset()) {
            return false;
        }

        // serialize the record into the data buffers
        int skip;
        try {
            skip = this.inputSerializer.serializeToPages(record, this.recordCollector);
        } catch (EOFException e) {
            return false;
        }

        final long newOffset = this.recordCollector.getCurrentOffset();
        long currOffset = currentDataBufferOffset + skip;

        writeIndexAndNormalizedKey(record, currOffset);

        this.sortIndexBytes += this.indexEntrySize;
        this.currentDataBufferOffset = newOffset;

        return true;
    }

    /**
     * 从缓冲区获取记录。
     *
     * @param reuse 可重用的二进制行对象
     * @param pointer 记录指针
     * @return 二进制行
     * @throws IOException 如果遇到IO问题
     */
    private BinaryRow getRecordFromBuffer(BinaryRow reuse, long pointer) throws IOException {
        this.recordBuffer.setReadPosition(pointer);
        return this.serializer.mapFromPages(reuse, this.recordBuffer);
    }

    // -------------------------------------------------------------------------

    /**
     * 获取遍历此缓冲区中所有记录的迭代器(按逻辑顺序)。
     *
     * @return 按逻辑顺序返回记录的迭代器
     */
    private MutableObjectIterator<BinaryRow> iterator() {
        tryInitialize();

        return new MutableObjectIterator<BinaryRow>() {
            private final int size = size();
            private int current = 0;

            private int currentSegment = 0;
            private int currentOffset = 0;

            private MemorySegment currentIndexSegment = sortIndex.get(0);

            @Override
            public BinaryRow next(BinaryRow target) {
                if (this.current < this.size) {
                    this.current++;
                    if (this.currentOffset > lastIndexEntryOffset) {
                        this.currentOffset = 0;
                        this.currentIndexSegment = sortIndex.get(++this.currentSegment);
                    }

                    long pointer = this.currentIndexSegment.getLong(this.currentOffset);
                    this.currentOffset += indexEntrySize;

                    try {
                        return getRecordFromBuffer(target, pointer);
                    } catch (IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                } else {
                    return null;
                }
            }

            @Override
            public BinaryRow next() {
                throw new RuntimeException("Not support!");
            }
        };
    }

    @Override
    public final MutableObjectIterator<BinaryRow> sortedIterator() {
        if (numRecords > 0) {
            new QuickSort().sort(this);
        }
        return iterator();
    }
}
