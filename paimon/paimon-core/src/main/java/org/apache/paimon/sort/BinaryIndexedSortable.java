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
import org.apache.paimon.data.AbstractPagedOutputView;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.RandomAccessInputView;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentPool;

import java.io.IOException;
import java.util.ArrayList;

/**
 * 提供基本比较和交换功能的抽象可排序对象。
 *
 * <p>支持索引和归一化键的写入。
 */
public abstract class BinaryIndexedSortable implements IndexedSortable {

    /** 偏移量长度(8字节) */
    public static final int OFFSET_LEN = 8;

    // put/compare/swap normalized key
    /** 归一化键计算器 */
    private final NormalizedKeyComputer normalizedKeyComputer;
    /** 二进制行序列化器 */
    protected final BinaryRowSerializer serializer;

    // if normalized key not fully determines, need compare record.
    /** 记录比较器 */
    private final RecordComparator comparator;

    /** 记录缓冲区 */
    protected final RandomAccessInputView recordBuffer;
    /** 用于比较的记录缓冲区 */
    private final RandomAccessInputView recordBufferForComparison;

    // segments
    /** 当前排序索引段 */
    protected MemorySegment currentSortIndexSegment;
    /** 内存段池 */
    protected final MemorySegmentPool memorySegmentPool;
    /** 排序索引段列表 */
    protected final ArrayList<MemorySegment> sortIndex;

    // normalized key attributes
    /** 归一化键字节数 */
    private final int numKeyBytes;
    /** 索引条目大小 */
    protected final int indexEntrySize;
    /** 每段索引条目数 */
    private final int indexEntriesPerSegment;
    /** 最后索引条目偏移量 */
    protected final int lastIndexEntryOffset;
    /** 归一化键是否完全确定 */
    private final boolean normalizedKeyFullyDetermines;
    /** 是否使用未反转的归一化键 */
    private final boolean useNormKeyUninverted;

    // for serialized comparison
    /** 序列化器1 */
    protected final BinaryRowSerializer serializer1;
    /** 序列化器2 */
    private final BinaryRowSerializer serializer2;
    /** 行1 */
    protected final BinaryRow row1;
    /** 行2 */
    private final BinaryRow row2;

    // runtime variables
    /** 当前排序索引偏移量 */
    protected int currentSortIndexOffset;
    /** 记录数 */
    protected int numRecords;

    /**
     * 构造二进制索引可排序对象。
     *
     * @param normalizedKeyComputer 归一化键计算器
     * @param serializer 序列化器
     * @param comparator 比较器
     * @param recordBufferSegments 记录缓冲区段列表
     * @param memorySegmentPool 内存段池
     */
    public BinaryIndexedSortable(
            NormalizedKeyComputer normalizedKeyComputer,
            BinaryRowSerializer serializer,
            RecordComparator comparator,
            ArrayList<MemorySegment> recordBufferSegments,
            MemorySegmentPool memorySegmentPool) {
        if (normalizedKeyComputer == null || serializer == null) {
            throw new NullPointerException();
        }
        this.normalizedKeyComputer = normalizedKeyComputer;
        this.serializer = serializer;
        this.comparator = comparator;
        this.memorySegmentPool = memorySegmentPool;
        this.useNormKeyUninverted = !normalizedKeyComputer.invertKey();

        this.numKeyBytes = normalizedKeyComputer.getNumKeyBytes();

        int segmentSize = memorySegmentPool.pageSize();
        this.recordBuffer = new RandomAccessInputView(recordBufferSegments, segmentSize);
        this.recordBufferForComparison =
                new RandomAccessInputView(recordBufferSegments, segmentSize);

        this.normalizedKeyFullyDetermines = normalizedKeyComputer.isKeyFullyDetermines();

        // compute the index entry size and limits
        this.indexEntrySize = numKeyBytes + OFFSET_LEN;
        this.indexEntriesPerSegment = segmentSize / this.indexEntrySize;
        this.lastIndexEntryOffset = (this.indexEntriesPerSegment - 1) * this.indexEntrySize;

        this.serializer1 = serializer.duplicate();
        this.serializer2 = serializer.duplicate();
        this.row1 = this.serializer1.createInstance();
        this.row2 = this.serializer2.createInstance();

        // set to initial state
        this.sortIndex = new ArrayList<>(16);
        this.currentSortIndexSegment = nextMemorySegment();
        sortIndex.add(currentSortIndexSegment);
    }

    /**
     * 获取下一个内存段。
     *
     * @return 内存段
     */
    protected MemorySegment nextMemorySegment() {
        return this.memorySegmentPool.nextSegment();
    }

    /**
     * 检查是否需要请求下一个索引内存段。
     *
     * @return 可以继续返回true,需要新段但无法获取返回false
     */
    protected boolean checkNextIndexOffset() {
        if (this.currentSortIndexOffset > this.lastIndexEntryOffset) {
            MemorySegment returnSegment = nextMemorySegment();
            if (returnSegment != null) {
                this.currentSortIndexSegment = returnSegment;
                this.sortIndex.add(this.currentSortIndexSegment);
                this.currentSortIndexOffset = 0;
            } else {
                return false;
            }
        }
        return true;
    }

    /**
     * 写入索引和归一化键。
     *
     * @param record 内部行记录
     * @param currOffset 当前偏移量
     */
    protected void writeIndexAndNormalizedKey(InternalRow record, long currOffset) {
        // add the pointer and the normalized key
        this.currentSortIndexSegment.putLong(this.currentSortIndexOffset, currOffset);

        if (this.numKeyBytes != 0) {
            normalizedKeyComputer.putKey(
                    record, this.currentSortIndexSegment, this.currentSortIndexOffset + OFFSET_LEN);
        }

        this.currentSortIndexOffset += this.indexEntrySize;
        this.numRecords++;
    }

    /**
     * 比较两个索引位置的记录。
     *
     * @param i 第一个记录的索引
     * @param j 第二个记录的索引
     * @return 比较结果
     */
    @Override
    public int compare(int i, int j) {
        final int segmentNumberI = i / this.indexEntriesPerSegment;
        final int segmentOffsetI = (i % this.indexEntriesPerSegment) * this.indexEntrySize;

        final int segmentNumberJ = j / this.indexEntriesPerSegment;
        final int segmentOffsetJ = (j % this.indexEntriesPerSegment) * this.indexEntrySize;

        return compare(segmentNumberI, segmentOffsetI, segmentNumberJ, segmentOffsetJ);
    }

    @Override
    public int compare(
            int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ) {
        final MemorySegment segI = this.sortIndex.get(segmentNumberI);
        final MemorySegment segJ = this.sortIndex.get(segmentNumberJ);

        int val =
                normalizedKeyComputer.compareKey(
                        segI, segmentOffsetI + OFFSET_LEN, segJ, segmentOffsetJ + OFFSET_LEN);

        if (val != 0 || this.normalizedKeyFullyDetermines) {
            return this.useNormKeyUninverted ? val : -val;
        }

        final long pointerI = segI.getLong(segmentOffsetI);
        final long pointerJ = segJ.getLong(segmentOffsetJ);

        return compareRecords(pointerI, pointerJ);
    }

    /**
     * 比较两个指针位置的记录。
     *
     * @param pointer1 第一个记录的指针
     * @param pointer2 第二个记录的指针
     * @return 比较结果
     */
    private int compareRecords(long pointer1, long pointer2) {
        this.recordBuffer.setReadPosition(pointer1);
        this.recordBufferForComparison.setReadPosition(pointer2);

        try {
            return this.comparator.compare(
                    serializer1.mapFromPages(row1, recordBuffer),
                    serializer2.mapFromPages(row2, recordBufferForComparison));
        } catch (IOException ioex) {
            throw new RuntimeException("Error comparing two records.", ioex);
        }
    }

    /**
     * 交换两个索引位置的记录。
     *
     * @param i 第一个记录的索引
     * @param j 第二个记录的索引
     */
    @Override
    public void swap(int i, int j) {
        final int segmentNumberI = i / this.indexEntriesPerSegment;
        final int segmentOffsetI = (i % this.indexEntriesPerSegment) * this.indexEntrySize;

        final int segmentNumberJ = j / this.indexEntriesPerSegment;
        final int segmentOffsetJ = (j % this.indexEntriesPerSegment) * this.indexEntrySize;

        swap(segmentNumberI, segmentOffsetI, segmentNumberJ, segmentOffsetJ);
    }

    @Override
    public void swap(
            int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ) {
        final MemorySegment segI = this.sortIndex.get(segmentNumberI);
        final MemorySegment segJ = this.sortIndex.get(segmentNumberJ);

        // swap offset
        long index = segI.getLong(segmentOffsetI);
        segI.putLong(segmentOffsetI, segJ.getLong(segmentOffsetJ));
        segJ.putLong(segmentOffsetJ, index);

        // swap key
        normalizedKeyComputer.swapKey(
                segI, segmentOffsetI + OFFSET_LEN, segJ, segmentOffsetJ + OFFSET_LEN);
    }

    @Override
    public int size() {
        return this.numRecords;
    }

    @Override
    public int recordSize() {
        return indexEntrySize;
    }

    @Override
    public int recordsPerSegment() {
        return indexEntriesPerSegment;
    }

    /**
     * 将所有记录写入 {@link AbstractPagedOutputView}(溢出操作)。
     *
     * @param output 输出视图
     * @throws IOException 如果遇到IO问题
     */
    public void writeToOutput(AbstractPagedOutputView output) throws IOException {
        final int numRecords = this.numRecords;
        int currentMemSeg = 0;
        int currentRecord = 0;

        while (currentRecord < numRecords) {
            final MemorySegment currentIndexSegment = this.sortIndex.get(currentMemSeg++);

            // go through all records in the memory segment
            for (int offset = 0;
                    currentRecord < numRecords && offset <= this.lastIndexEntryOffset;
                    currentRecord++, offset += this.indexEntrySize) {
                final long pointer = currentIndexSegment.getLong(offset);
                this.recordBuffer.setReadPosition(pointer);
                this.serializer.copyFromPagesToView(this.recordBuffer, output);
            }
        }
    }
}
