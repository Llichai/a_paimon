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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.RandomAccessInputView;
import org.apache.paimon.data.SimpleCollectingOutputView;
import org.apache.paimon.data.serializer.AbstractRowDataSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.utils.MutableObjectIterator;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;

/**
 * 内存缓冲区
 *
 * <p>仅在内存中缓存 {@link InternalRow}，不支持溢写到磁盘。
 *
 * <p>核心特性：
 * <ul>
 *   <li>内存管理：使用 {@link MemorySegmentPool} 管理内存页
 *   <li>序列化：将行序列化到内存页中
 *   <li>懒初始化：只在第一次写入时才分配内存
 *   <li>内存复用：reset 后释放内存，下次使用时重新分配
 * </ul>
 *
 * <p>存储结构：
 * <ul>
 *   <li>使用多个 MemorySegment 存储序列化的行数据
 *   <li>SimpleCollectingOutputView 负责跨页写入
 *   <li>RandomAccessInputView 负责跨页读取
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>小数据量缓存：内存足够时的行缓存
 *   <li>追加写入：AppendOnlyWriter 的写缓冲
 * </ul>
 */
public class InMemoryBuffer implements RowBuffer {

    /** 空迭代器（避免为空缓冲区分配内存） */
    private static final EmptyInMemoryBufferIterator EMPTY_ITERATOR =
            new EmptyInMemoryBufferIterator();

    /** 行序列化器 */
    private final AbstractRowDataSerializer<InternalRow> serializer;
    /** 记录缓冲区的内存段列表 */
    private final ArrayList<MemorySegment> recordBufferSegments;
    /** 记录收集输出视图（用于跨页写入） */
    private final SimpleCollectingOutputView recordCollector;
    /** 内存池 */
    private final MemorySegmentPool pool;
    /** 内存段大小 */
    private final int segmentSize;

    /** 当前数据缓冲区偏移量 */
    private long currentDataBufferOffset;
    /** 最后一个缓冲区中的字节数 */
    private int numBytesInLastBuffer;
    /** 记录数量 */
    private int numRecords = 0;

    /** 是否已初始化 */
    private boolean isInitialized;

    /**
     * 构造内存缓冲区
     *
     * @param pool 内存段池
     * @param serializer 行序列化器
     */
    public InMemoryBuffer(
            MemorySegmentPool pool, AbstractRowDataSerializer<InternalRow> serializer) {
        // 序列化器有状态，必须复制
        this.serializer = (AbstractRowDataSerializer<InternalRow>) serializer.duplicate();
        this.pool = pool;
        this.segmentSize = pool.pageSize();
        this.recordBufferSegments = new ArrayList<>();
        this.recordCollector =
                new SimpleCollectingOutputView(this.recordBufferSegments, pool, segmentSize);
        this.isInitialized = true;
    }

    /**
     * 尝试初始化缓冲区
     *
     * <p>如果所有数据都已丢弃，则重新初始化
     */
    private void tryInitialize() {
        if (!isInitialized) {
            this.recordCollector.reset();
            this.isInitialized = true;
        }
    }

    /**
     * 重置缓冲区
     *
     * <p>清空所有数据，将内存段归还给内存池
     */
    @Override
    public void reset() {
        if (this.isInitialized) {
            this.currentDataBufferOffset = 0;
            this.numBytesInLastBuffer = 0;
            this.numRecords = 0;
            returnToSegmentPool();
            this.isInitialized = false;
        }
    }

    /**
     * 刷新内存到磁盘
     *
     * <p>内存缓冲区不支持溢写，总是返回 false
     *
     * @return false（不支持溢写）
     */
    @Override
    public boolean flushMemory() throws IOException {
        return false;
    }

    /**
     * 将内存段归还给内存池
     */
    private void returnToSegmentPool() {
        pool.returnAll(this.recordBufferSegments);
        this.recordBufferSegments.clear();
    }

    /**
     * 写入一行数据
     *
     * @param row 行数据
     * @return true 表示成功写入，false 表示内存不足
     * @throws IOException IO 异常
     */
    @Override
    public boolean put(InternalRow row) throws IOException {
        try {
            // 懒初始化
            tryInitialize();
            // 序列化到内存页
            this.serializer.serializeToPages(row, this.recordCollector);
            // 更新偏移量和字节数
            currentDataBufferOffset = this.recordCollector.getCurrentOffset();
            numBytesInLastBuffer = this.recordCollector.getCurrentPositionInSegment();
            numRecords++;
            return true;
        } catch (EOFException e) {
            // 内存不足
            return false;
        }
    }

    /**
     * 获取记录数量
     *
     * @return 记录数量
     */
    @Override
    public int size() {
        return numRecords;
    }

    /**
     * 获取内存占用量
     *
     * @return 内存占用字节数
     */
    @Override
    public long memoryOccupancy() {
        return currentDataBufferOffset;
    }

    /**
     * 创建迭代器
     *
     * @return 行缓冲区迭代器
     */
    @Override
    public InMemoryBufferIterator newIterator() {
        if (!isInitialized) {
            // 避免为空缓冲区分配内存
            return EMPTY_ITERATOR;
        }
        // 创建随机访问输入视图（用于跨页读取）
        RandomAccessInputView recordBuffer =
                new RandomAccessInputView(
                        this.recordBufferSegments, segmentSize, numBytesInLastBuffer);
        return new InMemoryBufferIterator(recordBuffer, serializer);
    }

    /**
     * 获取记录缓冲区段列表（包可见）
     *
     * @return 内存段列表
     */
    ArrayList<MemorySegment> getRecordBufferSegments() {
        return recordBufferSegments;
    }

    /**
     * 获取当前数据缓冲区偏移量（包可见）
     *
     * @return 偏移量
     */
    long getCurrentDataBufferOffset() {
        return currentDataBufferOffset;
    }

    /**
     * 获取记录缓冲区数量（包可见）
     *
     * @return 缓冲区数量
     */
    int getNumRecordBuffers() {
        if (!isInitialized) {
            return 0;
        }
        // 计算使用的内存段数量
        int result = (int) (currentDataBufferOffset / segmentSize);
        long mod = currentDataBufferOffset % segmentSize;
        if (mod != 0) {
            result += 1;
        }
        return result;
    }

    /**
     * 获取最后一个缓冲区中的字节数（包可见）
     *
     * @return 字节数
     */
    int getNumBytesInLastBuffer() {
        return numBytesInLastBuffer;
    }

    /**
     * 获取序列化器（包可见）
     *
     * @return 序列化器
     */
    AbstractRowDataSerializer<InternalRow> getSerializer() {
        return serializer;
    }

    /**
     * 内存缓冲区迭代器
     *
     * <p>从内存缓冲区顺序读取行数据。
     *
     * <p>使用 {@link RandomAccessInputView} 跨页读取序列化的行数据。
     */
    private static class InMemoryBufferIterator
            implements RowBufferIterator, MutableObjectIterator<BinaryRow> {

        /** 记录缓冲区视图 */
        private final RandomAccessInputView recordBuffer;
        /** 行序列化器 */
        private final AbstractRowDataSerializer<InternalRow> serializer;
        /** 复用的行对象 */
        private final BinaryRow reuse;
        /** 当前行 */
        private BinaryRow row;

        /**
         * 构造迭代器
         *
         * @param recordBuffer 记录缓冲区视图
         * @param serializer 行序列化器
         */
        private InMemoryBufferIterator(
                RandomAccessInputView recordBuffer,
                AbstractRowDataSerializer<InternalRow> serializer) {
            this.recordBuffer = recordBuffer;
            this.serializer = serializer;
            this.reuse = new BinaryRow(serializer.getArity());
        }

        /**
         * 前进到下一行
         *
         * @return true 表示有下一行，false 表示已到末尾
         */
        @Override
        public boolean advanceNext() {
            try {
                row = next(reuse);
                return row != null;
            } catch (IOException ioException) {
                throw new RuntimeException(ioException);
            }
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
         * 读取下一行
         *
         * @param reuse 复用的行对象
         * @return 行数据，如果到达末尾则返回 null
         * @throws IOException IO 异常
         */
        @Override
        public BinaryRow next(BinaryRow reuse) throws IOException {
            try {
                return (BinaryRow) serializer.mapFromPages(reuse, recordBuffer);
            } catch (EOFException e) {
                return null;
            }
        }

        /**
         * 读取下一行（使用默认复用对象）
         *
         * @return 行数据
         * @throws IOException IO 异常
         */
        @Override
        public BinaryRow next() throws IOException {
            return next(reuse);
        }

        @Override
        public void close() {}
    }

    /**
     * 空迭代器
     *
     * <p>用于返回空迭代器，避免使用接口（虚函数调用会导致性能损失）。
     */
    private static class EmptyInMemoryBufferIterator extends InMemoryBufferIterator {

        /**
         * 构造空迭代器
         */
        private EmptyInMemoryBufferIterator() {
            super(null, new InternalRowSerializer());
        }

        /**
         * 前进到下一行（总是返回 false）
         *
         * @return false
         */
        @Override
        public boolean advanceNext() {
            return false;
        }

        /**
         * 获取当前行（不支持）
         *
         * @throws UnsupportedOperationException 总是抛出
         */
        @Override
        public BinaryRow getRow() {
            throw new UnsupportedOperationException();
        }

        /**
         * 读取下一行（总是返回 null）
         *
         * @param reuse 复用对象
         * @return null
         */
        @Override
        public BinaryRow next(BinaryRow reuse) {
            return null;
        }

        /**
         * 读取下一行（总是返回 null）
         *
         * @return null
         */
        @Override
        public BinaryRow next() {
            return null;
        }
    }
}
