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

package org.apache.paimon.hash;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.AbstractPagedInputView;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.RandomAccessInputView;
import org.apache.paimon.data.SimpleCollectingOutputView;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.data.serializer.PagedTypeSerializer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.KeyValueIterator;
import org.apache.paimon.utils.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 基于字节的哈希映射表。
 *
 * <p>用于执行聚合操作，其中聚合值必须是固定宽度的，因为数据存储在连续内存中，
 * 可变长度的 AggBuffer 无法应用于此 HashMap。哈希映射中的 KeyValue 形式旨在
 * 减少查找时获取键的成本。
 *
 * <p><b>内存布局：</b>
 *
 * <p>内存分为两个区域：
 *
 * <p><b>1. Bucket 区域（桶区域）：</b>存储 pointer + hashcode
 *
 * <ul>
 *   <li>字节 0 到 4: 指向记录区域中记录的指针
 *   <li>字节 4 到 8: 键的完整 32 位哈希码
 * </ul>
 *
 * <p><b>2. Record 区域（记录区域）：</b>存储链表记录中的实际数据，一条记录有四部分：
 *
 * <ul>
 *   <li>字节 0 到 4: len(k) - 键的长度
 *   <li>字节 4 到 4 + len(k): 键数据
 *   <li>字节 4 + len(k) 到 8 + len(k): len(v) - 值的长度
 *   <li>字节 8 + len(k) 到 8 + len(k) + len(v): 值数据
 * </ul>
 *
 * <p><b>设计说明：</b>
 * <ul>
 *   <li>受 Apache Spark BytesToBytesMap 的影响
 *   <li>使用开放地址法处理哈希冲突
 *   <li>支持 HashSet 模式（当值类型为空时）
 *   <li>支持溢出到磁盘以处理内存不足的情况
 * </ul>
 *
 * @param <K> 键的类型
 */
public class BytesHashMap<K> extends BytesMap<K, BinaryRow> {

    private static final Logger LOG = LoggerFactory.getLogger(BytesHashMap.class);

    /**
     * HashSet 模式标志。
     *
     * <p>当 valueTypeInfos.length == 0 时设置为 true。通常在这种情况下，
     * BytesHashMap 将作为 HashSet 使用。当 hashSetMode 设置时，
     * {@link BytesHashMap#append(LookupInfo info, BinaryRow value)} 中的 value 参数将被忽略。
     * reusedValue 将始终指向一个 16 字节长的 MemorySegment，作为每个 BytesHashMap 条目的值部分，
     * 以使 BytesHashMap 的溢出机制兼容。
     */
    private final boolean hashSetMode;

    /** 用于将映射键序列化到 RecordArea 的 MemorySegments 中的序列化器。 */
    protected final PagedTypeSerializer<K> keySerializer;

    /** 用于将哈希映射值序列化到 RecordArea 的 MemorySegments 中的序列化器。 */
    private final BinaryRowSerializer valueSerializer;

    private volatile RecordArea.EntryIterator destructiveIterator = null;

    /**
     * 构造一个 BytesHashMap。
     *
     * @param memoryPool 内存段池，用于分配内存
     * @param keySerializer 键的序列化器
     * @param valueArity 值的字段数量。如果为 0，则启用 HashSet 模式
     */
    public BytesHashMap(
            MemorySegmentPool memoryPool, PagedTypeSerializer<K> keySerializer, int valueArity) {
        super(memoryPool, keySerializer);

        this.recordArea = new RecordArea();

        this.keySerializer = keySerializer;
        this.valueSerializer = new BinaryRowSerializer(valueArity);
        if (valueArity == 0) {
            this.hashSetMode = true;
            this.reusedValue = new BinaryRow(0);
            this.reusedValue.pointTo(MemorySegment.wrap(new byte[8]), 0, 8);
            LOG.info("BytesHashMap with hashSetMode = true.");
        } else {
            this.hashSetMode = false;
            this.reusedValue = this.valueSerializer.createInstance();
        }

        final int initBucketSegmentNum =
                MathUtils.roundDownToPowerOf2((int) (INIT_BUCKET_MEMORY_IN_BYTES / segmentSize));

        // allocate and initialize MemorySegments for bucket area
        initBucketSegments(initBucketSegmentNum);

        LOG.info(
                "BytesHashMap with initial memory segments {}, {} in bytes, init allocating {} for bucket area.",
                reservedNumBuffers,
                reservedNumBuffers * segmentSize,
                initBucketSegmentNum);
    }

    // ----------------------- Abstract Interface -----------------------

    @Override
    public long getNumKeys() {
        return numElements;
    }

    // ----------------------- Public interface -----------------------

    /**
     * 将一个值追加到哈希映射的记录区域中。
     *
     * <p><b>注意：</b>在追加之前，如果元素数量超过增长阈值，会自动进行扩容和重哈希。
     *
     * @param lookupInfo 查找信息，包含键和桶位置
     * @param value 要追加的值（在 HashSet 模式下会被忽略）
     * @return 映射到哈希映射记录区域中内存段的 BinaryRow，属于新追加的值
     * @throws EOFException 如果映射无法分配更多内存
     */
    public BinaryRow append(LookupInfo<K, BinaryRow> lookupInfo, BinaryRow value)
            throws IOException {
        try {
            if (numElements >= growthThreshold) {
                // 需要扩容并重新哈希
                growAndRehash();
                // 更新 info 的 bucketSegmentIndex 和 bucketOffset
                lookup(lookupInfo.key);
            }
            // 在 HashSet 模式下使用 reusedValue，否则使用传入的 value
            BinaryRow toAppend = hashSetMode ? reusedValue : value;
            int pointerToAppended = recordArea.appendRecord(lookupInfo, toAppend);
            // 更新桶中的指针和哈希码
            bucketSegments
                    .get(lookupInfo.bucketSegmentIndex)
                    .putInt(lookupInfo.bucketOffset, pointerToAppended);
            bucketSegments
                    .get(lookupInfo.bucketSegmentIndex)
                    .putInt(lookupInfo.bucketOffset + ELEMENT_POINT_LENGTH, lookupInfo.keyHashCode);
            numElements++;
            // 读取并返回追加的值
            recordArea.setReadPosition(pointerToAppended);
            ((RecordArea) recordArea).skipKey();
            return recordArea.readValue(reusedValue);
        } catch (EOFException e) {
            // 内存不足，需要溢出
            numSpillFiles++;
            spillInBytes += recordArea.getSegmentsSize();
            throw e;
        }
    }

    /** 返回溢出文件的数量。 */
    public long getNumSpillFiles() {
        return numSpillFiles;
    }

    /** 返回已使用的内存字节数（包括桶区域和记录区域）。 */
    public long getUsedMemoryInBytes() {
        return bucketSegments.size() * ((long) segmentSize) + recordArea.getSegmentsSize();
    }

    /** 返回溢出到磁盘的字节数。 */
    public long getSpillInBytes() {
        return spillInBytes;
    }

    /** 返回哈希映射中的元素数量。 */
    public int getNumElements() {
        return numElements;
    }

    /**
     * 返回用于遍历此映射条目的迭代器。
     *
     * @param requiresCopy 是否需要复制键和值。如果为 true，迭代器将返回键和值的副本，
     *                     否则返回可重用的对象（性能更好，但不能在迭代外使用）
     * @return 键值对迭代器
     */
    @SuppressWarnings("WeakerAccess")
    public KeyValueIterator<K, BinaryRow> getEntryIterator(boolean requiresCopy) {
        if (destructiveIterator != null) {
            throw new IllegalArgumentException(
                    "DestructiveIterator is not null, so this method can't be invoke!");
        }
        return ((RecordArea) recordArea).entryIterator(requiresCopy);
    }

    /** 返回哈希映射记录区域的底层内存段。 */
    @SuppressWarnings("WeakerAccess")
    public ArrayList<MemorySegment> getRecordAreaMemorySegments() {
        return ((RecordArea) recordArea).segments;
    }

    /** 返回哈希映射桶区域的内存段列表。 */
    @SuppressWarnings("WeakerAccess")
    public List<MemorySegment> getBucketAreaMemorySegments() {
        return bucketSegments;
    }

    /** 释放哈希映射占用的所有内存资源。 */
    public void free() {
        recordArea.release();
        destructiveIterator = null;
        super.free();
    }

    /** 重置哈希映射的记录和桶区域的内存段以便重用。 */
    public void reset() {
        // reset the record segments.
        recordArea.reset();
        destructiveIterator = null;
        super.reset();
    }

    /**
     * 返回是否处于 HashSet 模式。
     *
     * @return 当 BytesHashMap 的 valueTypeInfos.length == 0 时返回 true。
     *         任何追加的值都将被忽略，并用 reusedValue 作为存在标记替换。
     */
    @VisibleForTesting
    boolean isHashSetMode() {
        return hashSetMode;
    }

    // ----------------------- Private methods -----------------------

    /**
     * 计算给定类型数组的可变长度总和。
     *
     * <p>对于不在固定长度部分的类型，使用 16 字节的估计值。
     */
    static int getVariableLength(DataType[] types) {
        int length = 0;
        for (DataType type : types) {
            if (!BinaryRow.isInFixedLengthPart(type)) {
                // find a better way of computing generic type field variable-length
                // right now we use a small value assumption
                length += 16;
            }
        }
        return length;
    }

    // ----------------------- Record Area -----------------------

    /**
     * 记录区域实现类。
     *
     * <p>负责管理记录的存储和读取，记录包含键值对数据。
     * 使用内存段列表存储数据，支持顺序写入和随机读取。
     */
    private final class RecordArea implements BytesMap.RecordArea<K, BinaryRow> {
        /** 存储记录数据的内存段列表。 */
        private final ArrayList<MemorySegment> segments = new ArrayList<>();

        /** 用于随机读取的输入视图。 */
        private final RandomAccessInputView inView;

        /** 用于顺序写入的输出视图。 */
        private final SimpleCollectingOutputView outView;

        RecordArea() {
            this.outView = new SimpleCollectingOutputView(segments, memoryPool, segmentSize);
            this.inView = new RandomAccessInputView(segments, segmentSize);
        }

        /** 释放所有内存段并返回到内存池。 */
        public void release() {
            returnSegments(segments);
            segments.clear();
        }

        /** 重置记录区域，释放内存并准备重用。 */
        public void reset() {
            release();
            // request a new memory segment from freeMemorySegments
            // reset segmentNum and positionInSegment
            outView.reset();
            inView.setReadPosition(0);
        }

        // ----------------------- Append -----------------------

        /**
         * 追加一条记录到记录区域。
         *
         * @param lookupInfo 包含键的查找信息
         * @param value 要追加的值
         * @return 记录在内存中的指针位置
         * @throws IOException 如果序列化失败或内存不足
         */
        public int appendRecord(LookupInfo<K, BinaryRow> lookupInfo, BinaryRow value)
                throws IOException {
            final long oldLastPosition = outView.getCurrentOffset();
            // 将键序列化到 BytesHashMap 记录区域
            int skip = keySerializer.serializeToPages(lookupInfo.getKey(), outView);
            long offset = oldLastPosition + skip;

            // 将值序列化到 BytesHashMap 记录区域
            valueSerializer.serializeToPages(value, outView);
            if (offset > Integer.MAX_VALUE) {
                LOG.warn(
                        "We can't handle key area with more than Integer.MAX_VALUE bytes,"
                                + " because the pointer is a integer.");
                throw new EOFException();
            }
            return (int) offset;
        }

        /** 返回记录区域占用的总字节数。 */
        @Override
        public long getSegmentsSize() {
            return segments.size() * ((long) segmentSize);
        }

        // ----------------------- Read -----------------------

        /** 设置读取位置。 */
        public void setReadPosition(int position) {
            inView.setReadPosition(position);
        }

        /**
         * 读取键并与给定的查找键比较是否相等。
         *
         * @param lookupKey 要比较的查找键
         * @return 如果键相等返回 true
         * @throws IOException 当访问无效的内存地址时
         */
        public boolean readKeyAndEquals(K lookupKey) throws IOException {
            reusedKey = keySerializer.mapFromPages(reusedKey, inView);
            return lookupKey.equals(reusedKey);
        }

        /**
         * 跳过当前位置的键。
         *
         * @throws IOException 当访问无效的内存地址时
         */
        void skipKey() throws IOException {
            keySerializer.skipRecordFromPages(inView);
        }

        /**
         * 读取当前位置的值。
         *
         * @param reuse 可重用的 BinaryRow 对象
         * @return 读取的值
         * @throws IOException 当访问无效的内存地址时
         */
        public BinaryRow readValue(BinaryRow reuse) throws IOException {
            // depends on BinaryRowSerializer to check writing skip
            // and to find the real start offset of the data
            return valueSerializer.mapFromPages(reuse, inView);
        }

        // ----------------------- Iterator -----------------------

        /**
         * 创建一个条目迭代器。
         *
         * @param requiresCopy 是否需要复制键和值
         * @return 键值对迭代器
         */
        private KeyValueIterator<K, BinaryRow> entryIterator(boolean requiresCopy) {
            return new EntryIterator(requiresCopy);
        }

        /**
         * 条目迭代器实现。
         *
         * <p>用于遍历哈希映射中的所有键值对。
         * 注意：这是一个破坏性迭代器，创建后会设置 destructiveIterator 标志。
         */
        private final class EntryIterator extends AbstractPagedInputView
                implements KeyValueIterator<K, BinaryRow> {

            /** 已迭代的元素数量。 */
            private int count = 0;

            /** 当前正在读取的内存段索引。 */
            private int currentSegmentIndex = 0;

            /** 是否需要复制键和值。 */
            private final boolean requiresCopy;

            private EntryIterator(boolean requiresCopy) {
                super(segments.get(0), segmentSize);
                destructiveIterator = this;
                this.requiresCopy = requiresCopy;
            }

            /**
             * 移动到下一个元素。
             *
             * @return 如果成功移动到下一个元素返回 true，否则返回 false
             * @throws IOException 如果读取失败
             */
            @Override
            public boolean advanceNext() throws IOException {
                if (count < numElements) {
                    count++;
                    // segment already is useless any more.
                    keySerializer.mapFromPages(reusedKey, this);
                    valueSerializer.mapFromPages(reusedValue, this);
                    return true;
                }
                return false;
            }

            /**
             * 获取当前键。
             *
             * @return 如果 requiresCopy 为 true，返回键的副本；否则返回可重用的键对象
             */
            @Override
            public K getKey() {
                return requiresCopy ? keySerializer.copy(reusedKey) : reusedKey;
            }

            /**
             * 获取当前值。
             *
             * @return 如果 requiresCopy 为 true，返回值的副本；否则返回可重用的值对象
             */
            @Override
            public BinaryRow getValue() {
                return requiresCopy ? reusedValue.copy() : reusedValue;
            }

            /** 判断是否还有下一个元素。 */
            public boolean hasNext() {
                return count < numElements;
            }

            /** 获取内存段的限制大小。 */
            @Override
            protected int getLimitForSegment(MemorySegment segment) {
                return segmentSize;
            }

            /** 获取下一个内存段。 */
            @Override
            protected MemorySegment nextSegment(MemorySegment current) {
                return segments.get(++currentSegmentIndex);
            }
        }
    }
}
