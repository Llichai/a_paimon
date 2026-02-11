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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.serializer.PagedTypeSerializer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.utils.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@code BytesHashMap} 的基类。
 *
 * <p>提供基于字节的哈希映射的核心功能，包括：
 * <ul>
 *   <li>内存管理：管理桶区域和记录区域的内存段
 *   <li>哈希冲突处理：使用双重哈希的开放地址法
 *   <li>动态扩容：当元素数量超过阈值时自动扩容和重哈希
 *   <li>查找操作：通过哈希码快速定位键值对
 * </ul>
 *
 * <p><b>内存布局：</b>
 * <ul>
 *   <li>桶区域（Bucket Area）：存储指针和哈希码，每个桶占用 8 字节
 *   <li>记录区域（Record Area）：存储实际的键值对数据
 * </ul>
 *
 * <p><b>哈希冲突解决：</b>
 * 使用双重哈希（Double Hashing）的开放地址法：
 * <ul>
 *   <li>第一个哈希函数：H1(K) = hashCode(K) & numBucketsMask
 *   <li>第二个哈希函数：H2(K) = 1 + 2 * ((H1(K)/M) mod (M-1))，保证为奇数
 *   <li>探测序列：pos = (H1(K) + i * H2(K)) & numBucketsMask
 * </ul>
 *
 * @param <K> 映射键的类型
 * @param <V> 映射值的类型
 */
public abstract class BytesMap<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(BytesMap.class);

    /** 每个桶的大小（字节）。 */
    public static final int BUCKET_SIZE = 8;

    /** 链表结束标记，表示该桶为空。 */
    protected static final int END_OF_LIST = Integer.MAX_VALUE;

    /** 探测步长增量，用于线性探测。 */
    protected static final int STEP_INCREMENT = 1;

    /** 元素指针的长度（字节）。 */
    protected static final int ELEMENT_POINT_LENGTH = 4;

    /** 记录额外信息的长度（字节）。 */
    public static final int RECORD_EXTRA_LENGTH = 8;

    /** 桶大小的位数（log2(BUCKET_SIZE)）。 */
    protected static final int BUCKET_SIZE_BITS = 3;

    /** 每个内存段中的桶数量。 */
    protected final int numBucketsPerSegment;

    /** 每个内存段中桶数量的位数。 */
    protected final int numBucketsPerSegmentBits;

    /** 每个内存段中桶数量的掩码。 */
    protected final int numBucketsPerSegmentMask;

    /** 最后一个桶在段中的位置。 */
    protected final int lastBucketPosition;

    /** 内存段的大小（字节）。 */
    protected final int segmentSize;

    /** 内存段池，用于分配和回收内存段。 */
    protected final MemorySegmentPool memoryPool;

    /** 存储桶的内存段列表。 */
    protected List<MemorySegment> bucketSegments;

    /** 预留的内存缓冲区数量。 */
    protected final int reservedNumBuffers;

    /** 哈希映射中的元素数量。 */
    protected int numElements = 0;

    /** 桶数量的掩码，用于计算桶索引。 */
    protected int numBucketsMask;

    /** 基于 log2NumBuckets 和 numBucketsMask2 计算第二个哈希码。 */
    protected int log2NumBuckets;

    /** 第二个哈希函数使用的掩码。 */
    protected int numBucketsMask2;

    /** 负载因子，当元素数量超过 (桶数量 * LOAD_FACTOR) 时触发扩容。 */
    protected static final double LOAD_FACTOR = 0.75;

    /** 初始桶内存大小（字节），使用较小的桶可以更好地利用 L1/L2/L3 缓存。 */
    protected static final long INIT_BUCKET_MEMORY_IN_BYTES = 1024 * 1024L;

    /** 扩容阈值，当元素数量超过此值时，哈希映射将进行扩容。 */
    protected int growthThreshold;

    /** 存储实际数据的记录区域。 */
    protected RecordArea<K, V> recordArea;

    /** 查找和迭代时使用的可重用键对象。 */
    protected K reusedKey;

    /** 通过键检索映射值和迭代时使用的可重用值对象。 */
    protected V reusedValue;

    /** 查找操作返回的可重用查找信息对象。 */
    private final LookupInfo<K, V> reuseLookupInfo;

    // metric - 性能指标

    /** 溢出文件的数量。 */
    protected long numSpillFiles;

    /** 溢出到磁盘的字节数。 */
    protected long spillInBytes;

    public BytesMap(MemorySegmentPool memoryPool, PagedTypeSerializer<K> keySerializer) {
        this.memoryPool = memoryPool;
        this.segmentSize = memoryPool.pageSize();
        this.reservedNumBuffers = memoryPool.freePages();
        this.numBucketsPerSegment = segmentSize / BUCKET_SIZE;
        this.numBucketsPerSegmentBits = MathUtils.log2strict(this.numBucketsPerSegment);
        this.numBucketsPerSegmentMask = (1 << this.numBucketsPerSegmentBits) - 1;
        this.lastBucketPosition = (numBucketsPerSegment - 1) * BUCKET_SIZE;

        this.reusedKey = keySerializer.createReuseInstance();
        this.reuseLookupInfo = new LookupInfo<>();
    }

    /** Returns the number of keys in this map. */
    public abstract long getNumKeys();

    protected void initBucketSegments(int numBucketSegments) {
        if (numBucketSegments < 1) {
            throw new RuntimeException("Too small memory allocated for BytesHashMap");
        }
        this.bucketSegments = new ArrayList<>(numBucketSegments);
        for (int i = 0; i < numBucketSegments; i++) {
            MemorySegment segment = memoryPool.nextSegment();
            if (segment == null) {
                throw new RuntimeException("Memory for hash map is too small.");
            }
            bucketSegments.add(i, segment);
        }

        resetBucketSegments(this.bucketSegments);
        int numBuckets = numBucketSegments * numBucketsPerSegment;
        this.log2NumBuckets = MathUtils.log2strict(numBuckets);
        this.numBucketsMask = (1 << MathUtils.log2strict(numBuckets)) - 1;
        this.numBucketsMask2 = (1 << MathUtils.log2strict(numBuckets >> 1)) - 1;
        this.growthThreshold = (int) (numBuckets * LOAD_FACTOR);
    }

    protected void resetBucketSegments(List<MemorySegment> resetBucketSegs) {
        for (MemorySegment segment : resetBucketSegs) {
            for (int j = 0; j <= lastBucketPosition; j += BUCKET_SIZE) {
                segment.putInt(j, END_OF_LIST);
            }
        }
    }

    public long getNumSpillFiles() {
        return numSpillFiles;
    }

    public long getSpillInBytes() {
        return spillInBytes;
    }

    public int getNumElements() {
        return numElements;
    }

    public void free() {
        returnSegments(this.bucketSegments);
        this.bucketSegments.clear();
        numElements = 0;
    }

    /** reset the map's record and bucket area's memory segments for reusing. */
    public void reset() {
        setBucketVariables(bucketSegments);
        resetBucketSegments(bucketSegments);
        numElements = 0;
        LOG.debug(
                "reset BytesHashMap with record memory segments {}, {} in bytes, init allocating {} for bucket area.",
                memoryPool.freePages(),
                memoryPool.freePages() * segmentSize,
                bucketSegments.size());
    }

    /**
     * @param key by which looking up the value in the hash map. Only support the key in the
     *     BinaryRowData form who has only one MemorySegment.
     * @return {@link LookupInfo}
     */
    public LookupInfo<K, V> lookup(K key) {
        final int hashCode1 = key.hashCode();
        int newPos = hashCode1 & numBucketsMask;
        // which segment contains the bucket
        int bucketSegmentIndex = newPos >>> numBucketsPerSegmentBits;
        // offset of the bucket in the segment
        int bucketOffset = (newPos & numBucketsPerSegmentMask) << BUCKET_SIZE_BITS;

        boolean found = false;
        int step = STEP_INCREMENT;
        int hashCode2 = 0;
        int findElementPtr;
        try {
            do {
                findElementPtr = bucketSegments.get(bucketSegmentIndex).getInt(bucketOffset);
                if (findElementPtr == END_OF_LIST) {
                    // This is a new key.
                    break;
                } else {
                    final int storedHashCode =
                            bucketSegments
                                    .get(bucketSegmentIndex)
                                    .getInt(bucketOffset + ELEMENT_POINT_LENGTH);
                    if (hashCode1 == storedHashCode) {
                        recordArea.setReadPosition(findElementPtr);
                        if (recordArea.readKeyAndEquals(key)) {
                            // we found an element with a matching key, and not just a hash
                            // collision
                            found = true;
                            reusedValue = recordArea.readValue(reusedValue);
                            break;
                        }
                    }
                }
                if (step == 1) {
                    hashCode2 = calcSecondHashCode(hashCode1);
                }
                newPos = (hashCode1 + step * hashCode2) & numBucketsMask;
                // which segment contains the bucket
                bucketSegmentIndex = newPos >>> numBucketsPerSegmentBits;
                // offset of the bucket in the segment
                bucketOffset = (newPos & numBucketsPerSegmentMask) << BUCKET_SIZE_BITS;
                step += STEP_INCREMENT;
            } while (true);
        } catch (IOException ex) {
            throw new RuntimeException(
                    "Error reading record from the aggregate map: " + ex.getMessage(), ex);
        }
        reuseLookupInfo.set(found, hashCode1, key, reusedValue, bucketSegmentIndex, bucketOffset);
        return reuseLookupInfo;
    }

    /** @throws EOFException if the map can't allocate much more memory. */
    protected void growAndRehash() throws EOFException {
        // allocate the new data structures
        int required = 2 * bucketSegments.size();
        if (required * (long) numBucketsPerSegment > Integer.MAX_VALUE) {
            LOG.warn(
                    "We can't handle more than Integer.MAX_VALUE buckets (eg. because hash functions return int)");
            throw new EOFException();
        }

        int numAllocatedSegments = required - memoryPool.freePages();
        if (numAllocatedSegments > 0) {
            LOG.warn(
                    "BytesHashMap can't allocate {} pages, and now used {} pages",
                    required,
                    reservedNumBuffers);
            throw new EOFException();
        }

        List<MemorySegment> newBucketSegments = new ArrayList<>(required);
        for (int i = 0; i < required; i++) {
            newBucketSegments.add(memoryPool.nextSegment());
        }
        setBucketVariables(newBucketSegments);

        long reHashStartTime = System.currentTimeMillis();
        resetBucketSegments(newBucketSegments);
        // Re-mask (we don't recompute the hashcode because we stored all 32 bits of it)
        for (MemorySegment memorySegment : bucketSegments) {
            for (int j = 0; j < numBucketsPerSegment; j++) {
                final int recordPointer = memorySegment.getInt(j * BUCKET_SIZE);
                if (recordPointer != END_OF_LIST) {
                    final int hashCode1 =
                            memorySegment.getInt(j * BUCKET_SIZE + ELEMENT_POINT_LENGTH);
                    int newPos = hashCode1 & numBucketsMask;
                    int bucketSegmentIndex = newPos >>> numBucketsPerSegmentBits;
                    int bucketOffset = (newPos & numBucketsPerSegmentMask) << BUCKET_SIZE_BITS;
                    int step = STEP_INCREMENT;
                    long hashCode2 = 0;
                    while (newBucketSegments.get(bucketSegmentIndex).getInt(bucketOffset)
                            != END_OF_LIST) {
                        if (step == 1) {
                            hashCode2 = calcSecondHashCode(hashCode1);
                        }
                        newPos = (int) ((hashCode1 + step * hashCode2) & numBucketsMask);
                        // which segment contains the bucket
                        bucketSegmentIndex = newPos >>> numBucketsPerSegmentBits;
                        // offset of the bucket in the segment
                        bucketOffset = (newPos & numBucketsPerSegmentMask) << BUCKET_SIZE_BITS;
                        step += STEP_INCREMENT;
                    }
                    newBucketSegments.get(bucketSegmentIndex).putInt(bucketOffset, recordPointer);
                    newBucketSegments
                            .get(bucketSegmentIndex)
                            .putInt(bucketOffset + ELEMENT_POINT_LENGTH, hashCode1);
                }
            }
        }
        LOG.info(
                "The rehash take {} ms for {} segments",
                (System.currentTimeMillis() - reHashStartTime),
                required);
        this.memoryPool.returnAll(this.bucketSegments);
        this.bucketSegments = newBucketSegments;
    }

    protected void returnSegments(List<MemorySegment> segments) {
        memoryPool.returnAll(segments);
    }

    private void setBucketVariables(List<MemorySegment> bucketSegments) {
        int numBuckets = bucketSegments.size() * numBucketsPerSegment;
        this.log2NumBuckets = MathUtils.log2strict(numBuckets);
        this.numBucketsMask = (1 << MathUtils.log2strict(numBuckets)) - 1;
        this.numBucketsMask2 = (1 << MathUtils.log2strict(numBuckets >> 1)) - 1;
        this.growthThreshold = (int) (numBuckets * LOAD_FACTOR);
    }

    // M(the num of buckets) is the nth power of 2,  so the second hash code must be odd, and always
    // is
    // H2(K) = 1 + 2 * ((H1(K)/M) mod (M-1))
    protected int calcSecondHashCode(final int firstHashCode) {
        return ((((firstHashCode >> log2NumBuckets)) & numBucketsMask2) << 1) + 1;
    }

    /** Record area. */
    interface RecordArea<K, V> {

        void setReadPosition(int position);

        boolean readKeyAndEquals(K lookupKey) throws IOException;

        V readValue(V reuse) throws IOException;

        int appendRecord(LookupInfo<K, V> lookupInfo, BinaryRow value) throws IOException;

        long getSegmentsSize();

        void release();

        void reset();
    }

    /** Result fetched when looking up a key. */
    public static final class LookupInfo<K, V> {
        boolean found;
        K key;
        V value;

        /**
         * The hashcode of the look up key passed to {@link BytesMap#lookup(K)}, Caching this
         * hashcode here allows us to avoid re-hashing the key when inserting a value for that key.
         * The same purpose with bucketSegmentIndex, bucketOffset.
         */
        int keyHashCode;

        int bucketSegmentIndex;
        int bucketOffset;

        LookupInfo() {
            this.found = false;
            this.keyHashCode = -1;
            this.key = null;
            this.value = null;
            this.bucketSegmentIndex = -1;
            this.bucketOffset = -1;
        }

        void set(
                boolean found,
                int keyHashCode,
                K key,
                V value,
                int bucketSegmentIndex,
                int bucketOffset) {
            this.found = found;
            this.keyHashCode = keyHashCode;
            this.key = key;
            this.value = value;
            this.bucketSegmentIndex = bucketSegmentIndex;
            this.bucketOffset = bucketOffset;
        }

        public boolean isFound() {
            return found;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }
}
