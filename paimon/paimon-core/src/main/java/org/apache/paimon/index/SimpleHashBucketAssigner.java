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

package org.apache.paimon.index;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.utils.Int2ShortHashMap;
import org.apache.paimon.utils.ListUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 简单的哈希Bucket分配器。
 *
 * <p>用于表覆盖写场景,无需加载已有索引。
 * 比 {@link HashBucketAssigner} 更轻量,适用于全量数据重写的场景。
 *
 * <p>主要特点:
 * <ul>
 *   <li>不需要读取现有索引文件</li>
 *   <li>内存中动态构建哈希到bucket的映射</li>
 *   <li>支持bucket数量上限控制</li>
 * </ul>
 */
public class SimpleHashBucketAssigner implements BucketAssigner {

    private final int numAssigners;
    private final int assignId;
    private final long targetBucketRowNumber;
    private final int maxBucketsNum;
    private int maxBucketId;

    private final Map<BinaryRow, SimplePartitionIndex> partitionIndex;

    public SimpleHashBucketAssigner(
            int numAssigners, int assignId, long targetBucketRowNumber, int maxBucketsNum) {
        this.numAssigners = numAssigners;
        this.assignId = assignId;
        this.targetBucketRowNumber = targetBucketRowNumber;
        this.partitionIndex = new HashMap<>();
        this.maxBucketsNum = maxBucketsNum;
    }

    @Override
    public int assign(BinaryRow partition, int hash) {
        SimplePartitionIndex index = partitionIndex.get(partition);
        if (index == null) {
            partition = partition.copy();
            index = new SimplePartitionIndex();
            this.partitionIndex.put(partition, index);
        }
        int assigned = index.assign(hash);
        if (assigned > maxBucketId) {
            maxBucketId = assigned;
        }
        return assigned;
    }

    @Override
    public void prepareCommit(long commitIdentifier) {
        // do nothing
    }

    @VisibleForTesting
    Set<BinaryRow> currentPartitions() {
        return partitionIndex.keySet();
    }

    /**
     * 简单的分区bucket哈希分配器。
     *
     * <p>每个分区维护独立的哈希到bucket映射。
     */
    private class SimplePartitionIndex {

        public final Int2ShortHashMap hash2Bucket = new Int2ShortHashMap();
        private final Map<Integer, Long> bucketInformation;
        private final List<Integer> bucketList;
        private int currentBucket;

        private SimplePartitionIndex() {
            bucketInformation = new LinkedHashMap<>();
            bucketList = new ArrayList<>();
            loadNewBucket();
        }

        public int assign(int hash) {
            // the same hash should go into the same bucket
            if (hash2Bucket.containsKey(hash)) {
                return hash2Bucket.get(hash);
            }

            Long num =
                    bucketInformation.computeIfAbsent(
                            currentBucket,
                            bucket -> {
                                bucketList.add(bucket);
                                return 0L;
                            });

            if (num >= targetBucketRowNumber) {
                if (-1 == maxBucketsNum
                        || bucketInformation.isEmpty()
                        || maxBucketId < maxBucketsNum - 1) {
                    loadNewBucket();
                } else {
                    currentBucket = ListUtils.pickRandomly(bucketList);
                }
            }
            bucketInformation.compute(currentBucket, (i, l) -> l == null ? 1L : l + 1);
            hash2Bucket.put(hash, (short) currentBucket);
            return currentBucket;
        }

        private void loadNewBucket() {
            for (int i = 0; i < Short.MAX_VALUE; i++) {
                if (isMyBucket(i) && !bucketInformation.containsKey(i)) {
                    // The new bucketId may still be larger than the upper bound
                    if (-1 == maxBucketsNum || i <= maxBucketsNum - 1) {
                        currentBucket = i;
                        return;
                    }
                    // No need to enter the next iteration when upper bound exceeded
                    return;
                }
            }
            throw new RuntimeException(
                    "Can't find a suitable bucket to assign, all the bucket are assigned?");
        }
    }

    private boolean isMyBucket(int bucket) {
        return BucketAssigner.isMyBucket(bucket, numAssigners, assignId);
    }
}
