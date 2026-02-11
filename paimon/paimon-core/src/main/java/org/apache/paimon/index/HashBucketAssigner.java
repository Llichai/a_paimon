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

import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 基于哈希的Bucket分配器。
 *
 * <p>根据键的哈希值为记录分配bucket。
 * 维护分区索引以跟踪bucket分配情况,支持动态扩展bucket数量。
 *
 * <p>主要特性:
 * <ul>
 *   <li>基于哈希的bucket分配策略</li>
 *   <li>支持多个分配器并行工作</li>
 *   <li>自动清理过期的分区索引</li>
 *   <li>支持动态调整bucket数量上限</li>
 * </ul>
 */
public class HashBucketAssigner implements BucketAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(HashBucketAssigner.class);

    private final SnapshotManager snapshotManager;
    private final String commitUser;
    private final IndexFileHandler indexFileHandler;
    private final int numChannels;
    private final int numAssigners;
    private final int assignId;
    private final long targetBucketRowNumber;
    private final int maxBucketsNum;
    private int maxBucketId;

    private final Map<BinaryRow, PartitionIndex> partitionIndex;

    public HashBucketAssigner(
            SnapshotManager snapshotManager,
            String commitUser,
            IndexFileHandler indexFileHandler,
            int numChannels,
            int numAssigners,
            int assignId,
            long targetBucketRowNumber,
            int maxBucketsNum) {
        this.snapshotManager = snapshotManager;
        this.commitUser = commitUser;
        this.indexFileHandler = indexFileHandler;
        this.numChannels = numChannels;
        this.numAssigners = numAssigners;
        this.assignId = assignId;
        this.targetBucketRowNumber = targetBucketRowNumber;
        this.partitionIndex = new HashMap<>();
        this.maxBucketsNum = maxBucketsNum;
    }

    /**
     * 为记录的键哈希值分配bucket。
     *
     * @param partition 分区
     * @param hash 键哈希值
     * @return 分配的bucket ID
     */
    @Override
    public int assign(BinaryRow partition, int hash) {
        int partitionHash = partition.hashCode();
        int recordAssignId = computeAssignId(partitionHash, hash);
        checkArgument(
                recordAssignId == assignId,
                "This is a bug, record assign id %s should equal to assign id %s.",
                recordAssignId,
                assignId);

        PartitionIndex index = this.partitionIndex.get(partition);
        if (index == null) {
            partition = partition.copy();
            index = loadIndex(partition, partitionHash);
            this.partitionIndex.put(partition, index);
        }

        int assigned = index.assign(hash, this::isMyBucket, maxBucketsNum, maxBucketId);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Assign {} to the partition {} key hash {}", assigned, partition, hash);
        }
        if (assigned > maxBucketId) {
            maxBucketId = assigned;
        }
        return assigned;
    }

    /**
     * 准备提交,清理过期的分区索引。
     *
     * <p>移除不再使用的分区索引以释放内存。
     *
     * @param commitIdentifier 提交标识符
     */
    @Override
    public void prepareCommit(long commitIdentifier) {
        long latestCommittedIdentifier;
        if (partitionIndex.values().stream()
                        .mapToLong(i -> i.lastAccessedCommitIdentifier)
                        .max()
                        .orElse(Long.MIN_VALUE)
                == Long.MIN_VALUE) {
            // Optimization for the first commit.
            //
            // If this is the first commit, no index has previous modified commit, so the value of
            // `latestCommittedIdentifier` does not matter.
            //
            // Without this optimization, we may need to scan through all snapshots only to find
            // that there is no previous snapshot by this user, which is very inefficient.
            latestCommittedIdentifier = Long.MIN_VALUE;
        } else {
            latestCommittedIdentifier =
                    snapshotManager
                            .latestSnapshotOfUserFromFilesystem(commitUser)
                            .map(Snapshot::commitIdentifier)
                            .orElse(Long.MIN_VALUE);
        }

        Iterator<Map.Entry<BinaryRow, PartitionIndex>> iterator =
                partitionIndex.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<BinaryRow, PartitionIndex> entry = iterator.next();
            BinaryRow partition = entry.getKey();
            PartitionIndex index = entry.getValue();
            if (index.accessed) {
                index.lastAccessedCommitIdentifier = commitIdentifier;
            } else {
                if (index.lastAccessedCommitIdentifier <= latestCommittedIdentifier) {
                    // Clear writer if no update, and if its latest modification has committed.
                    //
                    // We need a mechanism to clear index, otherwise there will be more and
                    // more such as yesterday's partition that no longer needs to be accessed.
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Removing index for partition {}. "
                                        + "Index's last accessed identifier is {}, "
                                        + "while latest committed identifier is {}, "
                                        + "current commit identifier is {}.",
                                partition,
                                index.lastAccessedCommitIdentifier,
                                latestCommittedIdentifier,
                                commitIdentifier);
                    }
                    iterator.remove();
                }
            }
            index.accessed = false;
        }
    }

    @VisibleForTesting
    Set<BinaryRow> currentPartitions() {
        return partitionIndex.keySet();
    }

    private int computeAssignId(int partitionHash, int keyHash) {
        return BucketAssigner.computeAssigner(partitionHash, keyHash, numChannels, numAssigners);
    }

    private boolean isMyBucket(int bucket) {
        return BucketAssigner.isMyBucket(bucket, numAssigners, assignId);
    }

    private PartitionIndex loadIndex(BinaryRow partition, int partitionHash) {
        return PartitionIndex.loadIndex(
                indexFileHandler,
                partition,
                targetBucketRowNumber,
                (hash) -> computeAssignId(partitionHash, hash) == assignId,
                this::isMyBucket);
    }
}
