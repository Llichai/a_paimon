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

import org.apache.paimon.data.BinaryRow;

/**
 * Bucket分配器接口。
 *
 * <p>为记录分配bucket,仅用于动态bucket表。
 * 动态bucket表可以根据数据分布自动调整bucket数量,以优化数据均衡性和查询性能。
 */
public interface BucketAssigner {

    /**
     * 为指定分区和哈希值分配bucket。
     *
     * @param partition 分区键
     * @param hash 记录的哈希值
     * @return 分配的bucket ID
     */
    int assign(BinaryRow partition, int hash);

    /**
     * 准备提交,清理过期的分区索引。
     *
     * @param commitIdentifier 提交标识符
     */
    void prepareCommit(long commitIdentifier);

    /**
     * 判断bucket是否属于当前分配器。
     *
     * @param bucket bucket ID
     * @param numAssigners 分配器总数
     * @param assignId 当前分配器ID
     * @return 如果bucket属于当前分配器则返回true
     */
    static boolean isMyBucket(int bucket, int numAssigners, int assignId) {
        return bucket % numAssigners == assignId % numAssigners;
    }

    /**
     * 计算哈希键。
     *
     * @param partitionHash 分区哈希值
     * @param keyHash 键哈希值
     * @param numChannels 通道数
     * @param numAssigners 分配器数量
     * @return 计算得到的哈希键
     */
    static int computeHashKey(int partitionHash, int keyHash, int numChannels, int numAssigners) {
        int start = Math.abs(partitionHash % numChannels);
        int id = Math.abs(keyHash % numAssigners);
        return start + id;
    }

    /**
     * 计算应该使用的分配器ID。
     *
     * @param partitionHash 分区哈希值
     * @param keyHash 键哈希值
     * @param numChannels 通道数
     * @param numAssigners 分配器数量
     * @return 分配器ID
     */
    static int computeAssigner(int partitionHash, int keyHash, int numChannels, int numAssigners) {
        return computeHashKey(partitionHash, keyHash, numChannels, numAssigners) % numChannels;
    }
}
