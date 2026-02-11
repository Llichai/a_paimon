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

package org.apache.paimon.table;

import java.util.List;

/**
 * 分桶规格，包含所有分桶信息，用于表扫描时的计划优化。
 *
 * <p>BucketSpec 定义了表的分桶策略，包括：
 * <ul>
 *   <li>分桶模式（Bucket Mode）：如何分配数据到桶
 *   <li>分桶键（Bucket Keys）：用于计算桶编号的列
 *   <li>桶数量（Num Buckets）：固定桶模式下的桶数量
 * </ul>
 *
 * <h3>分桶模式说明</h3>
 * <ul>
 *   <li>HASH_FIXED：固定数量的桶，numBuckets > 0
 *   <li>HASH_DYNAMIC：动态桶，numBuckets = -1
 *   <li>KEY_DYNAMIC：基于键的动态桶，numBuckets = -1
 *   <li>BUCKET_UNAWARE：不使用桶，numBuckets = -1
 *   <li>POSTPONE_MODE：延迟分桶，numBuckets = -1
 * </ul>
 *
 * <h3>计划优化</h3>
 * <p>BucketSpec 用于表扫描时的优化：
 * <ul>
 *   <li>分区裁剪：根据分区键过滤分区
 *   <li>桶裁剪：根据分桶键过滤桶
 *   <li>负载均衡：根据桶数量分配任务
 * </ul>
 *
 * @since 0.9
 */
public class BucketSpec {

    /** 分桶模式。 */
    private final BucketMode bucketMode;

    /** 分桶键列表。 */
    private final List<String> bucketKeys;

    /** 桶数量（动态桶模式下为 -1）。 */
    private final int numBuckets;

    /**
     * 构造 BucketSpec。
     *
     * @param bucketMode 分桶模式
     * @param bucketKeys 分桶键列表
     * @param numBuckets 桶数量，动态桶模式下为 -1
     */
    public BucketSpec(BucketMode bucketMode, List<String> bucketKeys, int numBuckets) {
        this.bucketMode = bucketMode;
        this.bucketKeys = bucketKeys;
        this.numBuckets = numBuckets;
    }

    /**
     * 返回分桶模式。
     *
     * @return BucketMode 枚举值
     */
    public BucketMode getBucketMode() {
        return bucketMode;
    }

    /**
     * 返回分桶键列表。
     *
     * <p>分桶键用于计算记录所属的桶编号。
     *
     * @return 分桶键列表
     */
    public List<String> getBucketKeys() {
        return bucketKeys;
    }

    /**
     * 返回桶数量。
     *
     * <p>如果 bucketMode 是 HASH_DYNAMIC 或其他动态模式，返回 -1。
     *
     * @return 桶数量，动态模式下返回 -1
     */
    public int getNumBuckets() {
        return numBuckets;
    }

    @Override
    public String toString() {
        return "BucketSpec{"
                + "bucketMode="
                + bucketMode
                + ", bucketKeys="
                + bucketKeys
                + ", numBuckets="
                + numBuckets
                + '}';
    }
}
