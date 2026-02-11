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

package org.apache.paimon.manifest;

import org.apache.paimon.utils.BiFilter;
import org.apache.paimon.utils.Filter;

import javax.annotation.Nullable;

/**
 * 桶过滤器
 *
 * <p>BucketFilter 用于过滤 Manifest 中的桶，支持多种过滤策略。
 *
 * <p>四种过滤策略：
 * <ul>
 *   <li>onlyReadRealBuckets：仅读取真实桶（bucket >= 0，过滤虚拟桶）
 *   <li>specifiedBucket：指定桶号（精确匹配）
 *   <li>bucketFilter：桶过滤器（自定义过滤逻辑）
 *   <li>totalAwareBucketFilter：总桶数感知的过滤器（考虑总桶数）
 * </ul>
 *
 * <p>虚拟桶（bucket < 0）：
 * <ul>
 *   <li>用于延迟桶表（Postpone Bucket Table）
 *   <li>在 Compaction 前使用虚拟桶号
 *   <li>Compaction 后分配真实桶号
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>扫描优化：仅读取需要的桶
 *   <li>分区裁剪：根据查询条件过滤桶
 *   <li>负载均衡：按桶号分配任务
 *   <li>Compaction：过滤需要 Compaction 的桶
 * </ul>
 *
 * <p>过滤逻辑（test 方法）：
 * <ol>
 *   <li>检查是否为虚拟桶（如果 onlyReadRealBuckets = true）
 *   <li>检查是否匹配指定桶号（如果 specifiedBucket != null）
 *   <li>检查是否通过桶过滤器（如果 bucketFilter != null）
 *   <li>检查是否通过总桶数感知过滤器（如果 totalAwareBucketFilter != null）
 * </ol>
 *
 * <p>示例：
 * <pre>{@code
 * // 创建仅读取真实桶的过滤器
 * BucketFilter filter = BucketFilter.create(
 *     true,     // onlyReadRealBuckets
 *     null,     // specifiedBucket
 *     null,     // bucketFilter
 *     null      // totalAwareBucketFilter
 * );
 *
 * // 创建指定桶号的过滤器
 * BucketFilter filter = BucketFilter.create(
 *     false,    // onlyReadRealBuckets
 *     5,        // specifiedBucket = 5
 *     null,
 *     null
 * );
 *
 * // 测试桶是否通过过滤
 * boolean pass = filter.test(5, 10);  // bucket=5, totalBucket=10
 * }</pre>
 */
public class BucketFilter {

    /** 仅读取真实桶（bucket >= 0） */
    private final boolean onlyReadRealBuckets;

    /** 指定桶号（精确匹配，可为 null） */
    private final @Nullable Integer specifiedBucket;

    /** 桶过滤器（自定义过滤逻辑，可为 null） */
    private final @Nullable Filter<Integer> bucketFilter;

    /** 总桶数感知的过滤器（可为 null） */
    private final @Nullable BiFilter<Integer, Integer> totalAwareBucketFilter;

    /**
     * 构造 BucketFilter
     *
     * @param onlyReadRealBuckets 仅读取真实桶
     * @param specifiedBucket 指定桶号（可为 null）
     * @param bucketFilter 桶过滤器（可为 null）
     * @param totalAwareBucketFilter 总桶数感知的过滤器（可为 null）
     */
    public BucketFilter(
            boolean onlyReadRealBuckets,
            @Nullable Integer specifiedBucket,
            @Nullable Filter<Integer> bucketFilter,
            @Nullable BiFilter<Integer, Integer> totalAwareBucketFilter) {
        this.onlyReadRealBuckets = onlyReadRealBuckets;
        this.specifiedBucket = specifiedBucket;
        this.bucketFilter = bucketFilter;
        this.totalAwareBucketFilter = totalAwareBucketFilter;
    }

    /**
     * 创建 BucketFilter（工厂方法）
     *
     * <p>如果所有过滤条件都为默认值，则返回 null（表示不过滤）。
     *
     * @param onlyReadRealBuckets 仅读取真实桶
     * @param specifiedBucket 指定桶号（可为 null）
     * @param bucketFilter 桶过滤器（可为 null）
     * @param totalAwareBucketFilter 总桶数感知的过滤器（可为 null）
     * @return BucketFilter 实例（如果不需要过滤则返回 null）
     */
    public static @Nullable BucketFilter create(
            boolean onlyReadRealBuckets,
            @Nullable Integer specifiedBucket,
            @Nullable Filter<Integer> bucketFilter,
            @Nullable BiFilter<Integer, Integer> totalAwareBucketFilter) {
        if (!onlyReadRealBuckets
                && specifiedBucket == null
                && bucketFilter == null
                && totalAwareBucketFilter == null) {
            return null;
        }

        return new BucketFilter(
                onlyReadRealBuckets, specifiedBucket, bucketFilter, totalAwareBucketFilter);
    }

    /**
     * 获取指定桶号
     *
     * @return 指定桶号（可为 null）
     */
    @Nullable
    public Integer specifiedBucket() {
        return specifiedBucket;
    }

    /**
     * 测试桶是否通过过滤
     *
     * <p>过滤逻辑：
     * <ol>
     *   <li>如果 onlyReadRealBuckets = true，则过滤虚拟桶（bucket < 0）
     *   <li>如果 specifiedBucket != null，则仅保留指定桶
     *   <li>如果 bucketFilter != null，则应用桶过滤器
     *   <li>如果 totalAwareBucketFilter != null，则应用总桶数感知过滤器
     * </ol>
     *
     * @param bucket 桶号
     * @param totalBucket 总桶数
     * @return true 表示通过过���，false 表示被过滤
     */
    public boolean test(int bucket, int totalBucket) {
        // 1. 检查是否为虚拟桶
        if (onlyReadRealBuckets && bucket < 0) {
            return false;
        }
        // 2. 检查是否匹配指定桶号
        if (specifiedBucket != null && bucket != specifiedBucket) {
            return false;
        }
        // 3. 检查是否通过桶过滤器
        if (bucketFilter != null && !bucketFilter.test(bucket)) {
            return false;
        }
        // 4. 检查是否通过总桶数感知过滤器
        return totalAwareBucketFilter == null || totalAwareBucketFilter.test(bucket, totalBucket);
    }
}
