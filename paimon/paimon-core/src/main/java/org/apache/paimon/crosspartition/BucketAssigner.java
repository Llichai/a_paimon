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

package org.apache.paimon.crosspartition;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.utils.Filter;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * 桶分配器
 *
 * <p>为跨分区更新场景中的记录分配桶（bucket）。
 *
 * <p>核心功能：
 * <ul>
 *   <li>桶初始化：记录已存在的桶及其记录数量
 *   <li>桶分配：为新记录分配合适的桶
 *   <li>计数管理：维护每个桶的记录计数
 * </ul>
 *
 * <p>分配策略：
 * <ol>
 *   <li>优先复用已存在且未满的桶（记录数 < maxCount）
 *   <li>如果所有桶已满，创建新桶（从 0 开始递增）
 * </ol>
 *
 * <p>使用场景：
 * <ul>
 *   <li>跨分区更新：主键相同但分区不同的记录需要分配到同一桶
 *   <li>动态桶：根据数据量动态创建桶，避免数据倾斜
 * </ul>
 *
 * <p>数据结构：
 * <pre>
 * stats: Map&lt;分区, TreeMap&lt;桶号, 记录数&gt;&gt;
 *
 * 示例：
 * 分区A -> {0 -> 100, 1 -> 50, 3 -> 200}
 * 分区B -> {0 -> 150, 2 -> 80}
 * </pre>
 *
 * <p>线程安全：此类非线程安全，需要外部同步。
 */
public class BucketAssigner {

    /** 分区 -> (桶号 -> 记录数) 映射，TreeMap 保证桶号有序 */
    private final Map<BinaryRow, TreeMap<Integer, Integer>> stats = new HashMap<>();

    /**
     * 初始化桶的记录计数
     *
     * <p>在启动时从已有数据中统计每个桶的记录数。
     *
     * @param part 分区
     * @param bucket 桶号
     */
    public void bootstrapBucket(BinaryRow part, int bucket) {
        TreeMap<Integer, Integer> bucketMap = bucketMap(part);
        Integer count = bucketMap.get(bucket);
        if (count == null) {
            count = 0;
        }
        bucketMap.put(bucket, count + 1);
    }

    /**
     * 为记录分配桶
     *
     * <p>分配算法：
     * <ol>
     *   <li>遍历已存在的桶（按桶号有序）
     *   <li>找到第一个满足 filter 且未满（count < maxCount）的桶
     *   <li>如果所有桶已满，创建新桶（从 0 开始递增找到第一个未使用的桶号）
     *   <li>更新桶的记录计数（+1）
     * </ol>
     *
     * @param part 分区
     * @param filter 桶过滤器（例如：只分配给当前任务负责的桶）
     * @param maxCount 每个桶的最大记录数
     * @return 分配的桶号
     */
    public int assignBucket(BinaryRow part, Filter<Integer> filter, int maxCount) {
        TreeMap<Integer, Integer> bucketMap = bucketMap(part);

        // 尝试复用已存在的桶
        for (Map.Entry<Integer, Integer> entry : bucketMap.entrySet()) {
            int bucket = entry.getKey();
            int count = entry.getValue();
            if (filter.test(bucket) && count < maxCount) {
                bucketMap.put(bucket, count + 1);
                return bucket;
            }
        }

        // 所有已存在的桶都已满，创建新桶
        for (int i = 0; ; i++) {
            if (filter.test(i) && !bucketMap.containsKey(i)) {
                bucketMap.put(i, 1);
                return i;
            }
        }
    }

    /**
     * 减少桶的记录计数
     *
     * <p>当记录被删除或移动到其他桶时调用。
     *
     * @param part 分区
     * @param bucket 桶号
     */
    public void decrement(BinaryRow part, int bucket) {
        bucketMap(part).compute(bucket, (k, v) -> v == null ? 0 : v - 1);
    }

    /**
     * 获取或创建分区的桶映射
     *
     * @param part 分区
     * @return 桶号 -> 记录数 的映射
     */
    private TreeMap<Integer, Integer> bucketMap(BinaryRow part) {
        TreeMap<Integer, Integer> map = stats.get(part);
        if (map == null) {
            map = new TreeMap<>();
            // 复制分区 key，避免外部修改
            stats.put(part.copy(), map);
        }
        return map;
    }
}
