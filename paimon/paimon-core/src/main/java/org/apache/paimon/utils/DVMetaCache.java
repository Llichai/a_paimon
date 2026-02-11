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

package org.apache.paimon.utils;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.source.DeletionFile;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;

/**
 * 删除向量元数据缓存
 *
 * <p>DVMetaCache 用于缓存删除向量（Deletion Vector）的元数据，提高删除向量的读取性能。
 *
 * <p>核心功能：
 * <ul>
 *   <li>元数据缓存：{@link #put} - 缓存删除向量元数据映射
 *   <li>读取缓存：{@link #read} - 读取缓存的删除向量元数据
 *   <li>Bucket 级别：按分区和 Bucket 粒度缓存
 * </ul>
 *
 * <p>删除向量（Deletion Vector）：
 * <ul>
 *   <li>DV 是一种标记已删除行的数据结构
 *   <li>避免物理删除数据，提高删除性能
 *   <li>DeletionFile 记录哪些行被删除
 * </ul>
 *
 * <p>缓存键结构：
 * <pre>
 * DVMetaCacheKey:
 *   - manifestPath: Manifest 文件路径
 *   - partition: 分区行数据（BinaryRow）
 *   - bucket: Bucket 编号
 * </pre>
 *
 * <p>缓存值：
 * <ul>
 *   <li>Map<String, DeletionFile>：文件名到删除文件的映射
 *   <li>String：数据文件名
 *   <li>DeletionFile：对应的删除向量文件
 * </ul>
 *
 * <p>缓存策略：
 * <ul>
 *   <li>权重计算：权重 = Map 大小 + 1
 *   <li>最大权重：限制缓存的总条目数
 *   <li>软引用：使用 softValues()，内存不足时可回收
 *   <li>LRU 淘汰：自动淘汰最少使用的缓存项
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>读取优化：缓存频繁访问的删除向量元数据
 *   <li>扫描加速：在表扫描时快速获取删除信息
 *   <li>内存控制：使用软引用避免 OOM
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建 DV 元数据缓存
 * DVMetaCache cache = new DVMetaCache(10000);  // 最多缓存 10000 个 Map
 *
 * // 读取缓存
 * Path manifestPath = new Path("/path/to/manifest");
 * BinaryRow partition = ...;
 * int bucket = 0;
 * Map<String, DeletionFile> dvFiles = cache.read(manifestPath, partition, bucket);
 *
 * if (dvFiles != null) {
 *     // 缓存命中
 *     DeletionFile dvFile = dvFiles.get("data-file-123.orc");
 *     if (dvFile != null) {
 *         // 使用删除向量过滤已删除的行
 *     }
 * } else {
 *     // 缓存未命中，从 Manifest 读取
 *     Map<String, DeletionFile> dvFilesMap = readFromManifest(manifestPath, partition, bucket);
 *     cache.put(manifestPath, partition, bucket, dvFilesMap);
 * }
 * }</pre>
 *
 * @see DeletionFile
 */
public class DVMetaCache {

    /** Caffeine 缓存实例 */
    private final Cache<DVMetaCacheKey, Map<String, DeletionFile>> cache;

    /**
     * 构造 DV 元数据缓存
     *
     * @param maxValueNumber 最大缓存值数量（权重总和）
     */
    public DVMetaCache(long maxValueNumber) {
        this.cache =
                Caffeine.newBuilder()
                        .weigher(DVMetaCache::weigh)  // 自定义权重计算
                        .maximumWeight(maxValueNumber)  // 最大权重
                        .softValues()  // 软引用值
                        .executor(Runnable::run)  // 同步执行
                        .build();
    }

    /**
     * 计算缓存项的权重
     *
     * <p>权重 = Map 大小 + 1
     *
     * @param cacheKey 缓存键
     * @param cacheValue 缓存值（删除文件映射）
     * @return 权重值
     */
    private static int weigh(DVMetaCacheKey cacheKey, Map<String, DeletionFile> cacheValue) {
        return cacheValue.size() + 1;
    }

    /**
     * 读取缓存的删除向量元数据
     *
     * @param manifestPath Manifest 文件路径
     * @param partition 分区行数据
     * @param bucket Bucket 编号
     * @return 删除文件映射，如果不存在返回 null
     */
    @Nullable
    public Map<String, DeletionFile> read(Path manifestPath, BinaryRow partition, int bucket) {
        DVMetaCacheKey cacheKey = new DVMetaCacheKey(manifestPath, partition, bucket);
        return this.cache.getIfPresent(cacheKey);
    }

    /**
     * 缓存删除向量元数据
     *
     * @param path Manifest 文件路径
     * @param partition 分区行数据
     * @param bucket Bucket 编号
     * @param dvFilesMap 删除文件映射
     */
    public void put(
            Path path, BinaryRow partition, int bucket, Map<String, DeletionFile> dvFilesMap) {
        DVMetaCacheKey key = new DVMetaCacheKey(path, partition, bucket);
        this.cache.put(key, dvFilesMap);
    }

    /**
     * Bucket 级别的删除向量元数据缓存键
     *
     * <p>缓存键由三部分组成：
     * <ul>
     *   <li>manifestPath：Manifest 文件路径
     *   <li>row：分区行数据
     *   <li>bucket：Bucket 编号
     * </ul>
     */
    private static final class DVMetaCacheKey {

        /** Manifest 文件路径 */
        private final Path manifestPath;
        /** 分区行数据 */
        private final BinaryRow row;
        /** Bucket 编号 */
        private final int bucket;

        /**
         * 构造缓存键
         *
         * @param manifestPath Manifest 文件路径
         * @param row 分区行数据
         * @param bucket Bucket 编号
         */
        public DVMetaCacheKey(Path manifestPath, BinaryRow row, int bucket) {
            this.manifestPath = manifestPath;
            this.row = row;
            this.bucket = bucket;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DVMetaCacheKey)) {
                return false;
            }
            DVMetaCacheKey that = (DVMetaCacheKey) o;
            return bucket == that.bucket
                    && Objects.equals(manifestPath, that.manifestPath)
                    && Objects.equals(row, that.row);
        }

        @Override
        public int hashCode() {
            return Objects.hash(manifestPath, row, bucket);
        }
    }
}
