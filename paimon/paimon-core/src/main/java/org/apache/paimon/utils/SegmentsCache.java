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

import org.apache.paimon.data.Segments;
import org.apache.paimon.options.MemorySize;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import javax.annotation.Nullable;

import static org.apache.paimon.CoreOptions.PAGE_SIZE;

/**
 * 分段缓存
 *
 * <p>SegmentsCache 用于缓存 {@link Segments} 对象（内存块），提供基于内存大小的 LRU 缓存。
 *
 * <p>核心功能：
 * <ul>
 *   <li>内存缓存：{@link #put(Object, Segments)} - 缓存 Segments 对象
 *   <li>读取缓存：{@link #getIfPresents(Object)} - 读取缓存的 Segments
 *   <li>内存控制：基于总内存大小限制缓存
 *   <li>大小限制：{@link #maxElementSize()} - 限制单个元素的最大大小
 * </ul>
 *
 * <p>Segments 是什么：
 * <ul>
 *   <li>内存块：Segments 表示连续的内存块（类似字节数组）
 *   <li>序列化数据：用于存储序列化后的对象
 *   <li>紧凑格式：使用紧凑的二进制格式存储数据
 * </ul>
 *
 * <p>缓存策略：
 * <ul>
 *   <li>权重计算：每个缓存项的权重 = 对象开销(1000字节) + Segments 内存大小
 *   <li>最大权重：缓存总权重不超过 maxMemorySize
 *   <li>LRU 淘汰：当缓存满时，淘汰最少使用的项
 *   <li>软引用：使用 softValues()，JVM 内存不足时可以回收
 * </ul>
 *
 * <p>大小限制：
 * <ul>
 *   <li>maxElementSize：单个元素的最大大小
 *   <li>超过限制的元素不会被缓存
 *   <li>避免大对象占用过多缓存空间
 * </ul>
 *
 * <p>分页大小：
 * <ul>
 *   <li>pageSize：内存分页大小（默认 64KB）
 *   <li>用于 Segments 的内存分配
 *   <li>影响内存使用效率
 * </ul>
 *
 * <p>缓存统计：
 * <ul>
 *   <li>{@link #estimatedSize()} - 缓存项数量
 *   <li>{@link #totalCacheBytes()} - 缓存总字节数
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>Manifest 缓存：缓存序列化的 Manifest 条目
 *   <li>统计信息缓存：缓存文件统计信息
 *   <li>元数据缓存：缓存其他元数据对象
 * </ul>
 *
 * <p>与 ObjectsCache 的关系：
 * <ul>
 *   <li>ObjectsCache 使用 SegmentsCache 存储序列化后的对象
 *   <li>SegmentsCache 提供底层的内存块缓存
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建分段缓存
 * SegmentsCache<Path> cache = SegmentsCache.create(
 *     MemorySize.ofMebiBytes(256),  // 最大缓存 256MB
 *     MemorySize.ofKibiBytes(512).getBytes()  // 单个元素最大 512KB
 * );
 *
 * // 缓存数据
 * Path filePath = new Path("/path/to/file");
 * Segments segments = ...; // 序列化后的数据
 * cache.put(filePath, segments);
 *
 * // 读取缓存
 * Segments cached = cache.getIfPresents(filePath);
 * if (cached != null) {
 *     // 缓存命中，使用缓存数据
 *     processSegments(cached);
 * } else {
 *     // 缓存未命中，从文件读取
 *     Segments data = readFromFile(filePath);
 *     cache.put(filePath, data);
 * }
 *
 * // 查看缓存统计
 * long cacheSize = cache.estimatedSize();  // 缓存项数量
 * long cacheBytes = cache.totalCacheBytes();  // 缓存总字节数
 * }</pre>
 *
 * @param <T> 缓存键的类型（如 Path、String 等）
 * @see Segments
 * @see ObjectsCache
 */
public class SegmentsCache<T> {

    /** 对象内存开销估算（每个缓存项的固定开销） */
    private static final int OBJECT_MEMORY_SIZE = 1000;

    /** 分页大小（内存分配单位） */
    private final int pageSize;
    /** Caffeine 缓存实例 */
    private final Cache<T, Segments> cache;
    /** 最大内存大小 */
    private final MemorySize maxMemorySize;
    /** 单个元素的最大大小 */
    private final long maxElementSize;

    /**
     * 构造分段缓存
     *
     * @param pageSize 分页大小
     * @param maxMemorySize 最大内存大小
     * @param maxElementSize 单个元素的最大大小
     */
    public SegmentsCache(int pageSize, MemorySize maxMemorySize, long maxElementSize) {
        this.pageSize = pageSize;
        this.cache =
                Caffeine.newBuilder()
                        .softValues()  // 使用软引用，内存不足时可回收
                        .weigher(this::weigh)  // 自定义权重计算
                        .maximumWeight(maxMemorySize.getBytes())  // 最大权重
                        .executor(Runnable::run)  // 同步执行
                        .build();
        this.maxMemorySize = maxMemorySize;
        this.maxElementSize = maxElementSize;
    }

    /**
     * 获取分页大小
     *
     * @return 分页大小（字节）
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * 获取最大内存大小
     *
     * @return 最大内存大小
     */
    public MemorySize maxMemorySize() {
        return maxMemorySize;
    }

    /**
     * 获取单个元素的最大大小
     *
     * @return 最大元素大小（字节）
     */
    public long maxElementSize() {
        return maxElementSize;
    }

    /**
     * 获取缓存的 Segments（如果存在）
     *
     * @param key 缓存键
     * @return 缓存的 Segments，如果不存在返回 null
     */
    @Nullable
    public Segments getIfPresents(T key) {
        return cache.getIfPresent(key);
    }

    /**
     * 缓存 Segments
     *
     * @param key 缓存键
     * @param segments Segments 对象
     */
    public void put(T key, Segments segments) {
        cache.put(key, segments);
    }

    /**
     * 计算缓存项的权重
     *
     * <p>权重 = 对象开销 + Segments 内存大小
     *
     * @param cacheKey 缓存键
     * @param segments Segments 对象
     * @return 权重（字节）
     */
    private int weigh(T cacheKey, Segments segments) {
        return (int) (OBJECT_MEMORY_SIZE + segments.totalMemorySize());
    }

    /**
     * 创建分段缓存（使用默认分页大小）
     *
     * @param maxMemorySize 最大内存大小
     * @param maxElementSize 单个元素的最大大小
     * @param <T> 缓存键类型
     * @return 分段缓存实例，如果 maxMemorySize 为 0 则返回 null
     */
    @Nullable
    public static <T> SegmentsCache<T> create(MemorySize maxMemorySize, long maxElementSize) {
        return create((int) PAGE_SIZE.defaultValue().getBytes(), maxMemorySize, maxElementSize);
    }

    /**
     * 创建分段缓存
     *
     * @param pageSize 分页大小
     * @param maxMemorySize 最大内存大小
     * @param maxElementSize 单个元素的最大大小
     * @param <T> 缓存键类型
     * @return 分段缓存实例，如果 maxMemorySize 为 0 则返回 null
     */
    @Nullable
    public static <T> SegmentsCache<T> create(
            int pageSize, MemorySize maxMemorySize, long maxElementSize) {
        if (maxMemorySize.getBytes() == 0) {
            return null;
        }

        return new SegmentsCache<>(pageSize, maxMemorySize, maxElementSize);
    }

    /**
     * 获取缓存项数量（估算值）
     *
     * @return 缓存项数量
     */
    public long estimatedSize() {
        return cache.estimatedSize();
    }

    /**
     * 获取缓存总字节数
     *
     * @return 缓存总字节数
     */
    public long totalCacheBytes() {
        return cache.asMap().entrySet().stream()
                .mapToLong(entry -> weigh(entry.getKey(), entry.getValue()))
                .sum();
    }
}
