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

package org.apache.paimon.io.cache;

import org.apache.paimon.memory.MemorySegment;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.function.Function;

/**
 * Paimon 缓存接口,支持 Caffeine 和 Guava 两种缓存实现。
 *
 * <p>该接口定义了统一的缓存 API,用于缓存文件数据块({@link MemorySegment})到内存中,
 * 以加速重复访问。支持自动的 LRU 淘汰和大小限制。
 *
 * <h3>核心特性</h3>
 * <ul>
 *   <li><b>多实现支持:</b> 支持 Caffeine(推荐)和 Guava 两种高性能缓存库</li>
 *   <li><b>基于权重的淘汰:</b> 根据 MemorySegment 的大小进行缓存淘汰</li>
 *   <li><b>回调机制:</b> 支持缓存淘汰时的回调通知</li>
 *   <li><b>线程安全:</b> 底层实现都是线程安全的</li>
 *   <li><b>自动加载:</b> 支持 compute-if-absent 语义</li>
 * </ul>
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>文件块缓存:</b> 缓存文件的数据块,减少磁盘/网络I/O</li>
 *   <li><b>索引缓存:</b> 缓存文件索引,加速查找</li>
 *   <li><b>页面缓存:</b> 缓存固定大小的页面数据</li>
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 创建缓存
 * Cache cache = CacheBuilder.newBuilder(Cache.CacheType.CAFFEINE)
 *     .maximumWeight(MemorySize.ofMebiBytes(100))
 *     .build();
 *
 * // 获取或加载缓存项
 * CacheKey key = CacheKey.forPosition(path, 0, 4096, false);
 * CacheValue value = cache.get(key, k -> {
 *     // 缓存未命中,加载数据
 *     byte[] data = readFromFile(k);
 *     return new CacheValue(
 *         MemorySegment.wrap(data),
 *         key -> System.out.println("Evicted: " + key)
 *     );
 * });
 *
 * // 手动放入缓存
 * cache.put(key, value);
 *
 * // 使淘汰特定项
 * cache.invalidate(key);
 *
 * // 清空所有缓存
 * cache.invalidateAll();
 * }</pre>
 *
 * @see CacheManager
 * @see CacheKey
 * @see CacheBuilder
 */
public interface Cache {

    /**
     * 获取缓存项,如果不存在则使用 supplier 加载。
     *
     * <p>这是一个原子操作:如果多个线程同时请求相同的 key,
     * 只有一个线程会执行 supplier,其他线程等待结果。
     *
     * @param key 缓存键
     * @param supplier 缓存未命中时的加载函数
     * @return 缓存值,如果 supplier 返回 null 则可能为 null
     */
    @Nullable
    CacheValue get(CacheKey key, Function<CacheKey, CacheValue> supplier);

    /**
     * 将键值对放入缓存。
     *
     * <p>如果 key 已存在,会覆盖旧值并触发旧值的淘汰回调。
     *
     * @param key 缓存键
     * @param value 缓存值
     */
    void put(CacheKey key, CacheValue value);

    /**
     * 使指定的缓存项失效。
     *
     * <p>如果存在该项,会触发淘汰回调。
     *
     * @param key 要失效的缓存键
     */
    void invalidate(CacheKey key);

    /**
     * 使所有缓存项失效。
     *
     * <p>会触发所有项的淘汰回调。
     */
    void invalidateAll();

    /**
     * 返回缓存的映射视图。
     *
     * <p>该视图是线程安全的,但不反映后续的缓存变化。
     *
     * @return 缓存内容的快照
     */
    Map<CacheKey, CacheValue> asMap();

    /**
     * 缓存值,包含数据段和淘汰回调。
     *
     * <p>缓存值由两部分组成:
     * <ul>
     *   <li><b>segment:</b> 实际的数据内存段</li>
     *   <li><b>callback:</b> 当该项被淘汰时的回调</li>
     * </ul>
     *
     * <p><b>权重计算:</b> 缓存项的权重等于 segment.size(),
     * 用于基于内存大小的淘汰策略。
     */
    class CacheValue {

        /** 缓存的内存段,包含实际数据。 */
        final MemorySegment segment;

        /** 淘汰回调,当缓存项被移除时调用。 */
        final CacheCallback callback;

        /**
         * 创建缓存值。
         *
         * @param segment 内存段
         * @param callback 淘汰回调
         */
        CacheValue(MemorySegment segment, CacheCallback callback) {
            this.segment = segment;
            this.callback = callback;
        }
    }

    /**
     * 缓存类型枚举。
     *
     * <p><b>CAFFEINE</b>(推荐):
     * <ul>
     *   <li>性能更好,尤其是高并发场景</li>
     *   <li>更现代的 API 和实现</li>
     *   <li>更低的 GC 压力</li>
     * </ul>
     *
     * <p><b>GUAVA</b>:
     * <ul>
     *   <li>兼容性好,广泛使用</li>
     *   <li>功能完善且稳定</li>
     * </ul>
     */
    enum CacheType {
        /** Caffeine 缓存实现(推荐)。 */
        CAFFEINE,

        /** Guava 缓存实现。 */
        GUAVA;
    }
}
