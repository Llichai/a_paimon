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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Segments;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.operation.metrics.CacheMetrics;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.List;

import static org.apache.paimon.utils.ObjectsFile.readFromIterator;

/**
 * 对象缓存抽象基类
 *
 * <p>ObjectsCache 将对象列表缓存到 {@link SegmentsCache} 中，
 * 使用紧凑的序列化格式存储，以减少内存占用。
 *
 * <p>核心功能：
 * <ul>
 *   <li>对象读取：{@link #read(Object, Long, Filters)} - 从缓存或文件读取对象列表
 *   <li>缓存管理：自动缓存小文件，跳过大文件
 *   <li>序列化优化：使用紧凑格式序列化对象，减少内存
 *   <li>投影支持：只读取需要的字段，减少反序列化开销
 * </ul>
 *
 * <p>工作原理：
 * <pre>
 * 读取流程：
 *   1. 检查缓存是否存在
 *   2. 如果命中缓存：从 Segments 反序列化对象列表
 *   3. 如果未命中：
 *      a. 从文件读取数据
 *      b. 如果文件大小 <= 最大缓存大小：序列化并缓存
 *      c. 返回对象列表
 *
 * 序列化格式：
 *   原始对象 → InternalRow（投影） → 紧凑序列化 → Segments（内存块）
 * </pre>
 *
 * <p>缓存策略：
 * <ul>
 *   <li>小文件缓存：文件大小 <= maxElementSize 时缓存
 *   <li>大文件跳过：大文件直接从磁盘读取，不缓存
 *   <li>LRU 淘汰：SegmentsCache 使用 LRU 策略淘汰旧数据
 * </ul>
 *
 * <p>投影优化：
 * <ul>
 *   <li>projectedSerializer：投影后的序列化器（只包含需要的字段）
 *   <li>formatSerializer：完整格式的序列化器（包含所有字段）
 *   <li>投影可以减少内存占用和反序列化时间
 * </ul>
 *
 * <p>缓存指标：
 * <ul>
 *   <li>命中次数：成功从缓存读取的次数
 *   <li>未命中次数：需要从文件读取的次数
 *   <li>通过 {@link CacheMetrics} 收集统计信息
 * </ul>
 *
 * <p>线程安全：
 * <ul>
 *   <li>本类是线程安全的（@ThreadSafe）
 *   <li>使用 ThreadLocal 存储序列化器，避免竞争
 *   <li>SegmentsCache 本身是线程安全的
 * </ul>
 *
 * <p>子类实现：
 * <ul>
 *   <li>需要实现如何从 Segments 读取对象列表
 *   <li>需要实现如何将对象列表序列化为 Segments
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建对象缓存
 * ObjectsCache<Path, MyObject, BytesSegments> cache = new MyObjectsCache(
 *     segmentsCache,
 *     objectSerializer,
 *     formatSchema,
 *     path -> fileIO.getFileSize(path),
 *     (path, size) -> fileIO.readObjects(path)
 * );
 *
 * // 设置缓存指标
 * cache.withCacheMetrics(cacheMetrics);
 *
 * // 读取对象（自动处理缓存）
 * Path filePath = new Path("/path/to/file");
 * List<MyObject> objects = cache.read(filePath, null, filters);
 * // 如果缓存命中，直接从内存返回
 * // 如果缓存未命中，从文件读取并缓存
 * }</pre>
 *
 * @param <K> 缓存键类型（如文件路径）
 * @param <V> 对象类型
 * @param <S> Segments 类型（内存块类型）
 * @see SegmentsCache
 * @see ObjectSerializer
 */
@ThreadSafe
public abstract class ObjectsCache<K, V, S extends Segments> {

    /** Segments 缓存，存储序列化后的对象 */
    protected final SegmentsCache<K> cache;
    /** 投影后的对象序列化器（只包含需要的字段） */
    protected final ObjectSerializer<V> projectedSerializer;
    /** 完整格式的行序列化器（ThreadLocal 避免竞争） */
    protected final ThreadLocal<InternalRowSerializer> formatSerializer;
    /** 文件大小获取函数 */
    protected final FunctionWithIOException<K, Long> fileSizeFunction;
    /** 对象读取器（从文件读取对象迭代器） */
    protected final BiFunctionWithIOE<K, Long, CloseableIterator<InternalRow>> reader;

    /** 缓存指标收集器（可选） */
    @Nullable protected CacheMetrics cacheMetrics;

    protected ObjectsCache(
            SegmentsCache<K> cache,
            ObjectSerializer<V> projectedSerializer,
            RowType formatSchema,
            FunctionWithIOException<K, Long> fileSizeFunction,
            BiFunctionWithIOE<K, Long, CloseableIterator<InternalRow>> reader) {
        this.cache = cache;
        this.projectedSerializer = projectedSerializer;
        this.formatSerializer =
                ThreadLocal.withInitial(() -> new InternalRowSerializer(formatSchema));
        this.fileSizeFunction = fileSizeFunction;
        this.reader = reader;
    }

    public void withCacheMetrics(@Nullable CacheMetrics cacheMetrics) {
        this.cacheMetrics = cacheMetrics;
    }

    public List<V> read(K key, @Nullable Long fileSize, Filters<V> filters) throws IOException {
        @SuppressWarnings("unchecked")
        S segments = (S) cache.getIfPresents(key);
        if (segments != null) {
            if (cacheMetrics != null) {
                cacheMetrics.increaseHitObject();
            }
            return readFromSegments(segments, filters);
        } else {
            if (cacheMetrics != null) {
                cacheMetrics.increaseMissedObject();
            }
            if (fileSize == null) {
                fileSize = fileSizeFunction.apply(key);
            }
            if (fileSize <= cache.maxElementSize()) {
                segments = createSegments(key, fileSize);
                cache.put(key, segments);
                return readFromSegments(segments, filters);
            } else {
                return readFromIterator(
                        reader.apply(key, fileSize),
                        projectedSerializer,
                        filters.readFilter(),
                        filters.readVFilter());
            }
        }
    }

    protected abstract List<V> readFromSegments(S segments, Filters<V> filters) throws IOException;

    protected abstract S createSegments(K k, @Nullable Long fileSize);

    /** Filter context for reading. */
    public static class Filters<V> {

        private final Filter<InternalRow> readFilter;
        private final Filter<V> readVFilter;

        public Filters(Filter<InternalRow> readFilter, Filter<V> readVFilter) {
            this.readFilter = readFilter;
            this.readVFilter = readVFilter;
        }

        public Filter<InternalRow> readFilter() {
            return readFilter;
        }

        public Filter<V> readVFilter() {
            return readVFilter;
        }
    }
}
