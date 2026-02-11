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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.MultiSegments;
import org.apache.paimon.data.RandomAccessInputView;
import org.apache.paimon.data.Segments;
import org.apache.paimon.data.SimpleCollectingOutputView;
import org.apache.paimon.data.SingleSegments;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentSource;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 简单对象缓存
 *
 * <p>SimpleObjectsCache 是 {@link ObjectsCache} 的简单实现，用于将对象列表缓存到 {@link SegmentsCache}。
 *
 * <p>核心功能：
 * <ul>
 *   <li>对象缓存：使用紧凑序列化格式缓存对象列表
 *   <li>过滤支持：支持在读取时进行过滤
 *   <li>内存优化：使用 Segments 减少内存占用
 * </ul>
 *
 * <p>与 ObjectsCache 的关系：
 * <ul>
 *   <li>继承自 ObjectsCache，实现了抽象方法
 *   <li>使用 Segments（而非子类如 BytesSegments）作为存储格式
 *   <li>提供了简单直接的实现
 * </ul>
 *
 * <p>过滤机制：
 * <ul>
 *   <li>readFilter：在 InternalRow 级别过滤
 *   <li>readVFilter：在业务对象级别过滤
 *   <li>两级过滤可以提高效率
 * </ul>
 *
 * <p>序列化流程：
 * <pre>
 * 写入：
 *   对象列表 → InternalRow → 紧凑序列化 → Segments → 缓存
 *
 * 读取：
 *   缓存 → Segments → 反序列化 → InternalRow → 对象列表
 * </pre>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建缓存
 * SegmentsCache<Path> segmentsCache = SegmentsCache.create(...);
 * SimpleObjectsCache<Path, MyObject> cache = new SimpleObjectsCache<>(
 *     segmentsCache,
 *     objectSerializer,
 *     formatSchema,
 *     path -> fileIO.getFileSize(path),
 *     (path, size) -> fileIO.readObjects(path)
 * );
 *
 * // 读取并缓存
 * Path filePath = new Path("/path/to/file");
 * List<MyObject> objects = cache.read(
 *     filePath,
 *     null,  // 文件大小（null 表示自动获取）
 *     row -> true,  // InternalRow 过滤器
 *     obj -> obj.isValid()  // 对象过滤器
 * );
 * }</pre>
 *
 * @param <K> 缓存键类型
 * @param <V> 对象类型
 * @see ObjectsCache
 * @see SegmentsCache
 */
@ThreadSafe
public class SimpleObjectsCache<K, V> extends ObjectsCache<K, V, Segments> {

    public SimpleObjectsCache(
            SegmentsCache<K> cache,
            ObjectSerializer<V> projectedSerializer,
            RowType formatSchema,
            FunctionWithIOException<K, Long> fileSizeFunction,
            BiFunctionWithIOE<K, Long, CloseableIterator<InternalRow>> reader) {
        super(cache, projectedSerializer, formatSchema, fileSizeFunction, reader);
    }

    public List<V> read(
            K key, @Nullable Long fileSize, Filter<InternalRow> readFilter, Filter<V> readVFilter)
            throws IOException {
        return read(key, fileSize, new Filters<>(readFilter, readVFilter));
    }

    @Override
    protected List<V> readFromSegments(Segments segments, Filters<V> filters) throws IOException {
        return readFromSegments(formatSerializer.get(), projectedSerializer, segments, filters);
    }

    @Override
    protected Segments createSegments(K key, @Nullable Long fileSize) {
        InternalRowSerializer formatSerializer = this.formatSerializer.get();
        try (CloseableIterator<InternalRow> iterator = reader.apply(key, fileSize)) {
            ArrayList<MemorySegment> segments = new ArrayList<>();
            MemorySegmentSource segmentSource =
                    () -> MemorySegment.allocateHeapMemory(cache.pageSize());
            SimpleCollectingOutputView output =
                    new SimpleCollectingOutputView(segments, segmentSource, cache.pageSize());
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                formatSerializer.serializeToPages(row, output);
            }
            return Segments.create(segments, output.getCurrentPositionInSegment());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <V> List<V> readFromSegments(
            InternalRowSerializer formatSerializer,
            ObjectSerializer<V> projectedSerializer,
            Segments segments,
            Filters<V> filters)
            throws IOException {
        List<V> entries = new ArrayList<>();
        RandomAccessInputView view = createInputView(segments);
        BinaryRow binaryRow = new BinaryRow(formatSerializer.getArity());
        Filter<InternalRow> readFilter = filters.readFilter();
        Filter<V> readVFilter = filters.readVFilter();
        while (true) {
            try {
                formatSerializer.mapFromPages(binaryRow, view);
                if (readFilter.test(binaryRow)) {
                    V v = projectedSerializer.fromRow(binaryRow);
                    if (readVFilter.test(v)) {
                        entries.add(v);
                    }
                }
            } catch (EOFException e) {
                return entries;
            }
        }
    }

    private static RandomAccessInputView createInputView(Segments segments) {
        if (segments instanceof MultiSegments) {
            MultiSegments memorySegments = (MultiSegments) segments;
            return new RandomAccessInputView(
                    memorySegments.segments(),
                    memorySegments.pageSize(),
                    memorySegments.limitInLastSegment());
        } else if (segments instanceof SingleSegments) {
            SingleSegments singleSegments = (SingleSegments) segments;
            ArrayList<MemorySegment> array = new ArrayList<>();
            array.add(singleSegments.segment());
            return new RandomAccessInputView(
                    array, singleSegments.segment().size(), singleSegments.limit());
        } else {
            throw new IllegalArgumentException("Unsupported segment type: " + segments.getClass());
        }
    }
}
