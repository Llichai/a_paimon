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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Segments;
import org.apache.paimon.data.SimpleCollectingOutputView;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataPagedOutputSerializer;
import org.apache.paimon.manifest.ManifestEntrySegments.RichSegments;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.partition.PartitionPredicate.MultiplePartitionPredicate;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BiFunctionWithIOE;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.FunctionWithIOException;
import org.apache.paimon.utils.ObjectSerializer;
import org.apache.paimon.utils.ObjectsCache;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.SimpleObjectsCache;
import org.apache.paimon.utils.Triple;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.paimon.manifest.ManifestEntrySerializer.bucketGetter;
import static org.apache.paimon.manifest.ManifestEntrySerializer.partitionGetter;
import static org.apache.paimon.manifest.ManifestEntrySerializer.totalBucketGetter;

/**
 * Manifest 条目缓存
 *
 * <p>ManifestEntryCache 将 {@link ManifestEntry} 记录缓存到 {@link SegmentsCache}。
 *
 * <p>与 {@link SimpleObjectsCache} 的区别：
 * <ul>
 *   <li>SimpleObjectsCache：简单缓存，顺序扫描
 *   <li>ManifestEntryCache：基于索引的快速查询
 * </ul>
 *
 * <p>核心特性：
 * <ul>
 *   <li>基于 {@link ManifestEntryFilters} 的快速查询
 *   <li>使用 {@link ManifestEntrySegments} 构建索引
 *   <li>支持分区和桶的快速定位
 *   <li>线程安全（@ThreadSafe）
 * </ul>
 *
 * <p>缓存策略：
 * <ul>
 *   <li>按分区和桶分段存储
 *   <li>查询时仅读取匹配的分段
 *   <li>LRU 淘汰策略
 * </ul>
 *
 * <p>工作流程：
 * <ol>
 *   <li>读取 Manifest 文件
 *   <li>按 (partition, bucket, totalBucket) 分组
 *   <li>为每组创建分段（DataPagedOutputSerializer）
 *   <li>构建 ManifestEntrySegments 索引
 *   <li>查询时根据过滤器快速定位分段
 * </ol>
 *
 * <p>使用场景：
 * <ul>
 *   <li>扫描优化：{@link FileStoreScan} 重复读取相同 Manifest
 *   <li>查询加速：多次查询相同分区/桶
 *   <li>Compaction：读取文件元数据
 * </ul>
 *
 * <p>示例：
 * <pre>{@code
 * // 创建缓存
 * ManifestEntryCache cache = new ManifestEntryCache(
 *     segmentsCache,
 *     serializer,
 *     formatType,
 *     fileSizeFunction,
 *     iteratorFunction
 * );
 *
 * // 读取（带缓存）
 * List<ManifestEntry> entries = cache.read(
 *     path,
 *     fileSize,
 *     filters
 * );
 * }</pre>
 */
@ThreadSafe
public class ManifestEntryCache extends ObjectsCache<Path, ManifestEntry, ManifestEntrySegments> {

    /**
     * 构造 ManifestEntryCache
     *
     * @param cache 底层 SegmentsCache
     * @param projectedSerializer ManifestEntry 序列化器
     * @param formatSchema Manifest 文件的 Schema
     * @param fileSizeFunction 获取文件大小的函数
     * @param reader 读取 Manifest 文件的函数
     */
    public ManifestEntryCache(
            SegmentsCache<Path> cache,
            ObjectSerializer<ManifestEntry> projectedSerializer,
            RowType formatSchema,
            FunctionWithIOException<Path, Long> fileSizeFunction,
            BiFunctionWithIOE<Path, Long, CloseableIterator<InternalRow>> reader) {
        super(cache, projectedSerializer, formatSchema, fileSizeFunction, reader);
    }

    /**
     * 创建 ManifestEntrySegments
     *
     * <p>按 (partition, bucket, totalBucket) 分组，为每组创建分段并构建索引。
     *
     * @param path Manifest 文件路径
     * @param fileSize 文件大小（可为 null）
     * @return ManifestEntrySegments 实例
     */
    @Override
    protected ManifestEntrySegments createSegments(Path path, @Nullable Long fileSize) {
        // 按 (partition, bucket, totalBucket) 分组
        Map<Triple<BinaryRow, Integer, Integer>, DataPagedOutputSerializer> segments =
                new HashMap<>();
        Function<InternalRow, BinaryRow> partitionGetter = partitionGetter();
        Function<InternalRow, Integer> bucketGetter = bucketGetter();
        Function<InternalRow, Integer> totalBucketGetter = totalBucketGetter();
        int pageSize = cache.pageSize();
        InternalRowSerializer formatSerializer = this.formatSerializer.get();
        Supplier<DataPagedOutputSerializer> outputSupplier =
                () -> new DataPagedOutputSerializer(formatSerializer, 2048, pageSize);
        try (CloseableIterator<InternalRow> iterator = reader.apply(path, fileSize)) {
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                BinaryRow partition = partitionGetter.apply(row);
                int bucket = bucketGetter.apply(row);
                int totalBucket = totalBucketGetter.apply(row);
                Triple<BinaryRow, Integer, Integer> key = Triple.of(partition, bucket, totalBucket);
                DataPagedOutputSerializer output =
                        segments.computeIfAbsent(key, k -> outputSupplier.get());
                output.write(row);
            }
            List<RichSegments> result = new ArrayList<>();
            for (Map.Entry<Triple<BinaryRow, Integer, Integer>, DataPagedOutputSerializer> entry :
                    segments.entrySet()) {
                Triple<BinaryRow, Integer, Integer> key = entry.getKey();
                SimpleCollectingOutputView view = entry.getValue().close();
                Segments seg =
                        Segments.create(view.fullSegments(), view.getCurrentPositionInSegment());
                result.add(new RichSegments(key.f0, key.f1, key.f2, seg));
            }
            return new ManifestEntrySegments(result);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected List<ManifestEntry> readFromSegments(
            ManifestEntrySegments manifestSegments, Filters<ManifestEntry> filters)
            throws IOException {
        PartitionPredicate partitionFilter = null;
        BucketFilter bucketFilter = null;
        if (filters instanceof ManifestEntryFilters) {
            partitionFilter = ((ManifestEntryFilters) filters).partitionFilter;
            bucketFilter = ((ManifestEntryFilters) filters).bucketFilter;
        }

        List<RichSegments> segments = manifestSegments.segments();

        // try to do fast filter first
        Optional<BinaryRow> partition = extractSinglePartition(partitionFilter);
        if (partition.isPresent()) {
            Map<Integer, List<RichSegments>> segMap =
                    manifestSegments.indexedSegments().get(partition.get());
            if (segMap == null) {
                return Collections.emptyList();
            }
            OptionalInt specifiedBucket = extractSpecifiedBucket(bucketFilter);
            if (specifiedBucket.isPresent()) {
                segments = segMap.get(specifiedBucket.getAsInt());
                if (segments == null) {
                    return Collections.emptyList();
                }
            } else {
                segments =
                        segMap.values().stream().flatMap(List::stream).collect(Collectors.toList());
            }
        }

        // do force loop filter
        List<Segments> segmentsList = new ArrayList<>();
        for (RichSegments richSegments : segments) {
            if (partitionFilter != null && !partitionFilter.test(richSegments.partition())) {
                continue;
            }
            if (bucketFilter != null
                    && !bucketFilter.test(richSegments.bucket(), richSegments.totalBucket())) {
                continue;
            }
            segmentsList.add(richSegments.segments());
        }

        // read manifest entries from segments with per record filter
        List<ManifestEntry> result = new ArrayList<>();
        InternalRowSerializer formatSerializer = this.formatSerializer.get();
        for (Segments subSegments : segmentsList) {
            result.addAll(
                    SimpleObjectsCache.readFromSegments(
                            formatSerializer, projectedSerializer, subSegments, filters));
        }
        return result;
    }

    private Optional<BinaryRow> extractSinglePartition(@Nullable PartitionPredicate predicate) {
        if (predicate instanceof MultiplePartitionPredicate) {
            return ((MultiplePartitionPredicate) predicate).extractSinglePartition();
        }
        return Optional.empty();
    }

    private OptionalInt extractSpecifiedBucket(@Nullable BucketFilter filter) {
        if (filter != null) {
            Integer specifiedBucket = filter.specifiedBucket();
            if (specifiedBucket != null) {
                return OptionalInt.of(specifiedBucket);
            }
        }
        return OptionalInt.empty();
    }
}
