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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.ObjectsCache;

import javax.annotation.Nullable;

/**
 * Manifest 条目过滤器
 *
 * <p>ManifestEntryFilters 封装了读取 {@link ManifestEntry} 时的多种过滤器。
 *
 * <p>四种过滤器：
 * <ul>
 *   <li>partitionFilter：分区谓词过滤器（{@link PartitionPredicate}）
 *   <li>bucketFilter：桶过滤器（{@link BucketFilter}）
 *   <li>readFilter：行级过滤器（{@link Filter<InternalRow>}）
 *   <li>readVFilter：ManifestEntry 级过滤器（{@link Filter<ManifestEntry>}）
 * </ul>
 *
 * <p>过滤顺序：
 * <ol>
 *   <li>分区裁剪：通过 partitionFilter 过滤分区
 *   <li>桶裁剪：通过 bucketFilter 过滤桶
 *   <li>行级过滤：通过 readFilter 在读取时过滤行（InternalRow）
 *   <li>条目过滤：通过 readVFilter 过滤 ManifestEntry
 * </ol>
 *
 * <p>使用场景：
 * <ul>
 *   <li>读取优化：{@link ManifestFile#read} 使用过滤器减少读取
 *   <li>扫描裁剪：{@link FileStoreScan} 根据查询条件过滤文件
 *   <li>缓存读取：{@link ManifestEntryCache} 使用过滤器
 * </ul>
 *
 * <p>与父类的关系：
 * <ul>
 *   <li>继承自 {@link ObjectsCache.Filters}
 *   <li>扩展了分区和桶过滤器
 * </ul>
 *
 * <p>示例：
 * <pre>{@code
 * // 创建过滤器
 * ManifestEntryFilters filters = new ManifestEntryFilters(
 *     partitionPredicate,  // 分区过滤
 *     bucketFilter,        // 桶过滤
 *     rowFilter,           // 行级过滤
 *     entryFilter          // 条目过滤
 * );
 *
 * // 在读取时应用过滤器
 * List<ManifestEntry> entries = manifestFile.read(
 *     fileName,
 *     fileSize,
 *     filters.partitionFilter,
 *     filters.bucketFilter,
 *     filters.readFilter,
 *     filters.readVFilter
 * );
 * }</pre>
 */
public class ManifestEntryFilters extends ObjectsCache.Filters<ManifestEntry> {

    /** 分区谓词过滤器（用于分区裁剪，可为 null） */
    public final @Nullable PartitionPredicate partitionFilter;

    /** 桶过滤器（用于桶裁剪，可为 null） */
    public final @Nullable BucketFilter bucketFilter;

    /**
     * 构造 ManifestEntryFilters
     *
     * @param partitionFilter 分区谓词过滤器（可为 null）
     * @param bucketFilter 桶过滤器（可为 null）
     * @param readFilter 行级过滤器（InternalRow）
     * @param readVFilter ManifestEntry 级过滤器
     */
    public ManifestEntryFilters(
            @Nullable PartitionPredicate partitionFilter,
            @Nullable BucketFilter bucketFilter,
            Filter<InternalRow> readFilter,
            Filter<ManifestEntry> readVFilter) {
        super(readFilter, readVFilter);
        this.partitionFilter = partitionFilter;
        this.bucketFilter = bucketFilter;
    }
}
