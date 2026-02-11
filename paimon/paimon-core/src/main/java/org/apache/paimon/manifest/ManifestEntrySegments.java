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
import org.apache.paimon.data.Segments;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manifest 条目分段
 *
 * <p>ManifestEntrySegments 是 {@link Segments} 的实现，基于分区和桶构建索引，加速后续查询。
 *
 * <p>核心字段：
 * <ul>
 *   <li>segments：RichSegments 列表（包含分区和桶信息的分段）
 *   <li>totalMemorySize：总内存大小
 *   <li>indexedSegments：索引 Map（partition -> bucket -> RichSegments 列表）
 * </ul>
 *
 * <p>索引结构：
 * <pre>
 * Map<BinaryRow, Map<Integer, List<RichSegments>>>
 *   ↑            ↑         ↑
 *   分区         桶号      分段列表
 * </pre>
 *
 * <p>工作原理：
 * <ol>
 *   <li>读取 Manifest 文件时，按分区和桶分段
 *   <li>为每个分段构建索引（partition + bucket -> segments）
 *   <li>查询时，根据 ManifestEntryFilters 快速定位分段
 *   <li>仅读取需要的分段，避免读取整个文件
 * </ol>
 *
 * <p>使用场景：
 * <ul>
 *   <li>分段读取：{@link ManifestEntryCache} 使用分段加速读取
 *   <li>查询优化：根据分区和桶快速定位数据
 *   <li>内存控制：仅加载需要的分段到内存
 * </ul>
 *
 * <p>性能优势：
 * <ul>
 *   <li>避免全文件扫描：仅读取匹配的分段
 *   <li>减少内存占用：按需加载分段
 *   <li>加速查询：通过索引快速定位
 * </ul>
 *
 * <p>示例：
 * <pre>{@code
 * // 创建分段
 * ManifestEntrySegments segments = new ManifestEntrySegments(richSegmentsList);
 *
 * // 获取总内存大小
 * long memorySize = segments.totalMemorySize();
 *
 * // 根据过滤器查询分段
 * List<RichSegments> matched = segments.query(manifestEntryFilters);
 * }</pre>
 */
public class ManifestEntrySegments implements Segments {

    /** RichSegments 列表（包含分区和桶信息） */
    private final List<RichSegments> segments;

    /** 总内存大小（字节） */
    private final long totalMemorySize;

    /** 索引 Map：partition -> bucket -> RichSegments 列表 */
    private final Map<BinaryRow, Map<Integer, List<RichSegments>>> indexedSegments;

    public ManifestEntrySegments(List<RichSegments> segments) {
        this.segments = segments;
        this.totalMemorySize =
                segments.stream()
                        .map(RichSegments::segments)
                        .mapToLong(Segments::totalMemorySize)
                        .sum();
        this.indexedSegments = new HashMap<>();
        for (RichSegments seg : segments) {
            indexedSegments
                    .computeIfAbsent(seg.partition(), k -> new HashMap<>())
                    .computeIfAbsent(seg.bucket(), k -> new ArrayList<>())
                    .add(seg);
        }
    }

    public List<RichSegments> segments() {
        return segments;
    }

    public Map<BinaryRow, Map<Integer, List<RichSegments>>> indexedSegments() {
        return indexedSegments;
    }

    @Override
    public long totalMemorySize() {
        return totalMemorySize;
    }

    /** Segments with partition and bucket information. */
    public static class RichSegments {

        private final BinaryRow partition;
        private final int bucket;
        private final int totalBucket;
        private final Segments segments;

        public RichSegments(BinaryRow partition, int bucket, int totalBucket, Segments segments) {
            this.partition = partition;
            this.bucket = bucket;
            this.totalBucket = totalBucket;
            this.segments = segments;
        }

        public BinaryRow partition() {
            return partition;
        }

        public int bucket() {
            return bucket;
        }

        public int totalBucket() {
            return totalBucket;
        }

        public Segments segments() {
            return segments;
        }
    }
}
