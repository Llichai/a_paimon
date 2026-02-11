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
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.table.BucketMode;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.index.HashIndexFile.HASH_INDEX;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * 索引 Manifest 文件处理器
 *
 * <p>IndexManifestFileHandler 负责管理索引 Manifest 文件，包括合并、清理和写入索引条目。
 *
 * <p>核心字段：
 * <ul>
 *   <li>indexManifestFile：索引 Manifest 文件读写器
 *   <li>bucketMode：桶模式（用于决定合并策略）
 * </ul>
 *
 * <p>三种合并器（Combiner）：
 * <ul>
 *   <li>{@link GlobalFileNameCombiner}：全局文件名合并器（用于普通索引）
 *   <li>{@link GlobalCombiner}：全局合并器（用于 Bucket-Unaware 的 DV 索引）
 *   <li>{@link PartitionBucketCombiner}：分区桶合并器（用于 Bucket-Aware 的索引）
 * </ul>
 *
 * <p>索引类型：
 * <ul>
 *   <li>DELETION_VECTORS_INDEX：删除向量索引
 *   <li>HASH_INDEX：哈希索引（全局索引）
 *   <li>其他索引类型
 * </ul>
 *
 * <p>合并策略：
 * <pre>
 * 索引类型                     桶模式              合并器
 * ────────────────────────────────────────────────────────
 * DELETION_VECTORS_INDEX   BUCKET_UNAWARE    GlobalCombiner
 * DELETION_VECTORS_INDEX   BUCKET_AWARE      PartitionBucketCombiner
 * HASH_INDEX               任意               PartitionBucketCombiner
 * 其他索引                  任意               GlobalFileNameCombiner
 * </pre>
 *
 * <p>工作流程：
 * <ol>
 *   <li>读取前一个索引 Manifest（如果存在）
 *   <li>按索引类型分组（separateIndexEntries）
 *   <li>为每种索引类型选择合并器（getIndexManifestFileCombine）
 *   <li>合并前一个和新的索引条目
 *   <li>写入新的索引 Manifest 文件
 * </ol>
 *
 * <p>使用场景：
 * <ul>
 *   <li>提交阶段：{@link FileStoreCommit} 写入索引 Manifest
 *   <li>索引更新：更新删除向量索引
 *   <li>索引清理：删除过期的索引文件
 * </ul>
 *
 * <p>示例：
 * <pre>{@code
 * // 创建处理器
 * IndexManifestFileHandler handler = new IndexManifestFileHandler(
 *     indexManifestFile,
 *     bucketMode
 * );
 *
 * // 写入新索引文件
 * String newIndexManifest = handler.write(
 *     previousIndexManifest,  // 前一个索引 Manifest
 *     newIndexFiles           // 新的索引条目列表
 * );
 * }</pre>
 */
public class IndexManifestFileHandler {

    /** 索引 Manifest 文件读写器 */
    private final IndexManifestFile indexManifestFile;

    /** 桶模式（用于决定合并策略） */
    private final BucketMode bucketMode;

    /**
     * 构造 IndexManifestFileHandler
     *
     * @param indexManifestFile 索引 Manifest 文件读写器
     * @param bucketMode 桶模式
     */
    IndexManifestFileHandler(IndexManifestFile indexManifestFile, BucketMode bucketMode) {
        this.indexManifestFile = indexManifestFile;
        this.bucketMode = bucketMode;
    }

    /**
     * 写入新的索引 Manifest 文件
     *
     * <p>合并前一个索引 Manifest 和新的索引条目，写入新的索引 Manifest 文件。
     *
     * @param previousIndexManifest 前一个索引 Manifest 文件名（可为 null）
     * @param newIndexFiles 新的索引条目列表
     * @return 新的索引 Manifest 文件名
     */
    String write(@Nullable String previousIndexManifest, List<IndexManifestEntry> newIndexFiles) {
        // 读取前一个索引 Manifest（如果存在）
        List<IndexManifestEntry> entries =
                previousIndexManifest == null
                        ? new ArrayList<>()
                        : indexManifestFile.read(previousIndexManifest);
        for (IndexManifestEntry entry : entries) {
            checkArgument(entry.kind() == FileKind.ADD);
        }

        // 按索引类型分组
        Map<String, List<IndexManifestEntry>> previous = separateIndexEntries(entries);
        Map<String, List<IndexManifestEntry>> current = separateIndexEntries(newIndexFiles);

        // 合并索引条目
        List<IndexManifestEntry> indexEntries = new ArrayList<>();
        Set<String> indexes = new HashSet<>();
        indexes.addAll(previous.keySet());
        indexes.addAll(current.keySet());
        for (String indexName : indexes) {
            indexEntries.addAll(
                    getIndexManifestFileCombine(indexName)
                            .combine(
                                    previous.getOrDefault(indexName, Collections.emptyList()),
                                    current.getOrDefault(indexName, Collections.emptyList())));
        }

        // 写入新的索引 Manifest 文件
        return indexManifestFile.writeWithoutRolling(indexEntries);
    }

    /**
     * 按索引类型分组索引条目
     *
     * @param indexFiles 索引条目列表
     * @return Map<索引类型, 索引条目列表>
     */
    private Map<String, List<IndexManifestEntry>> separateIndexEntries(
            List<IndexManifestEntry> indexFiles) {
        Map<String, List<IndexManifestEntry>> result = new HashMap<>();

        for (IndexManifestEntry entry : indexFiles) {
            String indexType = entry.indexFile().indexType();
            result.computeIfAbsent(indexType, k -> new ArrayList<>()).add(entry);
        }
        return result;
    }

    /**
     * 根据索引类型选择合并器
     *
     * <p>合并策略：
     * <ul>
     *   <li>DELETION_VECTORS_INDEX + BUCKET_UNAWARE → GlobalCombiner
     *   <li>DELETION_VECTORS_INDEX + BUCKET_AWARE → PartitionBucketCombiner
     *   <li>HASH_INDEX → PartitionBucketCombiner
     *   <li>其他索引 → GlobalFileNameCombiner
     * </ul>
     *
     * @param indexType 索引类型
     * @return 索引 Manifest 合并器
     */
    private IndexManifestFileCombiner getIndexManifestFileCombine(String indexType) {
        if (!DELETION_VECTORS_INDEX.equals(indexType) && !HASH_INDEX.equals(indexType)) {
            return new GlobalFileNameCombiner();
        }

        if (DELETION_VECTORS_INDEX.equals(indexType) && BucketMode.BUCKET_UNAWARE == bucketMode) {
            return new GlobalCombiner();
        } else {
            return new BucketedCombiner();
        }
    }

    interface IndexManifestFileCombiner {
        List<IndexManifestEntry> combine(
                List<IndexManifestEntry> prevIndexFiles, List<IndexManifestEntry> newIndexFiles);
    }

    /**
     * We combine the previous and new index files by the file name. This is only used for tables
     * without bucket.
     */
    static class GlobalCombiner implements IndexManifestFileCombiner {

        @Override
        public List<IndexManifestEntry> combine(
                List<IndexManifestEntry> prevIndexFiles, List<IndexManifestEntry> newIndexFiles) {
            Map<String, IndexManifestEntry> indexEntries = new HashMap<>();
            Set<String> dvDataFiles = new HashSet<>();
            for (IndexManifestEntry entry : prevIndexFiles) {
                indexEntries.put(entry.indexFile().fileName(), entry);
                LinkedHashMap<String, DeletionVectorMeta> dvRanges = entry.indexFile().dvRanges();
                if (dvRanges != null) {
                    dvDataFiles.addAll(dvRanges.keySet());
                }
            }

            for (IndexManifestEntry entry : newIndexFiles) {
                String fileName = entry.indexFile().fileName();
                LinkedHashMap<String, DeletionVectorMeta> dvRanges = entry.indexFile().dvRanges();
                if (entry.kind() == FileKind.ADD) {
                    checkState(
                            !indexEntries.containsKey(fileName),
                            "Trying to add file %s which is already added.",
                            fileName);
                    if (dvRanges != null) {
                        for (String dataFile : dvRanges.keySet()) {
                            checkState(
                                    !dvDataFiles.contains(dataFile),
                                    "Trying to add dv for data file %s which is already added.",
                                    dataFile);
                            dvDataFiles.add(dataFile);
                        }
                    }
                    indexEntries.put(fileName, entry);
                } else {
                    checkState(
                            indexEntries.containsKey(fileName),
                            "Trying to delete file %s which is not exists.",
                            fileName);
                    if (dvRanges != null) {
                        for (String dataFile : dvRanges.keySet()) {
                            checkState(
                                    dvDataFiles.contains(dataFile),
                                    "Trying to delete dv for data file %s which is not exists.",
                                    dataFile);
                            dvDataFiles.remove(dataFile);
                        }
                    }
                    indexEntries.remove(fileName);
                }
            }
            return new ArrayList<>(indexEntries.values());
        }
    }

    /** We combine the previous and new index files by {@link BucketIdentifier}. */
    static class BucketedCombiner implements IndexManifestFileCombiner {

        @Override
        public List<IndexManifestEntry> combine(
                List<IndexManifestEntry> prevIndexFiles, List<IndexManifestEntry> newIndexFiles) {
            Map<BucketIdentifier, IndexManifestEntry> indexEntries = new HashMap<>();
            for (IndexManifestEntry entry : prevIndexFiles) {
                indexEntries.put(identifier(entry), entry);
            }

            // The deleted entry is processed first to avoid overwriting a new entry.
            List<IndexManifestEntry> removed =
                    newIndexFiles.stream()
                            .filter(f -> f.kind() == FileKind.DELETE)
                            .collect(Collectors.toList());
            List<IndexManifestEntry> added =
                    newIndexFiles.stream()
                            .filter(f -> f.kind() == FileKind.ADD)
                            .collect(Collectors.toList());
            for (IndexManifestEntry entry : removed) {
                indexEntries.remove(identifier(entry));
            }
            for (IndexManifestEntry entry : added) {
                indexEntries.put(identifier(entry), entry);
            }
            return new ArrayList<>(indexEntries.values());
        }
    }

    /** We combine the previous and new index files by file name. */
    static class GlobalFileNameCombiner implements IndexManifestFileCombiner {

        @Override
        public List<IndexManifestEntry> combine(
                List<IndexManifestEntry> prevIndexFiles, List<IndexManifestEntry> newIndexFiles) {
            Map<String, IndexManifestEntry> indexEntries = new HashMap<>();
            for (IndexManifestEntry entry : prevIndexFiles) {
                indexEntries.put(entry.indexFile().fileName(), entry);
            }

            // The deleted entry is processed first to avoid overwriting a new entry.
            List<IndexManifestEntry> removed =
                    newIndexFiles.stream()
                            .filter(f -> f.kind() == FileKind.DELETE)
                            .collect(Collectors.toList());
            List<IndexManifestEntry> added =
                    newIndexFiles.stream()
                            .filter(f -> f.kind() == FileKind.ADD)
                            .collect(Collectors.toList());
            for (IndexManifestEntry entry : removed) {
                indexEntries.remove(entry.indexFile().fileName());
            }
            for (IndexManifestEntry entry : added) {
                indexEntries.put(entry.indexFile().fileName(), entry);
            }
            return new ArrayList<>(indexEntries.values());
        }
    }

    private static BucketIdentifier identifier(IndexManifestEntry indexManifestEntry) {
        return new BucketIdentifier(
                indexManifestEntry.partition(),
                indexManifestEntry.bucket(),
                indexManifestEntry.indexFile().indexType());
    }

    /** The {@link BucketIdentifier} of a {@link IndexFileMeta}. */
    private static class BucketIdentifier {

        public final BinaryRow partition;
        public final int bucket;
        public final String indexType;

        private Integer hash;

        private BucketIdentifier(BinaryRow partition, int bucket, String indexType) {
            this.partition = partition;
            this.bucket = bucket;
            this.indexType = indexType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BucketIdentifier that = (BucketIdentifier) o;
            return bucket == that.bucket
                    && Objects.equals(partition, that.partition)
                    && Objects.equals(indexType, that.indexType);
        }

        @Override
        public int hashCode() {
            if (hash == null) {
                hash = Objects.hash(partition, bucket, indexType);
            }
            return hash;
        }

        @Override
        public String toString() {
            return "BucketIdentifier{"
                    + "partition="
                    + partition
                    + ", bucket="
                    + bucket
                    + ", indexType='"
                    + indexType
                    + '\''
                    + '}';
        }
    }
}
