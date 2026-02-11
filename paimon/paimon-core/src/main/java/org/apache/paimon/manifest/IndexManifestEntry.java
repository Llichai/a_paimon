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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TinyIntType;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.SerializationUtils.newBytesType;
import static org.apache.paimon.utils.SerializationUtils.newStringType;

/**
 * 索引 Manifest 条目
 *
 * <p>IndexManifestEntry 是索引 Manifest 文件中的基本单元，记录了一个索引文件的元数据变更。
 *
 * <p>与数据 Manifest 的区别：
 * <ul>
 *   <li>数据 Manifest：管理数据文件（{@link DataFileMeta}）
 *   <li>索引 Manifest：管理索引文件（{@link IndexFileMeta}）
 * </ul>
 *
 * <p>四个核心字段：
 * <ul>
 *   <li>kind：文件类型（{@link FileKind}）- ADD=新增，DELETE=删除
 *   <li>partition：分区信息（{@link BinaryRow}）
 *   <li>bucket：桶号
 *   <li>indexFile：索引文件元数据（{@link IndexFileMeta}）
 * </ul>
 *
 * <p>索引文件类型：
 * <ul>
 *   <li>Deletion Vector：删除向量索引
 *   <li>Bloom Filter：布隆过滤器索引
 *   <li>Global Index：全局索引
 * </ul>
 *
 * <p>SCHEMA 包含 10 个字段：
 * <ul>
 *   <li>_KIND：文件类型
 *   <li>_PARTITION：分区信息
 *   <li>_BUCKET：桶号
 *   <li>_INDEX_TYPE：索引类型
 *   <li>_FILE_NAME：文件名
 *   <li>_FILE_SIZE：文件大小
 *   <li>_ROW_COUNT：行数
 *   <li>_DELETIONS_VECTORS_RANGES：删除向量范围
 *   <li>_EXTERNAL_PATH：外部路径
 *   <li>_GLOBAL_INDEX：全局索引元数据
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>写入阶段：{@link IndexWriter} 生成 IndexManifestEntry
 *   <li>提交阶段：{@link FileStoreCommit} 写入索引 Manifest
 *   <li>扫描阶段：{@link FileStoreScan} 读取索引 Manifest
 * </ul>
 *
 * <p>示例：
 * <pre>{@code
 * // 创建索引 Manifest 条目
 * IndexManifestEntry entry = new IndexManifestEntry(
 *     FileKind.ADD,              // 新增索引文件
 *     partition,                 // 分区信息
 *     bucket,                    // 桶号
 *     indexFileMeta              // 索引文件元数据
 * );
 *
 * // 转换为删除条目
 * IndexManifestEntry deleteEntry = entry.toDeleteEntry();
 * }</pre>
 *
 * @since 0.9.0
 */
@Public
public class IndexManifestEntry {

    /** IndexManifestEntry 的序列化 Schema，包含 10 个字段 */
    public static final RowType SCHEMA =
            new RowType(
                    false,
                    Arrays.asList(
                            new DataField(0, "_KIND", new TinyIntType(false)),
                            new DataField(1, "_PARTITION", newBytesType(false)),
                            new DataField(2, "_BUCKET", new IntType(false)),
                            new DataField(3, "_INDEX_TYPE", newStringType(false)),
                            new DataField(4, "_FILE_NAME", newStringType(false)),
                            new DataField(5, "_FILE_SIZE", new BigIntType(false)),
                            new DataField(6, "_ROW_COUNT", new BigIntType(false)),
                            new DataField(
                                    7,
                                    "_DELETIONS_VECTORS_RANGES",
                                    new ArrayType(true, DeletionVectorMeta.SCHEMA)),
                            new DataField(8, "_EXTERNAL_PATH", newStringType(true)),
                            new DataField(9, "_GLOBAL_INDEX", GlobalIndexMeta.SCHEMA)));

    private final FileKind kind;
    private final BinaryRow partition;
    private final int bucket;
    private final IndexFileMeta indexFile;

    /**
     * 构造 IndexManifestEntry
     *
     * @param kind 文件类型（ADD/DELETE）
     * @param partition 分区信息
     * @param bucket 桶号
     * @param indexFile 索引文件元数据
     */
    public IndexManifestEntry(
            FileKind kind, BinaryRow partition, int bucket, IndexFileMeta indexFile) {
        this.kind = kind;
        this.partition = partition;
        this.bucket = bucket;
        this.indexFile = indexFile;
    }

    /**
     * 转换为删除条目
     *
     * @return 删除类型的 IndexManifestEntry
     */
    public IndexManifestEntry toDeleteEntry() {
        checkArgument(kind == FileKind.ADD);
        return new IndexManifestEntry(FileKind.DELETE, partition, bucket, indexFile);
    }

    /** 获取文件类型 */
    public FileKind kind() {
        return kind;
    }

    /** 获取分区信息 */
    public BinaryRow partition() {
        return partition;
    }

    /** 获取桶号 */
    public int bucket() {
        return bucket;
    }

    /** 获取索引文件元数据 */
    public IndexFileMeta indexFile() {
        return indexFile;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexManifestEntry entry = (IndexManifestEntry) o;
        return bucket == entry.bucket
                && kind == entry.kind
                && Objects.equals(partition, entry.partition)
                && Objects.equals(indexFile, entry.indexFile);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, partition, bucket, indexFile);
    }

    @Override
    public String toString() {
        return "IndexManifestEntry{"
                + "kind="
                + kind
                + ", partition="
                + partition
                + ", bucket="
                + bucket
                + ", indexFile="
                + indexFile
                + '}';
    }
}
