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
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Manifest 文件元数据
 *
 * <p>ManifestFileMeta 描述了一个 Manifest 文件的元数据信息，存储在 {@link ManifestList} 中。
 *
 * <p>三层元数据结构：
 * <ul>
 *   <li>Snapshot：指向一个 ManifestList 文件
 *   <li>ManifestList：包含多个 ManifestFileMeta
 *   <li>ManifestFileMeta：描述一个 ManifestFile
 *   <li>ManifestFile：包含多个 ManifestEntry
 * </ul>
 *
 * <p>12 个核心字段（对应 SCHEMA）：
 * <ul>
 *   <li>fileName：Manifest 文件名
 *   <li>fileSize：文件大小（字节）
 *   <li>numAddedFiles：新增文件数量（FileKind.ADD）
 *   <li>numDeletedFiles：删除文件数量（FileKind.DELETE）
 *   <li>partitionStats：分区统计信息（{@link SimpleStats}）
 *   <li>schemaId：Schema ID
 *   <li>minBucket、maxBucket：最小/最大桶号（用于桶裁剪）
 *   <li>minLevel、maxLevel：最小/最大层级（用于层级裁剪）
 *   <li>minRowId、maxRowId：最小/最大行 ID（用于 Row Tracking）
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>提交阶段：{@link FileStoreCommit} 生成 ManifestFileMeta
 *   <li>扫描阶段：{@link FileStoreScan} 根据 ManifestFileMeta 裁剪文件
 *   <li>过期阶段：{@link SnapshotDeletion} 删除 ManifestFileMeta
 * </ul>
 *
 * <p>文件类型：
 * <ul>
 *   <li>Base Manifest：包含所有有效文件（numDeletedFiles = 0）
 *   <li>Delta Manifest：包含增量变更（numAddedFiles + numDeletedFiles > 0）
 *   <li>Changelog Manifest：包含 Changelog 文件
 * </ul>
 *
 * <p>示例：
 * <pre>{@code
 * ManifestFileMeta meta = new ManifestFileMeta(
 *     "manifest-abc123",         // fileName
 *     1024,                      // fileSize
 *     10,                        // numAddedFiles
 *     5,                         // numDeletedFiles
 *     partitionStats,            // partitionStats
 *     0L,                        // schemaId
 *     0,                         // minBucket
 *     3,                         // maxBucket
 *     0,                         // minLevel
 *     2,                         // maxLevel
 *     1000L,                     // minRowId
 *     2000L                      // maxRowId
 * );
 * }</pre>
 *
 * @since 0.9.0
 */
@Public
public class ManifestFileMeta {

    /** ManifestFileMeta 的序列化 Schema，包含 12 个字段 */
    public static final RowType SCHEMA =
            new RowType(
                    false,
                    Arrays.asList(
                            new DataField(
                                    0, "_FILE_NAME", new VarCharType(false, Integer.MAX_VALUE)),
                            new DataField(1, "_FILE_SIZE", new BigIntType(false)),
                            new DataField(2, "_NUM_ADDED_FILES", new BigIntType(false)),
                            new DataField(3, "_NUM_DELETED_FILES", new BigIntType(false)),
                            new DataField(4, "_PARTITION_STATS", SimpleStats.SCHEMA),
                            new DataField(5, "_SCHEMA_ID", new BigIntType(false)),
                            new DataField(6, "_MIN_BUCKET", new IntType(true)),
                            new DataField(7, "_MAX_BUCKET", new IntType(true)),
                            new DataField(8, "_MIN_LEVEL", new IntType(true)),
                            new DataField(9, "_MAX_LEVEL", new IntType(true)),
                            new DataField(10, "_MIN_ROW_ID", new BigIntType(true)),
                            new DataField(11, "_MAX_ROW_ID", new BigIntType(true))));

    private final String fileName;
    private final long fileSize;
    private final long numAddedFiles;
    private final long numDeletedFiles;
    private final SimpleStats partitionStats;
    private final long schemaId;
    private final @Nullable Integer minBucket;
    private final @Nullable Integer maxBucket;
    private final @Nullable Integer minLevel;
    private final @Nullable Integer maxLevel;
    private final @Nullable Long minRowId;
    private final @Nullable Long maxRowId;

    /**
     * 构造 ManifestFileMeta
     *
     * @param fileName Manifest 文件名
     * @param fileSize 文件大小（字节）
     * @param numAddedFiles 新增文件数量
     * @param numDeletedFiles 删除文件数量
     * @param partitionStats 分区统计信息
     * @param schemaId Schema ID
     * @param minBucket 最小桶号（可为 null）
     * @param maxBucket 最大桶号（可为 null）
     * @param minLevel 最小层级（可为 null）
     * @param maxLevel 最大层级（可为 null）
     * @param minRowId 最小行 ID（可为 null）
     * @param maxRowId 最大行 ID（可为 null）
     */
    public ManifestFileMeta(
            String fileName,
            long fileSize,
            long numAddedFiles,
            long numDeletedFiles,
            SimpleStats partitionStats,
            long schemaId,
            @Nullable Integer minBucket,
            @Nullable Integer maxBucket,
            @Nullable Integer minLevel,
            @Nullable Integer maxLevel,
            @Nullable Long minRowId,
            @Nullable Long maxRowId) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.numAddedFiles = numAddedFiles;
        this.numDeletedFiles = numDeletedFiles;
        this.partitionStats = partitionStats;
        this.schemaId = schemaId;
        this.minBucket = minBucket;
        this.maxBucket = maxBucket;
        this.minLevel = minLevel;
        this.maxLevel = maxLevel;
        this.minRowId = minRowId;
        this.maxRowId = maxRowId;
    }

    /** 获取 Manifest 文件名 */
    public String fileName() {
        return fileName;
    }

    /** 获取文件大小（字节） */
    public long fileSize() {
        return fileSize;
    }

    /** 获取新增文件数量（FileKind.ADD） */
    public long numAddedFiles() {
        return numAddedFiles;
    }

    /** 获取删除文件数量（FileKind.DELETE） */
    public long numDeletedFiles() {
        return numDeletedFiles;
    }

    /** 获取分区统计信息 */
    public SimpleStats partitionStats() {
        return partitionStats;
    }

    /** 获取 Schema ID */
    public long schemaId() {
        return schemaId;
    }

    /** 获取最小桶号（用于桶裁剪） */
    public @Nullable Integer minBucket() {
        return minBucket;
    }

    /** 获取最大桶号（用于桶裁剪） */
    public @Nullable Integer maxBucket() {
        return maxBucket;
    }

    /** 获取最小层级（用于层级裁剪） */
    public @Nullable Integer minLevel() {
        return minLevel;
    }

    /** 获取最大层级（用于层级裁剪） */
    public @Nullable Integer maxLevel() {
        return maxLevel;
    }

    /** 获取最小行 ID（用于 Row Tracking） */
    public @Nullable Long minRowId() {
        return minRowId;
    }

    /** 获取最大行 ID（用于 Row Tracking） */
    public @Nullable Long maxRowId() {
        return maxRowId;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ManifestFileMeta)) {
            return false;
        }
        ManifestFileMeta that = (ManifestFileMeta) o;
        return Objects.equals(fileName, that.fileName)
                && fileSize == that.fileSize
                && numAddedFiles == that.numAddedFiles
                && numDeletedFiles == that.numDeletedFiles
                && Objects.equals(partitionStats, that.partitionStats)
                && schemaId == that.schemaId
                && Objects.equals(minBucket, that.minBucket)
                && Objects.equals(maxBucket, that.maxBucket)
                && Objects.equals(minLevel, that.minLevel)
                && Objects.equals(maxLevel, that.maxLevel)
                && Objects.equals(minRowId, that.minRowId)
                && Objects.equals(maxRowId, that.maxRowId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                fileName,
                fileSize,
                numAddedFiles,
                numDeletedFiles,
                partitionStats,
                schemaId,
                minBucket,
                maxBucket,
                minLevel,
                maxLevel,
                minRowId,
                maxRowId);
    }

    @Override
    public String toString() {
        return String.format(
                "{%s, %d, %d, %d, %s, %d, %s, %s, %s, %s, %s, %s}",
                fileName,
                fileSize,
                numAddedFiles,
                numDeletedFiles,
                partitionStats,
                schemaId,
                minBucket,
                maxBucket,
                minLevel,
                maxLevel,
                minRowId,
                maxRowId);
    }

    // ----------------------- 序列化 -----------------------------

    /** 线程本地的序列化器，避免多线程竞争 */
    private static final ThreadLocal<ManifestFileMetaSerializer> SERIALIZER_THREAD_LOCAL =
            ThreadLocal.withInitial(ManifestFileMetaSerializer::new);

    /** 序列化为字节数组 */
    public byte[] toBytes() throws IOException {
        return SERIALIZER_THREAD_LOCAL.get().serializeToBytes(this);
    }

    /** 从字节数组反序列化 */
    public ManifestFileMeta fromBytes(byte[] bytes) throws IOException {
        return SERIALIZER_THREAD_LOCAL.get().deserializeFromBytes(bytes);
    }
}
