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

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 简单文件条目
 *
 * <p>SimpleFileEntry 是 {@link FileEntry} 的简化实现，仅包含标识符和键范围，不包含完整的 {@link DataFileMeta}。
 *
 * <p>与 ManifestEntry 的区别：
 * <ul>
 *   <li>{@link ManifestEntry}：包含完整的 DataFileMeta（包含统计信息、Schema ID 等）
 *   <li>SimpleFileEntry：仅包含核心字段（标识符、键范围、行数）
 * </ul>
 *
 * <p>13个核心字段：
 * <ul>
 *   <li>kind：文件类型（ADD/DELETE）
 *   <li>partition：分区信息
 *   <li>bucket：桶号
 *   <li>totalBuckets：总桶数
 *   <li>level：文件层级
 *   <li>fileName：文件名
 *   <li>extraFiles：额外文件列表
 *   <li>embeddedIndex：嵌入式索引（可为 null）
 *   <li>minKey：最小键
 *   <li>maxKey：最大键
 *   <li>externalPath：外部路径（可为 null）
 *   <li>rowCount：行数
 *   <li>firstRowId：第一行 ID（可为 null）
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>内存优化：减少内存占用（不包含统计信息）
 *   <li>快速比较：仅比较标识符和键范围
 *   <li>中间结果：在处理过程中暂存文件信息
 *   <li>过期处理：{@link ExpireFileEntry} 使用 SimpleFileEntry
 * </ul>
 *
 * <p>转换方法：
 * <ul>
 *   <li>from(ManifestEntry)：从 ManifestEntry 转换
 *   <li>from(List&lt;ManifestEntry&gt;)：批量转换
 *   <li>toDelete()：转换为 DELETE 类型
 * </ul>
 *
 * <p>示例：
 * <pre>{@code
 * // 从 ManifestEntry 转换
 * SimpleFileEntry simple = SimpleFileEntry.from(manifestEntry);
 *
 * // 批量转换
 * List<SimpleFileEntry> simples = SimpleFileEntry.from(manifestEntries);
 *
 * // 转换为 DELETE 类型
 * SimpleFileEntry delete = simple.toDelete();
 *
 * // 获取非空的 firstRowId
 * long rowId = simple.nonNullFirstRowId();
 * }</pre>
 */
public class SimpleFileEntry implements FileEntry {

    private final FileKind kind;
    private final BinaryRow partition;
    private final int bucket;
    private final int totalBuckets;
    private final int level;
    private final String fileName;
    private final List<String> extraFiles;
    @Nullable private final byte[] embeddedIndex;
    private final BinaryRow minKey;
    private final BinaryRow maxKey;
    @Nullable private final String externalPath;
    private final long rowCount;
    @Nullable private final Long firstRowId;

    /**
     * 构造 SimpleFileEntry
     *
     * @param kind 文件类型
     * @param partition 分区信息
     * @param bucket 桶号
     * @param totalBuckets 总桶数
     * @param level 文件层级
     * @param fileName 文件名
     * @param extraFiles 额外文件列表
     * @param embeddedIndex 嵌入式索引（可为 null）
     * @param minKey 最小键
     * @param maxKey 最大键
     * @param externalPath 外部路径（可为 null）
     * @param rowCount 行数
     * @param firstRowId 第一行 ID（可为 null）
     */
    public SimpleFileEntry(
            FileKind kind,
            BinaryRow partition,
            int bucket,
            int totalBuckets,
            int level,
            String fileName,
            List<String> extraFiles,
            @Nullable byte[] embeddedIndex,
            BinaryRow minKey,
            BinaryRow maxKey,
            @Nullable String externalPath,
            long rowCount,
            @Nullable Long firstRowId) {
        this.kind = kind;
        this.partition = partition;
        this.bucket = bucket;
        this.totalBuckets = totalBuckets;
        this.level = level;
        this.fileName = fileName;
        this.extraFiles = extraFiles;
        this.embeddedIndex = embeddedIndex;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.externalPath = externalPath;
        this.rowCount = rowCount;
        this.firstRowId = firstRowId;
    }

    /**
     * 从 ManifestEntry 转换为 SimpleFileEntry
     *
     * <p>提取核心字段，丢弃统计信息。
     *
     * @param entry ManifestEntry 实例
     * @return SimpleFileEntry 实例
     */
    public static SimpleFileEntry from(ManifestEntry entry) {
        return new SimpleFileEntry(
                entry.kind(),
                entry.partition(),
                entry.bucket(),
                entry.totalBuckets(),
                entry.level(),
                entry.fileName(),
                entry.file().extraFiles(),
                entry.file().embeddedIndex(),
                entry.minKey(),
                entry.maxKey(),
                entry.externalPath(),
                entry.file().rowCount(),
                entry.firstRowId());
    }

    /**
     * 转换为 DELETE 类型的 SimpleFileEntry
     *
     * <p>保持其他字段不变，仅修改 kind 为 DELETE。
     *
     * @return DELETE 类型的 SimpleFileEntry
     */
    public SimpleFileEntry toDelete() {
        return new SimpleFileEntry(
                FileKind.DELETE,
                partition,
                bucket,
                totalBuckets,
                level,
                fileName,
                extraFiles,
                embeddedIndex,
                minKey,
                maxKey,
                externalPath,
                rowCount,
                firstRowId);
    }

    /**
     * 批量从 ManifestEntry 转换为 SimpleFileEntry
     *
     * @param entries ManifestEntry 列表
     * @return SimpleFileEntry 列表
     */
    public static List<SimpleFileEntry> from(List<ManifestEntry> entries) {
        return entries.stream().map(SimpleFileEntry::from).collect(Collectors.toList());
    }

    /** 获取文件类型 */
    @Override
    public FileKind kind() {
        return kind;
    }

    /** 获取分区信息 */
    @Override
    public BinaryRow partition() {
        return partition;
    }

    /** 获取桶号 */
    @Override
    public int bucket() {
        return bucket;
    }

    /** 获取总桶数 */
    @Override
    public int totalBuckets() {
        return totalBuckets;
    }

    /** 获取文件层级 */
    @Override
    public int level() {
        return level;
    }

    /** 获取文件名 */
    @Override
    public String fileName() {
        return fileName;
    }

    /** 获取嵌入式索引（可为 null） */
    @Nullable
    public byte[] embeddedIndex() {
        return embeddedIndex;
    }

    /** 获取外部路径（可为 null） */
    @Nullable
    @Override
    public String externalPath() {
        return externalPath;
    }

    /** 获取唯一标识符 */
    @Override
    public Identifier identifier() {
        return new Identifier(
                partition, bucket, level, fileName, extraFiles, embeddedIndex, externalPath);
    }

    /** 获取最小键 */
    @Override
    public BinaryRow minKey() {
        return minKey;
    }

    /** 获取最大键 */
    @Override
    public BinaryRow maxKey() {
        return maxKey;
    }

    /** 获取额外文件列表 */
    @Override
    public List<String> extraFiles() {
        return extraFiles;
    }

    /** 获取行数 */
    @Override
    public long rowCount() {
        return rowCount;
    }

    /** 获取第一行 ID（可为 null） */
    @Override
    public @Nullable Long firstRowId() {
        return firstRowId;
    }

    /**
     * 获取非空的第一行 ID
     *
     * <p>如果 firstRowId 为 null，则抛出异常。
     *
     * @return 第一行 ID
     * @throws IllegalArgumentException 如果 firstRowId 为 null
     */
    public long nonNullFirstRowId() {
        Long firstRowId = firstRowId();
        checkArgument(firstRowId != null, "First row id of '%s' should not be null.", fileName());
        return firstRowId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimpleFileEntry that = (SimpleFileEntry) o;
        return bucket == that.bucket
                && totalBuckets == that.totalBuckets
                && level == that.level
                && kind == that.kind
                && Objects.equals(partition, that.partition)
                && Objects.equals(fileName, that.fileName)
                && Objects.equals(extraFiles, that.extraFiles)
                && Objects.equals(minKey, that.minKey)
                && Objects.equals(maxKey, that.maxKey)
                && Objects.equals(externalPath, that.externalPath)
                && rowCount == that.rowCount
                && Objects.equals(firstRowId, that.firstRowId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                kind,
                partition,
                bucket,
                totalBuckets,
                level,
                fileName,
                extraFiles,
                minKey,
                maxKey,
                externalPath,
                rowCount,
                firstRowId);
    }

    @Override
    public String toString() {
        return "{"
                + "kind="
                + kind
                + ", partition="
                + partition
                + ", bucket="
                + bucket
                + ", totalBuckets="
                + totalBuckets
                + ", level="
                + level
                + ", fileName="
                + fileName
                + ", extraFiles="
                + extraFiles
                + ", minKey="
                + minKey
                + ", maxKey="
                + maxKey
                + ", externalPath="
                + externalPath
                + ", rowCount="
                + rowCount
                + ", firstRowId="
                + firstRowId
                + '}';
    }
}
