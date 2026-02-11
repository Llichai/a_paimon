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
import org.apache.paimon.io.DataFileMeta;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * POJO 实现的 ManifestEntry
 *
 * <p>PojoManifestEntry 是 {@link ManifestEntry} 接口的标准 POJO 实现，使用简单的 Java 对象存储数据。
 *
 * <p>五个核心字段：
 * <ul>
 *   <li>kind：文件类型（{@link FileKind}）- ADD/DELETE
 *   <li>partition：分区信息（{@link BinaryRow}）- 对于非分区表是 0 列的行（不为 null）
 *   <li>bucket：桶号
 *   <li>totalBuckets：总桶数
 *   <li>file：数据文件元数据（{@link DataFileMeta}）
 * </ul>
 *
 * <p>实现特点：
 * <ul>
 *   <li>不可变对象：所有字段都是 final，通过创建新实例来修改
 *   <li>委托模式：大部分方法委托给 {@link DataFileMeta}
 *   <li>标识符：通过 identifier() 提供唯一标识
 *   <li>统计信息：支持复制时去除统计信息（copyWithoutStats）
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>提交阶段：{@link FileStoreCommit} 创建 PojoManifestEntry
 *   <li>序列化：{@link ManifestEntrySerializer} 序列化/反序列化
 *   <li>工厂方法：{@link ManifestEntry#create} 返回 PojoManifestEntry
 * </ul>
 *
 * <p>与其他实现的区别：
 * <ul>
 *   <li>{@link FilteredManifestEntry}：包装类，用于过滤
 *   <li>{@link SimpleFileEntry}：简化版本，仅包含部分字段
 * </ul>
 *
 * <p>示例：
 * <pre>{@code
 * // 创建新增 Entry
 * PojoManifestEntry entry = new PojoManifestEntry(
 *     FileKind.ADD,              // 新增文件
 *     BinaryRow.EMPTY_ROW,       // 非分区表
 *     0,                         // bucket 0
 *     1,                         // totalBuckets
 *     dataFileMeta               // 文件元数据
 * );
 *
 * // 分配序列号
 * PojoManifestEntry withSeq = entry.assignSequenceNumber(1000L, 2000L);
 *
 * // 去除统计信息（减小存储大小）
 * PojoManifestEntry withoutStats = entry.copyWithoutStats();
 * }</pre>
 */
public class PojoManifestEntry implements ManifestEntry {

    /** 文件类型（ADD/DELETE） */
    private final FileKind kind;

    /** 分区信息（对于非分区表是 0 列的行，不为 null） */
    private final BinaryRow partition;

    /** 桶号 */
    private final int bucket;

    /** 总桶数 */
    private final int totalBuckets;

    /** 数据文件元数据 */
    private final DataFileMeta file;

    /**
     * 构造 PojoManifestEntry
     *
     * @param kind 文件类型（ADD/DELETE）
     * @param partition 分区信息（非分区表使用 BinaryRow.EMPTY_ROW）
     * @param bucket 桶号
     * @param totalBuckets 总桶数
     * @param file 数据文件元数据
     */
    public PojoManifestEntry(
            FileKind kind, BinaryRow partition, int bucket, int totalBuckets, DataFileMeta file) {
        this.kind = kind;
        this.partition = partition;
        this.bucket = bucket;
        this.totalBuckets = totalBuckets;
        this.file = file;
    }

    /** 获取文件类型（ADD/DELETE） */
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

    /** 获取文件层级（委托给 DataFileMeta） */
    @Override
    public int level() {
        return file.level();
    }

    /** 获取文件名（委托给 DataFileMeta） */
    @Override
    public String fileName() {
        return file.fileName();
    }

    /** 获取外部路径（委托给 DataFileMeta） */
    @Nullable
    @Override
    public String externalPath() {
        return file.externalPath().orElse(null);
    }

    /** 获取最小键（委托给 DataFileMeta） */
    @Override
    public BinaryRow minKey() {
        return file.minKey();
    }

    /** 获取最大键（委托给 DataFileMeta） */
    @Override
    public BinaryRow maxKey() {
        return file.maxKey();
    }

    /** 获取额外文件列表（委托给 DataFileMeta） */
    @Override
    public List<String> extraFiles() {
        return file.extraFiles();
    }

    /** 获取行数（委托给 DataFileMeta） */
    @Override
    public long rowCount() {
        return file.rowCount();
    }

    /** 获取第一行 ID（委托给 DataFileMeta，用于 Row Tracking） */
    @Override
    public @Nullable Long firstRowId() {
        return file.firstRowId();
    }

    /** 获取总桶数 */
    @Override
    public int totalBuckets() {
        return totalBuckets;
    }

    /** 获取数据文件元数据 */
    @Override
    public DataFileMeta file() {
        return file;
    }

    /**
     * 获取唯一标识符
     *
     * <p>标识符包含：分区、桶号、层级、文件名、额外文件、嵌入式索引、外部路径。
     *
     * @return 标识符对象
     */
    @Override
    public Identifier identifier() {
        return new Identifier(
                partition,
                bucket,
                file.level(),
                file.fileName(),
                file.extraFiles(),
                file.embeddedIndex(),
                externalPath());
    }

    /**
     * 复制 ManifestEntry，但去除统计信息
     *
     * <p>用于减小存储大小，保留关键元数据。
     *
     * @return 新的 PojoManifestEntry（不包含统计信息）
     */
    @Override
    public PojoManifestEntry copyWithoutStats() {
        return new PojoManifestEntry(
                kind, partition, bucket, totalBuckets, file.copyWithoutStats());
    }

    /**
     * 分配序列号范围
     *
     * <p>序列号用于事务隔离和快照可见性控制。
     *
     * @param minSequenceNumber 最小序列号
     * @param maxSequenceNumber 最大序列号
     * @return 新的 PojoManifestEntry
     */
    @Override
    public PojoManifestEntry assignSequenceNumber(long minSequenceNumber, long maxSequenceNumber) {
        return new PojoManifestEntry(
                kind,
                partition,
                bucket,
                totalBuckets,
                file.assignSequenceNumber(minSequenceNumber, maxSequenceNumber));
    }

    /**
     * 分配第一行 ID（用于 Row Tracking）
     *
     * @param firstRowId 第一行 ID
     * @return 新的 PojoManifestEntry
     */
    @Override
    public PojoManifestEntry assignFirstRowId(long firstRowId) {
        return new PojoManifestEntry(
                kind, partition, bucket, totalBuckets, file.assignFirstRowId(firstRowId));
    }

    /**
     * 升级文件层级
     *
     * <p>用于 LSM 树的 Compaction 操作。
     *
     * @param newLevel 新层级
     * @return 新的 PojoManifestEntry
     */
    @Override
    public ManifestEntry upgrade(int newLevel) {
        return new PojoManifestEntry(kind, partition, bucket, totalBuckets, file.upgrade(newLevel));
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ManifestEntry)) {
            return false;
        }
        ManifestEntry that = (ManifestEntry) o;
        return Objects.equals(kind, that.kind())
                && Objects.equals(partition, that.partition())
                && bucket == that.bucket()
                && totalBuckets == that.totalBuckets()
                && Objects.equals(file, that.file());
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, partition, bucket, totalBuckets, file);
    }

    @Override
    public String toString() {
        return String.format("{%s, %s, %d, %d, %s}", kind, partition, bucket, totalBuckets, file);
    }
}
