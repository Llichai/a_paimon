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
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TinyIntType;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.utils.SerializationUtils.newBytesType;

/**
 * Manifest 条目接口
 *
 * <p>ManifestEntry 是 Manifest 文件中的基本单元，记录了一个数据文件的元数据变更。
 *
 * <p>三层元数据结构：
 * <ul>
 *   <li>Snapshot：指向一个 ManifestList
 *   <li>ManifestList：包含多个 ManifestFile
 *   <li>ManifestFile：包含多个 ManifestEntry
 *   <li>ManifestEntry：指向一个 {@link DataFileMeta}
 * </ul>
 *
 * <p>五个核心字段（对应 SCHEMA）：
 * <ul>
 *   <li>_KIND：文件类型（{@link FileKind}）- ADD=新增，DELETE=删除
 *   <li>_PARTITION：分区信息（{@link BinaryRow}）
 *   <li>_BUCKET：桶号（int）
 *   <li>_TOTAL_BUCKETS：总桶数（int）
 *   <li>_FILE：数据文件元数据（{@link DataFileMeta}）
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>提交阶段：{@link FileStoreCommit} 生成 ManifestEntry
 *   <li>扫描阶段：{@link FileStoreScan} 读取 ManifestEntry
 *   <li>过期阶段：{@link SnapshotDeletion} 删除 ManifestEntry
 *   <li>Compaction 阶段：{@link MergeTreeWriter} 生成 DELETE/ADD Entry
 * </ul>
 *
 * <p>实现类：
 * <ul>
 *   <li>{@link PojoManifestEntry}：POJO 实现
 *   <li>{@link FilteredManifestEntry}：过滤后的包装类
 * </ul>
 *
 * <p>示例：
 * <pre>{@code
 * // 创建新增 Entry
 * ManifestEntry entry = ManifestEntry.create(
 *     FileKind.ADD,              // 新增文件
 *     BinaryRow.EMPTY_ROW,       // 非分区表
 *     0,                         // bucket 0
 *     1,                         // totalBuckets
 *     dataFileMeta               // 文件元数据
 * );
 *
 * // 计算总行数
 * long totalRows = ManifestEntry.recordCount(manifestEntries);
 * }</pre>
 *
 * @since 0.9.0
 */
@Public
public interface ManifestEntry extends FileEntry {

    /** ManifestEntry 的序列化 Schema，包含 5 个字段 */
    RowType SCHEMA =
            new RowType(
                    false,
                    Arrays.asList(
                            new DataField(0, "_KIND", new TinyIntType(false)),
                            new DataField(1, "_PARTITION", newBytesType(false)),
                            new DataField(2, "_BUCKET", new IntType(false)),
                            new DataField(3, "_TOTAL_BUCKETS", new IntType(false)),
                            new DataField(4, "_FILE", DataFileMeta.SCHEMA)));

    /**
     * 创建 ManifestEntry
     *
     * @param kind 文件类型（ADD/DELETE）
     * @param partition 分区信息
     * @param bucket 桶号
     * @param totalBuckets 总桶数
     * @param file 数据文件元数据
     * @return ManifestEntry 实例
     */
    static ManifestEntry create(
            FileKind kind, BinaryRow partition, int bucket, int totalBuckets, DataFileMeta file) {
        return new PojoManifestEntry(kind, partition, bucket, totalBuckets, file);
    }

    /** 获取数据文件元数据 */
    DataFileMeta file();

    /** 复制 ManifestEntry，但不包含统计信息 */
    ManifestEntry copyWithoutStats();

    /**
     * 分配序列号范围
     *
     * @param minSequenceNumber 最小序列号
     * @param maxSequenceNumber 最大序列号
     * @return 新的 ManifestEntry
     */
    ManifestEntry assignSequenceNumber(long minSequenceNumber, long maxSequenceNumber);

    /**
     * 分配第一行 ID（用于 Row Tracking）
     *
     * @param firstRowId 第一行 ID
     * @return 新的 ManifestEntry
     */
    ManifestEntry assignFirstRowId(long firstRowId);

    /**
     * 升级文件层级
     *
     * @param newLevel 新层级
     * @return 新的 ManifestEntry
     */
    ManifestEntry upgrade(int newLevel);

    /**
     * 计算所有 ManifestEntry 的总行数
     *
     * @param manifestEntries ManifestEntry 列表
     * @return 总行数
     */
    static long recordCount(List<ManifestEntry> manifestEntries) {
        return manifestEntries.stream().mapToLong(manifest -> manifest.file().rowCount()).sum();
    }

    /**
     * 计算所有 ManifestEntry 的总行数（可为 null）
     *
     * @param manifestEntries ManifestEntry 列表
     * @return 总行数，如果列表为空则返回 null
     */
    @Nullable
    static Long nullableRecordCount(List<ManifestEntry> manifestEntries) {
        if (manifestEntries.isEmpty()) {
            return null;
        }
        return manifestEntries.stream().mapToLong(manifest -> manifest.file().rowCount()).sum();
    }

    /**
     * 计算所有 ADD 类型 ManifestEntry 的总行数
     *
     * @param manifestEntries ManifestEntry 列表
     * @return ADD 类型的总行数
     */
    static long recordCountAdd(List<ManifestEntry> manifestEntries) {
        return manifestEntries.stream()
                .filter(manifestEntry -> FileKind.ADD.equals(manifestEntry.kind()))
                .mapToLong(manifest -> manifest.file().rowCount())
                .sum();
    }

    /**
     * 计算所有 DELETE 类型 ManifestEntry 的总行数
     *
     * @param manifestEntries ManifestEntry 列表
     * @return DELETE 类型的总行数
     */
    static long recordCountDelete(List<ManifestEntry> manifestEntries) {
        return manifestEntries.stream()
                .filter(manifestEntry -> FileKind.DELETE.equals(manifestEntry.kind()))
                .mapToLong(manifest -> manifest.file().rowCount())
                .sum();
    }
}
