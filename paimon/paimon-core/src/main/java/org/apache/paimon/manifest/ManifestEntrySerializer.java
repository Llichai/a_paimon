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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.utils.VersionedObjectSerializer;

import java.util.function.Function;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/**
 * ManifestEntry 序列化器
 *
 * <p>负责将 {@link ManifestEntry} 序列化为 {@link InternalRow} 以及反序列化。
 *
 * <p>序列化格式（5个字段）：
 * <ul>
 *   <li>字段 0：kind（TinyInt）- 文件类型
 *   <li>字段 1：partition（Binary）- 分区信息
 *   <li>字段 2：bucket（Int）- 桶号
 *   <li>字段 3：totalBuckets（Int）- 总桶数
 *   <li>字段 4：file（Row）- DataFileMeta 行
 * </ul>
 *
 * <p>版本演化：
 * <ul>
 *   <li>版本 1：旧版本（不兼容，需要重建表）
 *   <li>版本 2：当前版本（添加了 totalBuckets 字段）
 * </ul>
 *
 * <p>getter 方法：
 * <ul>
 *   <li>提供静态 getter 方法，用于在不完全反序列化的情况下提取字段
 *   <li>用于过滤器、统计信息收集等场景
 *   <li>避免创建完整的 ManifestEntry 对象，提高性能
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>写入：{@link ManifestFile} 写入时序列化 ManifestEntry
 *   <li>读取：{@link ManifestFile} 读取时反序列化 ManifestEntry
 *   <li>过滤：使用 getter 方法快速提取字段进行过滤
 * </ul>
 *
 * <p>示例：
 * <pre>{@code
 * // 序列化
 * ManifestEntrySerializer serializer = new ManifestEntrySerializer();
 * InternalRow row = serializer.convertTo(manifestEntry);
 *
 * // 反序列化
 * ManifestEntry entry = serializer.convertFrom(version, row);
 *
 * // 使用 getter 提取字段（不完全反序列化）
 * FileKind kind = ManifestEntrySerializer.kindGetter().apply(row);
 * }</pre>
 */
public class ManifestEntrySerializer extends VersionedObjectSerializer<ManifestEntry> {

    private static final long serialVersionUID = 1L;

    /** DataFileMeta 序列化器（用于序列化嵌套的 file 字段） */
    private final DataFileMetaSerializer dataFileMetaSerializer;

    /** 构造 ManifestEntrySerializer */
    public ManifestEntrySerializer() {
        super(ManifestEntry.SCHEMA);
        this.dataFileMetaSerializer = new DataFileMetaSerializer();
    }

    /**
     * 获取当前版本号
     *
     * @return 版本号 2
     */
    @Override
    public int getVersion() {
        return 2;
    }

    /**
     * 序列化 ManifestEntry 为 InternalRow
     *
     * @param entry ManifestEntry 实例
     * @return InternalRow 实例（5个字段）
     */
    @Override
    public InternalRow convertTo(ManifestEntry entry) {
        GenericRow row = new GenericRow(5);
        row.setField(0, entry.kind().toByteValue());
        row.setField(1, serializeBinaryRow(entry.partition()));
        row.setField(2, entry.bucket());
        row.setField(3, entry.totalBuckets());
        row.setField(4, dataFileMetaSerializer.toRow(entry.file()));
        return row;
    }

    /**
     * 反序列化 InternalRow 为 ManifestEntry
     *
     * @param version 序列化版本号
     * @param row InternalRow 实例
     * @return ManifestEntry 实例
     * @throws IllegalArgumentException 如果版本不兼容
     */
    @Override
    public ManifestEntry convertFrom(int version, InternalRow row) {
        if (version != 2) {
            if (version == 1) {
                throw new IllegalArgumentException(
                        String.format(
                                "The current version %s is not compatible with the version %s, please recreate the table.",
                                getVersion(), version));
            }
            throw new IllegalArgumentException("Unsupported version: " + version);
        }
        return ManifestEntry.create(
                FileKind.fromByteValue(row.getByte(0)),
                deserializeBinaryRow(row.getBinary(1)),
                row.getInt(2),
                row.getInt(3),
                dataFileMetaSerializer.fromRow(row.getRow(4, dataFileMetaSerializer.numFields())));
    }

    /**
     * 获取 FileKind 的 getter 函数
     *
     * <p>用于在不完全反序列化的情况下提取 kind 字段。
     *
     * @return FileKind getter 函数
     */
    public static Function<InternalRow, FileKind> kindGetter() {
        return row -> FileKind.fromByteValue(row.getByte(1));
    }

    /**
     * 获取 partition 的 getter 函数
     *
     * <p>用于在不完全反序列化的情况下提取 partition 字段。
     *
     * @return Partition getter 函数
     */
    public static Function<InternalRow, BinaryRow> partitionGetter() {
        return row -> deserializeBinaryRow(row.getBinary(2));
    }

    /**
     * 获取 bucket 的 getter 函数
     *
     * <p>用于在不完全反序列化的情况下提取 bucket 字段。
     *
     * @return Bucket getter 函数
     */
    public static Function<InternalRow, Integer> bucketGetter() {
        return row -> row.getInt(3);
    }

    /**
     * 获取 totalBuckets 的 getter 函数
     *
     * <p>用于在不完全反序列化的情况下提取 totalBuckets 字段。
     *
     * @return TotalBuckets getter 函数
     */
    public static Function<InternalRow, Integer> totalBucketGetter() {
        return row -> row.getInt(4);
    }

    /**
     * 获取 fileName 的 getter 函数
     *
     * <p>从嵌套的 DataFileMeta 行中提取 fileName 字段。
     *
     * @return FileName getter 函数
     */
    public static Function<InternalRow, String> fileNameGetter() {
        return row -> row.getRow(5, DataFileMeta.SCHEMA.getFieldCount()).getString(0).toString();
    }

    /**
     * 获取 level 的 getter 函数
     *
     * <p>从嵌套的 DataFileMeta 行中提取 level 字段。
     *
     * @return Level getter 函数
     */
    public static Function<InternalRow, Integer> levelGetter() {
        return row -> row.getRow(5, DataFileMeta.SCHEMA.getFieldCount()).getInt(10);
    }
}
