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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.utils.VersionedObjectSerializer;

/**
 * ManifestFileMeta 序列化器
 *
 * <p>负责将 {@link ManifestFileMeta} 序列化为 {@link InternalRow} 以及反序列化。
 *
 * <p>序列化格式（12个字段）：
 * <ul>
 *   <li>字段 0：fileName（String）- Manifest 文件名
 *   <li>字段 1：fileSize（Long）- 文件大小
 *   <li>字段 2：numAddedFiles（Long）- 新增文件数量
 *   <li>字段 3：numDeletedFiles（Long）- 删除文件数量
 *   <li>字段 4：partitionStats（Row）- 分区统计信息
 *   <li>字段 5：schemaId（Long）- Schema ID
 *   <li>字段 6：minBucket（Int/Null）- 最小桶号
 *   <li>字段 7：maxBucket（Int/Null）- 最大桶号
 *   <li>字段 8：minLevel（Int/Null）- 最小层级
 *   <li>字段 9：maxLevel（Int/Null）- 最大层级
 *   <li>字段 10：minRowId（Long/Null）- 最小行 ID
 *   <li>字段 11：maxRowId（Long/Null）- 最大行 ID
 * </ul>
 *
 * <p>版本演化：
 * <ul>
 *   <li>版本 1：旧版本（不兼容，需要重建表）
 *   <li>版本 2：当前版本（添加了行 ID 字段）
 * </ul>
 *
 * <p>可为 null 的字段：
 * <ul>
 *   <li>minBucket、maxBucket：对于非桶表可能为 null
 *   <li>minLevel、maxLevel：对于没有层级信息的表可能为 null
 *   <li>minRowId、maxRowId：对于不支持 Row Tracking 的表为 null
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>写入：{@link ManifestList} 写入时序列化 ManifestFileMeta
 *   <li>读取：{@link ManifestList} 读取时反序列化 ManifestFileMeta
 * </ul>
 */
public class ManifestFileMetaSerializer extends VersionedObjectSerializer<ManifestFileMeta> {

    private static final long serialVersionUID = 1L;

    /** 构造 ManifestFileMetaSerializer */
    public ManifestFileMetaSerializer() {
        super(ManifestFileMeta.SCHEMA);
    }

    /** 获取当前版本号 */
    @Override
    public int getVersion() {
        return 2;
    }

    /**
     * 序列化 ManifestFileMeta 为 InternalRow
     *
     * @param meta ManifestFileMeta 实例
     * @return InternalRow 实例（12个字段）
     */
    @Override
    public InternalRow convertTo(ManifestFileMeta meta) {
        return GenericRow.of(
                BinaryString.fromString(meta.fileName()),
                meta.fileSize(),
                meta.numAddedFiles(),
                meta.numDeletedFiles(),
                meta.partitionStats().toRow(),
                meta.schemaId(),
                meta.minBucket(),
                meta.maxBucket(),
                meta.minLevel(),
                meta.maxLevel(),
                meta.minRowId(),
                meta.maxRowId());
    }

    /**
     * 反序列化 InternalRow 为 ManifestFileMeta
     *
     * @param version 序列化版本号
     * @param row InternalRow 实例
     * @return ManifestFileMeta 实例
     * @throws IllegalArgumentException 如果版本不兼容
     */
    @Override
    public ManifestFileMeta convertFrom(int version, InternalRow row) {
        if (version != 2) {
            if (version == 1) {
                throw new IllegalArgumentException(
                        String.format(
                                "The current version %s is not compatible with the version %s, please recreate the table.",
                                getVersion(), version));
            }
            throw new IllegalArgumentException("Unsupported version: " + version);
        }

        return new ManifestFileMeta(
                row.getString(0).toString(),
                row.getLong(1),
                row.getLong(2),
                row.getLong(3),
                SimpleStats.fromRow(row.getRow(4, 3)),
                row.getLong(5),
                row.isNullAt(6) ? null : row.getInt(6),
                row.isNullAt(7) ? null : row.getInt(7),
                row.isNullAt(8) ? null : row.getInt(8),
                row.isNullAt(9) ? null : row.getInt(9),
                row.isNullAt(10) ? null : row.getLong(10),
                row.isNullAt(11) ? null : row.getLong(11));
    }
}
