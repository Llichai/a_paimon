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

import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.utils.VersionedObjectSerializer;

import static org.apache.paimon.data.BinaryString.fromString;
import static org.apache.paimon.index.IndexFileMetaSerializer.dvMetasToRowArrayData;
import static org.apache.paimon.index.IndexFileMetaSerializer.rowArrayDataToDvMetas;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/**
 * IndexManifestEntry 序列化器
 *
 * <p>负责将 {@link IndexManifestEntry} 序列化为 {@link InternalRow} 以及反序列化。
 *
 * <p>序列化格式（10个字段）：
 * <ul>
 *   <li>字段 0：kind（TinyInt）- 文件类型（ADD/DELETE）
 *   <li>字段 1：partition（Binary）- 分区信息
 *   <li>字段 2：bucket（Int）- 桶号
 *   <li>字段 3：indexType（String）- 索引类型
 *   <li>字段 4：fileName（String）- 文件名
 *   <li>字段 5：fileSize（Long）- 文件大小
 *   <li>字段 6：rowCount（Long）- 行数
 *   <li>字段 7：dvRanges（Array/Null）- 删除向量范围
 *   <li>字段 8：externalPath（String/Null）- 外部路径
 *   <li>字段 9：globalIndexMeta（Row/Null）- 全局索引元数据
 * </ul>
 *
 * <p>全局索引元数据格式（5个字段）：
 * <ul>
 *   <li>rowRangeStart（Long）- 行范围起始
 *   <li>rowRangeEnd（Long）- 行范围结束
 *   <li>indexFieldId（Int）- 索引字段 ID
 *   <li>extraFieldIds（Array/Null）- 额外字段 ID 列表
 *   <li>indexMeta（Binary/Null）- 索引元数据
 * </ul>
 *
 * <p>版本信息：
 * <ul>
 *   <li>版本 1：当前版本（首个版本）
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>写入：{@link IndexManifestFile} 写入时序列化 IndexManifestEntry
 *   <li>读取：{@link IndexManifestFile} 读取时反序列化 IndexManifestEntry
 * </ul>
 *
 * <p>示例：
 * <pre>{@code
 * // 序列化
 * IndexManifestEntrySerializer serializer = new IndexManifestEntrySerializer();
 * InternalRow row = serializer.convertTo(indexEntry);
 *
 * // 反序列化
 * IndexManifestEntry entry = serializer.convertFrom(version, row);
 * }</pre>
 */
public class IndexManifestEntrySerializer extends VersionedObjectSerializer<IndexManifestEntry> {

    /** 构造 IndexManifestEntrySerializer */
    public IndexManifestEntrySerializer() {
        super(IndexManifestEntry.SCHEMA);
    }

    /** 获取当前版本号 */
    @Override
    public int getVersion() {
        return 1;
    }

    /**
     * 序列化 IndexManifestEntry 为 InternalRow
     *
     * @param record IndexManifestEntry 实例
     * @return InternalRow 实例（10个字段）
     */
    @Override
    public InternalRow convertTo(IndexManifestEntry record) {
        IndexFileMeta indexFile = record.indexFile();
        GlobalIndexMeta globalIndexMeta = indexFile.globalIndexMeta();
        // 序列化全局索引元数据（如果存在）
        InternalRow globalIndexRow =
                globalIndexMeta == null
                        ? null
                        : GenericRow.of(
                                globalIndexMeta.rowRangeStart(),
                                globalIndexMeta.rowRangeEnd(),
                                globalIndexMeta.indexFieldId(),
                                globalIndexMeta.extraFieldIds() == null
                                        ? null
                                        : new GenericArray(globalIndexMeta.extraFieldIds()),
                                globalIndexMeta.indexMeta());
        return GenericRow.of(
                record.kind().toByteValue(),
                serializeBinaryRow(record.partition()),
                record.bucket(),
                fromString(indexFile.indexType()),
                fromString(indexFile.fileName()),
                indexFile.fileSize(),
                indexFile.rowCount(),
                dvMetasToRowArrayData(indexFile.dvRanges()),
                fromString(indexFile.externalPath()),
                globalIndexRow);
    }

    /**
     * 反序列化 InternalRow 为 IndexManifestEntry
     *
     * @param version 序列化版本号
     * @param row InternalRow 实例
     * @return IndexManifestEntry 实例
     * @throws UnsupportedOperationException 如果版本不支持
     */
    @Override
    public IndexManifestEntry convertFrom(int version, InternalRow row) {
        if (version != 1) {
            throw new UnsupportedOperationException("Unsupported version: " + version);
        }

        // 反序列化全局索引元数据（如果存在）
        GlobalIndexMeta globalIndexMeta = null;
        if (!row.isNullAt(9)) {
            InternalRow globalIndexRow = row.getRow(9, 5);
            long rowRangeStart = globalIndexRow.getLong(0);
            long rowRangeEnd = globalIndexRow.getLong(1);
            int indexFieldId = globalIndexRow.getInt(2);
            int[] extralFields =
                    globalIndexRow.isNullAt(3) ? null : globalIndexRow.getArray(3).toIntArray();
            byte[] indexMeta = globalIndexRow.isNullAt(4) ? null : globalIndexRow.getBinary(4);
            globalIndexMeta =
                    new GlobalIndexMeta(
                            rowRangeStart, rowRangeEnd, indexFieldId, extralFields, indexMeta);
        }

        return new IndexManifestEntry(
                FileKind.fromByteValue(row.getByte(0)),
                deserializeBinaryRow(row.getBinary(1)),
                row.getInt(2),
                new IndexFileMeta(
                        row.getString(3).toString(),
                        row.getString(4).toString(),
                        row.getLong(5),
                        row.getLong(6),
                        row.isNullAt(7) ? null : rowArrayDataToDvMetas(row.getArray(7)),
                        row.isNullAt(8) ? null : row.getString(8).toString(),
                        globalIndexMeta));
    }
}
