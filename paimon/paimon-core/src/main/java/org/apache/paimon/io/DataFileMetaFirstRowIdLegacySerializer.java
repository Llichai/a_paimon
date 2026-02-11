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

package org.apache.paimon.io;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.utils.ObjectSerializer;

import static org.apache.paimon.utils.InternalRowUtils.fromStringArrayData;
import static org.apache.paimon.utils.InternalRowUtils.toStringArrayData;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/**
 * 数据文件元数据首行ID遗留版本序列化器。
 *
 * <p>相比 1.2 版本,该版本新增了以下字段:
 * <ul>
 *   <li>首行ID(firstRowId): 文件中第一行的全局行ID,用于行追踪</li>
 * </ul>
 *
 * <p>继承自 {@link ObjectSerializer},提供更简洁的序列化实现。
 * 该序列化器主要用于支持行追踪功能。
 */
public class DataFileMetaFirstRowIdLegacySerializer extends ObjectSerializer<DataFileMeta> {

    private static final long serialVersionUID = 1L;

    /**
     * 构造首行ID遗留版本的数据文件元数据序列化器。
     *
     * <p>使用 DataFileMeta.SCHEMA 作为模式定义。
     */
    public DataFileMetaFirstRowIdLegacySerializer() {
        super(DataFileMeta.SCHEMA);
    }

    /**
     * 将数据文件元数据对象转换为内部行表示。
     *
     * <p>序列化所有字段,包括首行ID,但分片ID字段填充为 null。
     *
     * @param meta 要转换的数据文件元数据对象
     * @return 内部行表示
     */
    @Override
    public InternalRow toRow(DataFileMeta meta) {
        return GenericRow.of(
                BinaryString.fromString(meta.fileName()),
                meta.fileSize(),
                meta.rowCount(),
                serializeBinaryRow(meta.minKey()),
                serializeBinaryRow(meta.maxKey()),
                meta.keyStats().toRow(),
                meta.valueStats().toRow(),
                meta.minSequenceNumber(),
                meta.maxSequenceNumber(),
                meta.schemaId(),
                meta.level(),
                toStringArrayData(meta.extraFiles()),
                meta.creationTime(),
                meta.deleteRowCount().orElse(null),
                meta.embeddedIndex(),
                meta.fileSource().map(FileSource::toByteValue).orElse(null),
                toStringArrayData(meta.valueStatsCols()),
                meta.externalPath().map(BinaryString::fromString).orElse(null),
                meta.firstRowId(),
                null);
    }

    /**
     * 从内部行表示转换为数据文件元数据对象。
     *
     * <p>反序列化所有字段,包括首行ID字段。
     * 分片ID字段在此版本中不存在,填充为 null。
     *
     * @param row 内部行数据
     * @return 数据文件元数据对象
     */
    @Override
    public DataFileMeta fromRow(InternalRow row) {
        return DataFileMeta.create(
                row.getString(0).toString(),
                row.getLong(1),
                row.getLong(2),
                deserializeBinaryRow(row.getBinary(3)),
                deserializeBinaryRow(row.getBinary(4)),
                SimpleStats.fromRow(row.getRow(5, 3)),
                SimpleStats.fromRow(row.getRow(6, 3)),
                row.getLong(7),
                row.getLong(8),
                row.getLong(9),
                row.getInt(10),
                fromStringArrayData(row.getArray(11)),
                row.getTimestamp(12, 3),
                row.isNullAt(13) ? null : row.getLong(13),
                row.isNullAt(14) ? null : row.getBinary(14),
                row.isNullAt(15) ? null : FileSource.fromByteValue(row.getByte(15)),
                row.isNullAt(16) ? null : fromStringArrayData(row.getArray(16)),
                row.isNullAt(17) ? null : row.getString(17).toString(),
                row.isNullAt(18) ? null : row.getLong(18),
                null);
    }
}
