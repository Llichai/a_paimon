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

package org.apache.paimon.iceberg.manifest;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.iceberg.manifest.IcebergManifestFileMeta.Content;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ObjectSerializer;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link IcebergManifestFileMeta} 的序列化器。
 *
 * <p>负责 Manifest File Meta 对象与 InternalRow 之间的转换。
 *
 * <h3>序列化字段（按顺序）</h3>
 * <ol>
 *   <li>manifest_path - Manifest 文件路径
 *   <li>manifest_length - 文件长度
 *   <li>partition_spec_id - 分区规范 ID
 *   <li>content - 内容类型（DATA/DELETES）
 *   <li>sequence_number - 序列号
 *   <li>min_sequence_number - 最小序列号
 *   <li>added_snapshot_id - 添加时的快照 ID
 *   <li>added_files_count - 新增文件数
 *   <li>existing_files_count - 保留文件数
 *   <li>deleted_files_count - 删除文件数
 *   <li>added_rows_count - 新增行数
 *   <li>existing_rows_count - 保留行数
 *   <li>deleted_rows_count - 删除行数
 *   <li>partitions - 分区摘要数组
 * </ol>
 *
 * <h3>依赖序列化器</h3>
 * <ul>
 *   <li><b>partitionSummarySerializer</b>：{@link IcebergPartitionSummarySerializer}
 * </ul>
 *
 * <h3>数组转换</h3>
 * <p>partitions 字段需要特殊处理：
 * <ul>
 *   <li>序列化：List -> InternalRow[] -> GenericArray
 *   <li>反序列化：InternalArray -> List
 * </ul>
 *
 * @see IcebergManifestFileMeta
 * @see IcebergPartitionSummarySerializer
 */
public class IcebergManifestFileMetaSerializer extends ObjectSerializer<IcebergManifestFileMeta> {

    private static final long serialVersionUID = 1L;

    private final IcebergPartitionSummarySerializer partitionSummarySerializer;

    public IcebergManifestFileMetaSerializer(RowType schema) {
        super(schema);
        this.partitionSummarySerializer = new IcebergPartitionSummarySerializer();
    }

    @Override
    public InternalRow toRow(IcebergManifestFileMeta file) {
        return GenericRow.of(
                BinaryString.fromString(file.manifestPath()),
                file.manifestLength(),
                file.partitionSpecId(),
                file.content().id(),
                file.sequenceNumber(),
                file.minSequenceNumber(),
                file.addedSnapshotId(),
                file.addedFilesCount(),
                file.existingFilesCount(),
                file.deletedFilesCount(),
                file.addedRowsCount(),
                file.existingRowsCount(),
                file.deletedRowsCount(),
                new GenericArray(
                        file.partitions().stream()
                                .map(partitionSummarySerializer::toRow)
                                .toArray(InternalRow[]::new)));
    }

    @Override
    public IcebergManifestFileMeta fromRow(InternalRow row) {
        return new IcebergManifestFileMeta(
                row.getString(0).toString(),
                row.getLong(1),
                row.getInt(2),
                Content.fromId(row.getInt(3)),
                row.getLong(4),
                row.getLong(5),
                row.getLong(6),
                row.getInt(7),
                row.getInt(8),
                row.getInt(9),
                row.getLong(10),
                row.getLong(11),
                row.getLong(12),
                toPartitionSummaries(row.getArray(13)));
    }

    private List<IcebergPartitionSummary> toPartitionSummaries(InternalArray array) {
        List<IcebergPartitionSummary> summaries = new ArrayList<>();
        for (int i = 0; i < array.size(); i++) {
            summaries.add(
                    partitionSummarySerializer.fromRow(
                            array.getRow(i, partitionSummarySerializer.numFields())));
        }
        return summaries;
    }
}
