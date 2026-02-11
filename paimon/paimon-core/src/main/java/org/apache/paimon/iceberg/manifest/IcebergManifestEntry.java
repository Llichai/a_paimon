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

import org.apache.paimon.iceberg.metadata.IcebergPartitionField;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Iceberg Manifest 文件的条目。
 *
 * <p>表示 Manifest 文件中的单个文件记录，包含文件元数据和状态信息。
 *
 * <h3>功能说明</h3>
 * <ul>
 *   <li>记录数据文件或删除文件的状态变更
 *   <li>跟踪文件的添加、删除或保持不变
 *   <li>维护序列号用于快照隔离
 * </ul>
 *
 * <h3>状态类型（Status）</h3>
 * <ul>
 *   <li><b>EXISTING (0)</b>：文件已存在（从上个快照继承）
 *   <li><b>ADDED (1)</b>：新添加的文件
 *   <li><b>DELETED (2)</b>：已删除的文件
 * </ul>
 *
 * <h3>序列号说明</h3>
 * <ul>
 *   <li><b>sequenceNumber</b>：数据写入时的序列号
 *   <li><b>fileSequenceNumber</b>：文件首次添加到表的序列号
 *   <li>示例：文件在快照3和快照5合并生成新文件在快照6
 *     <ul>
 *       <li>sequenceNumber = max(3, 5) = 5
 *       <li>fileSequenceNumber = 6
 *     </ul>
 *   </li>
 * </ul>
 *
 * <h3>生命周期</h3>
 * <ol>
 *   <li>新文件：状态为 ADDED
 *   <li>保留文件：状态变为 EXISTING
 *   <li>删除文件：状态变为 DELETED（用于跟踪）
 * </ol>
 *
 * <h3>Schema 结构</h3>
 * <p>提供了符合 Iceberg 规范的 RowType，字段ID固定：
 * <ul>
 *   <li>0 - status
 *   <li>1 - snapshot_id
 *   <li>2 - data_file
 *   <li>3 - sequence_number
 *   <li>4 - file_sequence_number
 * </ul>
 *
 * <h3>参考规范</h3>
 * <p>参见 <a href="https://iceberg.apache.org/spec/#manifests">Iceberg Manifest 规范</a>
 *
 * @see IcebergManifestFile
 * @see IcebergDataFileMeta
 */
public class IcebergManifestEntry {

    /** See Iceberg <code>manifest_entry</code> struct <code>status</code> field. */
    public enum Status {
        EXISTING(0),
        ADDED(1),
        DELETED(2);

        private final int id;

        Status(int id) {
            this.id = id;
        }

        public int id() {
            return id;
        }

        public static Status fromId(int id) {
            switch (id) {
                case 0:
                    return EXISTING;
                case 1:
                    return ADDED;
                case 2:
                    return DELETED;
            }
            throw new IllegalArgumentException("Unknown manifest content: " + id);
        }
    }

    private final Status status;
    private final long snapshotId;
    // sequenceNumber indicates when the records in the data files are written. It might be smaller
    // than fileSequenceNumber.
    // For example, when a file with sequenceNumber 3 and another file with sequenceNumber 5 are
    // compacted into one file during snapshot 6, the compacted file will have sequenceNumber =
    // max(3, 5) = 5, and fileSequenceNumber = 6.
    private final long sequenceNumber;
    private final long fileSequenceNumber;
    private final IcebergDataFileMeta dataFile;

    public IcebergManifestEntry(
            Status status,
            long snapshotId,
            long sequenceNumber,
            long fileSequenceNumber,
            IcebergDataFileMeta dataFile) {
        this.status = status;
        this.snapshotId = snapshotId;
        this.sequenceNumber = sequenceNumber;
        this.fileSequenceNumber = fileSequenceNumber;
        this.dataFile = dataFile;
    }

    public Status status() {
        return status;
    }

    public boolean isLive() {
        return status == Status.ADDED || status == Status.EXISTING;
    }

    public long snapshotId() {
        return snapshotId;
    }

    public long sequenceNumber() {
        return sequenceNumber;
    }

    public long fileSequenceNumber() {
        return fileSequenceNumber;
    }

    public IcebergDataFileMeta file() {
        return dataFile;
    }

    public static RowType schema(RowType partitionType) {

        RowType icebergPartition = icebergPartitionType(partitionType);

        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "status", DataTypes.INT().notNull()));
        fields.add(new DataField(1, "snapshot_id", DataTypes.BIGINT()));
        fields.add(new DataField(3, "sequence_number", DataTypes.BIGINT()));
        fields.add(new DataField(4, "file_sequence_number", DataTypes.BIGINT()));
        fields.add(
                new DataField(
                        2, "data_file", IcebergDataFileMeta.schema(icebergPartition).notNull()));
        return new RowType(false, fields);
    }

    // Use correct Field IDs to support ID-based column pruning.
    // https://iceberg.apache.org/spec/#avro
    public static RowType icebergPartitionType(RowType partitionType) {
        List<DataField> fields = new ArrayList<>();
        for (int i = 0; i < partitionType.getFields().size(); i++) {
            fields.add(
                    partitionType
                            .getFields()
                            .get(i)
                            .newId(IcebergPartitionField.FIRST_FIELD_ID + i));
        }
        return partitionType.copy(fields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IcebergManifestEntry that = (IcebergManifestEntry) o;
        return status == that.status && Objects.equals(dataFile, that.dataFile);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, dataFile);
    }
}
