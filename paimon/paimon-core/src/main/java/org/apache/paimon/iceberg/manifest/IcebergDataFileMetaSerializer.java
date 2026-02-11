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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalMapSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ObjectSerializer;

/**
 * {@link IcebergDataFileMeta} 的序列化器。
 *
 * <p>负责 IcebergDataFileMeta 对象与 InternalRow 之间的转换。
 *
 * <h3>序列化字段</h3>
 * <ol>
 *   <li>content - 内容类型（DATA/POSITION_DELETES/EQUALITY_DELETES）
 *   <li>file_path - 文件路径
 *   <li>file_format - 文件格式（orc/parquet/avro）
 *   <li>partition - 分区信息
 *   <li>record_count - 记录数量
 *   <li>file_size_in_bytes - 文件大小
 *   <li>null_value_counts - 空值计数 Map
 *   <li>lower_bounds - 最小值 Map
 *   <li>upper_bounds - 最大值 Map
 *   <li>referenced_data_file - 引用的数据文件（删除文件专用）
 *   <li>content_offset - 内容偏移量（删除文件专用）
 *   <li>content_size_in_bytes - 内容大小（删除文件专用）
 * </ol>
 *
 * <h3>嵌套序列化器</h3>
 * <ul>
 *   <li><b>partSerializer</b>：分区信息序列化
 *   <li><b>nullValueCountsSerializer</b>：空值计数 Map 序列化
 *   <li><b>lowerBoundsSerializer</b>：最小值 Map 序列化
 *   <li><b>upperBoundsSerializer</b>：最大值 Map 序列化
 * </ul>
 *
 * @see IcebergDataFileMeta
 * @see org.apache.paimon.utils.ObjectSerializer
 */
public class IcebergDataFileMetaSerializer extends ObjectSerializer<IcebergDataFileMeta> {

    private static final long serialVersionUID = 1L;

    private final InternalRowSerializer partSerializer;
    private final InternalMapSerializer nullValueCountsSerializer;
    private final InternalMapSerializer lowerBoundsSerializer;
    private final InternalMapSerializer upperBoundsSerializer;

    public IcebergDataFileMetaSerializer(RowType partitionType) {
        super(IcebergDataFileMeta.schema(partitionType));
        this.partSerializer = new InternalRowSerializer(partitionType);
        this.nullValueCountsSerializer =
                new InternalMapSerializer(DataTypes.INT(), DataTypes.BIGINT());
        this.lowerBoundsSerializer = new InternalMapSerializer(DataTypes.INT(), DataTypes.BYTES());
        this.upperBoundsSerializer = new InternalMapSerializer(DataTypes.INT(), DataTypes.BYTES());
    }

    @Override
    public InternalRow toRow(IcebergDataFileMeta file) {
        return GenericRow.of(
                file.content().id(),
                BinaryString.fromString(file.filePath()),
                BinaryString.fromString(file.fileFormat()),
                file.partition(),
                file.recordCount(),
                file.fileSizeInBytes(),
                file.nullValueCounts(),
                file.lowerBounds(),
                file.upperBounds(),
                BinaryString.fromString(file.referencedDataFile()),
                file.contentOffset(),
                file.contentSizeInBytes());
    }

    @Override
    public IcebergDataFileMeta fromRow(InternalRow row) {
        return new IcebergDataFileMeta(
                IcebergDataFileMeta.Content.fromId(row.getInt(0)),
                row.getString(1).toString(),
                row.getString(2).toString(),
                partSerializer.toBinaryRow(row.getRow(3, partSerializer.getArity())).copy(),
                row.getLong(4),
                row.getLong(5),
                nullValueCountsSerializer.copy(row.getMap(6)),
                lowerBoundsSerializer.copy(row.getMap(7)),
                upperBoundsSerializer.copy(row.getMap(8)),
                row.isNullAt(9) ? null : row.getString(9).toString(),
                row.isNullAt(10) ? null : row.getLong(10),
                row.isNullAt(11) ? null : row.getLong(11));
    }
}
