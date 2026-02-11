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
 * {@link DataFileMeta} 的序列化器。
 *
 * <p>这是 DataFileMeta 的最新版本序列化器,支持所有字段的序列化和反序列化。
 * 用于将 DataFileMeta 对象转换为 InternalRow 进行持久化存储,以及从 InternalRow 恢复对象。
 *
 * <h2>功能特点</h2>
 * <ul>
 *   <li>支持所有 DataFileMeta 字段的完整序列化</li>
 *   <li>使用 InternalRow 作为中间格式,便于存储和传输</li>
 *   <li>自动处理可选字段(nullable),保持向后兼容性</li>
 *   <li>使用 ObjectSerializer 基类提供的序列化框架</li>
 * </ul>
 *
 * <h2>序列化字段(按顺序)</h2>
 * <ol start="0">
 *   <li>fileName - 文件名</li>
 *   <li>fileSize - 文件大小</li>
 *   <li>rowCount - 行数</li>
 *   <li>minKey - 最小键(序列化为二进制)</li>
 *   <li>maxKey - 最大键(序列化为二进制)</li>
 *   <li>keyStats - 键统计信息</li>
 *   <li>valueStats - 值统计信息</li>
 *   <li>minSequenceNumber - 最小序列号</li>
 *   <li>maxSequenceNumber - 最大序列号</li>
 *   <li>schemaId - Schema ID</li>
 *   <li>level - LSM 层级</li>
 *   <li>extraFiles - 额外文件列表</li>
 *   <li>creationTime - 创建时间</li>
 *   <li>deleteRowCount - 删除行数(可选)</li>
 *   <li>embeddedIndex - 嵌入式索引(可选)</li>
 *   <li>fileSource - 文件来源(可选)</li>
 *   <li>valueStatsCols - 值统计列(可选)</li>
 *   <li>externalPath - 外部路径(可选)</li>
 *   <li>firstRowId - 首行 ID(可选)</li>
 *   <li>writeCols - 写入列(可选)</li>
 * </ol>
 *
 * @see DataFileMeta
 * @see PojoDataFileMeta
 */
public class DataFileMetaSerializer extends ObjectSerializer<DataFileMeta> {

    private static final long serialVersionUID = 1L;

    /**
     * 构造函数。
     *
     * <p>使用 DataFileMeta.SCHEMA 作为序列化的 Schema。
     */
    public DataFileMetaSerializer() {
        super(DataFileMeta.SCHEMA);
    }

    /**
     * 将 DataFileMeta 对象转换为 InternalRow。
     *
     * <p>按照定义的顺序将所有字段序列化到行中。可选字段如果不存在则设置为 null。
     *
     * @param meta 要序列化的 DataFileMeta 对象
     * @return 序列化后的 InternalRow
     */
    @Override
    public InternalRow toRow(DataFileMeta meta) {
        return GenericRow.of(
                BinaryString.fromString(meta.fileName()), // 0: 文件名
                meta.fileSize(), // 1: 文件大小
                meta.rowCount(), // 2: 行数
                serializeBinaryRow(meta.minKey()), // 3: 最小键(序列化为字节数组)
                serializeBinaryRow(meta.maxKey()), // 4: 最大键(序列化为字节数组)
                meta.keyStats().toRow(), // 5: 键统计信息
                meta.valueStats().toRow(), // 6: 值统计信息
                meta.minSequenceNumber(), // 7: 最小序列号
                meta.maxSequenceNumber(), // 8: 最大序列号
                meta.schemaId(), // 9: Schema ID
                meta.level(), // 10: LSM 层级
                toStringArrayData(meta.extraFiles()), // 11: 额外文件列表
                meta.creationTime(), // 12: 创建时间
                meta.deleteRowCount().orElse(null), // 13: 删除行数(可选)
                meta.embeddedIndex(), // 14: 嵌入式索引(可选)
                meta.fileSource().map(FileSource::toByteValue).orElse(null), // 15: 文件来源(可选)
                toStringArrayData(meta.valueStatsCols()), // 16: 值统计列(可选)
                meta.externalPath().map(BinaryString::fromString).orElse(null), // 17: 外部路径(可选)
                meta.firstRowId(), // 18: 首行 ID(可选)
                meta.writeCols() == null ? null : toStringArrayData(meta.writeCols())); // 19: 写入列(可选)
    }

    /**
     * 从 InternalRow 恢复 DataFileMeta 对象。
     *
     * <p>按照定义的顺序从行中读取所有字段,并创建新的 DataFileMeta 对象。
     * 可选字段会检查是否为 null,如果为 null 则传递 null 值。
     *
     * @param row 包含序列化数据的 InternalRow
     * @return 恢复的 DataFileMeta 对象
     */
    @Override
    public DataFileMeta fromRow(InternalRow row) {
        return DataFileMeta.create(
                row.getString(0).toString(), // 0: 文件名
                row.getLong(1), // 1: 文件大小
                row.getLong(2), // 2: 行数
                deserializeBinaryRow(row.getBinary(3)), // 3: 最小键(反序列化)
                deserializeBinaryRow(row.getBinary(4)), // 4: 最大键(反序列化)
                SimpleStats.fromRow(row.getRow(5, 3)), // 5: 键统计信息
                SimpleStats.fromRow(row.getRow(6, 3)), // 6: 值统计信息
                row.getLong(7), // 7: 最小序列号
                row.getLong(8), // 8: 最大序列号
                row.getLong(9), // 9: Schema ID
                row.getInt(10), // 10: LSM 层级
                fromStringArrayData(row.getArray(11)), // 11: 额外文件列表
                row.getTimestamp(12, 3), // 12: 创建时间
                row.isNullAt(13) ? null : row.getLong(13), // 13: 删除行数(可选)
                row.isNullAt(14) ? null : row.getBinary(14), // 14: 嵌入式索引(可选)
                row.isNullAt(15) ? null : FileSource.fromByteValue(row.getByte(15)), // 15: 文件来源(可选)
                row.isNullAt(16) ? null : fromStringArrayData(row.getArray(16)), // 16: 值统计列(可选)
                row.isNullAt(17) ? null : row.getString(17).toString(), // 17: 外部路径(可选)
                row.isNullAt(18) ? null : row.getLong(18), // 18: 首行 ID(可选)
                row.isNullAt(19) ? null : fromStringArrayData(row.getArray(19))); // 19: 写入列(可选)
    }
}
