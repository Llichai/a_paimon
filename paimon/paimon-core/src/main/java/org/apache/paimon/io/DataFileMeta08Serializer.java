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
import org.apache.paimon.data.safe.SafeBinaryRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.InternalRowUtils.fromStringArrayData;
import static org.apache.paimon.utils.InternalRowUtils.toStringArrayData;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.newBytesType;
import static org.apache.paimon.utils.SerializationUtils.newStringType;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/**
 * 数据文件元数据 0.8 版本序列化器。
 *
 * <p>这是一个遗留的序列化器,用于兼容 Paimon 0.8 版本的数据文件元数据格式。
 * 主要版本差异:
 * <ul>
 *   <li>不包含文件来源(fileSource)字段</li>
 *   <li>不包含值统计列(valueStatsCols)字段</li>
 *   <li>不包含外部路径(externalPath)字段</li>
 *   <li>不包含首行ID(firstRowId)和分片ID(shardId)字段</li>
 * </ul>
 *
 * <p>使用安全的二进制行反序列化器,确保向后兼容性。
 */
public class DataFileMeta08Serializer implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 内部行序列化器,用于序列化和反序列化二进制行数据 */
    protected final InternalRowSerializer rowSerializer;

    /**
     * 构造 0.8 版本的数据文件元数据序列化器。
     */
    public DataFileMeta08Serializer() {
        this.rowSerializer = InternalSerializers.create(schemaFor08());
    }

    /**
     * 定义 0.8 版本的数据文件元数据模式。
     *
     * <p>包含15个字段:
     * <ul>
     *   <li>文件名、大小、行数</li>
     *   <li>最小/最大键</li>
     *   <li>键/值统计信息</li>
     *   <li>序列号范围</li>
     *   <li>模式ID、层级</li>
     *   <li>额外文件列表</li>
     *   <li>创建时间</li>
     *   <li>删除行数(可选)</li>
     *   <li>嵌入式文件索引(可选)</li>
     * </ul>
     *
     * @return 0.8 版本的行类型定义
     */
    private static RowType schemaFor08() {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "_FILE_NAME", newStringType(false)));
        fields.add(new DataField(1, "_FILE_SIZE", new BigIntType(false)));
        fields.add(new DataField(2, "_ROW_COUNT", new BigIntType(false)));
        fields.add(new DataField(3, "_MIN_KEY", newBytesType(false)));
        fields.add(new DataField(4, "_MAX_KEY", newBytesType(false)));
        fields.add(new DataField(5, "_KEY_STATS", SimpleStats.SCHEMA));
        fields.add(new DataField(6, "_VALUE_STATS", SimpleStats.SCHEMA));
        fields.add(new DataField(7, "_MIN_SEQUENCE_NUMBER", new BigIntType(false)));
        fields.add(new DataField(8, "_MAX_SEQUENCE_NUMBER", new BigIntType(false)));
        fields.add(new DataField(9, "_SCHEMA_ID", new BigIntType(false)));
        fields.add(new DataField(10, "_LEVEL", new IntType(false)));
        fields.add(new DataField(11, "_EXTRA_FILES", new ArrayType(false, newStringType(false))));
        fields.add(new DataField(12, "_CREATION_TIME", DataTypes.TIMESTAMP_MILLIS()));
        fields.add(new DataField(13, "_DELETE_ROW_COUNT", new BigIntType(true)));
        fields.add(new DataField(14, "_EMBEDDED_FILE_INDEX", newBytesType(true)));
        return new RowType(fields);
    }

    /**
     * 序列化数据文件元数据列表。
     *
     * <p>先写入列表大小,然后逐个序列化每个元数据对象。
     *
     * @param records 要序列化的数据文件元数据列表
     * @param target 数据输出视图
     * @throws IOException 如果序列化过程中发生I/O错误
     */
    public final void serializeList(List<DataFileMeta> records, DataOutputView target)
            throws IOException {
        target.writeInt(records.size());
        for (DataFileMeta t : records) {
            serialize(t, target);
        }
    }

    /**
     * 序列化单个数据文件元数据对象。
     *
     * <p>将元数据的各个字段转换为通用行格式,然后使用行序列化器进行序列化。
     * 注意: 0.8 版本不包含 fileSource、valueStatsCols、externalPath、firstRowId 和 shardId 字段。
     *
     * @param meta 要序列化的数据文件元数据
     * @param target 数据输出视图
     * @throws IOException 如果序列化过程中发生I/O错误
     */
    public void serialize(DataFileMeta meta, DataOutputView target) throws IOException {
        GenericRow row =
                GenericRow.of(
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
                        meta.embeddedIndex());
        rowSerializer.serialize(row, target);
    }

    /**
     * 反序列化数据文件元数据列表。
     *
     * <p>先读取列表大小,然后逐个反序列化每个元数据对象。
     *
     * @param source 数据输入视图
     * @return 反序列化后的数据文件元数据列表
     * @throws IOException 如果反序列化过程中发生I/O错误
     */
    public final List<DataFileMeta> deserializeList(DataInputView source) throws IOException {
        int size = source.readInt();
        List<DataFileMeta> records = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            records.add(deserialize(source));
        }
        return records;
    }

    /**
     * 反序列化单个数据文件元数据对象。
     *
     * <p>从输入流中读取二进制数据,解析为安全二进制行,然后从行中提取各个字段。
     * 对于 0.8 版本不存在的字段(fileSource、valueStatsCols、externalPath、firstRowId、shardId),
     * 在反序列化时填充为 null。
     *
     * @param in 数据输入视图
     * @return 反序列化后的数据文件元数据对象
     * @throws IOException 如果反序列化过程中发生I/O错误
     */
    public DataFileMeta deserialize(DataInputView in) throws IOException {
        byte[] bytes = new byte[in.readInt()];
        in.readFully(bytes);
        SafeBinaryRow row = new SafeBinaryRow(rowSerializer.getArity(), bytes, 0);
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
                null,
                null,
                null,
                null,
                null);
    }
}
