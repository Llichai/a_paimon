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
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TinyIntType;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.utils.InternalRowUtils.fromStringArrayData;
import static org.apache.paimon.utils.InternalRowUtils.toStringArrayData;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.newBytesType;
import static org.apache.paimon.utils.SerializationUtils.newStringType;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/**
 * 数据文件元数据 0.9 版本序列化器。
 *
 * <p>相比 0.8 版本,0.9 版本新增了以下字段:
 * <ul>
 *   <li>文件来源(fileSource): 标识文件的来源类型(紧凑、追加等)</li>
 * </ul>
 *
 * <p>该序列化器使用公共静态的 SCHEMA 定义,方便其他组件复用模式定义。
 */
public class DataFileMeta09Serializer implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 0.9 版本的数据文件元数据模式定义,包含16个字段 */
    public static final RowType SCHEMA =
            new RowType(
                    false,
                    Arrays.asList(
                            new DataField(0, "_FILE_NAME", newStringType(false)),
                            new DataField(1, "_FILE_SIZE", new BigIntType(false)),
                            new DataField(2, "_ROW_COUNT", new BigIntType(false)),
                            new DataField(3, "_MIN_KEY", newBytesType(false)),
                            new DataField(4, "_MAX_KEY", newBytesType(false)),
                            new DataField(5, "_KEY_STATS", SimpleStats.SCHEMA),
                            new DataField(6, "_VALUE_STATS", SimpleStats.SCHEMA),
                            new DataField(7, "_MIN_SEQUENCE_NUMBER", new BigIntType(false)),
                            new DataField(8, "_MAX_SEQUENCE_NUMBER", new BigIntType(false)),
                            new DataField(9, "_SCHEMA_ID", new BigIntType(false)),
                            new DataField(10, "_LEVEL", new IntType(false)),
                            new DataField(
                                    11, "_EXTRA_FILES", new ArrayType(false, newStringType(false))),
                            new DataField(12, "_CREATION_TIME", DataTypes.TIMESTAMP_MILLIS()),
                            new DataField(13, "_DELETE_ROW_COUNT", new BigIntType(true)),
                            new DataField(14, "_EMBEDDED_FILE_INDEX", newBytesType(true)),
                            new DataField(15, "_FILE_SOURCE", new TinyIntType(true))));

    /** 内部行序列化器,用于序列化和反序列化二进制行数据 */
    protected final InternalRowSerializer rowSerializer;

    /**
     * 构造 0.9 版本的数据文件元数据序列化器。
     */
    public DataFileMeta09Serializer() {
        this.rowSerializer = InternalSerializers.create(SCHEMA);
    }

    /**
     * 序列化数据文件元数据列表。
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
     * <p>相比 0.8 版本,增加了文件来源(fileSource)字段的序列化。
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
                        meta.embeddedIndex(),
                        meta.fileSource().map(FileSource::toByteValue).orElse(null));
        rowSerializer.serialize(row, target);
    }

    /**
     * 反序列化数据文件元数据列表。
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
     * <p>从输入流中读取 0.9 版本格式的数据,包括文件来源字段。
     * 对于更高版本的字段(valueStatsCols、externalPath等),填充为 null。
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
                row.isNullAt(15) ? null : FileSource.fromByteValue(row.getByte(15)),
                null,
                null,
                null,
                null);
    }
}
