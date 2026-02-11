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

package org.apache.paimon.table.system;

import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.PackChangelogReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.apache.paimon.CoreOptions.TABLE_READ_SEQUENCE_NUMBER_ENABLED;
import static org.apache.paimon.catalog.Identifier.SYSTEM_TABLE_SPLITTER;

/**
 * Binlog 表。
 *
 * <p>用于读取表的 Binlog 格式变更日志。与审计日志表不同,Binlog 表将 UPDATE 操作合并为单行记录,
 * 使用数组格式同时展示更新前后的值,更接近传统数据库的 Binlog 格式。
 *
 * <h2>Binlog 格式说明</h2>
 * <p>不同操作类型的记录格式:
 * <ul>
 *   <li><b>INSERT</b>: [+I, [col1], [col2], ...]<br>
 *       每个字段值在长度为 1 的数组中</li>
 *   <li><b>UPDATE</b>: [+U, [col1_before, col1_after], [col2_before, col2_after], ...]<br>
 *       每个字段值在长度为 2 的数组中,分别表示更新前后的值</li>
 *   <li><b>DELETE</b>: [-D, [col1], [col2], ...]<br>
 *       每个字段值在长度为 1 的数组中</li>
 * </ul>
 *
 * <h2>表结构 (Schema)</h2>
 * <p>Binlog 表的 Schema 由以下部分组成:
 * <ul>
 *   <li><b>_ROW_KIND</b> (STRING): 行操作类型(+I/-D/+U)</li>
 *   <li><b>_SEQUENCE_NUMBER</b> (BIGINT, 可选): 序列号</li>
 *   <li><b>数据字段</b>: 原表的所有字段,但每个字段的类型变为 {@code ARRAY<原类型>}</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * -- 查询所有 Binlog 记录
 * SELECT * FROM my_table$binlog;
 *
 * -- 解析 UPDATE 操作的前后值
 * SELECT
 *   _ROW_KIND,
 *   id[0] AS id_before,
 *   id[1] AS id_after,
 *   name[0] AS name_before,
 *   name[1] AS name_after
 * FROM my_table$binlog
 * WHERE _ROW_KIND = '+U';
 *
 * -- 流式消费 Binlog
 * SELECT * FROM my_table$binlog /*+ OPTIONS('scan.mode'='latest') */;
 * }</pre>
 *
 * <h2>与审计日志表的对比</h2>
 * <table border="1">
 *   <tr><th>特性</th><th>AuditLogTable</th><th>BinlogTable</th></tr>
 *   <tr>
 *     <td>INSERT 格式</td>
 *     <td>[+I, col1, col2]</td>
 *     <td>[+I, [col1], [col2]]</td>
 *   </tr>
 *   <tr>
 *     <td>UPDATE 格式</td>
 *     <td>两行:<br>[-U, old_col1, old_col2]<br>[+U, new_col1, new_col2]</td>
 *     <td>一行:<br>[+U, [old_col1, new_col1], [old_col2, new_col2]]</td>
 *   </tr>
 *   <tr>
 *     <td>DELETE 格式</td>
 *     <td>[-D, col1, col2]</td>
 *     <td>[-D, [col1], [col2]]</td>
 *   </tr>
 *   <tr>
 *     <td>字段类型</td>
 *     <td>与原表相同</td>
 *     <td>ARRAY<原类型></td>
 *   </tr>
 * </table>
 *
 * <h2>流式读取支持</h2>
 * <p>在流式模式下,Binlog 表使用 {@link org.apache.paimon.reader.PackChangelogReader} 自动将
 * UPDATE 的前后值合并为单行记录。批式模式下每条记录独立输出。
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li>所有字段类型都转换为数组,需要使用数组下标访问具体值</li>
 *   <li>不支持通过 hint 动态设置 {@code table-read.sequence-number.enabled}</li>
 *   <li>UPDATE 操作在数组中的顺序固定为 [旧值, 新值]</li>
 * </ul>
 *
 * @see AuditLogTable
 * @see org.apache.paimon.reader.PackChangelogReader
 */
public class BinlogTable extends AuditLogTable {

    /** 系统表名称常量。 */
    public static final String BINLOG = "binlog";

    public BinlogTable(FileStoreTable wrapped) {
        super(wrapped);
    }

    @Override
    public String name() {
        return wrapped.name() + SYSTEM_TABLE_SPLITTER + BINLOG;
    }

    @Override
    public RowType rowType() {
        List<DataField> fields = new ArrayList<>(specialFields);
        for (DataField field : wrapped.rowType().getFields()) {
            // convert to nullable
            fields.add(field.newType(new ArrayType(field.type().nullable())));
        }
        return new RowType(fields);
    }

    @Override
    public InnerTableRead newRead() {
        return new BinlogRead(wrapped.newRead());
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        if (Boolean.parseBoolean(
                dynamicOptions.getOrDefault(TABLE_READ_SEQUENCE_NUMBER_ENABLED.key(), "false"))) {
            throw new UnsupportedOperationException(
                    "table-read.sequence-number.enabled is not supported by hint.");
        }
        return new BinlogTable(wrapped.copy(dynamicOptions));
    }

    private class BinlogRead extends AuditLogRead {

        private RowType wrappedReadType = wrapped.rowType();

        private BinlogRead(InnerTableRead dataRead) {
            super(dataRead);
        }

        @Override
        public InnerTableRead withReadType(RowType readType) {
            List<DataField> fields = new ArrayList<>();
            List<DataField> wrappedReadFields = new ArrayList<>();
            for (DataField field : readType.getFields()) {
                if (SpecialFields.isSystemField(field.name())) {
                    fields.add(field);
                } else {
                    DataField origin = field.newType(((ArrayType) field.type()).getElementType());
                    fields.add(origin);
                    wrappedReadFields.add(origin);
                }
            }
            this.wrappedReadType = this.wrappedReadType.copy(wrappedReadFields);
            return super.withReadType(readType.copy(fields));
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            DataSplit dataSplit = (DataSplit) split;
            // When sequence number is enabled, the underlying data layout is:
            // [_SEQUENCE_NUMBER, pk, pt, col1, ...]
            // We need to offset the field index to skip the sequence number field.
            int offset = specialFields.size() - 1;
            InternalRow.FieldGetter[] fieldGetters =
                    IntStream.range(0, wrappedReadType.getFieldCount())
                            .mapToObj(
                                    i ->
                                            InternalRow.createFieldGetter(
                                                    wrappedReadType.getTypeAt(i), i + offset))
                            .toArray(InternalRow.FieldGetter[]::new);

            if (dataSplit.isStreaming()) {
                return new PackChangelogReader(
                        dataRead.createReader(split),
                        (row1, row2) ->
                                new AuditLogRow(
                                        readProjection, convertToArray(row1, row2, fieldGetters)),
                        this.wrappedReadType);
            } else {
                return dataRead.createReader(split)
                        .transform(
                                (row) ->
                                        new AuditLogRow(
                                                readProjection,
                                                convertToArray(row, null, fieldGetters)));
            }
        }

        private InternalRow convertToArray(
                InternalRow row1,
                @Nullable InternalRow row2,
                InternalRow.FieldGetter[] fieldGetters) {
            // seqOffset is 1 if sequence number is enabled, 0 otherwise
            int seqOffset = specialFields.size() - 1;
            GenericRow row = new GenericRow(fieldGetters.length + seqOffset);

            // Copy sequence number if enabled (it's at index 0 in input row)
            if (seqOffset > 0) {
                row.setField(0, row1.getLong(0));
            }

            for (int i = 0; i < fieldGetters.length; i++) {
                Object o1 = fieldGetters[i].getFieldOrNull(row1);
                Object o2;
                if (row2 != null) {
                    o2 = fieldGetters[i].getFieldOrNull(row2);
                    row.setField(i + seqOffset, new GenericArray(new Object[] {o1, o2}));
                } else {
                    row.setField(i + seqOffset, new GenericArray(new Object[] {o1}));
                }
            }
            // If no row2 provided, then follow the row1 kind.
            if (row2 == null) {
                row.setRowKind(row1.getRowKind());
            } else {
                row.setRowKind(row2.getRowKind());
            }
            return row;
        }
    }
}
