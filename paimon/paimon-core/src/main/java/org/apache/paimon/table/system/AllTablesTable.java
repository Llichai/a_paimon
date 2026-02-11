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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadOnceTableScan;
import org.apache.paimon.table.source.SingletonSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.ProjectedRow;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

import static org.apache.paimon.CoreOptions.TYPE;
import static org.apache.paimon.rest.responses.AuditRESTResponse.FIELD_CREATED_AT;
import static org.apache.paimon.rest.responses.AuditRESTResponse.FIELD_CREATED_BY;
import static org.apache.paimon.rest.responses.AuditRESTResponse.FIELD_OWNER;
import static org.apache.paimon.rest.responses.AuditRESTResponse.FIELD_UPDATED_AT;
import static org.apache.paimon.rest.responses.AuditRESTResponse.FIELD_UPDATED_BY;

/**
 * 所有表系统表。
 *
 * <p>这是一个系统表,用于展示 Catalog 中所有数据库和表的信息。提供了表的基本元数据、统计信息和审计信息。
 *
 * <h2>表结构 (Schema)</h2>
 * <table border="1">
 *   <tr><th>字段名</th><th>类型</th><th>描述</th></tr>
 *   <tr><td>database_name</td><td>STRING</td><td>数据库名称</td></tr>
 *   <tr><td>table_name</td><td>STRING</td><td>表名称</td></tr>
 *   <tr><td>table_type</td><td>STRING</td><td>表类型(如 table-with-pk, append-only 等)</td></tr>
 *   <tr><td>partitioned</td><td>BOOLEAN</td><td>是否分区表</td></tr>
 *   <tr><td>primary_key</td><td>BOOLEAN</td><td>是否有主键</td></tr>
 *   <tr><td>owner</td><td>STRING</td><td>表的所有者</td></tr>
 *   <tr><td>created_at</td><td>BIGINT</td><td>创建时间戳(毫秒)</td></tr>
 *   <tr><td>created_by</td><td>STRING</td><td>创建者</td></tr>
 *   <tr><td>updated_at</td><td>BIGINT</td><td>最后更新时间戳(毫秒)</td></tr>
 *   <tr><td>updated_by</td><td>STRING</td><td>最后更新者</td></tr>
 *   <tr><td>record_count</td><td>BIGINT</td><td>记录总数(来自最新快照)</td></tr>
 *   <tr><td>file_size_in_bytes</td><td>BIGINT</td><td>文件总大小(字节)</td></tr>
 *   <tr><td>file_count</td><td>BIGINT</td><td>文件总数</td></tr>
 *   <tr><td>last_file_creation_time</td><td>BIGINT</td><td>最后文件创建时间戳(毫秒)</td></tr>
 * </table>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * -- Flink SQL 查询所有表
 * SELECT database_name, table_name, table_type, record_count
 * FROM sys.tables
 * WHERE partitioned = true;
 *
 * -- 查询特定数据库的表
 * SELECT * FROM sys.tables WHERE database_name = 'my_db';
 * }</pre>
 *
 * <p>注意:此系统表是全局级别的,在 Catalog 级别提供,不依赖于特定的数据表。
 *
 * @see ReadonlyTable
 */
public class AllTablesTable implements ReadonlyTable {

    /** 系统表名称常量。 */
    public static final String ALL_TABLES = "tables";

    /** 表的行类型定义,包含所有字段的类型和名称。 */
    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "database_name", DataTypes.STRING()),
                            new DataField(1, "table_name", DataTypes.STRING()),
                            new DataField(2, "table_type", DataTypes.STRING()),
                            new DataField(3, "partitioned", DataTypes.BOOLEAN()),
                            new DataField(4, "primary_key", DataTypes.BOOLEAN()),
                            new DataField(5, "owner", DataTypes.STRING()),
                            new DataField(6, "created_at", DataTypes.BIGINT()),
                            new DataField(7, "created_by", DataTypes.STRING()),
                            new DataField(8, "updated_at", DataTypes.BIGINT()),
                            new DataField(9, "updated_by", DataTypes.STRING()),
                            new DataField(10, "record_count", DataTypes.BIGINT()),
                            new DataField(11, "file_size_in_bytes", DataTypes.BIGINT()),
                            new DataField(12, "file_count", DataTypes.BIGINT()),
                            new DataField(13, "last_file_creation_time", DataTypes.BIGINT())));

    /** 表数据行列表。 */
    private final List<GenericRow> rows;

    /**
     * 构造函数。
     *
     * @param rows 表数据行列表
     */
    public AllTablesTable(List<GenericRow> rows) {
        this.rows = rows;
    }

    /**
     * 从表和快照列表创建 AllTablesTable 实例。
     *
     * @param tables 表和对应快照的列表
     * @return AllTablesTable 实例
     */
    public static AllTablesTable fromTables(List<Pair<Table, TableSnapshot>> tables) {
        List<GenericRow> rows = new ArrayList<>();
        for (Pair<Table, TableSnapshot> pair : tables) {
            Table table = pair.getKey();
            TableSnapshot snapshot = pair.getValue();
            Identifier identifier = Identifier.fromString(table.fullName());
            Map<String, String> options = table.options();
            rows.add(
                    GenericRow.of(
                            BinaryString.fromString(identifier.getDatabaseName()),
                            BinaryString.fromString(identifier.getObjectName()),
                            BinaryString.fromString(
                                    options.getOrDefault(
                                            TYPE.key(), TYPE.defaultValue().toString())),
                            !table.partitionKeys().isEmpty(),
                            !table.primaryKeys().isEmpty(),
                            BinaryString.fromString(options.get(FIELD_OWNER)),
                            parseLong(options.get(FIELD_CREATED_AT)),
                            BinaryString.fromString(options.get(FIELD_CREATED_BY)),
                            parseLong(options.get(FIELD_UPDATED_AT)),
                            BinaryString.fromString(options.get(FIELD_UPDATED_BY)),
                            snapshot == null ? null : snapshot.recordCount(),
                            snapshot == null ? null : snapshot.fileSizeInBytes(),
                            snapshot == null ? null : snapshot.fileCount(),
                            snapshot == null ? null : snapshot.lastFileCreationTime()));
        }
        return new AllTablesTable(rows);
    }

    private static Long parseLong(String s) {
        if (s == null) {
            return null;
        }
        return Long.parseLong(s);
    }

    @Override
    public String name() {
        return ALL_TABLES;
    }

    @Override
    public RowType rowType() {
        return TABLE_TYPE;
    }

    @Override
    public List<String> primaryKeys() {
        return Arrays.asList(TABLE_TYPE.getField(0).name(), TABLE_TYPE.getField(1).name());
    }

    @Override
    public FileIO fileIO() {
        // pass a useless file io, should never use this.
        return new LocalFileIO();
    }

    @Override
    public InnerTableScan newScan() {
        return new AllTablesScan();
    }

    @Override
    public InnerTableRead newRead() {
        return new AllTablesRead();
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new AllTablesTable(rows);
    }

    private class AllTablesScan extends ReadOnceTableScan {

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            return this;
        }

        @Override
        public Plan innerPlan() {
            return () -> Collections.singletonList(new AllTablesSplit(rows));
        }
    }

    private static class AllTablesSplit extends SingletonSplit {

        private static final long serialVersionUID = 1L;

        private final List<GenericRow> rows;

        private AllTablesSplit(List<GenericRow> rows) {
            this.rows = rows;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AllTablesSplit that = (AllTablesSplit) o;
            return Objects.equals(rows, that.rows);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rows);
        }

        @Override
        public OptionalLong mergedRowCount() {
            return OptionalLong.empty();
        }
    }

    private static class AllTablesRead implements InnerTableRead {

        private RowType readType;

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            return this;
        }

        @Override
        public InnerTableRead withReadType(RowType readType) {
            this.readType = readType;
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) {
            if (!(split instanceof AllTablesSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            List<? extends InternalRow> rows = ((AllTablesSplit) split).rows;
            Iterator<? extends InternalRow> iterator = rows.iterator();
            if (readType != null) {
                iterator =
                        Iterators.transform(
                                iterator,
                                row -> ProjectedRow.from(readType, TABLE_TYPE).replaceRow(row));
            }
            //noinspection ReassignedVariable,unchecked
            return new IteratorRecordReader<>((Iterator<InternalRow>) iterator);
        }
    }
}
