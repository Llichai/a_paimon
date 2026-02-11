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

package org.apache.paimon.table.object;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.util.Map;

/**
 * 对象表接口,用于表示包含多个对象(文件)的目录。
 *
 * <p>对象表为目录中的非结构化数据对象提供元数据索引,使用户能够在对象存储中分析非结构化数据。
 * 与其他表类型不同,对象表不存储业务数据本身,而是提供对文件元数据的结构化访问。
 *
 * <h3>什么是对象表</h3>
 * <p>对象表将一个文件系统目录映射为一个表,该表的每一行代表目录中的一个文件。
 * 通过这种方式,用户可以使用 SQL 查询来检索和分析文件元数据,例如:
 * <ul>
 *   <li>查找所有大于 1GB 的文件</li>
 *   <li>列出最近修改的文件</li>
 *   <li>按文件扩展名统计文件数量</li>
 *   <li>查找特定所有者的文件</li>
 * </ul>
 *
 * <h3>固定的表结构</h3>
 * <p>对象表具有固定的 Schema,由 {@link #SCHEMA} 常量定义:
 * <table border="1">
 *   <tr>
 *     <th>字段名</th>
 *     <th>类型</th>
 *     <th>说明</th>
 *   </tr>
 *   <tr>
 *     <td>path</td>
 *     <td>STRING NOT NULL</td>
 *     <td>对象的相对路径</td>
 *   </tr>
 *   <tr>
 *     <td>name</td>
 *     <td>STRING NOT NULL</td>
 *     <td>对象的名称(文件名)</td>
 *   </tr>
 *   <tr>
 *     <td>length</td>
 *     <td>BIGINT NOT NULL</td>
 *     <td>对象的字节大小</td>
 *   </tr>
 *   <tr>
 *     <td>mtime</td>
 *     <td>BIGINT NOT NULL</td>
 *     <td>修改时间(时间戳)</td>
 *   </tr>
 *   <tr>
 *     <td>atime</td>
 *     <td>BIGINT NOT NULL</td>
 *     <td>访问时间(时间戳)</td>
 *   </tr>
 *   <tr>
 *     <td>owner</td>
 *     <td>STRING NULLABLE</td>
 *     <td>对象的所有者</td>
 *   </tr>
 * </table>
 *
 * <h3>与其他表类型的关系</h3>
 * <ul>
 *   <li>继承 {@link org.apache.paimon.table.Table} 接口,作为 Paimon 表体系的一部分</li>
 *   <li>与 {@link org.apache.paimon.table.FileStoreTable} 不同,不存储业务数据,只提供元数据视图</li>
 *   <li>与 {@link org.apache.paimon.table.iceberg.IcebergTable} 不同,Iceberg 是结构化表格式,ObjectTable 是元数据表</li>
 *   <li>与 {@link org.apache.paimon.table.lance.LanceTable} 不同,Lance 存储实际数据,ObjectTable 只读取文件元数据</li>
 *   <li>实现了完整的读取功能,支持 {@link #newScan()} 和 {@link #newRead()}</li>
 * </ul>
 *
 * <h3>表类型对比</h3>
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>ObjectTable</th>
 *     <th>FileStoreTable</th>
 *     <th>IcebergTable</th>
 *     <th>LanceTable</th>
 *   </tr>
 *   <tr>
 *     <td>数据内容</td>
 *     <td>文件元数据</td>
 *     <td>业务数据</td>
 *     <td>业务数据</td>
 *     <td>AI/ML 数据</td>
 *   </tr>
 *   <tr>
 *     <td>Schema</td>
 *     <td>固定(SCHEMA 常量)</td>
 *     <td>用户定义</td>
 *     <td>用户定义</td>
 *     <td>用户定义</td>
 *   </tr>
 *   <tr>
 *     <td>读取支持</td>
 *     <td>支持</td>
 *     <td>支持</td>
 *     <td>不支持</td>
 *     <td>不支持</td>
 *   </tr>
 *   <tr>
 *     <td>写入支持</td>
 *     <td>不支持</td>
 *     <td>支持</td>
 *     <td>不支持</td>
 *     <td>不支持</td>
 *   </tr>
 *   <tr>
 *     <td>分区</td>
 *     <td>不支持</td>
 *     <td>支持</td>
 *     <td>支持</td>
 *     <td>不支持</td>
 *   </tr>
 *   <tr>
 *     <td>主键</td>
 *     <td>不支持</td>
 *     <td>支持</td>
 *     <td>不支持</td>
 *     <td>不支持</td>
 *   </tr>
 * </table>
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>数据湖管理</b>: 查询对象存储中的文件分布和大小</li>
 *   <li><b>文件审计</b>: 跟踪文件的修改和访问时间</li>
 *   <li><b>存储分析</b>: 统计存储使用情况和文件特征</li>
 *   <li><b>数据清理</b>: 识别过期或大文件进行清理</li>
 *   <li><b>ETL 预处理</b>: 在处理前了解源文件的元数据</li>
 * </ul>
 *
 * <h3>创建方式</h3>
 * <pre>{@code
 * ObjectTable table = ObjectTable.builder()
 *     .identifier(Identifier.create("db", "my_files"))
 *     .fileIO(fileIO)
 *     .location("s3://bucket/data/")
 *     .comment("All data files in S3 bucket")
 *     .build();
 *
 * // 扫描并读取文件元数据
 * InnerTableScan scan = table.newScan();
 * InnerTableRead read = table.newRead();
 * for (Split split : scan.plan().splits()) {
 *     RecordReader<InternalRow> reader = read.createReader(split);
 *     // 读取文件元数据...
 * }
 * }</pre>
 *
 * @see ObjectTableImpl 该接口的实现类
 * @see org.apache.paimon.table.Table 表的基础接口
 * @see org.apache.paimon.table.FileStoreTable Paimon 原生表
 * @see org.apache.paimon.table.iceberg.IcebergTable Iceberg 表
 * @see org.apache.paimon.table.lance.LanceTable Lance 表
 */
public interface ObjectTable extends Table {

    /**
     * 对象表的固定 Schema 定义。
     *
     * <p>该 Schema 包含以下字段:
     * <ul>
     *   <li><b>path</b>: 文件相对于表 location 的相对路径</li>
     *   <li><b>name</b>: 文件名(包含扩展名)</li>
     *   <li><b>length</b>: 文件大小(字节数)</li>
     *   <li><b>mtime</b>: 修改时间(Unix 时间戳,毫秒)</li>
     *   <li><b>atime</b>: 访问时间(Unix 时间戳,毫秒)</li>
     *   <li><b>owner</b>: 文件所有者(可能为 null)</li>
     * </ul>
     *
     * <p>所有对象表实例共���此固定 Schema,无法自定义。
     */
    RowType SCHEMA =
            RowType.builder()
                    .field("path", DataTypes.STRING().notNull(), "Relative path of object")
                    .field("name", DataTypes.STRING().notNull(), "Name of object")
                    .field("length", DataTypes.BIGINT().notNull(), "Bytes length of object")
                    .field("mtime", DataTypes.BIGINT().notNull(), "Modification time of object")
                    .field("atime", DataTypes.BIGINT().notNull(), "Access time of object")
                    .field("owner", DataTypes.STRING().nullable(), "Owner of object")
                    .build()
                    .notNull();

    /**
     * 获取对象在文件系统中的位置。
     *
     * <p>该位置是对象表监控的目录路径,该目录下的所有文件(包括子目录中的文件)
     * 都会被索引为表的行。
     *
     * <p>示例:
     * <ul>
     *   <li>HDFS: {@code hdfs://cluster/data/raw/}</li>
     *   <li>S3: {@code s3://bucket/path/}</li>
     *   <li>本地: {@code file:///data/files/}</li>
     * </ul>
     *
     * @return 对象目录的文件系统位置路径
     */
    String location();

    /**
     * 创建带有动态选项的表副本。
     *
     * <p>对于对象表,动态选项的作用有限,因为对象表没有配置选项。
     * 但保留此方法以保持接口一致性。
     *
     * @param dynamicOptions 动态选项映射(对对象表无实际影响)
     * @return 返回新的 ObjectTable 实例
     */
    @Override
    ObjectTable copy(Map<String, String> dynamicOptions);

    /**
     * 创建 ObjectTable 的构建器。
     *
     * <p>使用构建器模式可以方便地配置和创建 ObjectTable 实例。
     *
     * @return 返回新的 Builder 实例
     */
    static ObjectTable.Builder builder() {
        return new ObjectTable.Builder();
    }

    /**
     * ObjectTable 的构建器类。
     *
     * <p>该构建器提供了流式 API 来配置和创建 ObjectTable 实例。
     * 必须至少设置 identifier、fileIO 和 location。
     *
     * <p>注意:对象表不需要设置 rowType,因为它使用固定的 {@link #SCHEMA}。
     *
     * <h3>使用示例</h3>
     * <pre>{@code
     * ObjectTable table = ObjectTable.builder()
     *     .identifier(Identifier.create("metadata_db", "file_index"))
     *     .fileIO(fileIO)
     *     .location("hdfs://cluster/warehouse/data/")
     *     .comment("Index of all data files in warehouse")
     *     .build();
     * }</pre>
     */
    class Builder {

        /** 表的标识符,包含数据库名和表名。 */
        private Identifier identifier;

        /** 文件 I/O 接口,用于访问文件系统。 */
        private FileIO fileIO;

        /** 对象目录的位置路径。 */
        private String location;

        /** 表的注释说明。 */
        private String comment;

        /**
         * 设置表的标识符。
         *
         * @param identifier 表标识符,包含数据库名和表名
         * @return 返回当前 Builder 实例,支持链式调用
         */
        public ObjectTable.Builder identifier(Identifier identifier) {
            this.identifier = identifier;
            return this;
        }

        /**
         * 设置文件 I/O 接口。
         *
         * @param fileIO 文件 I/O 实现,用于访问底层文件系统和列出文件
         * @return 返回当前 Builder 实例,支持链式调用
         */
        public ObjectTable.Builder fileIO(FileIO fileIO) {
            this.fileIO = fileIO;
            return this;
        }

        /**
         * 设置对象目录的位置路径。
         *
         * <p>该路径是对象表监控的根目录,所有子目录和文件都会被递归索引。
         *
         * @param location 对象目录在文件系统中的路径
         * @return 返回当前 Builder 实例,支持链式调用
         */
        public ObjectTable.Builder location(String location) {
            this.location = location;
            return this;
        }

        /**
         * 设置表的注释。
         *
         * @param comment 表的描述性注释
         * @return 返回当前 Builder 实例,支持链式调用
         */
        public ObjectTable.Builder comment(String comment) {
            this.comment = comment;
            return this;
        }

        /**
         * 构建并返回 ObjectTable 实例。
         *
         * @return 返回配置好的 ObjectTable 实例
         */
        public ObjectTable build() {
            return new ObjectTableImpl(identifier, fileIO, location, comment);
        }
    }
}
