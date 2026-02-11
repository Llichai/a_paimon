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

package org.apache.paimon.table.iceberg;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowType;

import java.util.List;
import java.util.Map;

/**
 * Iceberg 表接口,代表一个 Apache Iceberg 格式的表。
 *
 * <p>该接口用于在 Paimon 系统中表示外部的 Iceberg 表,使得 Paimon 可以识别和管理 Iceberg 表的元数据。
 * <b>注意:当前 Paimon 尚不支持对 Iceberg 表进行读写操作。</b>
 *
 * <h3>什么是 Iceberg 表</h3>
 * <p>Apache Iceberg 是一种开放的表格式,专为大规模分析型数据湖设计。它提供了以下特性:
 * <ul>
 *   <li>ACID 事务支持</li>
 *   <li>模式演进</li>
 *   <li>隐藏分区</li>
 *   <li>时间旅行和快照管理</li>
 *   <li>增量读取</li>
 * </ul>
 *
 * <h3>与其他表类型的关系</h3>
 * <ul>
 *   <li>继承 {@link org.apache.paimon.table.Table} 接口,作为 Paimon 表体系的一部分</li>
 *   <li>与 {@link org.apache.paimon.table.FileStoreTable} 并列,代表不同的表格式</li>
 *   <li>与 {@link org.apache.paimon.table.lance.LanceTable} 类似,都是外部表格式的集成</li>
 *   <li>与 {@link org.apache.paimon.table.object.ObjectTable} 不同,Iceberg 表是结构化表格式</li>
 * </ul>
 *
 * <h3>当前限制</h3>
 * <ul>
 *   <li>只支持元数据管理,不支持数据读写</li>
 *   <li>主要用于 Catalog 层面的表发现和元数据查询</li>
 *   <li>未来可能会支持通过 Paimon 读写 Iceberg 表</li>
 * </ul>
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li>在 Paimon Catalog 中注册已有的 Iceberg 表</li>
 *   <li>查询 Iceberg 表的元数据信息(Schema、分区等)</li>
 *   <li>在混合数据湖环境中统一管理多种表格式</li>
 * </ul>
 *
 * <h3>创建方式</h3>
 * <pre>{@code
 * IcebergTable table = IcebergTable.builder()
 *     .identifier(identifier)
 *     .fileIO(fileIO)
 *     .rowType(rowType)
 *     .partitionKeys(partitionKeys)
 *     .location(location)
 *     .options(options)
 *     .build();
 * }</pre>
 *
 * @see IcebergTableImpl 该接口的实现类
 * @see org.apache.paimon.table.Table 表的基础接口
 * @see org.apache.paimon.table.lance.LanceTable 另一种外部表格式
 * @see org.apache.paimon.table.object.ObjectTable 对象存储表
 */
public interface IcebergTable extends Table {

    /**
     * 获取 Iceberg 表在文件系统中的位置。
     *
     * <p>该位置是 Iceberg 表的根目录,包含元数据目录和数据文件。
     * 典型的 Iceberg 表目录结构:
     * <pre>
     * /path/to/table/
     *   ├── metadata/       # 元数据目录
     *   │   ├── v1.metadata.json
     *   │   ├── v2.metadata.json
     *   │   └── snap-*.avro
     *   └── data/          # 数据目录
     *       └── ...
     * </pre>
     *
     * @return 表的文件系统位置路径
     */
    String location();

    /**
     * 创建带有动态选项的表副本。
     *
     * <p>该方法返回一个新的 IcebergTable 实例,其配置选项是原选项和动态选项的合并结果。
     * 动态选项会覆盖原有的同名选项。
     *
     * @param dynamicOptions 动态选项映射,用于覆盖或补充表的配置
     * @return 返回新的 IcebergTable 实例
     */
    @Override
    IcebergTable copy(Map<String, String> dynamicOptions);

    /**
     * 创建 IcebergTable 的构建器。
     *
     * <p>使用构建器模式可以方便地配置和创建 IcebergTable 实例。
     *
     * @return 返回新的 Builder 实例
     */
    static Builder builder() {
        return new Builder();
    }

    /**
     * IcebergTable 的构建器类。
     *
     * <p>该构建器提供了流式 API 来配置和创建 IcebergTable 实例。
     * 所有字段都是可选的,但建议至少设置 identifier、fileIO、rowType 和 location。
     *
     * <h3>使用示例</h3>
     * <pre>{@code
     * IcebergTable table = IcebergTable.builder()
     *     .identifier(Identifier.create("db", "table"))
     *     .fileIO(fileIO)
     *     .rowType(rowType)
     *     .partitionKeys(Arrays.asList("dt", "hour"))
     *     .location("hdfs://cluster/warehouse/db.db/table")
     *     .options(tableOptions)
     *     .comment("This is an Iceberg table")
     *     .uuid("unique-table-id")
     *     .build();
     * }</pre>
     */
    class Builder {

        /** 表的标识符,包含数据库名和表名。 */
        private Identifier identifier;

        /** 文件 I/O 接口,用于访问文件系统。 */
        private FileIO fileIO;

        /** 表的行类型定义,描述所有列的类型信息。 */
        private RowType rowType;

        /** 分区键列表,定义表的分区字段。 */
        private List<String> partitionKeys;

        /** 表在文件系统中的位置路径。 */
        private String location;

        /** 表的配置选项映射。 */
        private Map<String, String> options;

        /** 表的注释说明。 */
        private String comment;

        /** 表的唯一标识符 UUID。 */
        private String uuid;

        /**
         * 设置表的标识符。
         *
         * @param identifier 表标识符,包含数据库名和表名
         * @return 返回当前 Builder 实例,支持链式调用
         */
        public Builder identifier(Identifier identifier) {
            this.identifier = identifier;
            return this;
        }

        /**
         * 设置文件 I/O 接口。
         *
         * @param fileIO 文件 I/O 实现,用于访问底层文件系统
         * @return 返回当前 Builder 实例,支持链式调用
         */
        public Builder fileIO(FileIO fileIO) {
            this.fileIO = fileIO;
            return this;
        }

        /**
         * 设置表的行类型。
         *
         * @param rowType 行类型,定义表的所有列及其类型
         * @return 返回当前 Builder 实例,支持链式调用
         */
        public Builder rowType(RowType rowType) {
            this.rowType = rowType;
            return this;
        }

        /**
         * 设置分区键列表。
         *
         * @param partitionKeys 分区字段名称列表
         * @return 返回当前 Builder 实例,支持链式调用
         */
        public Builder partitionKeys(List<String> partitionKeys) {
            this.partitionKeys = partitionKeys;
            return this;
        }

        /**
         * 设置表的位置路径。
         *
         * @param location 表在文件系统中的根目录路径
         * @return 返回当前 Builder 实例,支持链式调用
         */
        public Builder location(String location) {
            this.location = location;
            return this;
        }

        /**
         * 设置表的配置选项。
         *
         * @param options 配置选项映射
         * @return 返回当前 Builder 实例,支持链式调用
         */
        public Builder options(Map<String, String> options) {
            this.options = options;
            return this;
        }

        /**
         * 设置表的注释。
         *
         * @param comment 表的描述性注释
         * @return 返回当前 Builder 实例,支持链式调用
         */
        public Builder comment(String comment) {
            this.comment = comment;
            return this;
        }

        /**
         * 设置表的 UUID。
         *
         * @param uuid 表的唯一标识符
         * @return 返回当前 Builder 实例,支持链式调用
         */
        public Builder uuid(String uuid) {
            this.uuid = uuid;
            return this;
        }

        /**
         * 构建并返回 IcebergTable 实例。
         *
         * @return 返回配置好的 IcebergTable 实例
         */
        public IcebergTable build() {
            return new IcebergTableImpl(
                    identifier, fileIO, rowType, partitionKeys, location, options, comment, uuid);
        }
    }
}
