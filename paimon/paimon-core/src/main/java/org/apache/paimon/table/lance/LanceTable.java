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

package org.apache.paimon.table.lance;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowType;

import java.util.Map;

/**
 * Lance 表格式接口,代表一个 Lance 格式的表。
 *
 * <p>该接口用于在 Paimon 系统中表示外部的 Lance 格式表,使得 Paimon 可以识别和管理 Lance 表的元数据。
 * <b>注意:当前 Paimon 尚不支持对 Lance 表进行读写操作。</b>
 *
 * <h3>什么是 Lance 表</h3>
 * <p>Lance 是一种为机器学习和 AI 工作负载优化的现代列式数据格式,具有以下特点:
 * <ul>
 *   <li>面向向量和多模态数据(图像、文本、嵌入向量等)</li>
 *   <li>高效的随机访问和向量搜索能力</li>
 *   <li>支持增量更新和版本管理</li>
 *   <li>专为 AI/ML 应用场景优化</li>
 *   <li>与 Apache Arrow 深度集成</li>
 * </ul>
 *
 * <h3>与其他表类型的关系</h3>
 * <ul>
 *   <li>继承 {@link org.apache.paimon.table.Table} 接口,作为 Paimon 表体系的一部分</li>
 *   <li>与 {@link org.apache.paimon.table.FileStoreTable} 并列,代表不同的表格式</li>
 *   <li>与 {@link org.apache.paimon.table.iceberg.IcebergTable} 类似,都是外部表格式的集成</li>
 *   <li>与 {@link org.apache.paimon.table.object.ObjectTable} 不同,Lance 表是针对 AI/ML 优化的结构化格式</li>
 * </ul>
 *
 * <h3>Lance vs Iceberg</h3>
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>Lance</th>
 *     <th>Iceberg</th>
 *   </tr>
 *   <tr>
 *     <td>主要用途</td>
 *     <td>AI/ML、向量搜索</td>
 *     <td>大数据分析</td>
 *   </tr>
 *   <tr>
 *     <td>数据类型</td>
 *     <td>向量、多模态数据</td>
 *     <td>传统结构化数据</td>
 *   </tr>
 *   <tr>
 *     <td>分区支持</td>
 *     <td>不支持(Lance 表不需要分区)</td>
 *     <td>支持</td>
 *   </tr>
 *   <tr>
 *     <td>查询模式</td>
 *     <td>向量相似度搜索</td>
 *     <td>SQL 查询</td>
 *   </tr>
 * </table>
 *
 * <h3>当前限制</h3>
 * <ul>
 *   <li>只支持元数据管理,不支持数据读写</li>
 *   <li>主要用于 Catalog 层面的表发现和元数据查询</li>
 *   <li>未来可能会支持通过 Paimon 读写 Lance 表</li>
 *   <li>Lance 表不支持分区,因此 partitionKeys 始终为空</li>
 * </ul>
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li>在 Paimon Catalog 中注册 AI/ML 数据集</li>
 *   <li>管理向量嵌入(embeddings)数据表</li>
 *   <li>存储多模态数据(图像、音频等的元数据和特征)</li>
 *   <li>在混合数据湖环境中统一管理多种表格式</li>
 * </ul>
 *
 * <h3>创建方式</h3>
 * <pre>{@code
 * LanceTable table = LanceTable.builder()
 *     .identifier(identifier)
 *     .fileIO(fileIO)
 *     .rowType(rowType)
 *     .location(location)
 *     .options(options)
 *     .comment("Vector embeddings table")
 *     .build();
 * }</pre>
 *
 * @see LanceTableImpl 该接口的实现类
 * @see org.apache.paimon.table.Table 表的基础接口
 * @see org.apache.paimon.table.iceberg.IcebergTable 另一种外部表格式
 * @see org.apache.paimon.table.object.ObjectTable 对象存储表
 */
public interface LanceTable extends Table {

    /**
     * 获取 Lance 表在文件系统中的位置。
     *
     * <p>该位置是 Lance 表的根目录,包含数据文件和元数据。
     * Lance 表目录结构示例:
     * <pre>
     * /path/to/table/
     *   ├── data/           # 数据文件
     *   │   └── *.lance
     *   └── _versions/      # 版本元数据
     *       └── *.manifest
     * </pre>
     *
     * @return 表的文件系统位置路径
     */
    String location();

    /**
     * 创建带有动态选项的表副本。
     *
     * <p>该方法返回一个新的 LanceTable 实例,其配置选项是原选项和动态选项的合并结果。
     * 动态选项会覆盖原有的同名选项。
     *
     * @param dynamicOptions 动态选项映射,用于覆盖或补充表的配置
     * @return 返回新的 LanceTable 实例
     */
    @Override
    LanceTable copy(Map<String, String> dynamicOptions);

    /**
     * 创建 LanceTable 的构建器。
     *
     * <p>使用构建器模式可以方便地配置和创建 LanceTable 实例。
     *
     * @return 返回新的 Builder 实例
     */
    static Builder builder() {
        return new Builder();
    }

    /**
     * LanceTable 的构建器类。
     *
     * <p>该构建器提供了流式 API 来配置和创建 LanceTable 实例。
     * 所有字段都是可选的,但建议至少设置 identifier、fileIO、rowType 和 location。
     *
     * <p>注意:Lance 表不支持分区,因此没有 partitionKeys 字段。
     *
     * <h3>使用示例</h3>
     * <pre>{@code
     * LanceTable table = LanceTable.builder()
     *     .identifier(Identifier.create("ml_db", "embeddings"))
     *     .fileIO(fileIO)
     *     .rowType(rowType)
     *     .location("s3://bucket/ml_data/embeddings")
     *     .options(tableOptions)
     *     .comment("User embeddings for recommendation")
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

        /** 表在文件系统中的位置路径。 */
        private String location;

        /** 表的配置选项映射。 */
        private Map<String, String> options;

        /** 表的注释说明。 */
        private String comment;

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
         * 构建并返回 LanceTable 实例。
         *
         * @return 返回配置好的 LanceTable 实例
         */
        public LanceTable build() {
            return new LanceTableImpl(identifier, fileIO, rowType, location, options, comment);
        }
    }
}
