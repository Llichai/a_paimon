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

package org.apache.paimon.catalog;

import org.apache.paimon.PagedList;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.Public;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.function.Function;
import org.apache.paimon.function.FunctionChange;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.rest.responses.GetTagResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Instant;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.utils.SnapshotNotExistException;
import org.apache.paimon.view.View;
import org.apache.paimon.view.ViewChange;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Catalog 接口 - Paimon 元数据管理的核心抽象
 *
 * <p>Catalog 负责管理 Paimon 的元数据层次结构,包括数据库、表、视图和函数的创建、读取、更新和删除操作。
 *
 * <p>层次结构:
 * <pre>
 * Catalog（元数据目录）
 *   ├─ Database（数据库）
 *   │   ├─ Table 1（表）
 *   │   ├─ Table 2
 *   │   ├─ View 1（视图）
 *   │   ├─ Function 1（函数）
 *   │   └─ ...
 *   ├─ Database 2
 *   └─ ...
 * </pre>
 *
 * <p>核心功能:
 * <ul>
 *   <li><b>数据库管理</b>: {@link #createDatabase}, {@link #dropDatabase}, {@link #alterDatabase}
 *   <li><b>表管理</b>: {@link #createTable}, {@link #dropTable}, {@link #alterTable}, {@link #renameTable}
 *   <li><b>表查询</b>: {@link #getTable}, {@link #listTables}, {@link #listTablesPaged}
 *   <li><b>分区管理</b>: {@link #createPartitions}, {@link #dropPartitions}, {@link #listPartitions}
 *   <li><b>视图管理</b>: {@link #createView}, {@link #dropView}, {@link #alterView}
 *   <li><b>版本管理</b>: {@link #commitSnapshot}, {@link #createTag}, {@link #createBranch}
 *   <li><b>函数管理</b>: {@link #createFunction}, {@link #dropFunction}, {@link #alterFunction}
 * </ul>
 *
 * <p>实现类:
 * <ul>
 *   <li>{@link FileSystemCatalog}: 基于文件系统（HDFS/S3/本地文件系统）的实现
 *   <li>{@link CachingCatalog}: 带缓存的 Catalog 包装器,提升查询性能
 *   <li>HiveCatalog: 基于 Hive Metastore（在 paimon-hive-connector 模块中）
 *   <li>JdbcCatalog: 基于 JDBC（在 paimon-jdbc-connector 模块中）
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 1. 创建 Catalog
 * Catalog catalog = new FileSystemCatalog(
 *     fileIO,
 *     new Path("hdfs://warehouse"),
 *     options
 * );
 *
 * // 2. 创建数据库
 * catalog.createDatabase("my_db", false);
 *
 * // 3. 创建表
 * Identifier identifier = Identifier.create("my_db", "my_table");
 * Schema schema = Schema.newBuilder()
 *     .column("id", DataTypes.INT())
 *     .column("name", DataTypes.STRING())
 *     .primaryKey("id")
 *     .build();
 * catalog.createTable(identifier, schema, false);
 *
 * // 4. 获取表
 * Table table = catalog.getTable(identifier);
 *
 * // 5. 写入数据
 * BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
 * // ... 写入逻辑
 * }</pre>
 *
 * <p>版本管理支持:
 * <p>如果 Catalog 实现支持版本管理（{@link #supportsVersionManagement()} 返回 true）,
 * 则可以使用以下功能:
 * <ul>
 *   <li>快照提交: {@link #commitSnapshot} - 原子性地提交新快照
 *   <li>标签管理: {@link #createTag}, {@link #deleteTag} - 为快照创建不可变引用
 *   <li>分支管理: {@link #createBranch}, {@link #dropBranch} - 创建独立的数据演进分支
 *   <li>时间旅行: {@link #loadSnapshot} - 查询历史版本
 *   <li>回滚操作: {@link #rollbackTo} - 回滚到指定快照或标签
 * </ul>
 *
 * <p>线程安全:
 * <p>Catalog 实现应该是线程安全的。多个线程可以同时调用 Catalog 的方法。
 * 但是,某些操作（如 {@link #createTable}, {@link #dropTable}）可能需要获取锁来保证原子性。
 *
 * @see CatalogFactory
 * @since 0.4.0
 */
@Public
public interface Catalog extends AutoCloseable {

    // ======================= database methods（数据库方法）===============================

    /**
     * 获取此 Catalog 中所有数据库的名称列表
     *
     * @return 所有数据库名称的列表
     */
    List<String> listDatabases();

    /**
     * 获取此 Catalog 中所有数据库的分页列表
     *
     * <p>用于大规模 Catalog 的分页查询,避免一次性加载所有数据库。
     *
     * @param maxResults 可选参数,指定结果中包含的最大结果数。如果未指定或设置为 0,
     *                   将返回默认的最大结果数
     * @param pageToken 可选参数,指示下一页令牌,允许列表从特定点开始
     * @param databaseNamePattern SQL LIKE 模式 (%) 用于数据库名称过滤。如果未设置或为空,
     *                            将返回所有数据库。目前仅支持前缀匹配
     * @return 此 Catalog 中具有指定页面大小的数据库名称列表和下一页令牌;
     *         如果 Catalog 不支持 {@link #supportsListObjectsPaged()},则返回所有数据库名称的列表
     * @throws UnsupportedOperationException 如果 databaseNamePattern 不为 null 且不支持 {@link #supportsListByPattern()}
     */
    PagedList<String> listDatabasesPaged(
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String databaseNamePattern);

    /**
     * 创建数据库（无属性版本）
     *
     * <p>这是一个便捷方法,内部调用 {@link #createDatabase(String, boolean, Map)} 并传入空属性。
     *
     * @param name 要创建的数据库名称
     * @param ignoreIfExists 如果为 true,当数据库已存在时不抛出异常;如果为 false,则抛出异常
     * @throws DatabaseAlreadyExistException 如果数据库已存在且 ignoreIfExists 为 false
     * @see #createDatabase(String, boolean, Map)
     */
    default void createDatabase(String name, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException {
        createDatabase(name, ignoreIfExists, Collections.emptyMap());
    }

    /**
     * 创建带属性的数据库
     *
     * <p>属性可以包括:
     * <ul>
     *   <li>{@link #COMMENT_PROP}: 数据库注释
     *   <li>{@link #OWNER_PROP}: 数据库所有者
     *   <li>{@link #DB_LOCATION_PROP}: 数据库存储路径（用于外部数据库）
     * </ul>
     *
     * @param name 要创建的数据库名称
     * @param ignoreIfExists 标志位,指定当数据库已存在时的行为:
     *                       如果为 false,抛出 DatabaseAlreadyExistException;
     *                       如果为 true,不做任何操作
     * @param properties 与数据库关联的属性
     * @throws DatabaseAlreadyExistException 如果给定数据库已存在且 ignoreIfExists 为 false
     */
    void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
            throws DatabaseAlreadyExistException;

    /**
     * 根据给定名称返回 {@link Database} 对象
     *
     * <p>Database 对象包含数据库的元数据信息,如名称、注释、属性等。
     *
     * @param name 数据库名称
     * @return 请求的 {@link Database} 对象
     * @throws DatabaseNotExistException 如果请求的数据库不存在
     */
    Database getDatabase(String name) throws DatabaseNotExistException;

    /**
     * 删除数据库
     *
     * <p>删除行为由 cascade 参数控制:
     * <ul>
     *   <li>cascade = true: 级联删除数据库中的所有表和函数,然后删除数据库
     *   <li>cascade = false: 如果数据库非空,抛出 DatabaseNotEmptyException
     * </ul>
     *
     * @param name 要删除的数据库名称
     * @param ignoreIfNotExists 标志位,指定当数据库不存在时的行为:
     *                          如果为 false,抛出异常;如果为 true,不做任何操作
     * @param cascade 标志位,指定当数据库包含表或函数时的行为:
     *                如果为 true,删除数据库中的所有表和函数,然后删除数据库;
     *                如果为 false,抛出异常
     * @throws DatabaseNotExistException 如果数据库不存在且 ignoreIfNotExists 为 false
     * @throws DatabaseNotEmptyException 如果给定数据库非空且 cascade 为 false
     */
    void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException;

    /**
     * 修改数据库属性
     *
     * <p>可以通过 {@link PropertyChange} 来:
     * <ul>
     *   <li>设置新属性: {@link PropertyChange#setProperty}
     *   <li>移除属性: {@link PropertyChange#removeProperty}
     * </ul>
     *
     * @param name 要修改的数据库名称
     * @param changes 属性变更列表
     * @param ignoreIfNotExists 标志位,指定当数据库不存在时的行为:
     *                          如果为 false,抛出异常;如果为 true,不做任何操作
     * @throws DatabaseNotExistException 如果给定数据库不存在且 ignoreIfNotExists 为 false
     */
    void alterDatabase(String name, List<PropertyChange> changes, boolean ignoreIfNotExists)
            throws DatabaseNotExistException;

    // ======================= table methods（表方法）===============================

    /**
     * 根据给定的 {@link Identifier} 返回 {@link Table} 对象
     *
     * <p>系统表可以通过 '$' 分隔符获取,例如:
     * <ul>
     *   <li>"db.table$snapshots" - 快照系统表
     *   <li>"db.table$schemas" - Schema 系统表
     *   <li>"db.table$options" - 选项系统表
     * </ul>
     *
     * @param identifier 表的路径标识符
     * @return 请求的表对象
     * @throws TableNotExistException 如果目标表不存在
     */
    Table getTable(Identifier identifier) throws TableNotExistException;

    /**
     * 根据给定的 tableId 返回 {@link Table} 对象
     *
     * <p>表 ID 是表的唯一标识符,在表重命名后仍然保持不变。
     *
     * @param tableId 表的 ID
     * @return 请求的表对象
     * @throws TableIdNotExistException 如果目标表不存在
     */
    Table getTableById(String tableId) throws TableIdNotExistException;

    /**
     * 获取此数据库下所有表的名称。如果不存在任何表,返回空列表
     *
     * <p>注意: 系统表不会被列出。
     *
     * @param databaseName 数据库名称
     * @return 此数据库中所有表名称的列表
     * @throws DatabaseNotExistException 如果数据库不存在
     */
    List<String> listTables(String databaseName) throws DatabaseNotExistException;

    /**
     * Get paged list names of tables under this database. An empty list is returned if none exists.
     *
     * <p>NOTE: System tables will not be listed.
     *
     * @param databaseName Name of the database to list tables.
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @param tableNamePattern A sql LIKE pattern (%) for table names. All tables will be returned
     *     if not set or empty. Currently, only prefix matching is supported.
     * @return a list of the names of tables with provided page size in this database and next page
     *     token, or a list of the names of all tables in this database if the catalog does not
     *     {@link #supportsListObjectsPaged()}.
     * @throws DatabaseNotExistException if the database does not exist.
     * @throws UnsupportedOperationException if does not {@link #supportsListByPattern()}
     */
    PagedList<String> listTablesPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tableNamePattern,
            @Nullable String tableType)
            throws DatabaseNotExistException;

    /**
     * Get paged list of table details under this database. An empty list is returned if none
     * exists.
     *
     * <p>NOTE: System tables will not be listed.
     *
     * @param databaseName Name of the database to list table details.
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @param tableNamePattern A sql LIKE pattern (%) for table names. All table details will be
     *     returned if not set or empty. Currently, only prefix matching is supported.
     * @param tableType Optional parameter to filter tables by table type. All table types will be
     *     returned if not set or empty.
     * @return a list of the table details with provided page size in this database and next page
     *     token, or a list of the details of all tables in this database if the catalog does not
     *     {@link #supportsListObjectsPaged()}.
     * @throws DatabaseNotExistException if the database does not exist
     * @throws UnsupportedOperationException if does not {@link #supportsListByPattern()}
     */
    PagedList<Table> listTableDetailsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tableNamePattern,
            @Nullable String tableType)
            throws DatabaseNotExistException;

    /**
     * Gets an array of tables for a catalog.
     *
     * <p>NOTE: System tables will not be listed.
     *
     * @param databaseNamePattern A sql LIKE pattern (%) for database names. All databases will be
     *     returned if not set or empty. Currently, only prefix matching is supported.
     * @param tableNamePattern A sql LIKE pattern (%) for table names. All tables will be returned
     *     if not set or empty. Currently, only prefix matching is supported.
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @return a list of the tables with provided page size under this databaseNamePattern &
     *     tableNamePattern and next page token
     * @throws UnsupportedOperationException if does not {@link #supportsListObjectsPaged()} or does
     *     not {@link #supportsListByPattern()}.
     */
    default PagedList<Identifier> listTablesPagedGlobally(
            @Nullable String databaseNamePattern,
            @Nullable String tableNamePattern,
            @Nullable Integer maxResults,
            @Nullable String pageToken) {
        throw new UnsupportedOperationException(
                "Current Catalog does not support listTablesPagedGlobally");
    }

    /**
     * 删除表
     *
     * <p>注意: 系统表无法被删除。
     *
     * <p>警告: 对于对象存储（如 S3、OSS）,删除操作不是原子的。
     * 在失败情况下,可能只删除了部分文件。
     *
     * @param identifier 要删除的表的路径
     * @param ignoreIfNotExists 标志位,指定当表不存在时的行为:
     *                          如果为 false,抛出异常;如果为 true,不做任何操作
     * @throws TableNotExistException 如果表不存在且 ignoreIfNotExists 为 false
     */
    void dropTable(Identifier identifier, boolean ignoreIfNotExists) throws TableNotExistException;

    /**
     * 创建新表
     *
     * <p>注意: 系统表无法被创建。
     *
     * <p>Schema 定义了表的结构,包括:
     * <ul>
     *   <li>列定义（名称、类型、注释）
     *   <li>主键
     *   <li>分区键
     *   <li>表选项
     * </ul>
     *
     * @param identifier 要创建的表的路径
     * @param schema 表定义
     * @param ignoreIfExists 标志位,指定当表已存在时的行为:
     *                       如果为 false,抛出 TableAlreadyExistException;
     *                       如果为 true,不做任何操作
     * @throws TableAlreadyExistException 如果表已存在且 ignoreIfExists 为 false
     * @throws DatabaseNotExistException 如果 identifier 中的数据库不存在
     */
    void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException;

    /**
     * 重命名表
     *
     * <p>注意: 如果使用对象存储（如 S3、OSS）,请谨慎使用此语法,
     * 因为对象存储的重命名不是原子的,在失败情况下可能只移动了部分文件。
     *
     * <p>注意: 系统表无法被重命名。
     *
     * @param fromTable 需要重命名的表名
     * @param toTable 新表名
     * @param ignoreIfNotExists 标志位,指定当表不存在时的行为:
     *                          如果为 false,抛出异常;如果为 true,不做任何操作
     * @throws TableNotExistException 如果 fromTable 不存在
     * @throws TableAlreadyExistException 如果 toTable 已存在
     */
    void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException;

    /**
     * 通过 {@link SchemaChange} 修改现有表
     *
     * <p>注意: 系统表无法被修改。
     *
     * <p>支持的 Schema 变更包括:
     * <ul>
     *   <li>{@link SchemaChange#addColumn} - 添加新列
     *   <li>{@link SchemaChange#dropColumn} - 删除列
     *   <li>{@link SchemaChange#renameColumn} - 重命名列
     *   <li>{@link SchemaChange#updateColumnType} - 更新列类型
     *   <li>{@link SchemaChange#updateColumnComment} - 更新列注释
     *   <li>{@link SchemaChange#updateColumnPosition} - 更新列位置
     *   <li>{@link SchemaChange#setOption} - 设置表选项
     *   <li>{@link SchemaChange#removeOption} - 移除表选项
     * </ul>
     *
     * @param identifier 要修改的表的路径
     * @param changes Schema 变更列表
     * @param ignoreIfNotExists 标志位,指定当表不存在时的行为:
     *                          如果为 false,抛出异常;如果为 true,不做任何操作
     * @throws TableNotExistException 如果表不存在
     * @throws ColumnAlreadyExistException 如果要添加的列已存在
     * @throws ColumnNotExistException 如果要操作的列不存在
     */
    void alterTable(Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException;

    /**
     * 使缓存的表元数据失效
     *
     * <p>如果表已加载或缓存,则删除缓存数据。如果表不存在或未缓存,不做任何操作。
     * 调用此方法不应查询远程服务。
     *
     * <p>使用场景:
     * <ul>
     *   <li>外部进程修改了表的 Schema 或选项
     *   <li>需要强制刷新缓存以获取最新元数据
     * </ul>
     *
     * @param identifier 表标识符
     */
    default void invalidateTable(Identifier identifier) {}

    /**
     * Modify an existing table from a {@link SchemaChange}.
     *
     * <p>NOTE: System tables can not be altered.
     *
     * @param identifier path of the table to be modified
     * @param change the schema change
     * @param ignoreIfNotExists flag to specify behavior when the table does not exist: if set to
     *     false, throw an exception, if set to true, do nothing.
     * @throws TableNotExistException if the table does not exist
     */
    default void alterTable(Identifier identifier, SchemaChange change, boolean ignoreIfNotExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        alterTable(identifier, Collections.singletonList(change), ignoreIfNotExists);
    }

    // ======================= partition methods（分区方法）===============================

    /**
     * 标记指定表的分区为完成状态。对于不存在的分区,将直接创建分区
     *
     * <p>此方法用于显式标记分区已就绪,可以被查询引擎发现和读取。
     *
     * @param identifier 要标记完成分区的表的路径
     * @param partitions 要标记为完成的分区列表,每个分区由 Map<分区列名, 分区值> 表示
     * @throws TableNotExistException 如果表不存在
     */
    void markDonePartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException;

    /**
     * 获取表的所有分区的 Partition 对象列表
     *
     * <p>Partition 对象包含分区的元数据,如分区值、记录数、文件数等统计信息。
     *
     * @param identifier 要列出分区的表的路径
     * @return 分区对象列表
     * @throws TableNotExistException 如果表不存在
     */
    List<Partition> listPartitions(Identifier identifier) throws TableNotExistException;

    /**
     * Get paged partition list of the table, the partition list will be returned in descending
     * order.
     *
     * @param identifier path of the table to list partitions
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @param partitionNamePattern A sql LIKE pattern (%) for partition names. All partitions will
     *     be returned if not set or empty. Currently, only prefix matching is supported.
     * @return a list of the partitions with provided page size(@param maxResults) in this table and
     *     next page token, or a list of all partitions of the table if the catalog does not {@link
     *     #supportsListObjectsPaged()}.
     * @throws TableNotExistException if the table does not exist
     * @throws UnsupportedOperationException if does not {@link #supportsListByPattern()}
     */
    PagedList<Partition> listPartitionsPaged(
            Identifier identifier,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String partitionNamePattern)
            throws TableNotExistException;

    // ======================= view methods ===============================

    /**
     * Return a {@link View} identified by the given {@link Identifier}.
     *
     * @param identifier Path of the view
     * @return The requested view
     * @throws ViewNotExistException if the target does not exist
     */
    default View getView(Identifier identifier) throws ViewNotExistException {
        throw new ViewNotExistException(identifier);
    }

    /**
     * Drop a view.
     *
     * @param identifier Path of the view to be dropped
     * @param ignoreIfNotExists Flag to specify behavior when the view does not exist: if set to
     *     false, throw an exception, if set to true, do nothing.
     * @throws ViewNotExistException if the view does not exist
     */
    default void dropView(Identifier identifier, boolean ignoreIfNotExists)
            throws ViewNotExistException {
        throw new UnsupportedOperationException();
    }

    /**
     * Create a new view.
     *
     * @param identifier path of the view to be created
     * @param view the view definition
     * @param ignoreIfExists flag to specify behavior when a view already exists at the given path:
     *     if set to false, it throws a ViewAlreadyExistException, if set to true, do nothing.
     * @throws ViewAlreadyExistException if view already exists and ignoreIfExists is false
     * @throws DatabaseNotExistException if the database in identifier doesn't exist
     */
    default void createView(Identifier identifier, View view, boolean ignoreIfExists)
            throws ViewAlreadyExistException, DatabaseNotExistException {
        throw new UnsupportedOperationException();
    }

    /**
     * Get names of all views under this database. An empty list is returned if none exists.
     *
     * @return a list of the names of all views in this database
     * @throws DatabaseNotExistException if the database does not exist
     */
    default List<String> listViews(String databaseName) throws DatabaseNotExistException {
        return Collections.emptyList();
    }

    /**
     * Get paged list names of views under this database. An empty list is returned if none view
     * exists.
     *
     * @param databaseName Name of the database to list views.
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @param viewNamePattern A sql LIKE pattern (%) for view names. All views will be returned if
     *     not set or empty. Currently, only prefix matching is supported.
     * @return a list of the names of views with provided page size in this database and next page
     *     token, or a list of the names of all views in this database if the catalog does not
     *     {@link #supportsListObjectsPaged()}.
     * @throws DatabaseNotExistException if the database does not exist
     * @throws UnsupportedOperationException if it does not {@link #supportsListByPattern()}
     */
    default PagedList<String> listViewsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String viewNamePattern)
            throws DatabaseNotExistException {
        return new PagedList<>(listViews(databaseName), null);
    }

    /**
     * Get paged list view details under this database. An empty list is returned if none view
     * exists.
     *
     * @param databaseName Name of the database to list views.
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @param viewNamePattern A sql LIKE pattern (%) for view names. All view details will be
     *     returned if not set or empty. Currently, only prefix matching is supported.
     * @return a list of the view details with provided page size (@param maxResults) in this
     *     database and next page token, or a list of the details of all views in this database if
     *     the catalog does not {@link #supportsListObjectsPaged()}.
     * @throws DatabaseNotExistException if the database does not exist
     * @throws UnsupportedOperationException if it does not {@link #supportsListByPattern()}
     */
    default PagedList<View> listViewDetailsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String viewNamePattern)
            throws DatabaseNotExistException {
        return new PagedList<>(Collections.emptyList(), null);
    }

    /**
     * Gets an array of views for a catalog.
     *
     * @param databaseNamePattern A sql LIKE pattern (%) for database names. All databases will be
     *     returned if not set or empty. Currently, only prefix matching is supported.
     * @param viewNamePattern A sql LIKE pattern (%) for view names. All views will be returned if
     *     not set or empty. Currently, only prefix matching is supported.
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @return a list of the views with provided page size under this databaseNamePattern &
     *     viewNamePattern and next page token
     * @throws UnsupportedOperationException if it does not {@link #supportsListObjectsPaged()} or
     *     does not {@link #supportsListByPattern()}}.
     */
    default PagedList<Identifier> listViewsPagedGlobally(
            @Nullable String databaseNamePattern,
            @Nullable String viewNamePattern,
            @Nullable Integer maxResults,
            @Nullable String pageToken) {
        throw new UnsupportedOperationException(
                "Current Catalog does not support listViewsPagedGlobally");
    }

    /**
     * Rename a view.
     *
     * @param fromView identifier of the view to rename
     * @param toView new view identifier
     * @throws ViewNotExistException if the fromView does not exist
     * @throws ViewAlreadyExistException if the toView already exists
     */
    default void renameView(Identifier fromView, Identifier toView, boolean ignoreIfNotExists)
            throws ViewNotExistException, ViewAlreadyExistException {
        throw new UnsupportedOperationException();
    }

    /**
     * Alter a view.
     *
     * @param view identifier of the view to alter
     * @param viewChanges - changes of view
     * @param ignoreIfNotExists Flag to specify behavior when the view does not exist
     * @throws ViewNotExistException if the view does not exist
     * @throws DialectAlreadyExistException if the dialect already exists
     * @throws DialectNotExistException if the dialect not exists
     */
    default void alterView(Identifier view, List<ViewChange> viewChanges, boolean ignoreIfNotExists)
            throws ViewNotExistException, DialectAlreadyExistException, DialectNotExistException {
        throw new UnsupportedOperationException();
    }

    // ======================= repair methods ===============================

    /**
     * Repair the entire Catalog, repair the metadata in the metastore consistent with the metadata
     * in the filesystem, register missing tables in the metastore.
     */
    default void repairCatalog() {
        throw new UnsupportedOperationException();
    }

    /**
     * Repair the entire database, repair the metadata in the metastore consistent with the metadata
     * in the filesystem, register missing tables in the metastore.
     */
    default void repairDatabase(String databaseName) {
        throw new UnsupportedOperationException();
    }

    /**
     * Repair the table, repair the metadata in the metastore consistent with the metadata in the
     * filesystem.
     */
    default void repairTable(Identifier identifier) throws TableNotExistException {
        throw new UnsupportedOperationException();
    }

    /**
     * Register a paimon table to the catalog if it does not exist. It is an asynchronous operation
     *
     * @param identifier a table identifier
     * @param path the path of the table
     * @throws TableAlreadyExistException if the table already exists in the catalog.
     */
    default void registerTable(Identifier identifier, String path)
            throws TableAlreadyExistException {
        throw new UnsupportedOperationException();
    }

    /**
     * Whether this catalog supports list objects paged. If not, corresponding methods will fall
     * back to listing all objects. For example, {@link #listTablesPaged(String, Integer, String,
     * String, String)} would fall back to {@link #listTables(String)}.
     *
     * <ul>
     *   <li>{@link #listDatabasesPaged(Integer, String, String)}.
     *   <li>{@link #listTablesPaged(String, Integer, String, String, String)}.
     *   <li>{@link #listTableDetailsPaged(String, Integer, String, String, String)}.
     *   <li>{@link #listViewsPaged(String, Integer, String, String)}.
     *   <li>{@link #listViewDetailsPaged(String, Integer, String, String)}.
     *   <li>{@link #listPartitionsPaged(Identifier, Integer, String, String)}.
     * </ul>
     */
    boolean supportsListObjectsPaged();

    /**
     * Whether this catalog supports name pattern filter when list objects paged. If not,
     * corresponding methods will throw exception if name pattern provided.
     *
     * <ul>
     *   <li>{@link #listDatabasesPaged(Integer, String, String)}.
     *   <li>{@link #listTablesPaged(String, Integer, String, String, String)}.
     *   <li>{@link #listTableDetailsPaged(String, Integer, String, String, String)}.
     *   <li>{@link #listViewsPaged(String, Integer, String, String)}.
     *   <li>{@link #listViewDetailsPaged(String, Integer, String, String)}.
     *   <li>{@link #listPartitionsPaged(Identifier, Integer, String, String)}.
     * </ul>
     */
    default boolean supportsListByPattern() {
        return false;
    }

    default boolean supportsListTableByType() {
        return false;
    }

    // ==================== Version management methods（版本管理方法）==========================

    /**
     * 此 Catalog 是否支持表的版本管理。如果不支持,相应方法将抛出 {@link UnsupportedOperationException},
     * 影响以下方法:
     *
     * <ul>
     *   <li>{@link #commitSnapshot(Identifier, String, Snapshot, List)} - 提交快照
     *   <li>{@link #loadSnapshot(Identifier)} - 加载快照
     *   <li>{@link #rollbackTo(Identifier, Instant)} - 回滚到指定版本
     *   <li>{@link #createBranch(Identifier, String, String)} - 创建分支
     *   <li>{@link #dropBranch(Identifier, String)} - 删除分支
     *   <li>{@link #listBranches(Identifier)} - 列出分支
     *   <li>{@link #getTag(Identifier, String)} - 获取标签
     *   <li>{@link #createTag(Identifier, String, Long, String, boolean)} - 创建标签
     *   <li>{@link #listTagsPaged(Identifier, Integer, String, String)} - 列出标签
     *   <li>{@link #deleteTag(Identifier, String)} - 删除标签
     * </ul>
     *
     * <p>版本管理功能说明:
     * <ul>
     *   <li><b>快照</b>: 表在某个时间点的不可变状态
     *   <li><b>标签</b>: 快照的命名引用,用于标记重要版本
     *   <li><b>分支</b>: 独立的数据演进路径,支持并行开发
     * </ul>
     *
     * @return 如果支持版本管理返回 true,否则返回 false
     */
    boolean supportsVersionManagement();

    /**
     * 为给定 {@link Identifier} 标识的表提交 {@link Snapshot}
     *
     * <p>快照提交是原子操作,保证:
     * <ul>
     *   <li>快照 ID 的单调递增
     *   <li>并发提交的串行化
     *   <li>提交失败时的回滚
     * </ul>
     *
     * <p>提交流程:
     * <ol>
     *   <li>验证 tableUuid 匹配,避免错误提交
     *   <li>写入快照文件到临时位置
     *   <li>使用 Catalog 锁保证原子性
     *   <li>重命名快照文件到最终位置
     * </ol>
     *
     * @param identifier 表的路径
     * @param tableUuid 表的 UUID,用于避免错误提交
     * @param snapshot 要提交的快照
     * @param statistics 此次变更的统计信息
     * @return 提交成功返回 true,失败返回 false
     * @throws Catalog.TableNotExistException 如果目标表不存在
     * @throws UnsupportedOperationException 如果 Catalog 不支持 {@link #supportsVersionManagement()}
     */
    boolean commitSnapshot(
            Identifier identifier,
            @Nullable String tableUuid,
            Snapshot snapshot,
            List<PartitionStatistics> statistics)
            throws Catalog.TableNotExistException;

    /**
     * 返回给定 {@link Identifier} 标识的表的快照
     *
     * <p>返回表的最新快照,如果表还没有任何快照则返回 {@link Optional#empty()}。
     *
     * @param identifier 表的路径
     * @return 表的请求快照,如果不存在返回 empty
     * @throws Catalog.TableNotExistException 如果目标表不存在
     * @throws UnsupportedOperationException 如果 Catalog 不支持 {@link #supportsVersionManagement()}
     */
    Optional<TableSnapshot> loadSnapshot(Identifier identifier)
            throws Catalog.TableNotExistException;

    /**
     * 返回给定版本的表快照。版本解析顺序:
     *
     * <ul>
     *   <li>1. 如果是 'EARLIEST',获取最早的快照
     *   <li>2. 如果是 'LATEST',获取最新的快照
     *   <li>3. 如果是数字,按快照 ID 获取快照
     *   <li>4. 否则尝试从标签名称获取快照
     * </ul>
     *
     * <p>使用示例:
     * <pre>{@code
     * // 获取最新快照
     * Optional<Snapshot> latest = catalog.loadSnapshot(identifier, "LATEST");
     *
     * // 获取指定快照 ID
     * Optional<Snapshot> snapshot = catalog.loadSnapshot(identifier, "10");
     *
     * // 获取指定标签
     * Optional<Snapshot> tagged = catalog.loadSnapshot(identifier, "v1.0");
     * }</pre>
     *
     * @param identifier 表的路径
     * @param version 快照版本
     * @return 请求的快照,如果不存在返回 empty
     * @throws Catalog.TableNotExistException 如果目标表不存在
     * @throws UnsupportedOperationException 如果 Catalog 不支持 {@link #supportsVersionManagement()}
     */
    Optional<Snapshot> loadSnapshot(Identifier identifier, String version)
            throws Catalog.TableNotExistException;

    /**
     * Get paged snapshot list of the table, the snapshot list will be returned in descending order.
     *
     * @param identifier path of the table to list partitions
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @return a list of the snapshots with provided page size(@param maxResults) in this table and
     *     next page token, or a list of all snapshots of the table if the catalog does not {@link
     *     #supportsListObjectsPaged()}.
     * @throws TableNotExistException if the table does not exist
     * @throws UnsupportedOperationException if the catalog does not {@link
     *     #supportsVersionManagement()}
     */
    PagedList<Snapshot> listSnapshotsPaged(
            Identifier identifier, @Nullable Integer maxResults, @Nullable String pageToken)
            throws TableNotExistException;

    /**
     * 根据给定的 {@link Identifier} 和 instant 回滚表
     *
     * <p>回滚操作会:
     * <ul>
     *   <li>将表状态恢复到指定的快照或标签
     *   <li>不会物理删除数据文件（由过期任务处理）
     *   <li>更新表的 LATEST 快照指针
     * </ul>
     *
     * @param identifier 表的路径
     * @param instant 如快照 ID 或标签名称
     * @throws Catalog.TableNotExistException 如果表不存在
     * @throws UnsupportedOperationException 如果 Catalog 不支持 {@link #supportsVersionManagement()}
     */
    default void rollbackTo(Identifier identifier, Instant instant)
            throws Catalog.TableNotExistException {
        rollbackTo(identifier, instant, null);
    }

    /**
     * 根据给定的 {@link Identifier} 和 instant 回滚表
     *
     * <p>此版本支持乐观锁,仅当最新快照是 fromSnapshot 时才成功回滚。
     *
     * @param identifier 表的路径
     * @param instant 如快照 ID 或标签名称
     * @param fromSnapshot 源快照,仅当最新快照是此快照时才成功
     * @throws Catalog.TableNotExistException 如果表不存在
     * @throws UnsupportedOperationException 如果 Catalog 不支持 {@link #supportsVersionManagement()}
     */
    void rollbackTo(Identifier identifier, Instant instant, @Nullable Long fromSnapshot)
            throws Catalog.TableNotExistException;

    /**
     * 为此表创建新分支。默认情况下,将使用最新 Schema 创建空分支。
     * 如果提供 {@code #fromTag},将从标签创建分支,并继承其数据文件
     *
     * <p>分支用途:
     * <ul>
     *   <li>并行开发: 在不影响主分支的情况下进行实验性更改
     *   <li>环境隔离: 为不同环境（开发、测试、生产）创建独立数据副本
     *   <li>热修复: 从生产标签创建分支进行紧急修复
     * </ul>
     *
     * @param identifier 表的路径,不能是系统表或分支名称
     * @param branch 分支名称
     * @param fromTag 从指定标签创建分支（可选）
     * @throws TableNotExistException 如果 identifier 中的表不存在
     * @throws BranchAlreadyExistException 如果分支已存在
     * @throws TagNotExistException 如果标签不存在
     * @throws UnsupportedOperationException 如果 Catalog 不支持 {@link #supportsVersionManagement()}
     */
    void createBranch(Identifier identifier, String branch, @Nullable String fromTag)
            throws TableNotExistException, BranchAlreadyExistException, TagNotExistException;

    /**
     * 删除此表的分支
     *
     * <p>注意: 删除分支是危险操作,会永久删除分支的所有数据。
     *
     * @param identifier 表的路径,不能是系统表或分支名称
     * @param branch 分支名称
     * @throws BranchNotExistException 如果分支不存在
     * @throws UnsupportedOperationException 如果 Catalog 不支持 {@link #supportsVersionManagement()}
     */
    void dropBranch(Identifier identifier, String branch) throws BranchNotExistException;

    /**
     * 将分支快进到主分支
     *
     * <p>快进操作将分支的快照指针移动到主分支的最新快照,用于合并分支的更改。
     *
     * @param identifier 表的路径,不能是系统表或分支名称
     * @param branch 分支名称
     * @throws BranchNotExistException 如果分支不存在
     * @throws UnsupportedOperationException 如果 Catalog 不支持 {@link #supportsVersionManagement()}
     */
    void fastForward(Identifier identifier, String branch) throws BranchNotExistException;

    /**
     * 列出表的所有分支
     *
     * @param identifier 表的路径,不能是系统表或分支名称
     * @return 分支名称列表
     * @throws TableNotExistException 如果 identifier 中的表不存在
     * @throws UnsupportedOperationException 如果 Catalog 不支持 {@link #supportsVersionManagement()}
     */
    List<String> listBranches(Identifier identifier) throws TableNotExistException;

    /**
     * Get tag for table.
     *
     * @param identifier path of the table, cannot be system name.
     * @param tagName tag name
     * @return {@link GetTagResponse} containing tag information
     * @throws TableNotExistException if the table does not exist
     * @throws TagNotExistException if the tag does not exist
     * @throws UnsupportedOperationException if the catalog does not {@link
     *     #supportsVersionManagement()}
     */
    GetTagResponse getTag(Identifier identifier, String tagName)
            throws TableNotExistException, TagNotExistException;

    /**
     * Create tag for table.
     *
     * @param identifier path of the table, cannot be system name.
     * @param tagName tag name
     * @param snapshotId optional snapshot id, if not provided uses latest snapshot
     * @param timeRetained optional time retained as string (e.g., "1d", "12h", "30m")
     * @param ignoreIfExists if true, ignore if tag already exists
     * @throws TableNotExistException if the table does not exist
     * @throws SnapshotNotExistException if the snapshot does not exist
     * @throws TagAlreadyExistException if the tag already exists and ignoreIfExists is false
     * @throws UnsupportedOperationException if the catalog does not {@link
     *     #supportsVersionManagement()}
     */
    void createTag(
            Identifier identifier,
            String tagName,
            @Nullable Long snapshotId,
            @Nullable String timeRetained,
            boolean ignoreIfExists)
            throws TableNotExistException, SnapshotNotExistException, TagAlreadyExistException;

    /**
     * Get paged list names of tags under this table. An empty list is returned if none tag exists.
     *
     * @param identifier path of the table, cannot be system name.
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @param tagNamePrefix A prefix for tag names. All tags will be returned if not set or empty.
     * @return a list of the names of tags with provided page size in this table and next page
     *     token, or a list of the names of all tags in this table if the catalog does not {@link
     *     #supportsListObjectsPaged()}.
     * @throws TableNotExistException if the table does not exist
     * @throws UnsupportedOperationException if the catalog does not {@link
     *     #supportsVersionManagement()} or it does not {@link #supportsListByPattern()}
     */
    PagedList<String> listTagsPaged(
            Identifier identifier,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tagNamePrefix)
            throws TableNotExistException;

    /**
     * Delete tag for table.
     *
     * @param identifier path of the table, cannot be system name.
     * @param tagName tag name
     * @throws TableNotExistException if the table does not exist
     * @throws TagNotExistException if the tag does not exist
     * @throws UnsupportedOperationException if the catalog does not {@link
     *     #supportsVersionManagement()}
     */
    void deleteTag(Identifier identifier, String tagName)
            throws TableNotExistException, TagNotExistException;

    // ==================== Partition Modifications ==========================

    /**
     * Create partitions of the specify table. Ignore existing partitions.
     *
     * @param identifier path of the table to create partitions
     * @param partitions partitions to be created
     * @throws TableNotExistException if the table does not exist
     */
    void createPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException;

    /**
     * Drop partitions of the specify table. Ignore non-existent partitions.
     *
     * @param identifier path of the table to drop partitions
     * @param partitions partitions to be deleted
     * @throws TableNotExistException if the table does not exist
     */
    void dropPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException;

    /**
     * Alter partitions of the specify table. For non-existent partitions, partitions will be
     * created directly.
     *
     * @param identifier path of the table to alter partitions
     * @param partitions partitions to be altered
     * @throws TableNotExistException if the table does not exist
     */
    void alterPartitions(Identifier identifier, List<PartitionStatistics> partitions)
            throws TableNotExistException;

    // ======================= Function methods ===============================

    /**
     * Get the names of all functions in this catalog.
     *
     * @return a list of the names of all functions
     * @throws DatabaseNotExistException if the database does not exist
     */
    List<String> listFunctions(String databaseName) throws DatabaseNotExistException;

    /**
     * Get paged list names of function under this database. An empty list is returned if none
     * function exists.
     *
     * @param databaseName Name of the database to list function.
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @param functionNamePattern A sql LIKE pattern (%) for function names. All function will be
     *     returned if not set or empty. Currently, only prefix matching is supported.
     * @return a list of the names of function with provided page size in this database and next
     *     page token, or a list of the names of all function in this database if the catalog does
     *     not {@link #supportsListObjectsPaged()}.
     * @throws DatabaseNotExistException if the database does not exist
     * @throws UnsupportedOperationException if it does not {@link #supportsListByPattern()}
     */
    default PagedList<String> listFunctionsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String functionNamePattern)
            throws DatabaseNotExistException {
        return new PagedList<>(listFunctions(databaseName), null);
    }

    /**
     * Gets an array of function for a catalog.
     *
     * @param databaseNamePattern A sql LIKE pattern (%) for database names. All databases will be
     *     returned if not set or empty. Currently, only prefix matching is supported.
     * @param functionNamePattern A sql LIKE pattern (%) for function names. All functions will be
     *     returned if not set or empty. Currently, only prefix matching is supported.
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @return a list of the function identifier with provided page size under this
     *     databaseNamePattern & functionNamePattern and next page token
     * @throws UnsupportedOperationException if it does not {@link #supportsListObjectsPaged()} or
     *     does not {@link #supportsListByPattern()}}.
     */
    default PagedList<Identifier> listFunctionsPagedGlobally(
            @Nullable String databaseNamePattern,
            @Nullable String functionNamePattern,
            @Nullable Integer maxResults,
            @Nullable String pageToken) {
        throw new UnsupportedOperationException(
                "Current Catalog does not support listFunctionsPagedGlobally");
    }

    /**
     * Get paged list function details under this database. An empty list is returned if none
     * function exists.
     *
     * @param databaseName Name of the database to list function detail.
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @param functionNamePattern A sql LIKE pattern (%) for function names. All function details
     *     will be returned if not set or empty. Currently, only prefix matching is supported.
     * @return a list of the function detail with provided page size (@param maxResults) in this
     *     database and next page token, or a list of the details of all functions in this database
     *     if the catalog does not {@link #supportsListObjectsPaged()}.
     * @throws DatabaseNotExistException if the database does not exist
     * @throws UnsupportedOperationException if it does not {@link #supportsListByPattern()}
     */
    default PagedList<Function> listFunctionDetailsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String functionNamePattern)
            throws DatabaseNotExistException {
        return new PagedList<>(Collections.emptyList(), null);
    }

    /**
     * Get function by name.
     *
     * @param identifier Path of the function to get
     * @return The requested function
     * @throws FunctionNotExistException if the function does not exist
     */
    Function getFunction(Identifier identifier) throws FunctionNotExistException;

    /**
     * Create a new function.
     *
     * <p>NOTE: System functions can not be created.
     *
     * @param identifier path of the function to be created
     * @param function the function definition
     * @param ignoreIfExists flag to specify behavior when a function already exists at the given
     *     path: if set to false, it throws a FunctionAlreadyExistException, if set to true, do
     *     nothing.
     * @throws FunctionAlreadyExistException if function already exists and ignoreIfExists is false
     * @throws DatabaseNotExistException if the database in identifier doesn't exist
     */
    void createFunction(Identifier identifier, Function function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException;

    /**
     * Drop function.
     *
     * @param identifier path of the function to be created
     * @param ignoreIfNotExists Flag to specify behavior when the function does not exist
     * @throws FunctionNotExistException if the function doesn't exist
     */
    void dropFunction(Identifier identifier, boolean ignoreIfNotExists)
            throws FunctionNotExistException;

    /**
     * Alter function.
     *
     * @param identifier path of the function to be created
     * @param changes the function changes
     * @param ignoreIfNotExists Flag to specify behavior when the function does not exist
     * @throws FunctionNotExistException if the function doesn't exist
     */
    void alterFunction(
            Identifier identifier, List<FunctionChange> changes, boolean ignoreIfNotExists)
            throws FunctionNotExistException, DefinitionAlreadyExistException,
                    DefinitionNotExistException;

    // ==================== Table Auth ==========================

    /**
     * Auth table query select and get the filter for row level access control and column masking
     * rules.
     *
     * @param identifier path of the table to alter partitions
     * @param select selected fields, null if select all
     * @return additional filter for row level access control and column masking rules
     * @throws TableNotExistException if the table does not exist
     */
    TableQueryAuthResult authTableQuery(Identifier identifier, @Nullable List<String> select)
            throws TableNotExistException;

    // ==================== Catalog Information ==========================

    /** Catalog options for re-creating this catalog. */
    Map<String, String> options();

    /** Serializable loader to create catalog. */
    CatalogLoader catalogLoader();

    /** Return a boolean that indicates whether this catalog is case-sensitive. */
    boolean caseSensitive();

    // ======================= Constants ===============================

    // constants for sys database
    String SYSTEM_DATABASE_NAME = "sys";

    // constants for table and database
    String COMMENT_PROP = "comment";
    String OWNER_PROP = "owner";

    // constants for database
    String DEFAULT_DATABASE = "default";
    String DB_SUFFIX = ".db";
    String DB_LOCATION_PROP = "location";

    // constants for table
    String TABLE_DEFAULT_OPTION_PREFIX = "table-default.";
    String NUM_ROWS_PROP = "numRows";
    String NUM_FILES_PROP = "numFiles";
    String TOTAL_SIZE_PROP = "totalSize";
    String LAST_UPDATE_TIME_PROP = "lastUpdateTime";
    String TOTAL_BUCKETS = "totalBuckets";

    // ======================= Exceptions ===============================

    /** Exception for trying to drop on a database that is not empty. */
    class DatabaseNotEmptyException extends Exception {
        private static final String MSG = "Database %s is not empty.";

        private final String database;

        public DatabaseNotEmptyException(String database, Throwable cause) {
            super(String.format(MSG, database), cause);
            this.database = database;
        }

        public DatabaseNotEmptyException(String database) {
            this(database, null);
        }

        public String database() {
            return database;
        }
    }

    /** Exception for trying to create a database that already exists. */
    class DatabaseAlreadyExistException extends Exception {
        private static final String MSG = "Database %s already exists.";

        private final String database;

        public DatabaseAlreadyExistException(String database, Throwable cause) {
            super(String.format(MSG, database), cause);
            this.database = database;
        }

        public DatabaseAlreadyExistException(String database) {
            this(database, null);
        }

        public String database() {
            return database;
        }
    }

    /** Exception for trying to operate on a database that doesn't exist. */
    class DatabaseNotExistException extends Exception {
        private static final String MSG = "Database %s does not exist.";

        private final String database;

        public DatabaseNotExistException(String database, Throwable cause) {
            super(String.format(MSG, database), cause);
            this.database = database;
        }

        public DatabaseNotExistException(String database) {
            this(database, null);
        }

        public String database() {
            return database;
        }
    }

    /** Exception for trying to operate on a system database. */
    class ProcessSystemDatabaseException extends IllegalArgumentException {
        private static final String MSG = "Can't do operation on system database.";

        public ProcessSystemDatabaseException() {
            super(MSG);
        }
    }

    /** Exception for trying to operate on the database that doesn't have permission. */
    class DatabaseNoPermissionException extends RuntimeException {
        private static final String MSG = "Database %s has no permission. Cause by %s.";

        private final String database;

        public DatabaseNoPermissionException(String database, Throwable cause) {
            super(
                    String.format(
                            MSG,
                            database,
                            cause != null && cause.getMessage() != null ? cause.getMessage() : ""),
                    cause);
            this.database = database;
        }

        @VisibleForTesting
        public DatabaseNoPermissionException(String database) {
            this(database, null);
        }

        public String database() {
            return database;
        }
    }

    /** Exception for trying to create a table that already exists. */
    class TableAlreadyExistException extends Exception {

        private static final String MSG = "Table %s already exists.";

        private final Identifier identifier;

        public TableAlreadyExistException(Identifier identifier) {
            this(identifier, null);
        }

        public TableAlreadyExistException(Identifier identifier, Throwable cause) {
            super(String.format(MSG, identifier.getFullName()), cause);
            this.identifier = identifier;
        }

        public Identifier identifier() {
            return identifier;
        }
    }

    /** Exception for trying to operate on a table that doesn't exist. */
    class TableNotExistException extends Exception {

        private static final String MSG = "Table %s does not exist.";

        private final Identifier identifier;

        public TableNotExistException(Identifier identifier) {
            this(identifier, null);
        }

        public TableNotExistException(Identifier identifier, Throwable cause) {
            super(String.format(MSG, identifier.getFullName()), cause);
            this.identifier = identifier;
        }

        public Identifier identifier() {
            return identifier;
        }
    }

    /** Exception for trying to operate on a table by ID that doesn't exist. */
    class TableIdNotExistException extends Exception {

        private static final String MSG = "Table id %s does not exist.";

        private final String tableId;

        public TableIdNotExistException(String tableId, Throwable cause) {
            super(String.format(MSG, tableId), cause);
            this.tableId = tableId;
        }

        public String getTableId() {
            return tableId;
        }
    }

    /** Exception for trying to operate on the table that doesn't have permission. */
    class TableNoPermissionException extends RuntimeException {

        private static final String MSG = "Table %s has no permission. Cause by %s.";

        private final Identifier identifier;

        public TableNoPermissionException(Identifier identifier, Throwable cause) {
            super(
                    String.format(
                            MSG,
                            identifier.getFullName(),
                            cause != null && cause.getMessage() != null ? cause.getMessage() : ""),
                    cause);
            this.identifier = identifier;
        }

        @VisibleForTesting
        public TableNoPermissionException(Identifier identifier) {
            this(identifier, null);
        }

        public Identifier identifier() {
            return identifier;
        }
    }

    /** Exception for trying to operate on a table by ID that doesn't have permission. */
    class TableIdNoPermissionException extends RuntimeException {

        private static final String MSG = "Table id %s has no permission. Cause by %s.";

        private final String tableId;

        public TableIdNoPermissionException(String tableId, Throwable cause) {
            super(
                    String.format(
                            MSG,
                            tableId,
                            cause != null && cause.getMessage() != null ? cause.getMessage() : ""),
                    cause);
            this.tableId = tableId;
        }

        public String getTableId() {
            return tableId;
        }
    }

    /** Exception for trying to alter a column that already exists. */
    class ColumnAlreadyExistException extends Exception {

        private static final String MSG = "Column %s already exists in the %s table.";

        private final Identifier identifier;
        private final String column;

        public ColumnAlreadyExistException(Identifier identifier, String column) {
            this(identifier, column, null);
        }

        public ColumnAlreadyExistException(Identifier identifier, String column, Throwable cause) {
            super(String.format(MSG, column, identifier.getFullName()), cause);
            this.identifier = identifier;
            this.column = column;
        }

        public Identifier identifier() {
            return identifier;
        }

        public String column() {
            return column;
        }
    }

    /** Exception for trying to operate on a column that doesn't exist. */
    class ColumnNotExistException extends Exception {

        private static final String MSG = "Column %s does not exist in the %s table.";

        private final Identifier identifier;
        private final String column;

        public ColumnNotExistException(Identifier identifier, String column) {
            this(identifier, column, null);
        }

        public ColumnNotExistException(Identifier identifier, String column, Throwable cause) {
            super(String.format(MSG, column, identifier.getFullName()), cause);
            this.identifier = identifier;
            this.column = column;
        }

        public Identifier identifier() {
            return identifier;
        }

        public String column() {
            return column;
        }
    }

    /** Exception for trying to create a view that already exists. */
    class ViewAlreadyExistException extends Exception {

        private static final String MSG = "View %s already exists.";

        private final Identifier identifier;

        public ViewAlreadyExistException(Identifier identifier) {
            this(identifier, null);
        }

        public ViewAlreadyExistException(Identifier identifier, Throwable cause) {
            super(String.format(MSG, identifier.getFullName()), cause);
            this.identifier = identifier;
        }

        public Identifier identifier() {
            return identifier;
        }
    }

    /** Exception for trying to operate on a view that doesn't exist. */
    class ViewNotExistException extends Exception {

        private static final String MSG = "View %s does not exist.";

        private final Identifier identifier;

        public ViewNotExistException(Identifier identifier) {
            this(identifier, null);
        }

        public ViewNotExistException(Identifier identifier, Throwable cause) {
            super(String.format(MSG, identifier.getFullName()), cause);
            this.identifier = identifier;
        }

        public Identifier identifier() {
            return identifier;
        }
    }

    /** Exception for trying to add a dialect that already exists. */
    class DialectAlreadyExistException extends Exception {

        private static final String MSG = "Dialect %s in view %s already exists.";

        private final Identifier identifier;
        private final String dialect;

        public DialectAlreadyExistException(Identifier identifier, String dialect) {
            this(identifier, dialect, null);
        }

        public DialectAlreadyExistException(
                Identifier identifier, String dialect, Throwable cause) {
            super(String.format(MSG, dialect, identifier.getFullName()), cause);
            this.identifier = identifier;
            this.dialect = dialect;
        }

        public Identifier identifier() {
            return identifier;
        }

        public String dialect() {
            return dialect;
        }
    }

    /** Exception for trying to create a branch that already exists. */
    class BranchAlreadyExistException extends Exception {

        private static final String MSG = "Branch %s in table %s already exists.";

        private final Identifier identifier;
        private final String branch;

        public BranchAlreadyExistException(Identifier identifier, String branch) {
            this(identifier, branch, null);
        }

        public BranchAlreadyExistException(Identifier identifier, String branch, Throwable cause) {
            super(String.format(MSG, branch, identifier.getFullName()), cause);
            this.identifier = identifier;
            this.branch = branch;
        }

        public Identifier identifier() {
            return identifier;
        }

        public String branch() {
            return branch;
        }
    }

    /** Exception for trying to operate on a branch that doesn't exist. */
    class BranchNotExistException extends Exception {

        private static final String MSG = "Branch %s in table %s doesn't exist.";

        private final Identifier identifier;
        private final String branch;

        public BranchNotExistException(Identifier identifier, String branch) {
            this(identifier, branch, null);
        }

        public BranchNotExistException(Identifier identifier, String branch, Throwable cause) {
            super(String.format(MSG, branch, identifier.getFullName()), cause);
            this.identifier = identifier;
            this.branch = branch;
        }

        public Identifier identifier() {
            return identifier;
        }

        public String branch() {
            return branch;
        }
    }

    /** Exception for trying to operate on a tag that doesn't exist. */
    class TagNotExistException extends Exception {

        private static final String MSG = "Tag %s in table %s doesn't exist.";

        private final Identifier identifier;
        private final String tag;

        public TagNotExistException(Identifier identifier, String tag) {
            this(identifier, tag, null);
        }

        public TagNotExistException(Identifier identifier, String tag, Throwable cause) {
            super(String.format(MSG, tag, identifier.getFullName()), cause);
            this.identifier = identifier;
            this.tag = tag;
        }

        public Identifier identifier() {
            return identifier;
        }

        public String tag() {
            return tag;
        }
    }

    /** Exception for trying to create a tag that already exists. */
    class TagAlreadyExistException extends Exception {

        private static final String MSG = "Tag %s in table %s already exists.";

        private final Identifier identifier;
        private final String tag;

        public TagAlreadyExistException(Identifier identifier, String tag) {
            this(identifier, tag, null);
        }

        public TagAlreadyExistException(Identifier identifier, String tag, Throwable cause) {
            super(String.format(MSG, tag, identifier.getFullName()), cause);
            this.identifier = identifier;
            this.tag = tag;
        }

        public Identifier identifier() {
            return identifier;
        }

        public String tag() {
            return tag;
        }
    }

    /** Exception for trying to update dialect that doesn't exist. */
    class DialectNotExistException extends Exception {

        private static final String MSG = "Dialect %s in view %s doesn't exist.";

        private final Identifier identifier;
        private final String dialect;

        public DialectNotExistException(Identifier identifier, String dialect) {
            this(identifier, dialect, null);
        }

        public DialectNotExistException(Identifier identifier, String dialect, Throwable cause) {
            super(String.format(MSG, dialect, identifier.getFullName()), cause);
            this.identifier = identifier;
            this.dialect = dialect;
        }

        public Identifier identifier() {
            return identifier;
        }

        public String dialect() {
            return dialect;
        }
    }

    /** Exception for trying to create a function that already exists. */
    class FunctionAlreadyExistException extends Exception {

        private static final String MSG = "Function %s already exists.";

        private final Identifier identifier;

        public FunctionAlreadyExistException(Identifier identifier) {
            this(identifier, null);
        }

        public FunctionAlreadyExistException(Identifier identifier, Throwable cause) {
            super(String.format(MSG, identifier.getFullName()), cause);
            this.identifier = identifier;
        }

        public Identifier identifier() {
            return identifier;
        }
    }

    /** Exception for trying to get a function that doesn't exist. */
    class FunctionNotExistException extends Exception {

        private static final String MSG = "Function %s doesn't exist.";

        private final Identifier identifier;

        public FunctionNotExistException(Identifier identifier) {
            this(identifier, null);
        }

        public FunctionNotExistException(Identifier identifier, Throwable cause) {
            super(String.format(MSG, identifier), cause);
            this.identifier = identifier;
        }

        public Identifier identifier() {
            return identifier;
        }
    }

    /** Exception for trying to add a definition that already exists. */
    class DefinitionAlreadyExistException extends Exception {

        private static final String MSG = "Definition %s in function %s already exists.";

        private final Identifier identifier;
        private final String name;

        public DefinitionAlreadyExistException(Identifier identifier, String name) {
            this(identifier, name, null);
        }

        public DefinitionAlreadyExistException(
                Identifier identifier, String name, Throwable cause) {
            super(String.format(MSG, name, identifier.getFullName()), cause);
            this.identifier = identifier;
            this.name = name;
        }

        public Identifier identifier() {
            return identifier;
        }

        public String name() {
            return name;
        }
    }

    /** Exception for trying to update definition that doesn't exist. */
    class DefinitionNotExistException extends Exception {

        private static final String MSG = "Definition %s in function %s doesn't exist.";

        private final Identifier identifier;
        private final String name;

        public DefinitionNotExistException(Identifier identifier, String name) {
            this(identifier, name, null);
        }

        public DefinitionNotExistException(Identifier identifier, String name, Throwable cause) {
            super(String.format(MSG, name, identifier.getFullName()), cause);
            this.identifier = identifier;
            this.name = name;
        }

        public Identifier identifier() {
            return identifier;
        }

        public String name() {
            return name;
        }
    }
}
