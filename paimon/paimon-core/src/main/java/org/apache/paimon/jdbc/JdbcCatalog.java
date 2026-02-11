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

package org.apache.paimon.jdbc;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;
import org.apache.paimon.shade.guava30.com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static org.apache.paimon.jdbc.JdbcCatalogLock.acquireTimeout;
import static org.apache.paimon.jdbc.JdbcCatalogLock.checkMaxSleep;
import static org.apache.paimon.jdbc.JdbcUtils.deleteProperties;
import static org.apache.paimon.jdbc.JdbcUtils.execute;
import static org.apache.paimon.jdbc.JdbcUtils.insertProperties;
import static org.apache.paimon.jdbc.JdbcUtils.updateProperties;
import static org.apache.paimon.jdbc.JdbcUtils.updateTable;

/* This file is based on source code from the Iceberg Project (http://iceberg.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * 基于 JDBC 的 Catalog 实现.
 *
 * <p>将 Paimon 的元数据存储在关系数据库中,支持 MySQL、PostgreSQL、SQLite 等数据库。
 * 表数据仍然存储在文件系统中,只有元数据(数据库、表结构、属性等)存储在 JDBC 数据库中。
 *
 * <p>数据库表结构:
 * <ul>
 *   <li>paimon_tables: 存储表的基本信息(catalog_key, database_name, table_name)</li>
 *   <li>paimon_database_properties: 存储数据库的属性(catalog_key, database_name, property_key, property_value)</li>
 *   <li>paimon_distributed_locks: 分布式锁表(lock_id, acquired_at, expire_time_seconds)</li>
 * </ul>
 *
 * <p>特性:
 * <ul>
 *   <li>元数据集中管理,便于多客户端共享</li>
 *   <li>支持分布式锁,保证并发操作的安全性</li>
 *   <li>大小写不敏感的表名和列名查询</li>
 *   <li>使用连接池管理 JDBC 连接</li>
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>需要多个客户端访问同一个 Catalog</li>
 *   <li>需要持久化元数据到数据库</li>
 *   <li>需要与现有的关系数据库基础设施集成</li>
 * </ul>
 *
 * <p>基于 Apache Iceberg 项目的源代码修改。
 */
public class JdbcCatalog extends AbstractCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcCatalog.class);

    /** JDBC 相关属性的前缀,用于从配置中提取 JDBC 配置 */
    public static final String PROPERTY_PREFIX = "jdbc.";

    /** 数据库存在标识属性,用于标记数据库是否已创建 */
    private static final String DATABASE_EXISTS_PROPERTY = "exists";

    /** JDBC 连接池,用于管理数据库连接 */
    private final JdbcClientPool connections;

    /** Catalog 键,用于在多租户场景下区分不同的 Catalog */
    private final String catalogKey;

    /** Catalog 配置选项 */
    private final Options options;

    /** 仓库路径,表数据文件的根目录 */
    private final String warehouse;

    /**
     * 构造 JDBC Catalog.
     *
     * @param fileIO 文件 IO 实现,用于访问表数据文件
     * @param catalogKey Catalog 键,用于多租户场景
     * @param context Catalog 上下文,包含配置信息
     * @param warehouse 仓库路径,表数据文件的根目录
     */
    protected JdbcCatalog(
            FileIO fileIO, String catalogKey, CatalogContext context, String warehouse) {
        super(fileIO, context);
        this.catalogKey = catalogKey;
        this.options = context.options();
        this.warehouse = warehouse;
        Preconditions.checkNotNull(options, "Invalid catalog properties: null");
        // 创建 JDBC 连接池
        this.connections =
                new JdbcClientPool(
                        options.get(CatalogOptions.CLIENT_POOL_SIZE),
                        options.get(CatalogOptions.URI.key()),
                        options.toMap());
        try {
            // 初始化 Catalog 所需的数据库表
            initializeCatalogTablesIfNeed();
        } catch (SQLException e) {
            throw new RuntimeException("Cannot initialize JDBC catalog", e);
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted in call to initialize", e);
        }
    }

    @VisibleForTesting
    public JdbcClientPool getConnections() {
        return connections;
    }

    /**
     * 初始化 Catalog 所需的数据库表.
     *
     * <p>创建三张表:
     * <ul>
     *   <li>paimon_tables: 存储表信息</li>
     *   <li>paimon_database_properties: 存储数据库属性</li>
     *   <li>paimon_distributed_locks: 分布式锁表(如果启用锁)</li>
     * </ul>
     *
     * @throws SQLException SQL 异常
     * @throws InterruptedException 中断异常
     */
    private void initializeCatalogTablesIfNeed() throws SQLException, InterruptedException {
        String uri = options.get(CatalogOptions.URI.key());
        Preconditions.checkNotNull(uri, "JDBC connection URI is required");
        // 检查并创建 Catalog 表
        connections.run(
                conn -> {
                    DatabaseMetaData dbMeta = conn.getMetaData();
                    ResultSet tableExists =
                            dbMeta.getTables(null, null, JdbcUtils.CATALOG_TABLE_NAME, null);
                    if (tableExists.next()) {
                        return true;
                    }
                    return conn.prepareStatement(JdbcUtils.CREATE_CATALOG_TABLE).execute();
                });

        // 检查并创建数据库属性表
        connections.run(
                conn -> {
                    DatabaseMetaData dbMeta = conn.getMetaData();
                    ResultSet tableExists =
                            dbMeta.getTables(
                                    null, null, JdbcUtils.DATABASE_PROPERTIES_TABLE_NAME, null);
                    if (tableExists.next()) {
                        return true;
                    }
                    return conn.prepareStatement(JdbcUtils.CREATE_DATABASE_PROPERTIES_TABLE)
                            .execute();
                });

        // 如果启用锁,检查并创建分布式锁表
        if (lockEnabled()) {
            JdbcUtils.createDistributedLockTable(connections, options);
        }
    }

    @Override
    public String warehouse() {
        return warehouse;
    }

    @Override
    public CatalogLoader catalogLoader() {
        return new JdbcCatalogLoader(fileIO, catalogKey, context, warehouse);
    }

    /**
     * 列出所有数据库.
     *
     * <p>从两个来源获取数据库列表并合并:
     * <ul>
     *   <li>paimon_tables 表: 包含表的数据库</li>
     *   <li>paimon_database_properties 表: 显式创建但可能没有表的数据库</li>
     * </ul>
     *
     * @return 所有数据库名称的不重复列表
     */
    @Override
    public List<String> listDatabases() {
        List<String> databases = Lists.newArrayList();
        // 从表列表中获取数据库
        databases.addAll(
                fetch(
                        row -> row.getString(JdbcUtils.TABLE_DATABASE),
                        JdbcUtils.LIST_ALL_TABLE_DATABASES_SQL,
                        catalogKey));

        // 从数据库属性表中获取数据库
        databases.addAll(
                fetch(
                        row -> row.getString(JdbcUtils.DATABASE_NAME),
                        JdbcUtils.LIST_ALL_PROPERTY_DATABASES_SQL,
                        catalogKey));
        // 去重并返回
        return databases.stream().distinct().collect(Collectors.toList());
    }

    /**
     * 获取数据库信息.
     *
     * @param databaseName 数据库名称
     * @return 数据库对象,包含属性和位置信息
     * @throws DatabaseNotExistException 数据库不存在
     */
    @Override
    protected Database getDatabaseImpl(String databaseName) throws DatabaseNotExistException {
        if (!JdbcUtils.databaseExists(connections, catalogKey, databaseName)) {
            throw new DatabaseNotExistException(databaseName);
        }
        Map<String, String> options = Maps.newHashMap();
        // 获取数据库的所有属性
        options.putAll(fetchProperties(databaseName));
        // 如果没有位置属性,使用默认位置
        if (!options.containsKey(DB_LOCATION_PROP)) {
            options.put(DB_LOCATION_PROP, newDatabasePath(databaseName).getName());
        }
        // 移除内部使用的存在标识属性
        options.remove(DATABASE_EXISTS_PROPERTY);
        return Database.of(databaseName, options, null);
    }

    /**
     * 创建数据库.
     *
     * <p>在 paimon_database_properties 表中插入数据库属性。
     *
     * @param name 数据库名称
     * @param properties 数据库属性
     */
    @Override
    protected void createDatabaseImpl(String name, Map<String, String> properties) {
        Map<String, String> createProps = new HashMap<>();
        // 添加存在标识
        createProps.put(DATABASE_EXISTS_PROPERTY, "true");
        if (properties != null && !properties.isEmpty()) {
            createProps.putAll(properties);
        }

        // 如果未指定位置,使用默认位置
        if (!createProps.containsKey(DB_LOCATION_PROP)) {
            Path databasePath = newDatabasePath(name);
            createProps.put(DB_LOCATION_PROP, databasePath.toString());
        }
        // 插入属性到数据库
        insertProperties(connections, catalogKey, name, createProps);
    }

    /**
     * 删除数据库.
     *
     * <p>从两个表中删除数据库相关的所有记录:
     * <ul>
     *   <li>paimon_tables: 删除该数据库下的所有表</li>
     *   <li>paimon_database_properties: 删除数据库的所有属性</li>
     * </ul>
     *
     * @param name 数据库名称
     */
    @Override
    protected void dropDatabaseImpl(String name) {
        // 从 paimon_tables 删除表
        execute(connections, JdbcUtils.DELETE_TABLES_SQL, catalogKey, name);
        // 从 paimon_database_properties 删除属性
        execute(connections, JdbcUtils.DELETE_ALL_DATABASE_PROPERTIES_SQL, catalogKey, name);
    }

    /**
     * 修改数据库属性.
     *
     * <p>处理三种操作:
     * <ul>
     *   <li>INSERT: 插入新属性</li>
     *   <li>UPDATE: 更新已存在的属性</li>
     *   <li>DELETE: 删除属性</li>
     * </ul>
     *
     * @param name 数据库名称
     * @param changes 属性变更列表
     */
    @Override
    protected void alterDatabaseImpl(String name, List<PropertyChange> changes) {
        // 解析属性变更为设置和删除两类
        Pair<Map<String, String>, Set<String>> setPropertiesToRemoveKeys =
                PropertyChange.getSetPropertiesToRemoveKeys(changes);
        Map<String, String> setProperties = setPropertiesToRemoveKeys.getLeft();
        Set<String> removeKeys = setPropertiesToRemoveKeys.getRight();
        // 获取当前属性
        Map<String, String> startingProperties = fetchProperties(name);
        Map<String, String> inserts = Maps.newHashMap();
        Map<String, String> updates = Maps.newHashMap();
        Set<String> removes = Sets.newHashSet();
        // 区分插入和更新
        if (!setProperties.isEmpty()) {
            setProperties.forEach(
                    (k, v) -> {
                        if (!startingProperties.containsKey(k)) {
                            inserts.put(k, v);
                        } else {
                            updates.put(k, v);
                        }
                    });
        }
        // 确定需要删除的属性
        if (!removeKeys.isEmpty()) {
            removeKeys.forEach(
                    k -> {
                        if (startingProperties.containsKey(k)) {
                            removes.add(k);
                        }
                    });
        }
        // 执行插入
        if (!inserts.isEmpty()) {
            insertProperties(connections, catalogKey, name, inserts);
        }
        // 执行更新
        if (!updates.isEmpty()) {
            updateProperties(connections, catalogKey, name, updates);
        }
        // 执行删除
        if (!removes.isEmpty()) {
            deleteProperties(connections, catalogKey, name, removes);
        }
    }

    /**
     * 列出数据库中的所有表.
     *
     * @param databaseName 数据库名称
     * @return 表名列表
     */
    @Override
    protected List<String> listTablesImpl(String databaseName) {
        return fetch(
                row -> row.getString(JdbcUtils.TABLE_NAME),
                JdbcUtils.LIST_TABLES_SQL,
                catalogKey,
                databaseName);
    }

    /**
     * 删除表.
     *
     * <p>执行两步操作:
     * <ul>
     *   <li>从 paimon_tables 表中删除元数据记录</li>
     *   <li>从文件系统中删除表目录和外部路径</li>
     * </ul>
     *
     * @param identifier 表标识符
     * @param externalPaths 外部路径列表,需要一并删除
     */
    @Override
    protected void dropTableImpl(Identifier identifier, List<Path> externalPaths) {
        try {
            // 从元数据表中删除记录
            int deletedRecords =
                    execute(
                            connections,
                            JdbcUtils.DROP_TABLE_SQL,
                            catalogKey,
                            identifier.getDatabaseName(),
                            identifier.getTableName());

            if (deletedRecords == 0) {
                LOG.info("Skipping drop, table does not exist: {}", identifier);
                return;
            }
            // 删除表目录
            Path path = getTableLocation(identifier);
            try {
                if (fileIO.exists(path)) {
                    fileIO.deleteDirectoryQuietly(path);
                }
                // 删除外部路径
                for (Path externalPath : externalPaths) {
                    if (fileIO.exists(externalPath)) {
                        fileIO.deleteDirectoryQuietly(externalPath);
                    }
                }
            } catch (Exception ex) {
                LOG.error("Delete directory[{}] fail for table {}", path, identifier, ex);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to drop table " + identifier.getFullName(), e);
        }
    }

    /**
     * 创建表.
     *
     * <p>执行两步操作:
     * <ul>
     *   <li>在文件系统中创建表的 Schema 文件</li>
     *   <li>在 paimon_tables 表中插入元数据记录</li>
     * </ul>
     *
     * <p>使用分布式锁保证并发安全。
     *
     * @param identifier 表标识符
     * @param schema 表的 Schema
     */
    @Override
    protected void createTableImpl(Identifier identifier, Schema schema) {
        try {
            // 在文件系统中创建 Schema 文件
            SchemaManager schemaManager = getSchemaManager(identifier);
            runWithLock(identifier, () -> schemaManager.createTable(schema));
            // 在元数据表中插入记录
            Path path = getTableLocation(identifier);
            int insertRecord =
                    connections.run(
                            conn -> {
                                try (PreparedStatement sql =
                                        conn.prepareStatement(
                                                JdbcUtils.DO_COMMIT_CREATE_TABLE_SQL)) {
                                    sql.setString(1, catalogKey);
                                    sql.setString(2, identifier.getDatabaseName());
                                    sql.setString(3, identifier.getTableName());
                                    return sql.executeUpdate();
                                }
                            });
            if (insertRecord == 1) {
                LOG.debug("Successfully committed to new table: {}", identifier);
            } else {
                // 插入失败,清理文件系统中的表目录
                try {
                    fileIO.deleteDirectoryQuietly(path);
                } catch (Exception ee) {
                    LOG.error("Delete directory[{}] fail for table {}", path, identifier, ee);
                }
                throw new RuntimeException(
                        String.format(
                                "Failed to create table %s in catalog %s",
                                identifier.getFullName(), catalogKey));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create table " + identifier.getFullName(), e);
        }
    }

    /**
     * 重命名表.
     *
     * <p>执行两步操作:
     * <ul>
     *   <li>更新 paimon_tables 表中的元数据</li>
     *   <li>重命名文件系统中的表目录</li>
     * </ul>
     *
     * @param fromTable 原表标识符
     * @param toTable 新表标识符
     */
    @Override
    protected void renameTableImpl(Identifier fromTable, Identifier toTable) {
        try {
            // 更新元数据表
            updateTable(connections, catalogKey, fromTable, toTable);

            // 重命名文件系统中的表目录
            Path fromPath = getTableLocation(fromTable);
            if (!new SchemaManager(fileIO, fromPath).listAllIds().isEmpty()) {
                // 保持文件系统与元数据的一致性
                Path toPath = getTableLocation(toTable);
                try {
                    fileIO.rename(fromPath, toPath);
                } catch (IOException e) {
                    throw new RuntimeException(
                            "Failed to rename changes of table "
                                    + toTable.getFullName()
                                    + " to underlying files.",
                            e);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to rename table " + fromTable.getFullName(), e);
        }
    }

    /**
     * 修改表结构.
     *
     * <p>在文件系统中提交 Schema 变更,使用分布式锁保证并发安全。
     *
     * @param identifier 表标识符
     * @param changes Schema 变更列表
     * @throws ColumnAlreadyExistException 列已存在
     * @throws TableNotExistException 表不存在
     * @throws ColumnNotExistException 列不存在
     */
    @Override
    protected void alterTableImpl(Identifier identifier, List<SchemaChange> changes)
            throws ColumnAlreadyExistException, TableNotExistException, ColumnNotExistException {
        assertMainBranch(identifier);
        SchemaManager schemaManager = getSchemaManager(identifier);
        try {
            // 使用锁保护 Schema 变更
            runWithLock(identifier, () -> schemaManager.commitChanges(changes));
        } catch (TableNotExistException
                | ColumnAlreadyExistException
                | ColumnNotExistException
                | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to alter table " + identifier.getFullName(), e);
        }
    }

    /**
     * 加载表的 Schema.
     *
     * <p>先检查元数据表中是否存在,再从文件系统中读取 Schema。
     *
     * @param identifier 表标识符
     * @return 表的 Schema
     * @throws TableNotExistException 表不存在
     */
    @Override
    protected TableSchema loadTableSchema(Identifier identifier) throws TableNotExistException {
        assertMainBranch(identifier);
        // 检查元数据表
        if (!JdbcUtils.tableExists(
                connections, catalogKey, identifier.getDatabaseName(), identifier.getTableName())) {
            throw new TableNotExistException(identifier);
        }
        // 从文件系统读取 Schema
        Path tableLocation = getTableLocation(identifier);
        return tableSchemaInFileSystem(tableLocation, identifier.getBranchNameOrDefault())
                .orElseThrow(
                        () -> new RuntimeException("There is no paimon table in " + tableLocation));
    }

    /**
     * 是否区分大小写.
     *
     * @return false,JDBC Catalog 不区分大小写
     */
    @Override
    public boolean caseSensitive() {
        return false;
    }

    /**
     * 获取默认的锁工厂.
     *
     * @return JDBC Catalog 锁工厂
     */
    @Override
    public Optional<CatalogLockFactory> defaultLockFactory() {
        return Optional.of(new JdbcCatalogLockFactory());
    }

    /**
     * 获取锁上下文.
     *
     * @return JDBC Catalog 锁上下文
     */
    @Override
    public Optional<CatalogLockContext> lockContext() {
        return Optional.of(new JdbcCatalogLockContext(catalogKey, options));
    }

    /**
     * 使用分布式锁执行操作.
     *
     * <p>如果未启用锁,直接执行操作;否则获取锁后执行。
     *
     * @param identifier 表标识符,用于生成锁 ID
     * @param callable 要执行的操作
     * @param <T> 返回值类型
     * @return 操作返回值
     * @throws Exception 操作异常
     */
    public <T> T runWithLock(Identifier identifier, Callable<T> callable) throws Exception {
        if (!lockEnabled()) {
            return callable.call();
        }
        JdbcCatalogLock lock =
                new JdbcCatalogLock(
                        connections,
                        catalogKey,
                        checkMaxSleep(options.toMap()),
                        acquireTimeout(options.toMap()));
        return Lock.fromCatalog(lock, identifier).runWithLock(callable);
    }

    @Override
    public void close() throws Exception {
        connections.close();
    }

    /**
     * 获取表的 Schema 管理器.
     *
     * @param identifier 表标识符
     * @return Schema 管理器
     */
    private SchemaManager getSchemaManager(Identifier identifier) {
        return new SchemaManager(fileIO, getTableLocation(identifier));
    }

    /**
     * 获取数据库的所有属性.
     *
     * @param databaseName 数据库名称
     * @return 属性键值对映射
     */
    private Map<String, String> fetchProperties(String databaseName) {
        List<Map.Entry<String, String>> entries =
                fetch(
                        row ->
                                new AbstractMap.SimpleImmutableEntry<>(
                                        row.getString(JdbcUtils.DATABASE_PROPERTY_KEY),
                                        row.getString(JdbcUtils.DATABASE_PROPERTY_VALUE)),
                        JdbcUtils.GET_ALL_DATABASE_PROPERTIES_SQL,
                        catalogKey,
                        databaseName);
        return ImmutableMap.<String, String>builder().putAll(entries).build();
    }

    /**
     * 结果集行处理函数接口.
     *
     * <p>用于将 ResultSet 的一行转换为指定类型的对象。
     *
     * @param <R> 结果类型
     */
    @FunctionalInterface
    interface RowProducer<R> {
        /**
         * 处理结果集的一行.
         *
         * @param result 结果集
         * @return 转换后的对象
         * @throws SQLException SQL 异常
         */
        R apply(ResultSet result) throws SQLException;
    }

    /**
     * 执行查询并获取结果列表.
     *
     * <p>通用的查询方法,使用 RowProducer 将每一行转换为指定类型。
     *
     * @param toRow 行转换函数
     * @param sql SQL 语句
     * @param args SQL 参数
     * @param <R> 结果类型
     * @return 结果列表
     */
    @SuppressWarnings("checkstyle:NestedTryDepth")
    private <R> List<R> fetch(RowProducer<R> toRow, String sql, String... args) {
        try {
            return connections.run(
                    conn -> {
                        List<R> result = Lists.newArrayList();
                        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                            // 设置参数
                            for (int pos = 0; pos < args.length; pos += 1) {
                                preparedStatement.setString(pos + 1, args[pos]);
                            }
                            // 执行查询
                            try (ResultSet rs = preparedStatement.executeQuery()) {
                                while (rs.next()) {
                                    result.add(toRow.apply(rs));
                                }
                            }
                        }
                        return result;
                    });
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Failed to execute query: %s", sql), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted in SQL query", e);
        }
    }
}
