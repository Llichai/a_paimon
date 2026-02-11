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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.PagedList;
import org.apache.paimon.Snapshot;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.function.Function;
import org.apache.paimon.function.FunctionChange;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.rest.responses.GetTagResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.Instant;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.utils.SnapshotNotExistException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.DATA_FILE_EXTERNAL_PATHS;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.CoreOptions.TYPE;
import static org.apache.paimon.catalog.CatalogUtils.checkNotBranch;
import static org.apache.paimon.catalog.CatalogUtils.checkNotSystemDatabase;
import static org.apache.paimon.catalog.CatalogUtils.checkNotSystemTable;
import static org.apache.paimon.catalog.CatalogUtils.isSystemDatabase;
import static org.apache.paimon.catalog.CatalogUtils.listPartitionsFromFileSystem;
import static org.apache.paimon.catalog.CatalogUtils.validateCreateTable;
import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.options.CatalogOptions.LOCK_ENABLED;
import static org.apache.paimon.options.CatalogOptions.LOCK_TYPE;

/**
 * Catalog 的抽象基类实现
 *
 * <p>AbstractCatalog 提供了 {@link Catalog} 接口的通用实现逻辑,子类只需实现特定于存储系统的方法即可。
 *
 * <p>核心功能:
 * <ul>
 *   <li><b>Schema 管理</b>: 通过 {@link SchemaManager} 管理表的 Schema 版本
 *   <li><b>路径计算</b>: 提供统一的路径计算逻辑（数据库路径、表路径）
 *   <li><b>表实例化</b>: 根据 Schema 创建 {@link FileStoreTable} 或 {@link FormatTable}
 *   <li><b>锁管理</b>: 支持可选的分布式锁（通过 {@link CatalogLockFactory}）
 *   <li><b>默认选项</b>: 处理表的默认配置选项
 *   <li><b>系统表支持</b>: 处理系统数据库和系统表的特殊逻辑
 * </ul>
 *
 * <p>子类实现示例:
 * <ul>
 *   <li>{@link FileSystemCatalog}: 基于文件系统的实现
 *   <li>HiveCatalog: 基于 Hive Metastore 的实现（在 paimon-hive-connector 中）
 *   <li>JdbcCatalog: 基于 JDBC 的实现（在 paimon-jdbc-connector 中）
 * </ul>
 *
 * <p>子类需要实现的抽象方法:
 * <pre>
 * 数据库操作:
 *   - {@link #getDatabaseImpl(String)}
 *   - {@link #createDatabaseImpl(String, Map)}
 *   - {@link #dropDatabaseImpl(String)}
 *   - {@link #alterDatabaseImpl(String, List)}
 *
 * 表操作:
 *   - {@link #listTablesImpl(String)}
 *   - {@link #getTableImpl(Identifier)}
 *   - {@link #dropTableImpl(Identifier)}
 *   - {@link #createTableImpl(Identifier, Schema)}
 *   - {@link #renameTableImpl(Identifier, Identifier)}
 *   - {@link #alterTableImpl(Identifier, List, boolean)}
 *
 * 其他:
 *   - {@link #warehouse()} - 返回仓库根路径
 * </pre>
 *
 * <p>通用实现逻辑:
 * <ul>
 *   <li><b>系统表/数据库检查</b>: 在修改操作前验证不是系统对象
 *   <li><b>存在性检查</b>: 根据 ignoreIfExists/ignoreIfNotExists 标志处理
 *   <li><b>级联删除</b>: 在删除数据库时根据 cascade 标志决定是否删除表
 *   <li><b>Schema 演进</b>: 通过 SchemaManager 管理 Schema 变更
 *   <li><b>外部路径清理</b>: 删除表时清理关联的外部数据路径
 * </ul>
 *
 * <p>路径结构:
 * <pre>
 * warehouse/
 *   ├─ database1.db/           # 数据库目录
 *   │   ├─ table1/              # 表目录
 *   │   │   ├─ schema/          # Schema 文件
 *   │   │   ├─ snapshot/        # 快照文件
 *   │   │   └─ manifest/        # Manifest 文件
 *   │   └─ table2/
 *   └─ database2.db/
 * </pre>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 子类实现
 * public class MyCustomCatalog extends AbstractCatalog {
 *     @Override
 *     public String warehouse() {
 *         return warehousePath;
 *     }
 *
 *     @Override
 *     protected Database getDatabaseImpl(String name) throws DatabaseNotExistException {
 *         // 从元数据存储加载数据库信息
 *         return loadDatabaseFromMetastore(name);
 *     }
 *
 *     // ... 实现其他抽象方法
 * }
 * }</pre>
 *
 * <p>线程安全:
 * <p>AbstractCatalog 本身是线程安全的。但具体的线程安全保证取决于:
 * <ul>
 *   <li>子类的实现（如 Hive Metastore 客户端是否线程安全）
 *   <li>是否启用了 CatalogLock（通过 {@link #lockEnabled()}）
 * </ul>
 */
public abstract class AbstractCatalog implements Catalog {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractCatalog.class);

    /** 文件 I/O 接口,用于访问底层存储系统 */
    protected final FileIO fileIO;

    /** 表的默认选项,从 Catalog 配置中提取（前缀为 "table-default."） */
    protected final Map<String, String> tableDefaultOptions;

    /** Catalog 上下文,包含配置选项和环境信息 */
    protected final CatalogContext context;

    /**
     * 构造函数 - 使用默认上下文
     *
     * @param fileIO 文件 I/O 接口
     */
    protected AbstractCatalog(FileIO fileIO) {
        this.fileIO = fileIO;
        this.tableDefaultOptions = new HashMap<>();
        this.context = CatalogContext.create(new Options());
    }

    /**
     * 构造函数 - 使用指定上下文
     *
     * @param fileIO 文件 I/O 接口
     * @param context Catalog 上下文,包含配置选项
     */
    protected AbstractCatalog(FileIO fileIO, CatalogContext context) {
        this.fileIO = fileIO;
        this.tableDefaultOptions = CatalogUtils.tableDefaultOptions(context.options().toMap());
        this.context = context;
    }

    @Override
    public Map<String, String> options() {
        return context.options().toMap();
    }

    /**
     * 返回仓库根路径
     *
     * <p>仓库路径是所有数据库和表的根目录。例如: "hdfs://namenode:8020/warehouse"
     *
     * @return 仓库根路径
     */
    public abstract String warehouse();

    /**
     * 返回文件 I/O 接口
     *
     * @return 文件 I/O 实例
     */
    public FileIO fileIO() {
        return fileIO;
    }

    /**
     * 返回指定路径的文件 I/O 接口
     *
     * <p>子类可以重写此方法以支持不同路径使用不同的 FileIO 实现。
     *
     * @param path 文件路径
     * @return 文件 I/O 实例
     */
    protected FileIO fileIO(Path path) {
        return fileIO;
    }

    /**
     * 获取锁工厂实例
     *
     * <p>根据配置返回相应的 {@link CatalogLockFactory}:
     * <ul>
     *   <li>如果未启用锁（{@link #lockEnabled()} 返回 false）,返回 empty
     *   <li>如果配置了 lock.type,使用 SPI 机制加载对应的工厂
     *   <li>否则使用默认锁工厂（{@link #defaultLockFactory()}）
     * </ul>
     *
     * @return 锁工厂的 Optional,如果未启用锁则为 empty
     */
    public Optional<CatalogLockFactory> lockFactory() {
        if (!lockEnabled()) {
            return Optional.empty();
        }

        String lock = context.options().get(LOCK_TYPE);
        if (lock == null) {
            return defaultLockFactory();
        }

        return Optional.of(
                FactoryUtil.discoverFactory(
                        AbstractCatalog.class.getClassLoader(), CatalogLockFactory.class, lock));
    }

    /**
     * 返回默认的锁工厂
     *
     * <p>子类可以重写此方法提供特定的默认锁实现。
     * 例如,HiveCatalog 返回 HiveLockFactory。
     *
     * @return 默认锁工厂的 Optional,默认为 empty
     */
    public Optional<CatalogLockFactory> defaultLockFactory() {
        return Optional.empty();
    }

    /**
     * 返回锁上下文
     *
     * @return 锁上下文的 Optional
     */
    public Optional<CatalogLockContext> lockContext() {
        return Optional.of(CatalogLockContext.fromOptions(context.options()));
    }

    /**
     * 是否启用锁
     *
     * <p>锁启用逻辑:
     * <ul>
     *   <li>如果显式配置了 lock.enabled,使用配置值
     *   <li>否则,如果使用对象存储（如 S3/OSS）,默认启用锁
     *   <li>其他情况默认不启用
     * </ul>
     *
     * @return 是否启用锁
     */
    protected boolean lockEnabled() {
        return context.options().getOptional(LOCK_ENABLED).orElse(fileIO.isObjectStore());
    }

    /**
     * 是否允许自定义表路径
     *
     * <p>某些 Catalog 实现（如 HiveCatalog）允许表指定自定义的存储路径,
     * 而不是使用默认的 warehouse/database.db/table 结构。
     *
     * @return 是否允许自定义表路径,默认为 false
     */
    protected boolean allowCustomTablePath() {
        return false;
    }

    @Override
    public PagedList<String> listDatabasesPaged(
            Integer maxResults, String pageToken, String databaseNamePattern) {
        CatalogUtils.validateNamePattern(this, databaseNamePattern);
        return new PagedList<>(listDatabases(), null);
    }

    /**
     * 创建数据库的通用实现
     *
     * <p>执行以下逻辑:
     * <ol>
     *   <li>检查不是系统数据库（sys）
     *   <li>检查数据库是否已存在
     *   <li>如果已存在且 ignoreIfExists=true,直接返回
     *   <li>如果已存在且 ignoreIfExists=false,抛出异常
     *   <li>调用子类实现 {@link #createDatabaseImpl(String, Map)} 创建数据库
     * </ol>
     *
     * @param name 数据库名称
     * @param ignoreIfExists 如果为 true,数据库已存在时不抛出异常
     * @param properties 数据库属性
     * @throws DatabaseAlreadyExistException 如果数据库已存在且 ignoreIfExists=false
     */
    @Override
    public void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
            throws DatabaseAlreadyExistException {
        checkNotSystemDatabase(name);
        try {
            getDatabase(name);
            if (ignoreIfExists) {
                return;
            }
            throw new DatabaseAlreadyExistException(name);
        } catch (DatabaseNotExistException ignored) {
        }
        createDatabaseImpl(name, properties);
    }

    /**
     * 获取数据库的通用实现
     *
     * <p>如果是系统数据库（sys）,直接返回默认的 Database 对象。
     * 否则调用子类实现 {@link #getDatabaseImpl(String)}。
     *
     * @param name 数据库名称
     * @return Database 对象
     * @throws DatabaseNotExistException 如果数据库不存在
     */
    @Override
    public Database getDatabase(String name) throws DatabaseNotExistException {
        if (isSystemDatabase(name)) {
            return Database.of(name);
        }
        return getDatabaseImpl(name);
    }

    /**
     * 获取数据库的子类实现
     *
     * <p>子类需要实现此方法从底层存储（如 Hive Metastore、JDBC）加载数据库信息。
     *
     * @param name 数据库名称
     * @return Database 对象
     * @throws DatabaseNotExistException 如果数据库不存在
     */
    protected abstract Database getDatabaseImpl(String name) throws DatabaseNotExistException;

    @Override
    public void markDonePartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {}

    @Override
    public List<Partition> listPartitions(Identifier identifier) throws TableNotExistException {
        return listPartitionsFromFileSystem(getTable(identifier));
    }

    @Override
    public PagedList<Partition> listPartitionsPaged(
            Identifier identifier,
            Integer maxResults,
            String pageToken,
            String partitionNamePattern)
            throws TableNotExistException {
        CatalogUtils.validateNamePattern(this, partitionNamePattern);
        return new PagedList<>(listPartitions(identifier), null);
    }

    protected abstract void createDatabaseImpl(String name, Map<String, String> properties);

    /**
     * 删除数据库的通用实现
     *
     * <p>执行以下逻辑:
     * <ol>
     *   <li>检查不是系统数据库（sys）
     *   <li>检查数据库是否存在
     *   <li>如果不存在且 ignoreIfNotExists=true,直接返回
     *   <li>如果 cascade=false 且数据库不为空,抛出 DatabaseNotEmptyException
     *   <li>调用子类实现 {@link #dropDatabaseImpl(String)} 删除数据库
     * </ol>
     *
     * @param name 数据库名称
     * @param ignoreIfNotExists 如果为 true,数据库不存在时不抛出异常
     * @param cascade 如果为 true,级联删除数据库中的所有表
     * @throws DatabaseNotExistException 如果数据库不存在且 ignoreIfNotExists=false
     * @throws DatabaseNotEmptyException 如果数据库不为空且 cascade=false
     */
    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        checkNotSystemDatabase(name);
        try {
            getDatabase(name);
        } catch (DatabaseNotExistException e) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new DatabaseNotExistException(name);
        }

        if (!cascade && !listTables(name).isEmpty()) {
            throw new DatabaseNotEmptyException(name);
        }

        dropDatabaseImpl(name);
    }

    /**
     * 删除数据库的子类实现
     *
     * @param name 数据库名称
     */
    protected abstract void dropDatabaseImpl(String name);

    @Override
    public void alterDatabase(String name, List<PropertyChange> changes, boolean ignoreIfNotExists)
            throws DatabaseNotExistException {
        checkNotSystemDatabase(name);
        try {
            if (changes == null || changes.isEmpty()) {
                return;
            }
            this.alterDatabaseImpl(name, changes);
        } catch (DatabaseNotExistException e) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new DatabaseNotExistException(name);
        }
    }

    protected abstract void alterDatabaseImpl(String name, List<PropertyChange> changes)
            throws DatabaseNotExistException;

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        if (isSystemDatabase(databaseName)) {
            return SystemTableLoader.loadGlobalTableNames();
        }

        // check db exists
        getDatabase(databaseName);

        return listTablesImpl(databaseName).stream().sorted().collect(Collectors.toList());
    }

    @Override
    public PagedList<String> listTablesPaged(
            String databaseName,
            Integer maxResults,
            String pageToken,
            String tableNamePattern,
            String tableType)
            throws DatabaseNotExistException {
        CatalogUtils.validateNamePattern(this, tableNamePattern);
        CatalogUtils.validateTableType(this, tableType);
        return new PagedList<>(listTables(databaseName), null);
    }

    @Override
    public PagedList<Table> listTableDetailsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tableNamePattern,
            @Nullable String tableType)
            throws DatabaseNotExistException {
        CatalogUtils.validateNamePattern(this, tableNamePattern);
        CatalogUtils.validateTableType(this, tableType);
        if (isSystemDatabase(databaseName)) {
            List<Table> systemTables =
                    SystemTableLoader.loadGlobalTableNames().stream()
                            .map(
                                    tableName -> {
                                        try {
                                            return getTable(
                                                    Identifier.create(databaseName, tableName));
                                        } catch (TableNotExistException ignored) {
                                            LOG.warn(
                                                    "system table {}.{} does not exist",
                                                    databaseName,
                                                    tableName);
                                            return null;
                                        }
                                    })
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
            return new PagedList<>(systemTables, null);
        }

        // check db exists
        getDatabase(databaseName);

        return listTableDetailsPagedImpl(
                databaseName, maxResults, pageToken, tableNamePattern, tableType);
    }

    protected abstract List<String> listTablesImpl(String databaseName);

    protected PagedList<Table> listTableDetailsPagedImpl(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tableNamePattern,
            @Nullable String tableType)
            throws DatabaseNotExistException {
        PagedList<String> pagedTableNames =
                listTablesPaged(databaseName, maxResults, pageToken, tableNamePattern, tableType);
        return new PagedList<>(
                pagedTableNames.getElements().stream()
                        .map(
                                tableName -> {
                                    try {
                                        return getTable(Identifier.create(databaseName, tableName));
                                    } catch (TableNotExistException ignored) {
                                        LOG.warn(
                                                "table {}.{} does not exist",
                                                databaseName,
                                                tableName);
                                        return null;
                                    }
                                })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()),
                pagedTableNames.getNextPageToken());
    }

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws TableNotExistException {
        checkNotBranch(identifier, "dropTable");
        checkNotSystemTable(identifier, "dropTable");

        Set<Path> externalPaths = new HashSet<>();
        try {
            Table table = getTable(identifier);
            if (table instanceof FileStoreTable) {
                FileStoreTable fileStoreTable = (FileStoreTable) table;
                List<Path> schemaExternalPaths =
                        getSchemaExternalPaths(fileStoreTable.schemaManager().listAll());
                externalPaths.addAll(schemaExternalPaths);
                // get table branch external path
                List<String> branches = fileStoreTable.branchManager().branches();
                for (String branch : branches) {
                    SchemaManager schemaManager =
                            fileStoreTable.schemaManager().copyWithBranch(branch);
                    externalPaths.addAll(getSchemaExternalPaths(schemaManager.listAll()));
                }
            }
        } catch (TableNotExistException e) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotExistException(identifier);
        }

        dropTableImpl(identifier, new ArrayList<>(externalPaths));
    }

    private List<Path> getSchemaExternalPaths(List<TableSchema> schemas) {
        if (schemas == null) {
            return Collections.emptyList();
        }
        return schemas.stream()
                .map(schema -> schema.toSchema().options().get(DATA_FILE_EXTERNAL_PATHS.key()))
                .filter(Objects::nonNull)
                .flatMap(externalPath -> Arrays.stream(externalPath.split(",")))
                .map(Path::new)
                .distinct()
                .collect(Collectors.toList());
    }

    protected abstract void dropTableImpl(Identifier identifier, List<Path> externalPaths);

    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        checkNotBranch(identifier, "createTable");
        checkNotSystemTable(identifier, "createTable");
        validateCreateTable(schema, false);
        validateCustomTablePath(schema.options());

        // check db exists
        getDatabase(identifier.getDatabaseName());

        try {
            getTable(identifier);
            if (ignoreIfExists) {
                return;
            }
            throw new TableAlreadyExistException(identifier);
        } catch (TableNotExistException ignored) {
        }

        copyTableDefaultOptions(schema.options());

        switch (Options.fromMap(schema.options()).get(TYPE)) {
            case TABLE:
            case MATERIALIZED_TABLE:
                createTableImpl(identifier, schema);
                break;
            case FORMAT_TABLE:
                createFormatTable(identifier, schema);
                break;
            case OBJECT_TABLE:
                throw new UnsupportedOperationException(
                        String.format(
                                "Catalog %s cannot support object tables.",
                                this.getClass().getName()));
        }
    }

    protected abstract void createTableImpl(Identifier identifier, Schema schema);

    @Override
    public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException {
        checkNotBranch(fromTable, "renameTable");
        checkNotBranch(toTable, "renameTable");
        checkNotSystemTable(fromTable, "renameTable");
        checkNotSystemTable(toTable, "renameTable");

        try {
            getTable(fromTable);
        } catch (TableNotExistException e) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotExistException(fromTable);
        }

        try {
            getTable(toTable);
            throw new TableAlreadyExistException(toTable);
        } catch (TableNotExistException ignored) {
        }

        renameTableImpl(fromTable, toTable);
    }

    protected abstract void renameTableImpl(Identifier fromTable, Identifier toTable);

    @Override
    public void alterTable(
            Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        checkNotSystemTable(identifier, "alterTable");

        try {
            getTable(identifier);
        } catch (TableNotExistException e) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotExistException(identifier);
        }

        alterTableImpl(identifier, changes);
    }

    protected abstract void alterTableImpl(Identifier identifier, List<SchemaChange> changes)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException;

    @Override
    public Table getTable(Identifier identifier) throws TableNotExistException {
        return CatalogUtils.loadTable(
                this,
                identifier,
                p -> fileIO(),
                this::fileIO,
                this::loadTableMetadata,
                lockFactory().orElse(null),
                lockContext().orElse(null),
                context,
                false);
    }

    @Override
    public Table getTableById(String tableId) throws TableIdNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createBranch(Identifier identifier, String branch, @Nullable String fromTag)
            throws TableNotExistException, BranchAlreadyExistException, TagNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropBranch(Identifier identifier, String branch) throws BranchNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void fastForward(Identifier identifier, String branch) throws BranchNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listBranches(Identifier identifier) throws TableNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public GetTagResponse getTag(Identifier identifier, String tagName)
            throws TableNotExistException, TagNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTag(
            Identifier identifier,
            String tagName,
            @Nullable Long snapshotId,
            @Nullable String timeRetained,
            boolean ignoreIfExists)
            throws TableNotExistException, SnapshotNotExistException, TagAlreadyExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PagedList<String> listTagsPaged(
            Identifier identifier,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tagNamePrefix)
            throws TableNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteTag(Identifier identifier, String tagName)
            throws TableNotExistException, TagNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean commitSnapshot(
            Identifier identifier,
            @Nullable String tableUuid,
            Snapshot snapshot,
            List<PartitionStatistics> statistics) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<TableSnapshot> loadSnapshot(Identifier identifier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Snapshot> loadSnapshot(Identifier identifier, String version) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PagedList<Snapshot> listSnapshotsPaged(
            Identifier identifier, @Nullable Integer maxResults, @Nullable String pageToken) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollbackTo(Identifier identifier, Instant instant, @Nullable Long fromSnapshot)
            throws Catalog.TableNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsListObjectsPaged() {
        return false;
    }

    @Override
    public boolean supportsVersionManagement() {
        return false;
    }

    @Override
    public TableQueryAuthResult authTableQuery(Identifier identifier, List<String> select) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {}

    @Override
    public void dropPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        Table table = getTable(identifier);
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.truncatePartitions(partitions);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void alterPartitions(Identifier identifier, List<PartitionStatistics> partitions)
            throws TableNotExistException {}

    @Override
    public List<String> listFunctions(String databaseName) {
        return Collections.emptyList();
    }

    @Override
    public Function getFunction(Identifier identifier) throws FunctionNotExistException {
        throw new FunctionNotExistException(identifier);
    }

    @Override
    public void createFunction(Identifier identifier, Function function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropFunction(Identifier identifier, boolean ignoreIfNotExists)
            throws FunctionNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterFunction(
            Identifier identifier, List<FunctionChange> changes, boolean ignoreIfNotExists)
            throws FunctionNotExistException, DefinitionAlreadyExistException,
                    DefinitionNotExistException {
        throw new UnsupportedOperationException();
    }

    /**
     * Create a {@link FormatTable} identified by the given {@link Identifier}.
     *
     * @param identifier Path of the table
     * @param schema Schema of the table
     */
    public void createFormatTable(Identifier identifier, Schema schema) {
        throw new UnsupportedOperationException(
                this.getClass().getName() + " currently does not support format table");
    }

    /**
     * Get warehouse path for specified database. If a catalog would like to provide individual path
     * for each database, this method can be `Override` in that catalog.
     *
     * @param database The given database name
     * @return The warehouse path for the database
     */
    public Path newDatabasePath(String database) {
        return newDatabasePath(warehouse(), database);
    }

    protected TableMetadata loadTableMetadata(Identifier identifier) throws TableNotExistException {
        return new TableMetadata(loadTableSchema(identifier), false, null);
    }

    protected abstract TableSchema loadTableSchema(Identifier identifier)
            throws TableNotExistException;

    public Path getTableLocation(Identifier identifier) {
        return new Path(newDatabasePath(identifier.getDatabaseName()), identifier.getTableName());
    }

    protected void assertMainBranch(Identifier identifier) {
        if (identifier.getBranchName() != null
                && !DEFAULT_MAIN_BRANCH.equals(identifier.getBranchName())) {
            throw new UnsupportedOperationException(
                    this.getClass().getName() + " currently does not support table branches");
        }
    }

    public static Path newTableLocation(String warehouse, Identifier identifier) {
        checkNotBranch(identifier, "newTableLocation");
        checkNotSystemTable(identifier, "newTableLocation");
        return new Path(
                newDatabasePath(warehouse, identifier.getDatabaseName()),
                identifier.getTableName());
    }

    public static Path newDatabasePath(String warehouse, String database) {
        return new Path(warehouse, database + DB_SUFFIX);
    }

    private void copyTableDefaultOptions(Map<String, String> options) {
        tableDefaultOptions.forEach(options::putIfAbsent);
    }

    private void validateCustomTablePath(Map<String, String> options) {
        if (!allowCustomTablePath() && options.containsKey(CoreOptions.PATH.key())) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The current catalog %s does not support specifying the table path when creating a table.",
                            this.getClass().getSimpleName()));
        }
    }

    // =============================== Meta in File System =====================================

    protected List<String> listDatabasesInFileSystem(Path warehouse) throws IOException {
        List<String> databases = new ArrayList<>();
        for (FileStatus status : fileIO(warehouse).listDirectories(warehouse)) {
            Path path = status.getPath();
            if (status.isDir() && path.getName().endsWith(DB_SUFFIX)) {
                String fileName = path.getName();
                databases.add(fileName.substring(0, fileName.length() - DB_SUFFIX.length()));
            }
        }
        return databases;
    }

    protected List<String> listTablesInFileSystem(Path databasePath) throws IOException {
        List<String> tables = new ArrayList<>();
        for (FileStatus status : fileIO(databasePath).listDirectories(databasePath)) {
            if (status.isDir() && tableExistsInFileSystem(status.getPath(), DEFAULT_MAIN_BRANCH)) {
                tables.add(status.getPath().getName());
            }
        }
        return tables;
    }

    protected boolean tableExistsInFileSystem(Path tablePath, String branchName) {
        SchemaManager schemaManager = new SchemaManager(fileIO(tablePath), tablePath, branchName);

        // in order to improve the performance, check the schema-0 firstly.
        boolean schemaZeroExists = schemaManager.schemaExists(0);
        if (schemaZeroExists) {
            return true;
        } else {
            // if schema-0 not exists, fallback to check other schemas
            return !schemaManager.listAllIds().isEmpty();
        }
    }

    public Optional<TableSchema> tableSchemaInFileSystem(Path tablePath, String branchName) {
        Optional<TableSchema> schema =
                new SchemaManager(fileIO(tablePath), tablePath, branchName).latest();
        if (!DEFAULT_MAIN_BRANCH.equals(branchName)) {
            schema =
                    schema.map(
                            s -> {
                                Options branchOptions = new Options(s.options());
                                branchOptions.set(CoreOptions.BRANCH, branchName);
                                return s.copy(branchOptions.toMap());
                            });
        }
        schema.ifPresent(s -> s.options().put(PATH.key(), tablePath.toString()));
        return schema;
    }
}
