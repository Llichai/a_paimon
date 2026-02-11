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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

import static org.apache.paimon.options.CatalogOptions.CASE_SENSITIVE;

/**
 * 基于文件系统的 Catalog 实现
 *
 * <p>FileSystemCatalog 是 Paimon 最基础的 Catalog 实现,直接使用文件系统存储元数据。
 * 支持各种文件系统:HDFS、S3、OSS、本地文件系统等。
 *
 * <p>目录结构:
 * <pre>
 * warehouse/                          # 仓库根目录
 *   ├─ database1.db/                  # 数据库目录（以 .db 结尾）
 *   │   ├─ table1/                    # 表目录
 *   │   │   ├─ schema/                # Schema 文件目录
 *   │   │   │   ├─ schema-0           # Schema 版本 0
 *   │   │   │   ├─ schema-1           # Schema 版本 1
 *   │   │   │   └─ ...
 *   │   │   ├─ snapshot/              # 快照文件目录
 *   │   │   │   ├─ snapshot-1
 *   │   │   │   ├─ snapshot-2
 *   │   │   │   └─ ...
 *   │   │   ├─ manifest/              # Manifest 文件目录
 *   │   │   └─ bucket-0/              # 数据文件目录（按 bucket 分）
 *   │   │       └─ data-xxx.orc
 *   │   └─ table2/
 *   └─ database2.db/
 * </pre>
 *
 * <p>元数据存储:
 * <ul>
 *   <li><b>数据库</b>: 通过目录是否存在判断数据库是否存在（不支持存储数据库属性）
 *   <li><b>表</b>: Schema 文件存储在 table/schema/ 目录下
 *   <li><b>快照</b>: 快照文件存储在 table/snapshot/ 目录下
 * </ul>
 *
 * <p>特点:
 * <ul>
 *   <li><b>简单</b>: 无需额外的元数据服务（如 Hive Metastore）
 *   <li><b>轻量</b>: 直接使用文件系统,适合小规模场景
 *   <li><b>局限</b>: 不支持数据库属性存储、不支持跨表事务
 *   <li><b>锁机制</b>: 对象存储（S3/OSS）默认启用分布式锁
 * </ul>
 *
 * <p>锁机制:
 * <p>由于文件系统操作不是原子的（特别是对象存储）,FileSystemCatalog 支持可选的分布式锁:
 * <ul>
 *   <li>对象存储（S3/OSS/COS）: 默认启用锁
 *   <li>HDFS: 默认不启用锁（HDFS 操作是原子的）
 *   <li>可通过 {@code lock.enabled} 配置强制启用/禁用
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 1. 创建 FileSystemCatalog
 * FileIO fileIO = FileIO.get(new Path("hdfs://namenode:8020"), new Configuration());
 * Path warehouse = new Path("hdfs://namenode:8020/warehouse");
 * FileSystemCatalog catalog = new FileSystemCatalog(fileIO, warehouse);
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
 * // 表的物理路径: hdfs://namenode:8020/warehouse/my_db.db/my_table
 * }</pre>
 *
 * <p>与其他 Catalog 的比较:
 * <ul>
 *   <li>{@link FileSystemCatalog}: 适合测试和小规模场景,无需额外服务
 *   <li>HiveCatalog: 适合生产环境,与 Hive 生态集成,支持丰富的元数据管理
 *   <li>JdbcCatalog: 适合需要关系型数据库管理元数据的场景
 * </ul>
 *
 * <p>线程安全:
 * <p>FileSystemCatalog 本身是线程安全的。对于并发写入,通过以下机制保证:
 * <ul>
 *   <li>Schema 提交使用乐观锁（版本号递增）
 *   <li>快照提交使用原子重命名（rename）
 *   <li>对象存储使用可选的分布式锁（如 Zookeeper/JDBC）
 * </ul>
 *
 * @see AbstractCatalog
 * @see FileSystemCatalogFactory
 */
public class FileSystemCatalog extends AbstractCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemCatalog.class);

    /** 仓库根路径 */
    private final Path warehouse;

    /**
     * 构造函数 - 使用默认上下文
     *
     * @param fileIO 文件 I/O 接口
     * @param warehouse 仓库根路径
     */
    public FileSystemCatalog(FileIO fileIO, Path warehouse) {
        super(fileIO);
        this.warehouse = warehouse;
    }

    /**
     * 构造函数 - 使用指定上下文
     *
     * @param fileIO 文件 I/O 接口
     * @param warehouse 仓库根路径
     * @param context Catalog 上下文
     */
    public FileSystemCatalog(FileIO fileIO, Path warehouse, CatalogContext context) {
        super(fileIO, context);
        this.warehouse = warehouse;
    }

    /**
     * 列出所有数据库
     *
     * <p>通过扫描 warehouse 目录下所有以 ".db" 结尾的子目录来获取数据库列表。
     *
     * @return 数据库名称列表
     */
    @Override
    public List<String> listDatabases() {
        return uncheck(() -> listDatabasesInFileSystem(warehouse));
    }

    /**
     * 创建数据库的实现
     *
     * <p>在 FileSystemCatalog 中,创建数据库就是创建一个目录。
     *
     * <p>限制:
     * <ul>
     *   <li>不支持自定义数据库位置（location）
     *   <li>不支持存储数据库属性（properties 会被忽略并警告）
     * </ul>
     *
     * @param name 数据库名称
     * @param properties 数据库属性（会被忽略）
     * @throws IllegalArgumentException 如果指定了 location 属性
     * @throws RuntimeException 如果创建目录失败
     */
    @Override
    protected void createDatabaseImpl(String name, Map<String, String> properties) {
        if (properties.containsKey(Catalog.DB_LOCATION_PROP)) {
            throw new IllegalArgumentException(
                    "Cannot specify location for a database when using fileSystem catalog.");
        }
        if (!properties.isEmpty()) {
            LOG.warn(
                    "Currently filesystem catalog can't store database properties, discard properties: {}",
                    properties);
        }

        Path databasePath = newDatabasePath(name);
        if (!uncheck(() -> fileIO.mkdirs(databasePath))) {
            throw new RuntimeException(
                    String.format(
                            "Create database location failed, " + "database: %s, location: %s",
                            name, databasePath));
        }
    }

    /**
     * 获取数据库的实现
     *
     * <p>通过检查数据库目录是否存在来判断数据库是否存在。
     *
     * @param name 数据库名称
     * @return Database 对象
     * @throws DatabaseNotExistException 如果数据库目录不存在
     */
    @Override
    public Database getDatabaseImpl(String name) throws DatabaseNotExistException {
        if (!uncheck(() -> fileIO.exists(newDatabasePath(name)))) {
            throw new DatabaseNotExistException(name);
        }
        return Database.of(name);
    }

    /**
     * 删除数据库的实现
     *
     * <p>递归删除数据库目录及其所有内容。
     *
     * @param name 数据库名称
     * @throws RuntimeException 如果删除失败
     */
    @Override
    protected void dropDatabaseImpl(String name) {
        Path databasePath = newDatabasePath(name);
        if (!uncheck(() -> fileIO.delete(databasePath, true))) {
            throw new RuntimeException(
                    String.format(
                            "Delete database failed, " + "database: %s, location: %s",
                            name, databasePath));
        }
    }

    /**
     * 修改数据库的实现
     *
     * <p>FileSystemCatalog 不支持修改数据库属性,因为不存储属性。
     *
     * @param name 数据库名称
     * @param changes 属性变更
     * @throws UnsupportedOperationException 总是抛出此异常
     */
    @Override
    protected void alterDatabaseImpl(String name, List<PropertyChange> changes)
            throws DatabaseNotExistException {
        throw new UnsupportedOperationException("Alter database is not supported.");
    }

    /**
     * 列出数据库中所有表的实现
     *
     * <p>通过扫描数据库目录下的所有子目录来获取表列表。
     *
     * @param databaseName 数据库名称
     * @return 表名称列表
     */
    @Override
    protected List<String> listTablesImpl(String databaseName) {
        return uncheck(() -> listTablesInFileSystem(newDatabasePath(databaseName)));
    }

    /**
     * 加载表的 Schema
     *
     * @param identifier 表标识符
     * @return 表的 Schema
     * @throws TableNotExistException 如果表不存在
     */
    @Override
    public TableSchema loadTableSchema(Identifier identifier) throws TableNotExistException {
        return tableSchemaInFileSystem(
                        getTableLocation(identifier), identifier.getBranchNameOrDefault())
                .orElseThrow(() -> new TableNotExistException(identifier));
    }

    /**
     * 删除表的实现
     *
     * <p>删除表目录及所有关联的外部路径。
     *
     * @param identifier 表标识符
     * @param externalPaths 外部数据路径列表
     */
    @Override
    protected void dropTableImpl(Identifier identifier, List<Path> externalPaths) {
        Path path = getTableLocation(identifier);
        uncheck(() -> fileIO.delete(path, true));
        for (Path externalPath : externalPaths) {
            uncheck(() -> fileIO.delete(externalPath, true));
        }
    }

    /**
     * 创建表的实现
     *
     * <p>通过 {@link SchemaManager} 创建表的初始 Schema 文件。
     *
     * <p>使用锁保证并发安全:
     * <ul>
     *   <li>如果启用了锁,在锁保护下创建表
     *   <li>否则直接创建（依赖文件系统的原子性）
     * </ul>
     *
     * @param identifier 表标识符
     * @param schema 表的 Schema
     */
    @Override
    public void createTableImpl(Identifier identifier, Schema schema) {
        SchemaManager schemaManager = schemaManager(identifier);
        try {
            runWithLock(identifier, () -> uncheck(() -> schemaManager.createTable(schema)));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 在锁保护下执行操作
     *
     * <p>如果配置了锁工厂,则在锁保护下执行;否则直接执行。
     *
     * @param identifier 表标识符（用于确定锁的粒度）
     * @param callable 要执行的操作
     * @param <T> 返回值类型
     * @return 操作的返回值
     * @throws Exception 如果操作失败
     */
    public <T> T runWithLock(Identifier identifier, Callable<T> callable) throws Exception {
        Optional<CatalogLockFactory> lockFactory = lockFactory();
        try (Lock lock =
                lockFactory
                        .map(factory -> factory.createLock(lockContext().orElse(null)))
                        .map(l -> Lock.fromCatalog(l, identifier))
                        .orElseGet(Lock::empty)) {
            return lock.runWithLock(callable);
        }
    }

    /**
     * 创建表的 SchemaManager
     *
     * @param identifier 表标识符
     * @return SchemaManager 实例
     */
    private SchemaManager schemaManager(Identifier identifier) {
        Path path = getTableLocation(identifier);
        return new SchemaManager(fileIO, path, identifier.getBranchNameOrDefault());
    }

    /**
     * 重命名表的实现
     *
     * <p>通过文件系统的 rename 操作重命名表目录。
     *
     * <p>警告: 对象存储（S3/OSS）的 rename 不是原子操作,可能导致部分文件移动失败。
     *
     * @param fromTable 源表标识符
     * @param toTable 目标表标识符
     * @throws RuntimeException 如果重命名失败
     */
    @Override
    public void renameTableImpl(Identifier fromTable, Identifier toTable) {
        Path fromPath = getTableLocation(fromTable);
        Path toPath = getTableLocation(toTable);
        if (!uncheck(() -> fileIO.rename(fromPath, toPath))) {
            throw new RuntimeException(
                    String.format("Failed to rename table %s to table %s.", fromTable, toTable));
        }
    }

    /**
     * 修改表的实现
     *
     * <p>通过 {@link SchemaManager} 提交 Schema 变更。
     *
     * @param identifier 表标识符
     * @param changes Schema 变更列表
     * @throws TableNotExistException 如果表不存在
     * @throws ColumnAlreadyExistException 如果列已存在
     * @throws ColumnNotExistException 如果列不存在
     */
    @Override
    protected void alterTableImpl(Identifier identifier, List<SchemaChange> changes)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        SchemaManager schemaManager = schemaManager(identifier);
        try {
            runWithLock(identifier, () -> schemaManager.commitChanges(changes));
        } catch (TableNotExistException
                | ColumnAlreadyExistException
                | ColumnNotExistException
                | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 工具方法 - 将受检异常转换为非受检异常
     *
     * @param callable 要执行的操作
     * @param <T> 返回值类型
     * @return 操作的返回值
     * @throws RuntimeException 如果操作抛出异常
     */
    protected static <T> T uncheck(Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {}

    /**
     * 返回仓库根路径
     *
     * @return 仓库路径字符串
     */
    @Override
    public String warehouse() {
        return warehouse.toString();
    }

    /**
     * 返回 Catalog 加载器
     *
     * @return FileSystemCatalogLoader 实例
     */
    @Override
    public CatalogLoader catalogLoader() {
        return new FileSystemCatalogLoader(fileIO, warehouse, context);
    }

    /**
     * 是否大小写敏感
     *
     * <p>默认为 true（大小写敏感）。
     *
     * @return 是否大小写敏感
     */
    @Override
    public boolean caseSensitive() {
        return context.options().getOptional(CASE_SENSITIVE).orElse(true);
    }
}
