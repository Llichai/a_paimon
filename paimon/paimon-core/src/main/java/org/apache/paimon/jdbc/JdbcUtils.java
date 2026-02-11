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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;

import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * JDBC Catalog 工具类.
 *
 * <p>提供了 JDBC Catalog 操作所需的各种工具方法,包括:
 * <ul>
 *   <li>SQL 语句定义: 定义了所有元数据操作的 SQL 语句</li>
 *   <li>表操作: 创建、查询、更新、删除表元数据</li>
 *   <li>数据库操作: 检查数据库是否存在,管理数据库属性</li>
 *   <li>属性管理: 插入、更新、删除数据库属性</li>
 *   <li>锁操作: 获取和释放分布式锁</li>
 *   <li>配置提取: 从配置中提取 JDBC 相关配置</li>
 * </ul>
 *
 * <p>数据库表结构:
 * <pre>
 * 1. paimon_tables (表信息表)
 *    - catalog_key VARCHAR(255) NOT NULL
 *    - database_name VARCHAR(255) NOT NULL
 *    - table_name VARCHAR(255) NOT NULL
 *    - PRIMARY KEY (catalog_key, database_name, table_name)
 *
 * 2. paimon_database_properties (数据库属性表)
 *    - catalog_key VARCHAR(255) NOT NULL
 *    - database_name VARCHAR(255) NOT NULL
 *    - property_key VARCHAR(255)
 *    - property_value VARCHAR(1000)
 *    - PRIMARY KEY (catalog_key, database_name, property_key)
 *
 * 3. paimon_distributed_locks (分布式锁表)
 *    - lock_id VARCHAR(N) NOT NULL
 *    - acquired_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
 *    - expire_time_seconds BIGINT DEFAULT 0 NOT NULL
 *    - PRIMARY KEY (lock_id)
 * </pre>
 */
public class JdbcUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcUtils.class);

    // ==================== 表信息表相关常量 ====================

    /** 表信息表名称 */
    public static final String CATALOG_TABLE_NAME = "paimon_tables";

    /** Catalog 键字段名 */
    public static final String CATALOG_KEY = "catalog_key";

    /** 数据库名字段名 */
    public static final String TABLE_DATABASE = "database_name";

    /** 表名字段名 */
    /** 表名字段名 */
    public static final String TABLE_NAME = "table_name";

    /** 创建表信息表的 SQL 语句 */
    static final String CREATE_CATALOG_TABLE =
            "CREATE TABLE "
                    + CATALOG_TABLE_NAME
                    + "("
                    + CATALOG_KEY
                    + " VARCHAR(255) NOT NULL,"
                    + TABLE_DATABASE
                    + " VARCHAR(255) NOT NULL,"
                    + TABLE_NAME
                    + " VARCHAR(255) NOT NULL,"
                    + " PRIMARY KEY ("
                    + CATALOG_KEY
                    + ", "
                    + TABLE_DATABASE
                    + ", "
                    + TABLE_NAME
                    + ")"
                    + ")";

    /** 查询表是否存在的 SQL 语句 */
    static final String GET_TABLE_SQL =
            "SELECT * FROM "
                    + CATALOG_TABLE_NAME
                    + " WHERE "
                    + CATALOG_KEY
                    + " = ? AND "
                    + TABLE_DATABASE
                    + " = ? AND "
                    + TABLE_NAME
                    + " = ? ";

    /** 列出数据库中所有表的 SQL 语句 */
    static final String LIST_TABLES_SQL =
            "SELECT * FROM "
                    + CATALOG_TABLE_NAME
                    + " WHERE "
                    + CATALOG_KEY
                    + " = ? AND "
                    + TABLE_DATABASE
                    + " = ?";

    /** 删除数据库中所有表的 SQL 语句 */
    static final String DELETE_TABLES_SQL =
            "DELETE FROM  "
                    + CATALOG_TABLE_NAME
                    + " WHERE "
                    + CATALOG_KEY
                    + " = ? AND "
                    + TABLE_DATABASE
                    + " = ?";

    /** 重命名表的 SQL 语句 */
    static final String RENAME_TABLE_SQL =
            "UPDATE "
                    + CATALOG_TABLE_NAME
                    + " SET "
                    + TABLE_DATABASE
                    + " = ? , "
                    + TABLE_NAME
                    + " = ? "
                    + " WHERE "
                    + CATALOG_KEY
                    + " = ? AND "
                    + TABLE_DATABASE
                    + " = ? AND "
                    + TABLE_NAME
                    + " = ? ";

    /** 删除单个表的 SQL 语句 */
    static final String DROP_TABLE_SQL =
            "DELETE FROM "
                    + CATALOG_TABLE_NAME
                    + " WHERE "
                    + CATALOG_KEY
                    + " = ? AND "
                    + TABLE_DATABASE
                    + " = ? AND "
                    + TABLE_NAME
                    + " = ? ";

    /** 查询数据库是否存在的 SQL 语句(从表信息表) */
    static final String GET_DATABASE_SQL =
            "SELECT "
                    + TABLE_DATABASE
                    + " FROM "
                    + CATALOG_TABLE_NAME
                    + " WHERE "
                    + CATALOG_KEY
                    + " = ? AND "
                    + TABLE_DATABASE
                    + " = ? LIMIT 1";

    /** 列出所有数据库的 SQL 语句(从表信息表) */
    static final String LIST_ALL_TABLE_DATABASES_SQL =
            "SELECT DISTINCT "
                    + TABLE_DATABASE
                    + " FROM "
                    + CATALOG_TABLE_NAME
                    + " WHERE "
                    + CATALOG_KEY
                    + " = ?";

    /** 创建表时插入元数据的 SQL 语句 */
    static final String DO_COMMIT_CREATE_TABLE_SQL =
            "INSERT INTO "
                    + CATALOG_TABLE_NAME
                    + " ("
                    + CATALOG_KEY
                    + ", "
                    + TABLE_DATABASE
                    + ", "
                    + TABLE_NAME
                    + ") "
                    + " VALUES (?,?,?)";

    // ==================== 数据库属性表相关常量 ====================

    /** 数据库属性表名称 */
    static final String DATABASE_PROPERTIES_TABLE_NAME = "paimon_database_properties";

    /** 数据库名字段名 */
    static final String DATABASE_NAME = "database_name";

    /** 属性键字段名 */
    static final String DATABASE_PROPERTY_KEY = "property_key";

    /** 属性值字段名 */
    static final String DATABASE_PROPERTY_VALUE = "property_value";

    /** 创建数据库属性表的 SQL 语句 */
    static final String CREATE_DATABASE_PROPERTIES_TABLE =
            "CREATE TABLE "
                    + DATABASE_PROPERTIES_TABLE_NAME
                    + "("
                    + CATALOG_KEY
                    + " VARCHAR(255) NOT NULL,"
                    + DATABASE_NAME
                    + " VARCHAR(255) NOT NULL,"
                    + DATABASE_PROPERTY_KEY
                    + " VARCHAR(255),"
                    + DATABASE_PROPERTY_VALUE
                    + " VARCHAR(1000),"
                    + "PRIMARY KEY ("
                    + CATALOG_KEY
                    + ", "
                    + DATABASE_NAME
                    + ", "
                    + DATABASE_PROPERTY_KEY
                    + ")"
                    + ")";

    /** 查询数据库是否存在的 SQL 语句(从属性表) */
    static final String GET_DATABASE_PROPERTIES_SQL =
            "SELECT "
                    + DATABASE_NAME
                    + " FROM "
                    + DATABASE_PROPERTIES_TABLE_NAME
                    + " WHERE "
                    + CATALOG_KEY
                    + " = ? AND "
                    + DATABASE_NAME
                    + " = ? ";

    /** 插入数据库属性的 SQL 语句(基础部分) */
    static final String INSERT_DATABASE_PROPERTIES_SQL =
            "INSERT INTO "
                    + DATABASE_PROPERTIES_TABLE_NAME
                    + " ("
                    + CATALOG_KEY
                    + ", "
                    + DATABASE_NAME
                    + ", "
                    + DATABASE_PROPERTY_KEY
                    + ", "
                    + DATABASE_PROPERTY_VALUE
                    + ") VALUES ";

    /** 插入属性值的占位符 */
    static final String INSERT_PROPERTIES_VALUES_BASE = "(?,?,?,?)";

    /** 获取数据库所有属性的 SQL 语句 */
    static final String GET_ALL_DATABASE_PROPERTIES_SQL =
            "SELECT * "
                    + " FROM "
                    + DATABASE_PROPERTIES_TABLE_NAME
                    + " WHERE "
                    + CATALOG_KEY
                    + " = ? AND "
                    + DATABASE_NAME
                    + " = ? ";

    /** 删除数据库属性的 SQL 语句(基础部分) */
    static final String DELETE_DATABASE_PROPERTIES_SQL =
            "DELETE FROM "
                    + DATABASE_PROPERTIES_TABLE_NAME
                    + " WHERE "
                    + CATALOG_KEY
                    + " = ? AND "
                    + DATABASE_NAME
                    + " = ? AND "
                    + DATABASE_PROPERTY_KEY
                    + " IN ";

    /** 删除数据库所有属性的 SQL 语句 */
    static final String DELETE_ALL_DATABASE_PROPERTIES_SQL =
            "DELETE FROM "
                    + DATABASE_PROPERTIES_TABLE_NAME
                    + " WHERE "
                    + CATALOG_KEY
                    + " = ? AND "
                    + DATABASE_NAME
                    + " = ?";

    /** 列出所有数据库的 SQL 语句(从属性表) */
    static final String LIST_ALL_PROPERTY_DATABASES_SQL =
            "SELECT DISTINCT "
                    + DATABASE_NAME
                    + " FROM "
                    + DATABASE_PROPERTIES_TABLE_NAME
                    + " WHERE "
                    + CATALOG_KEY
                    + " = ?";

    // ==================== 分布式锁表相关常量 ====================

    /** 分布式锁表名称 */
    static final String DISTRIBUTED_LOCKS_TABLE_NAME = "paimon_distributed_locks";

    /** 锁 ID 字段名 */
    static final String LOCK_ID = "lock_id";

    /** 锁获取时间字段名 */
    static final String ACQUIRED_AT = "acquired_at";

    /** 锁过期时间字段名(秒) */
    /** 锁过期时间字段名(秒) */
    static final String EXPIRE_TIME = "expire_time_seconds";

    /**
     * 从配置中提取 JDBC 相关配置.
     *
     * <p>从配置映射中提取具有指定前缀的配置项,并移除前缀后作为 JDBC 属性。
     *
     * <p>示例:
     * <pre>
     * 输入配置:
     *   jdbc.user = root
     *   jdbc.password = 123456
     *   jdbc.socket_timeout = 30000
     *
     * 输出 Properties:
     *   user = root
     *   password = 123456
     *   socket_timeout = 30000
     * </pre>
     *
     * @param properties 配置映射
     * @param prefix 配置前缀(如 "jdbc.")
     * @return JDBC 属性对象
     */
    public static Properties extractJdbcConfiguration(
            Map<String, String> properties, String prefix) {
        Properties result = new Properties();
        properties.forEach(
                (key, value) -> {
                    if (key.startsWith(prefix)) {
                        // 移除前缀并添加到结果中
                        result.put(key.substring(prefix.length()), value);
                    }
                });
        return result;
    }

    /**
     * 获取 Paimon 表的元数据.
     *
     * <p>从 paimon_tables 表中查询指定表的元数据记录。
     *
     * @param connections JDBC 连接池
     * @param storeKey Catalog 键
     * @param databaseName 数据库名
     * @param tableName 表名
     * @return 表元数据映射,包含 catalog_key、database_name、table_name 字段
     * @throws SQLException SQL 异常
     * @throws InterruptedException 中断异常
     */
    public static Map<String, String> getTable(
            JdbcClientPool connections, String storeKey, String databaseName, String tableName)
            throws SQLException, InterruptedException {
        return connections.run(
                conn -> {
                    Map<String, String> table = Maps.newHashMap();

                    try (PreparedStatement sql = conn.prepareStatement(JdbcUtils.GET_TABLE_SQL)) {
                        sql.setString(1, storeKey);
                        sql.setString(2, databaseName);
                        sql.setString(3, tableName);
                        ResultSet rs = sql.executeQuery();
                        if (rs.next()) {
                            table.put(CATALOG_KEY, rs.getString(CATALOG_KEY));
                            table.put(TABLE_DATABASE, rs.getString(TABLE_DATABASE));
                            table.put(TABLE_NAME, rs.getString(TABLE_NAME));
                        }
                        rs.close();
                    }
                    return table;
                });
    }

    /**
     * 更新表元数据(重命名表).
     *
     * <p>在 paimon_tables 表中更新表的数据库名和表名。
     * 如果目标表已存在则抛出异常,如果原表不存在也抛出异常。
     *
     * @param connections JDBC 连接池
     * @param storeKey Catalog 键
     * @param fromTable 原表标识符
     * @param toTable 新表标识符
     * @throws RuntimeException 如果目标表已存在或原表不存在
     */
    public static void updateTable(
            JdbcClientPool connections, String storeKey, Identifier fromTable, Identifier toTable) {
        int updatedRecords =
                execute(
                        err -> {
                            // 处理主键约束冲突(目标表已存在)
                            if (err instanceof SQLIntegrityConstraintViolationException
                                    || (err.getMessage() != null
                                            && err.getMessage().contains("constraint failed"))) {
                                throw new RuntimeException(
                                        String.format("Table already exists: %s", toTable));
                            }
                        },
                        connections,
                        JdbcUtils.RENAME_TABLE_SQL,
                        toTable.getDatabaseName(),
                        toTable.getObjectName(),
                        storeKey,
                        fromTable.getDatabaseName(),
                        fromTable.getObjectName());

        if (updatedRecords == 1) {
            LOG.info("Renamed table from {}, to {}", fromTable, toTable);
        } else if (updatedRecords == 0) {
            throw new RuntimeException(String.format("Table does not exist: %s", fromTable));
        } else {
            LOG.warn(
                    "Rename operation affected {} rows: the catalog table's primary key assumption has been violated",
                    updatedRecords);
        }
    }

    /**
     * 检查数据库是否存在.
     *
     * <p>从两个来源检查:
     * <ul>
     *   <li>paimon_tables: 包含表的数据库</li>
     *   <li>paimon_database_properties: 显式创建但可能没有表的数据库</li>
     * </ul>
     *
     * @param connections JDBC 连接池
     * @param storeKey Catalog 键
     * @param databaseName 数据库名
     * @return 是否存在
     */
    public static boolean databaseExists(
            JdbcClientPool connections, String storeKey, String databaseName) {

        if (exists(connections, JdbcUtils.GET_DATABASE_SQL, storeKey, databaseName)) {
            return true;
        }

        if (exists(connections, JdbcUtils.GET_DATABASE_PROPERTIES_SQL, storeKey, databaseName)) {
            return true;
        }
        return false;
    }

    /**
     * 检查表是否存在.
     *
     * @param connections JDBC 连接池
     * @param storeKey Catalog 键
     * @param databaseName 数据库名
     * @param tableName 表名
     * @return 是否存在
     */
    public static boolean tableExists(
            JdbcClientPool connections, String storeKey, String databaseName, String tableName) {
        if (exists(connections, JdbcUtils.GET_TABLE_SQL, storeKey, databaseName, tableName)) {
            return true;
        }
        return false;
    }

    /**
     * 通用的存在性检查方法.
     *
     * <p>执行查询,如果有结果则返回 true。
     *
     * @param connections JDBC 连接池
     * @param sql SQL 查询语句
     * @param args SQL 参数
     * @return 是否存在
     */
    @SuppressWarnings("checkstyle:NestedTryDepth")
    private static boolean exists(JdbcClientPool connections, String sql, String... args) {
        try {
            return connections.run(
                    conn -> {
                        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                            // 设置参数
                            for (int index = 0; index < args.length; index++) {
                                preparedStatement.setString(index + 1, args[index]);
                            }
                            try (ResultSet rs = preparedStatement.executeQuery()) {
                                if (rs.next()) {
                                    return true;
                                }
                            }
                        }
                        return false;
                    });
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Failed to execute exists query: %s", sql), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted in SQL query", e);
        }
    }

    /**
     * 执行 SQL 更新语句.
     *
     * @param connections JDBC 连接池
     * @param sql SQL 语句
     * @param args SQL 参数
     * @return 影响的行数
     */
    public static int execute(JdbcClientPool connections, String sql, String... args) {
        return execute(err -> {}, connections, sql, args);
    }

    /**
     * 执行 SQL 更新语句,带错误处理器.
     *
     * <p>允许调用者提供 SQL 异常处理逻辑。
     *
     * @param sqlErrorHandler SQL 异常处理器
     * @param connections JDBC 连接池
     * @param sql SQL 语句
     * @param args SQL 参数
     * @return 影响的行数
     */
    public static int execute(
            Consumer<SQLException> sqlErrorHandler,
            JdbcClientPool connections,
            String sql,
            String... args) {
        try {
            return connections.run(
                    conn -> {
                        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                            // 设置参数
                            for (int pos = 0; pos < args.length; pos++) {
                                preparedStatement.setString(pos + 1, args[pos]);
                            }
                            return preparedStatement.executeUpdate();
                        }
                    });
        } catch (SQLException e) {
            sqlErrorHandler.accept(e);
            throw new RuntimeException(String.format("Failed to execute: %s", sql), e);
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted in SQL command", e);
        }
    }

    /**
     * 批量插入数据库属性.
     *
     * <p>构造批量插入 SQL 语句,一次性插入多个属性。
     *
     * @param connections JDBC 连接池
     * @param storeKey Catalog 键
     * @param databaseName 数据库名
     * @param properties 属性映射
     * @return 是否成功插入所有属性
     * @throws IllegalStateException 如果部分插入失败
     */
    public static boolean insertProperties(
            JdbcClientPool connections,
            String storeKey,
            String databaseName,
            Map<String, String> properties) {
        // 将属性展开为参数数组: [storeKey, db, key1, val1, storeKey, db, key2, val2, ...]
        String[] args =
                properties.entrySet().stream()
                        .flatMap(
                                entry ->
                                        Stream.of(
                                                storeKey,
                                                databaseName,
                                                entry.getKey(),
                                                entry.getValue()))
                        .toArray(String[]::new);

        // 构造批量插入 SQL
        int insertedRecords =
                execute(connections, JdbcUtils.insertPropertiesStatement(properties.size()), args);
        if (insertedRecords == properties.size()) {
            return true;
        }
        throw new IllegalStateException(
                String.format(
                        "Failed to insert: %d of %d succeeded",
                        insertedRecords, properties.size()));
    }

    /**
     * 生成批量插入属性的 SQL 语句.
     *
     * <p>根据属性数量生成多个 VALUES 子句。
     *
     * <p>示例(size=2):
     * <pre>
     * INSERT INTO paimon_database_properties
     * (catalog_key, database_name, property_key, property_value)
     * VALUES (?,?,?,?), (?,?,?,?)
     * </pre>
     *
     * @param size 属性数量
     * @return SQL 语句
     */
    private static String insertPropertiesStatement(int size) {
        StringBuilder sqlStatement = new StringBuilder(JdbcUtils.INSERT_DATABASE_PROPERTIES_SQL);
        for (int i = 0; i < size; i++) {
            if (i != 0) {
                sqlStatement.append(", ");
            }
            sqlStatement.append(JdbcUtils.INSERT_PROPERTIES_VALUES_BASE);
        }
        return sqlStatement.toString();
    }

    /**
     * 批量更新数据库属性.
     *
     * <p>使用 CASE WHEN 语句批量更新多个属性的值。
     *
     * @param connections JDBC 连接池
     * @param storeKey Catalog 键
     * @param databaseName 数据库名
     * @param properties 属性映射
     * @return 是否成功更新所有属性
     * @throws IllegalStateException 如果部分更新失败
     */
    public static boolean updateProperties(
            JdbcClientPool connections,
            String storeKey,
            String databaseName,
            Map<String, String> properties) {
        // CASE WHEN 子句的参数: [key1, val1, key2, val2, ...]
        Stream<String> caseArgs =
                properties.entrySet().stream()
                        .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()));
        // WHERE 子句的参数: [storeKey, db, key1, key2, ...]
        Stream<String> whereArgs =
                Stream.concat(Stream.of(storeKey, databaseName), properties.keySet().stream());

        String[] args = Stream.concat(caseArgs, whereArgs).toArray(String[]::new);

        int updatedRecords =
                execute(connections, JdbcUtils.updatePropertiesStatement(properties.size()), args);
        if (updatedRecords == properties.size()) {
            return true;
        }
        throw new IllegalStateException(
                String.format(
                        "Failed to update: %d of %d succeeded", updatedRecords, properties.size()));
    }

    /**
     * 生成批量更新属性的 SQL 语句.
     *
     * <p>使用 CASE WHEN 语句实现批量更新。
     *
     * <p>示例(size=2):
     * <pre>
     * UPDATE paimon_database_properties
     * SET property_value = CASE
     *   WHEN property_key = ? THEN ?
     *   WHEN property_key = ? THEN ?
     * END
     * WHERE catalog_key = ? AND database_name = ?
     *   AND property_key IN (?, ?)
     * </pre>
     *
     * @param size 属性数量
     * @return SQL 语句
     */
    private static String updatePropertiesStatement(int size) {
        StringBuilder sqlStatement =
                new StringBuilder(
                        "UPDATE "
                                + DATABASE_PROPERTIES_TABLE_NAME
                                + " SET "
                                + DATABASE_PROPERTY_VALUE
                                + " = CASE");
        // 添加 CASE WHEN 子句
        for (int i = 0; i < size; i += 1) {
            sqlStatement.append(" WHEN " + DATABASE_PROPERTY_KEY + " = ? THEN ?");
        }

        // 添加 WHERE 子句
        sqlStatement.append(
                " END WHERE "
                        + CATALOG_KEY
                        + " = ? AND "
                        + DATABASE_NAME
                        + " = ? AND "
                        + DATABASE_PROPERTY_KEY
                        + " IN ");

        // 添加 IN 子句
        String values = String.join(",", Collections.nCopies(size, String.valueOf('?')));
        sqlStatement.append("(").append(values).append(")");

        return sqlStatement.toString();
    }

    /**
     * 批量删除数据库属性.
     *
     * @param connections JDBC 连接池
     * @param storeKey Catalog 键
     * @param databaseName 数据库名
     * @param removeKeys 要删除的属性键集合
     * @return 是否成功删除属性
     * @throws IllegalStateException 如果删除失败
     */
    public static boolean deleteProperties(
            JdbcClientPool connections,
            String storeKey,
            String databaseName,
            Set<String> removeKeys) {
        // 参数: [storeKey, db, key1, key2, ...]
        String[] args =
                Stream.concat(Stream.of(storeKey, databaseName), removeKeys.stream())
                        .toArray(String[]::new);

        int deleteRecords =
                execute(connections, JdbcUtils.deletePropertiesStatement(removeKeys), args);
        if (deleteRecords > 0) {
            return true;
        }
        throw new IllegalStateException(
                String.format(
                        "Failed to delete: %d of %d succeeded", deleteRecords, removeKeys.size()));
    }

    /**
     * 创建分布式锁表.
     *
     * <p>根据数据库协议创建对应的锁表。
     *
     * @param connections JDBC 连接池
     * @param options 配置选项
     * @throws SQLException SQL 异常
     * @throws InterruptedException 中断异常
     */
    public static void createDistributedLockTable(JdbcClientPool connections, Options options)
            throws SQLException, InterruptedException {
        DistributedLockDialectFactory.create(connections.getProtocol())
                .createTable(connections, options);
    }

    /**
     * 获取分布式锁.
     *
     * <p>在获取锁之前,先清理已超时的锁记录。
     *
     * @param connections JDBC 连接池
     * @param lockId 锁 ID
     * @param timeoutMillSeconds 超时时间(毫秒)
     * @return 是否成功获取锁
     * @throws SQLException SQL 异常
     * @throws InterruptedException 中断异常
     */
    public static boolean acquire(
            JdbcClientPool connections, String lockId, long timeoutMillSeconds)
            throws SQLException, InterruptedException {
        JdbcDistributedLockDialect distributedLockDialect =
                DistributedLockDialectFactory.create(connections.getProtocol());
        // 先清理超时的锁
        int affectedRows = distributedLockDialect.tryReleaseTimedOutLock(connections, lockId);
        if (affectedRows > 0) {
            LOG.debug("Successfully cleared " + affectedRows + " lock records");
        }
        // 尝试获取锁
        return distributedLockDialect.lockAcquire(connections, lockId, timeoutMillSeconds);
    }

    /**
     * 释放分布式锁.
     *
     * @param connections JDBC 连接池
     * @param lockId 锁 ID
     * @throws SQLException SQL 异常
     * @throws InterruptedException 中断异常
     */
    public static void release(JdbcClientPool connections, String lockId)
            throws SQLException, InterruptedException {
        DistributedLockDialectFactory.create(connections.getProtocol())
                .releaseLock(connections, lockId);
    }

    /**
     * 生成删除属性的 SQL 语句.
     *
     * <p>示例(2个属性):
     * <pre>
     * DELETE FROM paimon_database_properties
     * WHERE catalog_key = ? AND database_name = ?
     *   AND property_key IN (?, ?)
     * </pre>
     *
     * @param properties 要删除的属性键集合
     * @return SQL 语句
     */
    private static String deletePropertiesStatement(Set<String> properties) {
        StringBuilder sqlStatement = new StringBuilder(JdbcUtils.DELETE_DATABASE_PROPERTIES_SQL);
        String values =
                String.join(",", Collections.nCopies(properties.size(), String.valueOf('?')));
        sqlStatement.append("(").append(values).append(")");

        return sqlStatement.toString();
    }
}
