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

import org.apache.paimon.options.Options;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * JDBC 分布式锁方言抽象基类.
 *
 * <p>提供了 JDBC 分布式锁实现的通用逻辑,包括创建锁表、获取锁、释放锁等操作。
 * 不同数据库(MySQL、PostgreSQL、SQLite)的具体 SQL 语句由子类实现。
 *
 * <p>使用场景:
 * <ul>
 *   <li>多客户端并发访问 JDBC Catalog 时保证元数据操作的原子性</li>
 *   <li>表的创建、修改、删除等需要独占访问的场景</li>
 * </ul>
 *
 * <p>锁表结构:
 * <ul>
 *   <li>lock_id: 锁标识,格式为 "catalog.database.table"</li>
 *   <li>acquired_at: 锁获取时间</li>
 *   <li>expire_time_seconds: 锁过期时间(秒)</li>
 * </ul>
 */
public abstract class AbstractDistributedLockDialect implements JdbcDistributedLockDialect {

    /**
     * 创建分布式锁表.
     *
     * <p>如果锁表已存在则跳过创建,否则使用子类提供的 SQL 创建表。
     *
     * @param connections JDBC 连接池
     * @param options 配置选项,包含锁键最大长度等参数
     * @throws SQLException SQL 异常
     * @throws InterruptedException 中断异常
     */
    @Override
    public void createTable(JdbcClientPool connections, Options options)
            throws SQLException, InterruptedException {
        // 获取锁键的最大长度配置
        Integer lockKeyMaxLength = JdbcCatalogOptions.lockKeyMaxLength(options);
        connections.run(
                conn -> {
                    // 检查锁表是否已存在
                    DatabaseMetaData dbMeta = conn.getMetaData();
                    ResultSet tableExists =
                            dbMeta.getTables(
                                    null, null, JdbcUtils.DISTRIBUTED_LOCKS_TABLE_NAME, null);
                    if (tableExists.next()) {
                        return true;
                    }
                    // 使用锁键最大长度格式化 SQL 并创建表
                    String createDistributedLockTableSql =
                            String.format(getCreateTableSql(), lockKeyMaxLength);
                    return conn.prepareStatement(createDistributedLockTableSql).execute();
                });
    }

    /**
     * 获取创建锁表的 SQL 语句.
     *
     * <p>由子类实现,不同数据库的 SQL 语法可能不同。
     *
     * @return 创建锁表的 SQL 语句,应包含 %s 占位符用于设置锁键最大长度
     */
    public abstract String getCreateTableSql();

    /**
     * 获取分布式锁.
     *
     * <p>尝试在锁表中插入一条锁记录。如果插入成功(没有主键冲突)则获取锁成功,
     * 如果插入失败(主键冲突)则说明锁已被其他客户端持有。
     *
     * @param connections JDBC 连接池
     * @param lockId 锁 ID,格式为 "catalog.database.table"
     * @param timeoutMillSeconds 锁超时时间(毫秒),转换为秒存储
     * @return 是否成功获取锁,true 表示获取成功,false 表示获取失败
     * @throws SQLException SQL 异常
     * @throws InterruptedException 中断异常
     */
    @Override
    public boolean lockAcquire(JdbcClientPool connections, String lockId, long timeoutMillSeconds)
            throws SQLException, InterruptedException {
        return connections.run(
                connection -> {
                    try (PreparedStatement preparedStatement =
                            connection.prepareStatement(getLockAcquireSql())) {
                        // 设置锁 ID
                        preparedStatement.setString(1, lockId);
                        // 设置过期时间(转换为秒)
                        preparedStatement.setLong(2, timeoutMillSeconds / 1000);
                        // 执行插入,影响行数大于0表示成功
                        return preparedStatement.executeUpdate() > 0;
                    } catch (SQLException ex) {
                        // 捕获主键冲突等异常,返回获取锁失败
                        return false;
                    }
                });
    }

    /**
     * 获取获取锁的 SQL 语句.
     *
     * <p>由子类实现,通常是一个 INSERT 语句。
     *
     * @return 获取锁的 SQL 语句
     */
    public abstract String getLockAcquireSql();

    /**
     * 释放分布式锁.
     *
     * <p>从锁表中删除指定的锁记录。
     *
     * @param connections JDBC 连接池
     * @param lockId 锁 ID,格式为 "catalog.database.table"
     * @return 是否成功释放锁,true 表示释放成功,false 表示锁不存在或已被释放
     * @throws SQLException SQL 异常
     * @throws InterruptedException 中断异常
     */
    @Override
    public boolean releaseLock(JdbcClientPool connections, String lockId)
            throws SQLException, InterruptedException {
        return connections.run(
                connection -> {
                    try (PreparedStatement preparedStatement =
                            connection.prepareStatement(getReleaseLockSql())) {
                        // 设置锁 ID
                        preparedStatement.setString(1, lockId);
                        // 执行删除,影响行数大于0表示成功
                        return preparedStatement.executeUpdate() > 0;
                    }
                });
    }

    /**
     * 获取释放锁的 SQL 语句.
     *
     * <p>由子类实现,通常是一个 DELETE 语句。
     *
     * @return 释放锁的 SQL 语句
     */
    public abstract String getReleaseLockSql();

    /**
     * 尝试释放超时的锁.
     *
     * <p>清理已超过过期时间的锁记录,防止锁永久持有导致死锁。
     *
     * @param connections JDBC 连接池
     * @param lockId 锁 ID,格式为 "catalog.database.table"
     * @return 释放的锁记录数量
     * @throws SQLException SQL 异常
     * @throws InterruptedException 中断异常
     */
    @Override
    public int tryReleaseTimedOutLock(JdbcClientPool connections, String lockId)
            throws SQLException, InterruptedException {
        return connections.run(
                connection -> {
                    try (PreparedStatement preparedStatement =
                            connection.prepareStatement(getTryReleaseTimedOutLock())) {
                        preparedStatement.setString(1, lockId);
                        return preparedStatement.executeUpdate();
                    }
                });
    }

    /**
     * 获取释放超时锁的 SQL 语句.
     *
     * <p>由子类实现,不同数据库计算时间差的 SQL 函数不同。
     *
     * @return 释放超时锁的 SQL 语句
     */
    public abstract String getTryReleaseTimedOutLock();
}
