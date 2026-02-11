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

import java.sql.SQLException;

/**
 * JDBC 分布式锁方言接口.
 *
 * <p>定义了基于 JDBC 实现分布式锁的标准接口。不同数据库(MySQL、PostgreSQL、SQLite)
 * 的具体实现需要实现此接口,提供数据库特定的 SQL 语句。
 *
 * <p>核心操作:
 * <ul>
 *   <li>创建锁表: 初始化分布式锁所需的数据库表</li>
 *   <li>获取锁: 尝试在锁表中插入记录以获取锁</li>
 *   <li>释放锁: 从锁表中删除记录以释放锁</li>
 *   <li>清理超时锁: 删除已超时的锁记录,防止死锁</li>
 * </ul>
 *
 * <p>锁机制:
 * <ul>
 *   <li>基于主键约束: 锁 ID 作为主键,确保同一时刻只有一个客户端能持有锁</li>
 *   <li>超时保护: 记录锁的获取时间和过期时间,支持自动清理</li>
 *   <li>乐观并发: 通过 INSERT 操作的主键冲突来检测锁竞争</li>
 * </ul>
 *
 * <p>实现类:
 * <ul>
 *   <li>{@link MysqlDistributedLockDialect}: MySQL 实现</li>
 *   <li>{@link PostgresqlDistributedLockDialect}: PostgreSQL 实现</li>
 *   <li>{@link SqlLiteDistributedLockDialect}: SQLite 实现</li>
 * </ul>
 */
public interface JdbcDistributedLockDialect {

    /**
     * 创建分布式锁表.
     *
     * <p>如果表已存在则跳过创建。表结构应包含:
     * <ul>
     *   <li>lock_id: 锁标识,主键,VARCHAR 类型</li>
     *   <li>acquired_at: 锁获取时间,TIMESTAMP 类型</li>
     *   <li>expire_time_seconds: 锁过期时间(秒),BIGINT 类型</li>
     * </ul>
     *
     * @param connections JDBC 连接池
     * @param options 配置选项,包含锁键最大长度等参数
     * @throws SQLException SQL 异常
     * @throws InterruptedException 中断异常
     */
    void createTable(JdbcClientPool connections, Options options)
            throws SQLException, InterruptedException;

    /**
     * 获取分布式锁.
     *
     * <p>尝试在锁表中插入一条记录。如果插入成功(没有主键冲突)则获取锁成功,
     * 如果插入失败(主键冲突)则说明锁已被其他客户端持有。
     *
     * @param connections JDBC 连接池
     * @param lockId 锁 ID,格式为 "catalog.database.table"
     * @param timeoutMillSeconds 锁超时时间(毫秒)
     * @return 是否成功获取锁,true 表示获取成功,false 表示获取失败
     * @throws SQLException SQL 异常
     * @throws InterruptedException 中断异常
     */
    boolean lockAcquire(JdbcClientPool connections, String lockId, long timeoutMillSeconds)
            throws SQLException, InterruptedException;

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
    boolean releaseLock(JdbcClientPool connections, String lockId)
            throws SQLException, InterruptedException;

    /**
     * 尝试释放超时的锁.
     *
     * <p>清理已超过过期时间的锁记录,防止锁永久持有导致死锁。
     * 实现需要计算当前时间与 acquired_at 的差值,并与 expire_time_seconds 比较。
     *
     * @param connections JDBC 连接池
     * @param lockId 锁 ID,格式为 "catalog.database.table"
     * @return 释放的锁记录数量
     * @throws SQLException SQL 异常
     * @throws InterruptedException 中断异常
     */
    int tryReleaseTimedOutLock(JdbcClientPool connections, String lockId)
            throws SQLException, InterruptedException;
}
