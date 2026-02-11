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

import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.utils.TimeUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.apache.paimon.options.CatalogOptions.LOCK_ACQUIRE_TIMEOUT;
import static org.apache.paimon.options.CatalogOptions.LOCK_CHECK_MAX_SLEEP;

/**
 * JDBC Catalog 分布式锁实现.
 *
 * <p>基于数据库表实现的分布式锁,用于保证多客户端并发访问 Catalog 时的元数据操作原子性。
 *
 * <p>锁机制:
 * <ul>
 *   <li>锁 ID 格式: "catalog.database.table"</li>
 *   <li>获取锁: 在锁表中插入记录,如果主键冲突则获取失败</li>
 *   <li>释放锁: 删除锁表中的记录</li>
 *   <li>超时处理: 自动清理超时的锁记录</li>
 * </ul>
 *
 * <p>重试策略:
 * <ul>
 *   <li>初始睡眠时间: 50ms</li>
 *   <li>指数退避: 每次重试睡眠时间翻倍</li>
 *   <li>最大睡眠时间: 可配置,默认 8 秒</li>
 *   <li>总超时时间: 可配置,默认 8 分钟</li>
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>表的创建、修改、删除等元数据操作</li>
 *   <li>Schema 变更操作</li>
 *   <li>需要独占访问的场景</li>
 * </ul>
 */
public class JdbcCatalogLock implements CatalogLock {
    /** JDBC 连接池 */
    private final JdbcClientPool connections;

    /** 每次重试的最大睡眠时间(毫秒) */
    private final long checkMaxSleep;

    /** 获取锁的总超时时间(毫秒) */
    private final long acquireTimeout;

    /** Catalog 键,用于生成锁 ID */
    private final String catalogKey;

    /**
     * 构造 JDBC Catalog 锁.
     *
     * @param connections JDBC 连接池
     * @param catalogKey Catalog 键
     * @param checkMaxSleep 每次重试的最大睡眠时间(毫秒)
     * @param acquireTimeout 获取锁的总超时时间(毫秒)
     */
    public JdbcCatalogLock(
            JdbcClientPool connections,
            String catalogKey,
            long checkMaxSleep,
            long acquireTimeout) {
        this.connections = connections;
        this.checkMaxSleep = checkMaxSleep;
        this.acquireTimeout = acquireTimeout;
        this.catalogKey = catalogKey;
    }

    /**
     * 使用锁执行操作.
     *
     * <p>执行流程:
     * <ol>
     *   <li>生成锁 ID: "catalog.database.table"</li>
     *   <li>获取锁(带重试)</li>
     *   <li>执行操作</li>
     *   <li>释放锁(finally 块中确保释放)</li>
     * </ol>
     *
     * @param database 数据库名
     * @param table 表名
     * @param callable 要执行的操作
     * @param <T> 返回值类型
     * @return 操作返回值
     * @throws Exception 操作异常或获取锁失败
     */
    @Override
    public <T> T runWithLock(String database, String table, Callable<T> callable) throws Exception {
        // 生成唯一的锁 ID
        String lockUniqueName = String.format("%s.%s.%s", catalogKey, database, table);
        // 获取锁
        lock(lockUniqueName);
        try {
            // 执行操作
            return callable.call();
        } finally {
            // 释放锁
            JdbcUtils.release(connections, lockUniqueName);
        }
    }

    /**
     * 获取锁,带重试和指数退避.
     *
     * <p>重试策略:
     * <ul>
     *   <li>初始睡眠: 50ms</li>
     *   <li>每次失败后睡眠时间翻倍</li>
     *   <li>最大睡眠时间: checkMaxSleep</li>
     *   <li>总超时时间: acquireTimeout</li>
     * </ul>
     *
     * @param lockUniqueName 锁的唯一名称
     * @throws SQLException SQL 异常
     * @throws InterruptedException 中断异常
     * @throws RuntimeException 获取锁超时
     */
    private void lock(String lockUniqueName) throws SQLException, InterruptedException {
        // 首次尝试获取锁
        boolean lock = JdbcUtils.acquire(connections, lockUniqueName, acquireTimeout);
        long nextSleep = 50; // 初始睡眠时间 50ms
        long startRetry = System.currentTimeMillis();
        // 获取失败则重试
        while (!lock) {
            // 指数退避:睡眠时间翻倍
            nextSleep *= 2;
            if (nextSleep > checkMaxSleep) {
                nextSleep = checkMaxSleep;
            }
            Thread.sleep(nextSleep);
            // 再次尝试获取锁
            lock = JdbcUtils.acquire(connections, lockUniqueName, acquireTimeout);
            // 检查是否超时
            if (System.currentTimeMillis() - startRetry > acquireTimeout) {
                break;
            }
        }
        long retryDuration = System.currentTimeMillis() - startRetry;
        if (!lock) {
            throw new RuntimeException(
                    "Acquire lock failed with time: " + Duration.ofMillis(retryDuration));
        }
    }

    @Override
    public void close() throws IOException {
        // 锁的生命周期由 runWithLock 管理,这里不需要额外清理
    }

    /**
     * 从配置中获取最大睡眠时间.
     *
     * @param conf 配置映射
     * @return 最大睡眠时间(毫秒)
     */
    public static long checkMaxSleep(Map<String, String> conf) {
        return TimeUtils.parseDuration(
                        conf.getOrDefault(
                                LOCK_CHECK_MAX_SLEEP.key(),
                                TimeUtils.getStringInMillis(LOCK_CHECK_MAX_SLEEP.defaultValue())))
                .toMillis();
    }

    /**
     * 从配置中获取获取锁的超时时间.
     *
     * @param conf 配置映射
     * @return 超时时间(毫秒)
     */
    public static long acquireTimeout(Map<String, String> conf) {
        return TimeUtils.parseDuration(
                        conf.getOrDefault(
                                LOCK_ACQUIRE_TIMEOUT.key(),
                                TimeUtils.getStringInMillis(LOCK_ACQUIRE_TIMEOUT.defaultValue())))
                .toMillis();
    }
}
