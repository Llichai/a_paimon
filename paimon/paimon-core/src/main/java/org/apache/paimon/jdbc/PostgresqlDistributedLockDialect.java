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

/**
 * 基于 PostgreSQL 的分布式锁实现.
 *
 * <p>提供 PostgreSQL 数据库特定的 SQL 语句实现分布式锁功能。
 *
 * <p>锁表结构:
 * <pre>
 * CREATE TABLE paimon_distributed_locks (
 *   lock_id VARCHAR(N) NOT NULL,
 *   acquired_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
 *   expire_time_seconds BIGINT DEFAULT 0 NOT NULL,
 *   PRIMARY KEY (lock_id)
 * )
 * </pre>
 *
 * <p>锁机制:
 * <ul>
 *   <li>获取锁: 通过 INSERT 操作,主键冲突则获取失败</li>
 *   <li>释放锁: 通过 DELETE 操作删除锁记录</li>
 *   <li>超时检测: 使用 EXTRACT(EPOCH FROM AGE(...)) 函数计算时间差</li>
 * </ul>
 *
 * <p>PostgreSQL 特性:
 * <ul>
 *   <li>TIMESTAMP: 自动记录锁获取时间</li>
 *   <li>AGE 函数: 计算两个时间戳的间隔(返回 interval 类型)</li>
 *   <li>EXTRACT(EPOCH FROM interval): 将间隔转换为秒数</li>
 *   <li>PRIMARY KEY: 保证锁的唯一性</li>
 * </ul>
 */
public class PostgresqlDistributedLockDialect extends AbstractDistributedLockDialect {

    /**
     * 获取创建 PostgreSQL 锁表的 SQL 语句.
     *
     * <p>使用 PostgreSQL 特定的 TIMESTAMP 类型和默认值语法。
     *
     * @return 创建锁表的 SQL 语句,包含 %s 占位符用于设置锁键最大长度
     */
    @Override
    public String getCreateTableSql() {
        return "CREATE TABLE "
                + JdbcUtils.DISTRIBUTED_LOCKS_TABLE_NAME
                + "("
                + JdbcUtils.LOCK_ID
                + " VARCHAR(%s) NOT NULL,"
                + JdbcUtils.ACQUIRED_AT
                + " TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,"
                + JdbcUtils.EXPIRE_TIME
                + " BIGINT DEFAULT 0 NOT NULL,"
                + "PRIMARY KEY ("
                + JdbcUtils.LOCK_ID
                + ")"
                + ")";
    }

    /**
     * 获取 PostgreSQL 的获取锁 SQL 语句.
     *
     * <p>通过 INSERT 操作插入锁记录,如果主键冲突则获取失败。
     *
     * @return INSERT 语句,参数为 [lockId, expireTimeSeconds]
     */
    @Override
    public String getLockAcquireSql() {
        return "INSERT INTO "
                + JdbcUtils.DISTRIBUTED_LOCKS_TABLE_NAME
                + " ("
                + JdbcUtils.LOCK_ID
                + ","
                + JdbcUtils.EXPIRE_TIME
                + ") VALUES (?,?)";
    }

    /**
     * 获取 PostgreSQL 的释放锁 SQL 语句.
     *
     * <p>通过 DELETE 操作删除锁记录。
     *
     * @return DELETE 语句,参数为 [lockId]
     */
    @Override
    public String getReleaseLockSql() {
        return "DELETE FROM "
                + JdbcUtils.DISTRIBUTED_LOCKS_TABLE_NAME
                + " WHERE "
                + JdbcUtils.LOCK_ID
                + " = ?";
    }

    /**
     * 获取 PostgreSQL 的释放超时锁 SQL 语句.
     *
     * <p>使用 PostgreSQL 的 AGE 和 EXTRACT 函数组合计算时间差:
     * <ol>
     *   <li>AGE(now, acquired_at): 计算当前时间与获取时间的间隔(interval 类型)</li>
     *   <li>EXTRACT(EPOCH FROM interval): 将间隔转换为秒数(浮点数)</li>
     *   <li>与 expire_time_seconds 比较,如果超时则删除</li>
     * </ol>
     *
     * <p>PostgreSQL 时间函数:
     * <pre>
     * AGE(timestamp, timestamp) -> interval
     * EXTRACT(EPOCH FROM interval) -> double precision(秒数)
     * NOW() -> timestamp with time zone
     * </pre>
     *
     * @return DELETE 语句,参数为 [lockId]
     */
    @Override
    public String getTryReleaseTimedOutLock() {
        return "DELETE FROM "
                + JdbcUtils.DISTRIBUTED_LOCKS_TABLE_NAME
                + " WHERE EXTRACT(EPOCH FROM AGE(NOW(), "
                + JdbcUtils.ACQUIRED_AT
                + ")) >"
                + JdbcUtils.EXPIRE_TIME
                + " and "
                + JdbcUtils.LOCK_ID
                + " = ?";
    }
}
