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
 * 基于 SQLite 的分布式锁实现.
 *
 * <p>提供 SQLite 数据库特定的 SQL 语句实现分布式锁功能。
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
 *   <li>超时检测: 使用 strftime 函数计算时间差</li>
 * </ul>
 *
 * <p>SQLite 特性:
 * <ul>
 *   <li>TIMESTAMP: 存储为文本格式的时间戳</li>
 *   <li>strftime('%s', timestamp): 将时间戳转换为 Unix 时间(秒)</li>
 *   <li>strftime('%s', 'now'): 获取当前 Unix 时间(秒)</li>
 *   <li>PRIMARY KEY: 保证锁的唯一性</li>
 * </ul>
 *
 * <p>注意事项:
 * <ul>
 *   <li>SQLite 主要用于测试和单机场景,不适合高并发场景</li>
 *   <li>SQLite 的事务隔离级别较简单,可能影响锁的可靠性</li>
 *   <li>生产环境建议使用 MySQL 或 PostgreSQL</li>
 * </ul>
 */
public class SqlLiteDistributedLockDialect extends AbstractDistributedLockDialect {

    /**
     * 获取创建 SQLite 锁表的 SQL 语句.
     *
     * <p>使用 SQLite 特定的 TIMESTAMP 类型和默认值语法。
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
     * 获取 SQLite 的获取锁 SQL 语句.
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
     * 获取 SQLite 的释放锁 SQL 语句.
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
     * 获取 SQLite 的释放超时锁 SQL 语句.
     *
     * <p>使用 SQLite 的 strftime 函数计算时间差:
     * <ol>
     *   <li>strftime('%s', 'now'): 获取当前 Unix 时间戳(秒)</li>
     *   <li>strftime('%s', acquired_at): 将获取时间转换为 Unix 时间戳(秒)</li>
     *   <li>两者相减得到时间差(秒)</li>
     *   <li>与 expire_time_seconds 比较,如果超时则删除</li>
     * </ol>
     *
     * <p>SQLite 时间函数:
     * <pre>
     * strftime(format, timestring, modifier...)
     * - format: 格式字符串,%s 表示 Unix 时间戳(秒)
     * - timestring: 时间字符串或 'now'
     * - 返回: 格式化后的时间字符串
     * </pre>
     *
     * @return DELETE 语句,参数为 [lockId]
     */
    @Override
    public String getTryReleaseTimedOutLock() {
        return "DELETE FROM "
                + JdbcUtils.DISTRIBUTED_LOCKS_TABLE_NAME
                + " WHERE  strftime('%s', 'now') - strftime('%s', "
                + JdbcUtils.ACQUIRED_AT
                + ") > "
                + JdbcUtils.EXPIRE_TIME
                + " and "
                + JdbcUtils.LOCK_ID
                + " = ?";
    }
}
