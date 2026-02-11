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

import org.apache.paimon.annotation.Public;

import java.io.Closeable;
import java.util.concurrent.Callable;

/**
 * CatalogLock 接口 - Catalog 全局锁,用于事务相关操作
 *
 * <p>CatalogLock 提供分布式锁机制,保证跨节点的事务原子性。
 * 主要用于快照提交、Schema 变更等需要互斥访问的操作。
 *
 * <p>锁的粒度:
 * <ul>
 *   <li><b>表级锁</b>: 锁定特定数据库和表,允许不同表的并发操作
 *   <li><b>操作隔离</b>: 同一时刻只有一个线程/进程可以修改表的元数据
 * </ul>
 *
 * <p>支持的锁实现:
 * <ul>
 *   <li><b>HiveLock</b>: 基于 Hive Metastore 的锁（需要 paimon-hive-connector）
 *   <li><b>ZooKeeperLock</b>: 基于 ZooKeeper 的分布式锁
 *   <li><b>JdbcLock</b>: 基于 JDBC 的数据库锁
 *   <li><b>本地锁</b>: 用于单机测试（不支持分布式）
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li><b>快照提交</b>: {@link SnapshotCommit} 使用锁保证快照 ID 单调递增
 *   <li><b>Schema 变更</b>: {@link Catalog#alterTable} 使用锁避免并发修改冲突
 *   <li><b>表重命名</b>: {@link Catalog#renameTable} 使用锁保证原子性
 *   <li><b>分区操作</b>: {@link Catalog#createPartitions} 等操作需要锁保护
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * CatalogLock lock = ...;  // 从 CatalogLockFactory 创建
 *
 * // 在锁保护下提交快照
 * Snapshot newSnapshot = lock.runWithLock("my_db", "my_table", () -> {
 *     // 1. 读取当前快照 ID
 *     long currentId = getCurrentSnapshotId();
 *
 *     // 2. 生成新快照
 *     Snapshot snapshot = new Snapshot(
 *         currentId + 1,  // 新 ID = 当前 ID + 1
 *         ...
 *     );
 *
 *     // 3. 写入快照文件
 *     writeSnapshot(snapshot);
 *
 *     return snapshot;
 * });
 * }</pre>
 *
 * <p>配置示例:
 * <pre>{@code
 * // ZooKeeper 锁
 * options.set("lock.type", "zookeeper");
 * options.set("lock.zookeeper.address", "localhost:2181");
 *
 * // Hive 锁
 * options.set("lock.type", "hive");
 * options.set("lock.hive.metastore.uri", "thrift://localhost:9083");
 * }</pre>
 *
 * <p>注意事项:
 * <ul>
 *   <li><b>避免死锁</b>: 始终按相同顺序获取锁（先数据库后表）
 *   <li><b>及时释放</b>: 在 finally 块或 try-with-resources 中关闭锁
 *   <li><b>超时处理</b>: 设置合理的锁超时时间,避免永久阻塞
 *   <li><b>性能影响</b>: 锁会降低并发性能,仅在必要时使用
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public interface CatalogLock extends Closeable {

    /**
     * 在 Catalog 锁保护下运行任务
     *
     * <p>此方法会:
     * <ol>
     *   <li>获取指定数据库和表的锁
     *   <li>执行 callable 任务
     *   <li>释放锁
     *   <li>返回任务结果
     * </ol>
     *
     * <p>如果获取锁失败或任务执行失败,会抛出异常。
     *
     * @param database 数据库名称,用于定位锁
     * @param table 表名称,用于定位锁
     * @param callable 要在锁保护下执行的任务
     * @param <T> 返回值类型
     * @return 任务执行结果
     * @throws Exception 如果获取锁失败或任务执行失败
     */
    <T> T runWithLock(String database, String table, Callable<T> callable) throws Exception;
}
