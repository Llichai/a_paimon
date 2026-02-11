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

package org.apache.paimon.utils;

import org.apache.paimon.Snapshot;
import org.apache.paimon.table.Instant;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;

/**
 * 快照加载器接口
 *
 * <p>SnapshotLoader 定义了快照加载的抽象接口，用于自定义快照的加载和回滚逻辑。
 *
 * <p>主要功能：
 * <ul>
 *   <li>加载最新快照：{@link #load()} - 加载最新的快照对象
 *   <li>回滚操作：{@link #rollback(Instant)} - 回滚到指定时刻
 *   <li>分支支持：{@link #copyWithBranch(String)} - 创建指定分支的加载器副本
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>Catalog 集成：在 Catalog 中管理快照元数据，提供统一的快照访问接口
 *   <li>自定义存储：支持将快照元数据存储在外部系统（如数据库、KV 存储）
 *   <li>性能优化：通过缓存或索引加速快照查找
 * </ul>
 *
 * <p>实现要点：
 * <ul>
 *   <li>线程安全：实现类应保证线程安全
 *   <li>序列化：必须实现 Serializable 接口
 *   <li>分支隔离：copyWithBranch 应返回独立的加载器实例
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 实现自定义加载器
 * class MySnapshotLoader implements SnapshotLoader {
 *     private final CatalogService catalog;
 *     private final String tablePath;
 *     private final String branch;
 *
 *     @Override
 *     public Optional<Snapshot> load() throws IOException {
 *         // 从 Catalog 加载最新快照
 *         return catalog.getLatestSnapshot(tablePath, branch);
 *     }
 *
 *     @Override
 *     public void rollback(Instant instant) throws IOException {
 *         // 执行回滚操作
 *         catalog.rollbackTo(tablePath, branch, instant);
 *     }
 *
 *     @Override
 *     public SnapshotLoader copyWithBranch(String branch) {
 *         return new MySnapshotLoader(catalog, tablePath, branch);
 *     }
 * }
 *
 * // 在 SnapshotManager 中使用
 * SnapshotLoader loader = new MySnapshotLoader(catalog, tablePath, "main");
 * SnapshotManager manager = new SnapshotManager(
 *     fileIO, tablePath, null, loader, cache
 * );
 * }</pre>
 *
 * @see SnapshotManager
 * @see Snapshot
 */
public interface SnapshotLoader extends Serializable {

    /**
     * 加载最新快照
     *
     * <p>从存储系统中加载最新的快照对象。
     *
     * @return 最新快照的 Optional，如果不存在返回 empty
     * @throws IOException 如果加载过程中发生 IO 错误
     */
    Optional<Snapshot> load() throws IOException;

    /**
     * 回滚到指定时刻
     *
     * <p>将表回滚到指定时刻的快照状态。
     *
     * @param instant 目标时刻
     * @throws IOException 如果回滚过程中发生 IO 错误
     */
    void rollback(Instant instant) throws IOException;

    /**
     * 创建指定分支的加载器副本
     *
     * <p>返回一个新的加载器实例，指向指定的分支。
     *
     * @param branch 分支名称
     * @return 新的加载器实例
     */
    SnapshotLoader copyWithBranch(String branch);
}
