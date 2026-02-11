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

import org.apache.paimon.Snapshot;
import org.apache.paimon.partition.PartitionStatistics;

import javax.annotation.Nullable;

import java.util.List;

/**
 * 基于 Catalog API 的快照提交实现
 *
 * <p>CatalogSnapshotCommit 通过 {@link Catalog#commitSnapshot} 方法来提交快照。
 * 适用于支持版本管理的 Catalog（如 Hive Metastore、JDBC Catalog）。
 *
 * <p>与 {@link RenamingSnapshotCommit} 的区别:
 * <ul>
 *   <li>{@link RenamingSnapshotCommit}: 直接操作文件系统,通过原子重命名提交
 *   <li>{@link CatalogSnapshotCommit}: 通过 Catalog API 提交,由 Catalog 实现保证原子性
 * </ul>
 *
 * <p>优势:
 * <ul>
 *   <li>统一接口: 所有 Catalog 实现使用相同的提交逻辑
 *   <li>元数据一致性: Catalog 可以同时更新快照和元数据（如 Hive 分区统计信息）
 *   <li>版本管理: 支持 Catalog 级别的版本管理功能
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>HiveCatalog: 需要同步更新 Hive Metastore
 *   <li>JdbcCatalog: 需要在数据库中记录快照信息
 *   <li>RestCatalog: 需要通过 REST API 提交
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 1. 创建 Catalog
 * Catalog catalog = ...;
 *
 * // 2. 创建提交器
 * Identifier identifier = Identifier.create("db", "table");
 * String uuid = "table-uuid";
 * SnapshotCommit commit = new CatalogSnapshotCommit(catalog, identifier, uuid);
 *
 * // 3. 提交快照
 * Snapshot snapshot = ...;
 * List<PartitionStatistics> statistics = ...;
 * boolean success = commit.commit(snapshot, "main", statistics);
 *
 * // 4. 关闭
 * commit.close();
 * }</pre>
 *
 * @see SnapshotCommit
 * @see Catalog#commitSnapshot
 * @see RenamingSnapshotCommit
 */
public class CatalogSnapshotCommit implements SnapshotCommit {

    /** Catalog 实例 */
    private final Catalog catalog;

    /** 表标识符 */
    private final Identifier identifier;

    /** 表 UUID（用于避免错误提交） */
    @Nullable private final String uuid;

    /**
     * 构造函数
     *
     * @param catalog Catalog 实例
     * @param identifier 表标识符
     * @param uuid 表 UUID（可选）
     */
    public CatalogSnapshotCommit(Catalog catalog, Identifier identifier, @Nullable String uuid) {
        this.catalog = catalog;
        this.identifier = identifier;
        this.uuid = uuid;
    }

    /**
     * 提交快照
     *
     * <p>通过 {@link Catalog#commitSnapshot} 方法提交快照。
     * Catalog 实现会保证提交的原子性和一致性。
     *
     * @param snapshot 要提交的快照
     * @param branch 分支名称
     * @param statistics 分区统计信息
     * @return 是否提交成功
     * @throws Exception 如果提交失败
     */
    @Override
    public boolean commit(Snapshot snapshot, String branch, List<PartitionStatistics> statistics)
            throws Exception {
        Identifier newIdentifier =
                new Identifier(identifier.getDatabaseName(), identifier.getTableName(), branch);
        return catalog.commitSnapshot(newIdentifier, uuid, snapshot, statistics);
    }

    /**
     * 关闭提交器
     *
     * <p>关闭 Catalog 连接。
     *
     * @throws Exception 如果关闭失败
     */
    @Override
    public void close() throws Exception {
        catalog.close();
    }
}
