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

import java.util.List;

/**
 * SnapshotCommit 接口 - 原子性地提交快照
 *
 * <p>SnapshotCommit 是快照提交的抽象接口,用于将新快照原子性地提交到 Catalog。
 * 它是 Paimon 写入路径的最后一步,保证数据的一致性和可见性。
 *
 * <p>提交过程:
 * <ol>
 *   <li><b>准备快照</b>: 生成包含新数据文件的快照对象
 *   <li><b>获取锁</b>: 使用 {@link CatalogLock} 保证互斥访问
 *   <li><b>验证</b>: 检查快照 ID 是否合法（单调递增）
 *   <li><b>提交</b>: 将快照文件原子性地写入存储
 *   <li><b>更新元数据</b>: 在 Catalog 中注册快照（如果支持）
 * </ol>
 *
 * <p>原子性保证:
 * <ul>
 *   <li><b>全有或全无</b>: 快照要么完全提交成功,要么完全失败
 *   <li><b>唯一性</b>: 每个快照 ID 全局唯一,不会重复
 *   <li><b>顺序性</b>: 快照 ID 严格单调递增
 *   <li><b>可见性</b>: 提交成功后,快照立即对读取可见
 * </ul>
 *
 * <p>实现类:
 * <ul>
 *   <li>{@link CatalogSnapshotCommit}: 通过 Catalog 提交快照（支持版本管理的 Catalog）
 *   <li>{@link RenamingSnapshotCommit}: 通过文件重命名提交快照（FileSystemCatalog 的默认方式）
 * </ul>
 *
 * <p>与 FileStoreCommit 的关系:
 * <pre>
 * FileStoreCommit（表级提交）
 *   └─ 生成快照对象
 *       └─ SnapshotCommit（Catalog 级提交）← 此接口
 *           └─ 原子性地提交到存储
 * </pre>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建 SnapshotCommit
 * SnapshotCommit snapshotCommit = ...;
 *
 * // 准备快照
 * Snapshot snapshot = new Snapshot(
 *     snapshotId,
 *     schemaId,
 *     baseManifestList,
 *     deltaManifestList,
 *     commitUser,
 *     commitKind
 * );
 *
 * // 准备分区统计信息
 * List<PartitionStatistics> statistics = ...;
 *
 * // 原子性提交
 * boolean success = snapshotCommit.commit(
 *     snapshot,
 *     "main",  // 分支名
 *     statistics
 * );
 *
 * if (success) {
 *     // 提交成功
 * } else {
 *     // 提交失败（可能由于冲突）
 * }
 * }</pre>
 *
 * @see CatalogSnapshotCommit
 * @see RenamingSnapshotCommit
 */
public interface SnapshotCommit extends AutoCloseable {

    /**
     * 提交快照
     *
     * <p>此方法会原子性地提交快照,保证快照 ID 的唯一性和单调性。
     *
     * @param snapshot 要提交的快照对象
     * @param branch 分支名称（如 "main"、"dev"）
     * @param statistics 分区统计信息列表,用于更新 Catalog 元数据
     * @return 如果提交成功返回 true,失败返回 false（通常由于并发冲突）
     * @throws Exception 如果提交过程中发生错误
     */
    boolean commit(Snapshot snapshot, String branch, List<PartitionStatistics> statistics)
            throws Exception;
}
