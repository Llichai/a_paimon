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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.utils.SnapshotManager;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * 基于文件重命名的快照提交实现
 *
 * <p>RenamingSnapshotCommit 通过原子重命名操作来提交快照,是 FileSystemCatalog 的默认快照提交方式。
 *
 * <p>提交流程:
 * <ol>
 *   <li>生成快照 JSON 内容
 *   <li>写入临时文件（如 snapshot-1.tmp）
 *   <li>原子重命名为正式文件（如 snapshot-1）
 *   <li>更新 LATEST 提示文件
 * </ol>
 *
 * <p>原子性保证:
 * <ul>
 *   <li><b>本地文件系统/HDFS</b>: rename 操作是原子的,天然保证并发安全
 *   <li><b>对象存储（S3/OSS）</b>: rename 不是原子的,需要额外的锁保护
 * </ul>
 *
 * <p>锁机制:
 * <p>通过 {@link Lock} 接口提供并发控制:
 * <ul>
 *   <li>HDFS: 使用空锁（{@link Lock#empty()}）,依赖文件系统原子性
 *   <li>对象存储: 使用分布式锁（Zookeeper/JDBC）
 * </ul>
 *
 * <p>对象存储的特殊处理:
 * <p>由于对象存储的 rename 不保证原子性,且可能在目标文件已存在时不返回 false,
 * 因此在锁保护下额外检查目标文件是否存在:
 * <pre>
 * 1. 获取分布式锁
 * 2. 检查目标快照文件是否已存在
 * 3. 如果不存在,执行原子写入
 * 4. 更新 LATEST 提示文件
 * 5. 释放锁
 * </pre>
 *
 * <p>与其他提交方式的比较:
 * <ul>
 *   <li>{@link RenamingSnapshotCommit}: 基于文件重命名,适合文件系统
 *   <li>{@link CatalogSnapshotCommit}: 基于 Catalog API,适合 Hive Metastore/JDBC
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 1. 创建 SnapshotManager
 * SnapshotManager snapshotManager = new SnapshotManager(fileIO, tablePath);
 *
 * // 2. 创建锁（根据存储类型）
 * Lock lock = fileIO.isObjectStore()
 *     ? Lock.fromCatalog(catalogLock, identifier)  // 对象存储使用分布式锁
 *     : Lock.empty();                               // HDFS 使用空锁
 *
 * // 3. 创建提交器
 * SnapshotCommit commit = new RenamingSnapshotCommit(snapshotManager, lock);
 *
 * // 4. 提交快照
 * Snapshot snapshot = ...;
 * boolean success = commit.commit(snapshot, "main", statistics);
 *
 * // 5. 关闭
 * commit.close();
 * }</pre>
 *
 * <p>线程安全:
 * <ul>
 *   <li><b>HDFS</b>: 依赖 rename 的原子性,多线程安全
 *   <li><b>对象存储</b>: 依赖分布式锁,多进程安全
 * </ul>
 *
 * <p>性能考虑:
 * <ul>
 *   <li><b>HDFS</b>: 无锁开销,性能高
 *   <li><b>对象存储</b>: 有锁开销（网络往返),但保证正确性
 * </ul>
 *
 * @see SnapshotCommit
 * @see SnapshotManager
 * @see Lock
 */
public class RenamingSnapshotCommit implements SnapshotCommit {

    /** 快照管理器 */
    private final SnapshotManager snapshotManager;

    /** 文件 I/O 接口 */
    private final FileIO fileIO;

    /** 锁（可能是空锁或分布式锁） */
    private final Lock lock;

    /**
     * 构造函数
     *
     * @param snapshotManager 快照管理器
     * @param lock 锁实例
     */
    public RenamingSnapshotCommit(SnapshotManager snapshotManager, Lock lock) {
        this.snapshotManager = snapshotManager;
        this.fileIO = snapshotManager.fileIO();
        this.lock = lock;
    }

    /**
     * 提交快照
     *
     * <p>提交流程:
     * <ol>
     *   <li>确定快照文件路径（根据分支名）
     *   <li>在锁保护下执行:
     *     <ol>
     *       <li>检查快照文件是否已存在
     *       <li>如果不存在,原子写入快照文件
     *       <li>更新 LATEST 提示文件
     *     </ol>
     * </ol>
     *
     * <p>对象存储的特殊处理:
     * <p>fs.rename 在对象存储上可能不可靠（不原子、不返回 false）,
     * 因此依赖外部锁,并先检查文件是否存在。
     *
     * @param snapshot 要提交的快照
     * @param branch 分支名称
     * @param statistics 分区统计信息（在此实现中未使用）
     * @return 是否提交成功（如果快照已存在返回 false）
     * @throws Exception 如果提交失败
     */
    @Override
    public boolean commit(Snapshot snapshot, String branch, List<PartitionStatistics> statistics)
            throws Exception {
        Path newSnapshotPath =
                snapshotManager.branch().equals(branch)
                        ? snapshotManager.snapshotPath(snapshot.id())
                        : snapshotManager.copyWithBranch(branch).snapshotPath(snapshot.id());

        Callable<Boolean> callable =
                () -> {
                    boolean committed = fileIO.tryToWriteAtomic(newSnapshotPath, snapshot.toJson());
                    if (committed) {
                        snapshotManager.commitLatestHint(snapshot.id());
                    }
                    return committed;
                };
        return lock.runWithLock(
                () ->
                        // fs.rename 在目标文件已存在时可能不返回 false,
                        // 甚至不保证原子性。
                        // 由于我们依赖外部锁,可以先检查文件是否存在,
                        // 然后再重命名来解决这个问题。
                        !fileIO.exists(newSnapshotPath) && callable.call());
    }

    /**
     * 关闭提交器
     *
     * <p>释放锁资源。
     *
     * @throws Exception 如果关闭失败
     */
    @Override
    public void close() throws Exception {
        this.lock.close();
    }
}
