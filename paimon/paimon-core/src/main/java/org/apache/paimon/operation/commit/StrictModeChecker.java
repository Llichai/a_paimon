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

package org.apache.paimon.operation.commit;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.function.Supplier;

/**
 * 严格模式检查器
 *
 * <p>基于最后安全快照检查提交的严格模式约束。
 *
 * <h2>严格模式概述</h2>
 * <p>严格模式用于防止多个作业同时写入同一表时的冲突：
 * <ul>
 *   <li>通过 {@link CoreOptions#COMMIT_STRICT_MODE_LAST_SAFE_SNAPSHOT} 配置
 *   <li>记录最后一个已知安全的快照ID
 *   <li>检查该快照之后是否有其他用户的危险操作
 * </ul>
 *
 * <h2>检查规则</h2>
 * <p>对于最后安全快照之后的每个快照，检查以下冲突：
 * <table border="1">
 *   <tr>
 *     <th>快照类型</th>
 *     <th>当前提交类型</th>
 *     <th>检查条件</th>
 *     <th>是否拒绝</th>
 *   </tr>
 *   <tr>
 *     <td>COMPACT</td>
 *     <td>任意</td>
 *     <td>不同用户</td>
 *     <td>是</td>
 *   </tr>
 *   <tr>
 *     <td>OVERWRITE</td>
 *     <td>任意</td>
 *     <td>不同用户</td>
 *     <td>是</td>
 *   </tr>
 *   <tr>
 *     <td>APPEND</td>
 *     <td>OVERWRITE</td>
 *     <td>不同用户且有固定Bucket文件</td>
 *     <td>是</td>
 *   </tr>
 *   <tr>
 *     <td>APPEND</td>
 *     <td>APPEND/COMPACT</td>
 *     <td>-</td>
 *     <td>否</td>
 *   </tr>
 * </table>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>多作业写入</b>：防止同时运行的作业相互干扰
 *   <li><b>从旧Savepoint恢复</b>：防止从过期Savepoint恢复导致数据损坏
 *   <li><b>多个作业从同一Savepoint启动</b>：防止重复提交
 * </ul>
 *
 * <h2>冲突示例</h2>
 * <pre>
 * 快照序列：
 * - Snapshot 100: 用户A的APPEND (lastSafeSnapshot)
 * - Snapshot 101: 用户B的COMPACT (冲突!)
 * - Snapshot 102: 当前用户A尝试提交 (被拒绝)
 * </pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建检查器
 * StrictModeChecker checker = StrictModeChecker.create(
 *     snapshotManager,
 *     commitUser,
 *     scanSupplier,
 *     lastSafeSnapshot
 * );
 *
 * // 检查提交
 * try {
 *     checker.check(newSnapshotId, commitKind);
 *     // 检查通过，可以提交
 * } catch (RuntimeException e) {
 *     // 检查失败，拒绝提交
 * }
 *
 * // 提交成功后更新
 * checker.update(newSnapshotId);
 * }</pre>
 *
 * @see CoreOptions#COMMIT_STRICT_MODE_LAST_SAFE_SNAPSHOT
 * @see org.apache.paimon.Snapshot.CommitKind
 */
public class StrictModeChecker {

    /** 快照管理器 */
    private final SnapshotManager snapshotManager;

    /** 当前提交用户 */
    private final String commitUser;

    /** 文件存储扫描器 */
    private final FileStoreScan scan;

    /** 最后安全快照ID */
    private long strictModeLastSafeSnapshot;

    /**
     * 构造严格模式检查器
     *
     * @param snapshotManager 快照管理器
     * @param commitUser 当前提交用户
     * @param scan 文件存储扫描器
     * @param strictModeLastSafeSnapshot 最后安全快照ID
     */
    public StrictModeChecker(
            SnapshotManager snapshotManager,
            String commitUser,
            FileStoreScan scan,
            long strictModeLastSafeSnapshot) {
        this.snapshotManager = snapshotManager;
        this.commitUser = commitUser;
        this.scan = scan;
        this.strictModeLastSafeSnapshot = strictModeLastSafeSnapshot;
    }

    /**
     * 创建严格模式检查器
     *
     * <p>如果未配置严格模式，返回null。
     *
     * @param snapshotManager 快照管理器
     * @param commitUser 当前提交用户
     * @param scanSupplier 扫描器提供者
     * @param strictModeLastSafeSnapshot 最后安全快照ID，null表示未启用
     * @return 检查器实例，如果未启用则返回null
     */
    @Nullable
    public static StrictModeChecker create(
            SnapshotManager snapshotManager,
            String commitUser,
            Supplier<FileStoreScan> scanSupplier,
            @Nullable Long strictModeLastSafeSnapshot) {
        if (strictModeLastSafeSnapshot == null) {
            return null;
        }
        return new StrictModeChecker(
                snapshotManager, commitUser, scanSupplier.get(), strictModeLastSafeSnapshot);
    }

    /**
     * 检查严格模式约束
     *
     * <p>检查从最后安全快照到新快照之间是否有其他用户的冲突操作。
     *
     * <h3>检查流程</h3>
     * <p>遍历 {@code (strictModeLastSafeSnapshot, newSnapshotId)} 区间的每个快照：
     * <ol>
     *   <li>如果是同一用户的快照，跳过
     *   <li>如果是其他用户的COMPACT或OVERWRITE快照，抛出异常
     *   <li>如果是其他用户的APPEND快照且当前是OVERWRITE提交：
     *       <ul>
     *         <li>检查是否有固定Bucket文件
     *         <li>如果有，抛出异常
     *       </ul>
     * </ol>
     *
     * <h3>异常场景</h3>
     * <ul>
     *   <li><b>COMPACT冲突</b>：其他用户执行了COMPACT，可能改变文件结构
     *   <li><b>OVERWRITE冲突</b>：其他用户执行了OVERWRITE，可能删除数据
     *   <li><b>APPEND冲突</b>：当前OVERWRITE会删除其他用户刚追加的固定Bucket数据
     * </ul>
     *
     * @param newSnapshotId 新快照ID
     * @param newCommitKind 新提交类型
     * @throws RuntimeException 如果检测到冲突
     */
    public void check(long newSnapshotId, CommitKind newCommitKind) {
        for (long id = strictModeLastSafeSnapshot + 1; id < newSnapshotId; id++) {
            Snapshot snapshot = snapshotManager.snapshot(id);
            if (snapshot.commitUser().equals(commitUser)) {
                continue;
            }
            if (snapshot.commitKind() == CommitKind.COMPACT
                    || snapshot.commitKind() == CommitKind.OVERWRITE) {
                throw new RuntimeException(
                        String.format(
                                "When trying to commit snapshot %d, "
                                        + "commit user %s has found a %s snapshot (id: %d) by another user %s. "
                                        + "Giving up committing as %s is set.",
                                newSnapshotId,
                                commitUser,
                                snapshot.commitKind().name(),
                                id,
                                snapshot.commitUser(),
                                CoreOptions.COMMIT_STRICT_MODE_LAST_SAFE_SNAPSHOT.key()));
            }
            if (snapshot.commitKind() == CommitKind.APPEND
                    && newCommitKind == CommitKind.OVERWRITE) {
                Iterator<ManifestEntry> entries =
                        scan.withSnapshot(snapshot)
                                .withKind(ScanMode.DELTA)
                                .onlyReadRealBuckets()
                                .dropStats()
                                .readFileIterator();
                if (entries.hasNext()) {
                    throw new RuntimeException(
                            String.format(
                                    "When trying to commit snapshot %d, "
                                            + "commit user %s has found a APPEND snapshot (id: %d) by another user %s "
                                            + "which committed files to fixed bucket. Giving up committing as %s is set.",
                                    newSnapshotId,
                                    commitUser,
                                    id,
                                    snapshot.commitUser(),
                                    CoreOptions.COMMIT_STRICT_MODE_LAST_SAFE_SNAPSHOT.key()));
                }
            }
        }
    }

    /**
     * 更新最后安全快照
     *
     * <p>提交成功后，更新最后安全快照ID。
     *
     * @param newSafeSnapshot 新的安全快照ID
     */
    public void update(long newSafeSnapshot) {
        strictModeLastSafeSnapshot = newSafeSnapshot;
    }
}
