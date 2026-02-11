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
import org.apache.paimon.table.source.OutOfRangeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * 下一个快照获取器
 *
 * <p>NextSnapshotFetcher 用于获取指定 ID 的下一个快照，支持快照和 changelog 的查找。
 *
 * <p>核心功能：
 * <ul>
 *   <li>快照获取：{@link #getNextSnapshot(long)} - 获取下一个快照
 *   <li>范围检查：定期检查快照 ID 是否超出范围
 *   <li>Changelog 支持：在 changelog-decoupled 模式下支持读取 changelog
 * </ul>
 *
 * <p>查找顺序：
 * <pre>
 * 1. 检查快照是否存在（snapshotManager.snapshotExists）
 * 2. 如果存在，返回快照
 * 3. 如果启用 changelog-decoupled，检查 changelog 是否存在
 * 4. 如果都不存在，返回 null（等待快照生成）
 * 5. 定期执行范围检查（每 16 次检查一次）
 * </pre>
 *
 * <p>范围检查：
 * <ul>
 *   <li>检查频率：每 {@link #RANGE_CHECK_INTERVAL} 次（16 次）执行一次
 *   <li>检查目的：防止请求过期的快照或不存在的快照
 *   <li>异常抛出：如果快照 ID 超出范围，抛出 {@link OutOfRangeException}
 * </ul>
 *
 * <p>范围检查规则：
 * <ul>
 *   <li>快照过期：nextSnapshotId < earliestSnapshotId（快照已被过期删除）
 *   <li>快照超前：nextSnapshotId > latestSnapshotId + 1（表可能被重建）
 *   <li>正常等待：earliestSnapshotId <= nextSnapshotId <= latestSnapshotId + 1
 * </ul>
 *
 * <p>Changelog Decoupled 模式：
 * <ul>
 *   <li>启用后，即使快照过期，仍可从 changelog 读取数据
 *   <li>Long-lived changelog 保留时间可能比快照更长
 *   <li>适用于需要长时间保留变更历史的场景
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>流式读取：按快照 ID 顺序消费数据
 *   <li>增量读取：读取从指定快照开始的增量数据
 *   <li>CDC 场景：消费 changelog 变更流
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建快照获取器
 * NextSnapshotFetcher fetcher = new NextSnapshotFetcher(
 *     snapshotManager,
 *     changelogManager,
 *     true  // 启用 changelog-decoupled
 * );
 *
 * // 获取下一个快照
 * long nextSnapshotId = 100L;
 * Snapshot snapshot = fetcher.getNextSnapshot(nextSnapshotId);
 *
 * if (snapshot != null) {
 *     // 快照存在，处理数据
 *     processSnapshot(snapshot);
 *     nextSnapshotId++;
 * } else {
 *     // 快照不存在，等待生成
 *     Thread.sleep(1000);
 * }
 *
 * // 循环消费
 * while (running) {
 *     try {
 *         snapshot = fetcher.getNextSnapshot(nextSnapshotId);
 *         if (snapshot != null) {
 *             processSnapshot(snapshot);
 *             nextSnapshotId++;
 *         } else {
 *             // 等待新快照
 *             Thread.sleep(pollInterval);
 *         }
 *     } catch (OutOfRangeException e) {
 *         // 快照已过期或超出范围
 *         log.error("Snapshot out of range: " + e.getMessage());
 *         break;
 *     }
 * }
 * }</pre>
 *
 * @see Snapshot
 * @see SnapshotManager
 * @see ChangelogManager
 * @see OutOfRangeException
 */
public class NextSnapshotFetcher {

    public static final Logger LOG = LoggerFactory.getLogger(NextSnapshotFetcher.class);

    /** 范围检查间隔（每 16 次检查一次） */
    public static final int RANGE_CHECK_INTERVAL = 16;

    /** 快照管理器 */
    private final SnapshotManager snapshotManager;
    /** Changelog 管理器 */
    private final ChangelogManager changelogManager;
    /** 是否启用 changelog-decoupled 模式 */
    private final boolean changelogDecoupled;

    /** 范围检查计数器 */
    private int rangeCheckCnt = 0;

    public NextSnapshotFetcher(
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            boolean changelogDecoupled) {
        this.snapshotManager = snapshotManager;
        this.changelogManager = changelogManager;
        this.changelogDecoupled = changelogDecoupled;
    }

    @Nullable
    public Snapshot getNextSnapshot(long nextSnapshotId) {
        if (snapshotManager.snapshotExists(nextSnapshotId)) {
            rangeCheckCnt = 0;
            return snapshotManager.snapshot(nextSnapshotId);
        }

        if (changelogDecoupled && changelogManager.longLivedChangelogExists(nextSnapshotId)) {
            return changelogManager.changelog(nextSnapshotId);
        }

        rangeCheckCnt++;
        if (rangeCheckCnt % RANGE_CHECK_INTERVAL == 0) {
            rangeCheck(nextSnapshotId);
        }

        return null;
    }

    private void rangeCheck(long nextSnapshotId) {
        Long earliestSnapshotId = snapshotManager.earliestSnapshotId();
        Long latestSnapshotId = snapshotManager.latestSnapshotIdFromFileSystem();

        // No snapshot now
        if (earliestSnapshotId == null || earliestSnapshotId <= nextSnapshotId) {
            if ((earliestSnapshotId == null && nextSnapshotId > 1)
                    || (latestSnapshotId != null && nextSnapshotId > latestSnapshotId + 1)) {
                throw new OutOfRangeException(
                        String.format(
                                "The next expected snapshot is too big! Most possible cause might be the table had been recreated."
                                        + "The next snapshot id is %d, while the latest snapshot id is %s",
                                nextSnapshotId, latestSnapshotId));
            }

            LOG.debug(
                    "Next snapshot id {} does not exist, wait for the snapshot generation.",
                    nextSnapshotId);
        } else {
            if (!changelogDecoupled) {
                throw new OutOfRangeException(
                        String.format(
                                "The snapshot with id %d has expired. You can: "
                                        + "1. increase the snapshot or changelog expiration time. "
                                        + "2. use consumer-id to ensure that unconsumed snapshots will not be expired.",
                                nextSnapshotId));
            }
        }
    }
}
