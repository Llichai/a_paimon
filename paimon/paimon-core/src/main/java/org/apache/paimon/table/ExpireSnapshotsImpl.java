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

package org.apache.paimon.table;

import org.apache.paimon.Changelog;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.manifest.ExpireFileEntry;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.paimon.utils.SnapshotManager.findPreviousOrEqualSnapshot;
import static org.apache.paimon.utils.SnapshotManager.findPreviousSnapshot;

/**
 * 快照过期实现类 - 根据保留策略清理过期快照
 *
 * <p>ExpireSnapshotsImpl 是 {@link ExpireSnapshots} 接口的完整实现，负责根据配置的保留策略
 * 定期清理过期的快照及其相关文件（数据文件、Manifest 文件、Changelog 文件等）。
 *
 * <p><b>为什么需要清理快照？</b>
 * <ul>
 *   <li>快照会不断积累，占用大量存储空间
 *   <li>过多的快照会降低元数据查询性能
 *   <li>清理未被使用的快照可以释放存储空间
 * </ul>
 *
 * <p><b>保留策略配置项：</b>
 * <ul>
 *   <li><b>snapshot.num-retained.min</b>：最少保留的快照数量（默认 10）
 *   <li><b>snapshot.num-retained.max</b>：最多保留的快照数量（默认 Integer.MAX_VALUE）
 *   <li><b>snapshot.time-retained</b>：快照最长保留时间（默认 1 小时）
 *   <li><b>snapshot.expire.limit</b>：单次最多删除的快照数量（默认 10）
 *   <li><b>changelog.lifecycle-decoupled</b>：Changelog 生命周期是否与快照分离（默认 false）
 * </ul>
 *
 * <p><b>过期算法核心逻辑：</b>
 * <pre>
 * 1. 计算可删除范围：
 *    min = max(latestSnapshotId - retainMax + 1, earliestSnapshotId)
 *    maxExclusive = latestSnapshotId - retainMin + 1
 *
 * 2. 应用限制条件：
 *    - Consumer 进度限制：不能删除正在被消费的快照
 *    - 单次删除限制：maxExclusive = min(maxExclusive, earliestId + maxDeletes)
 *
 * 3. 时间保留检查：
 *    遍历 [min, maxExclusive)，找到第一个未过期的快照 → endExclusiveId
 *
 * 4. 执行删除：expireUntil(earliestId, endExclusiveId)
 * </pre>
 *
 * <p><b>删除流程（expireUntil 方法）：</b>
 * <ol>
 *   <li><b>删除数据文件</b>（Merge Tree Files）：
 *       <ul>
 *         <li>遍历 [beginInclusiveId+1, endExclusiveId]
 *         <li>对每个快照，调用 {@link org.apache.paimon.operation.SnapshotDeletion#cleanUnusedDataFiles}
 *         <li>保护被 Tag 引用的数据文件（通过 skipper）
 *       </ul>
 *   <li><b>删除 Changelog 文件</b>（如果未分离生命周期）：
 *       <ul>
 *         <li>遍历 [beginInclusiveId, endExclusiveId)
 *         <li>删除快照的 changelogManifestList 中的文件
 *       </ul>
 *   <li><b>清理空目录</b>：
 *       <ul>
 *         <li>删除空的 Bucket 目录
 *       </ul>
 *   <li><b>删除 Manifest 文件</b>：
 *       <ul>
 *         <li>构建跳过集合（skippingSet）：包括 Tag 引用的 Manifest + endExclusiveId 的 Manifest
 *         <li>遍历 [beginInclusiveId, endExclusiveId)
 *         <li>删除不在 skippingSet 中的 Manifest
 *       </ul>
 *   <li><b>删除快照文件</b>：
 *       <ul>
 *         <li>遍历 [beginInclusiveId, endExclusiveId)
 *         <li>如果 Changelog 生命周期分离，先提交 Changelog 到独立目录
 *         <li>删除快照文件本身
 *       </ul>
 *   <li><b>更新 earliest hint</b>：
 *       <ul>
 *         <li>将 earliest hint 更新为 endExclusiveId
 *         <li>下次过期时可以快速找到起点
 *       </ul>
 * </ol>
 *
 * <p><b>Tag 保护机制：</b>
 * <ul>
 *   <li>被 Tag 标记的快照不会被删除
 *   <li>被 Tag 引用的数据文件不会被删除
 *   <li>被 Tag 引用的 Manifest 文件不会被删除
 *   <li>通过 {@link #findSkippingTags} 找到需要保护的快照
 * </ul>
 *
 * <p><b>Consumer 保护机制：</b>
 * <ul>
 *   <li>通过 {@link ConsumerManager#minNextSnapshot()} 获取所有消费者的最小进度
 *   <li>不能删除 ≥ minNextSnapshot 的快照（消费者可能正在读取）
 * </ul>
 *
 * <p><b>Changelog 生命周期分离：</b>
 * <ul>
 *   <li>如果 changelog.lifecycle-decoupled = true：
 *       <ul>
 *         <li>删除快照时，先将 Changelog 提交到独立的 changelog 目录
 *         <li>Changelog 文件不随快照一起删除
 *         <li>Changelog 由 {@link ExpireChangelogImpl} 独立管理
 *       </ul>
 *   <li>如果 changelog.lifecycle-decoupled = false：
 *       <ul>
 *         <li>Changelog 文件随快照一起删除
 *       </ul>
 * </ul>
 *
 * <p><b>示例配置：</b>
 * <pre>
 * snapshot.num-retained.min = 10       # 至少保留 10 个快照
 * snapshot.num-retained.max = 100      # 最多保留 100 个快照
 * snapshot.time-retained = 7d          # 保留 7 天内的快照
 * snapshot.expire.limit = 20           # 单次最多删除 20 个快照
 * changelog.lifecycle-decoupled = true # Changelog 独立管理
 * </pre>
 *
 * <p><b>典型执行流程：</b>
 * <pre>
 * 假设：
 * - 当前最新快照 ID = 50
 * - 最早快照 ID = 10
 * - retainMin = 5, retainMax = 30
 * - timeRetain = 1 小时
 *
 * 计算：
 * - min = max(50 - 30 + 1, 10) = 21
 * - maxExclusive = 50 - 5 + 1 = 46
 *
 * 遍历 [21, 46)，找到第一个未过期的快照（假设 ID=35 未过期）
 * → 删除 [10, 35) 范围的快照
 * </pre>
 *
 * @see ExpireSnapshots
 * @see org.apache.paimon.operation.SnapshotDeletion
 * @see ExpireChangelogImpl
 */
public class ExpireSnapshotsImpl implements ExpireSnapshots {

    private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotsImpl.class);

    /** Snapshot 管理器（用于查询和删除快照） */
    private final SnapshotManager snapshotManager;

    /** Changelog 管理器（用于提交分离的 Changelog） */
    private final ChangelogManager changelogManager;

    /** Consumer 管理器（用于查询消费者进度，保护正在被消费的快照） */
    private final ConsumerManager consumerManager;

    /** Snapshot 删除器（负责实际的文件删除工作） */
    private final SnapshotDeletion snapshotDeletion;

    /** Tag 管理器（用于查询被 Tag 引用的快照，避免误删） */
    private final TagManager tagManager;

    /** 过期配置（保留策略） */
    private ExpireConfig expireConfig;

    /**
     * 构造 ExpireSnapshotsImpl
     *
     * @param snapshotManager Snapshot 管理器
     * @param changelogManager Changelog 管理器
     * @param snapshotDeletion Snapshot 删除器
     * @param tagManager Tag 管理器
     */
    public ExpireSnapshotsImpl(
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            SnapshotDeletion snapshotDeletion,
            TagManager tagManager) {
        this.snapshotManager = snapshotManager;
        this.changelogManager = changelogManager;
        this.consumerManager =
                new ConsumerManager(
                        snapshotManager.fileIO(),
                        snapshotManager.tablePath(),
                        snapshotManager.branch());
        this.snapshotDeletion = snapshotDeletion;
        this.tagManager = tagManager;
        this.expireConfig = ExpireConfig.builder().build();
    }

    @Override
    public ExpireSnapshots config(ExpireConfig expireConfig) {
        this.expireConfig = expireConfig;
        return this;
    }

    @Override
    public int expire() {
        snapshotDeletion.setChangelogDecoupled(expireConfig.isChangelogDecoupled());
        int retainMax = expireConfig.getSnapshotRetainMax();
        int retainMin = expireConfig.getSnapshotRetainMin();
        int maxDeletes = expireConfig.getSnapshotMaxDeletes();
        long olderThanMills =
                System.currentTimeMillis() - expireConfig.getSnapshotTimeRetain().toMillis();

        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        if (latestSnapshotId == null) {
            // no snapshot, nothing to expire
            return 0;
        }

        Long earliest = snapshotManager.earliestSnapshotId();
        if (earliest == null) {
            return 0;
        }

        Preconditions.checkArgument(
                retainMax >= retainMin,
                String.format(
                        "retainMax (%s) must not be less than retainMin (%s).",
                        retainMax, retainMin));

        // the min snapshot to retain from 'snapshot.num-retained.max'
        // (the maximum number of snapshots to retain)
        long min = Math.max(latestSnapshotId - retainMax + 1, earliest);

        // the max exclusive snapshot to expire until
        // protected by 'snapshot.num-retained.min'
        // (the minimum number of completed snapshots to retain)
        long maxExclusive = latestSnapshotId - retainMin + 1;

        // the snapshot being read by the consumer cannot be deleted
        maxExclusive =
                Math.min(maxExclusive, consumerManager.minNextSnapshot().orElse(Long.MAX_VALUE));

        // protected by 'snapshot.expire.limit'
        // (the maximum number of snapshots allowed to expire at a time)
        maxExclusive = Math.min(maxExclusive, earliest + maxDeletes);

        for (long id = min; id < maxExclusive; id++) {
            // Early exit the loop for 'snapshot.time-retained'
            // (the maximum time of snapshots to retain)
            if (snapshotManager.snapshotExists(id)
                    && olderThanMills <= snapshotManager.snapshot(id).timeMillis()) {
                return expireUntil(earliest, id);
            }
        }

        return expireUntil(earliest, maxExclusive);
    }

    @VisibleForTesting
    public int expireUntil(long earliestId, long endExclusiveId) {
        long startTime = System.currentTimeMillis();

        if (endExclusiveId <= earliestId) {
            // No expire happens:
            // write the hint file in order to see the earliest snapshot directly next time
            // should avoid duplicate writes when the file exists
            if (snapshotManager.earliestFileNotExists()) {
                writeEarliestHint(earliestId);
            }

            // fast exit
            return 0;
        }

        // find first snapshot to expire
        long beginInclusiveId = earliestId;
        for (long id = endExclusiveId - 1; id >= earliestId; id--) {
            if (!snapshotManager.snapshotExists(id)) {
                // only latest snapshots are retained, as we cannot find this snapshot, we can
                // assume that all snapshots preceding it have been removed
                beginInclusiveId = id + 1;
                break;
            }
        }

        List<Snapshot> taggedSnapshots = tagManager.taggedSnapshots();

        // delete merge tree files
        // deleted merge tree files in a snapshot are not used by the next snapshot, so the range of
        // id should be (beginInclusiveId, endExclusiveId]
        for (long id = beginInclusiveId + 1; id <= endExclusiveId; id++) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ready to delete merge tree files not used by snapshot #" + id);
            }
            Snapshot snapshot;
            try {
                snapshot = snapshotManager.tryGetSnapshot(id);
            } catch (FileNotFoundException e) {
                beginInclusiveId = id + 1;
                continue;
            }
            // expire merge tree files and collect changed buckets
            Predicate<ExpireFileEntry> skipper;
            try {
                skipper = snapshotDeletion.createDataFileSkipperForTags(taggedSnapshots, id);
            } catch (Exception e) {
                LOG.info(
                        String.format(
                                "Skip cleaning data files of snapshot '%s' due to failed to build skipping set.",
                                id),
                        e);
                continue;
            }

            snapshotDeletion.cleanUnusedDataFiles(snapshot, skipper);
        }

        // delete changelog files
        if (!expireConfig.isChangelogDecoupled()) {
            for (long id = beginInclusiveId; id < endExclusiveId; id++) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ready to delete changelog files from snapshot #" + id);
                }
                Snapshot snapshot;
                try {
                    snapshot = snapshotManager.tryGetSnapshot(id);
                } catch (FileNotFoundException e) {
                    beginInclusiveId = id + 1;
                    continue;
                }
                if (snapshot.changelogManifestList() != null) {
                    snapshotDeletion.deleteAddedDataFiles(snapshot.changelogManifestList());
                }
            }
        }

        // data files and changelog files in bucket directories has been deleted
        // then delete changed bucket directories if they are empty
        snapshotDeletion.cleanEmptyDirectories();

        // delete manifests and indexFiles
        List<Snapshot> skippingSnapshots =
                findSkippingTags(taggedSnapshots, beginInclusiveId, endExclusiveId);

        try {
            skippingSnapshots.add(snapshotManager.tryGetSnapshot(endExclusiveId));
        } catch (FileNotFoundException e) {
            // the end exclusive snapshot is gone
            // there is no need to proceed
            return 0;
        }

        Set<String> skippingSet = null;
        try {
            skippingSet = new HashSet<>(snapshotDeletion.manifestSkippingSet(skippingSnapshots));
        } catch (Exception e) {
            LOG.info("Skip cleaning manifest files due to failed to build skipping set.", e);
        }
        if (skippingSet != null) {
            for (long id = beginInclusiveId; id < endExclusiveId; id++) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ready to delete manifests in snapshot #" + id);
                }

                Snapshot snapshot;
                try {
                    snapshot = snapshotManager.tryGetSnapshot(id);
                } catch (FileNotFoundException e) {
                    beginInclusiveId = id + 1;
                    continue;
                }
                snapshotDeletion.cleanUnusedManifests(snapshot, skippingSet);
            }
        }

        // delete snapshot file finally
        for (long id = beginInclusiveId; id < endExclusiveId; id++) {
            Snapshot snapshot;
            try {
                snapshot = snapshotManager.tryGetSnapshot(id);
            } catch (FileNotFoundException e) {
                beginInclusiveId = id + 1;
                continue;
            }
            if (expireConfig.isChangelogDecoupled()) {
                commitChangelog(new Changelog(snapshot));
            }
            snapshotManager.deleteSnapshot(id);
        }

        writeEarliestHint(endExclusiveId);
        long duration = System.currentTimeMillis() - startTime;
        LOG.info(
                "Finished expire snapshots, duration {} ms, range is [{}, {})",
                duration,
                beginInclusiveId,
                endExclusiveId);
        return (int) (endExclusiveId - beginInclusiveId);
    }

    private void commitChangelog(Changelog changelog) {
        try {
            changelogManager.commitChangelog(changelog, changelog.id());
            changelogManager.commitLongLivedChangelogLatestHint(changelog.id());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeEarliestHint(long earliest) {
        try {
            snapshotManager.commitEarliestHint(earliest);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @VisibleForTesting
    public SnapshotDeletion snapshotDeletion() {
        return snapshotDeletion;
    }

    /**
     * 查找需要跳过（保护）的 Tag 快照
     *
     * <p>该方法在 sortedTags 中查找与 [beginInclusive, endExclusive) 范围重叠的快照。
     * 这些快照被 Tag 引用，因此它们引用的文件不能被删除。
     *
     * <p><b>算法：</b>
     * <ol>
     *   <li>sortedTags 是按 snapshot ID 升序排列的列表
     *   <li>使用二分查找找到 < endExclusive 的最大快照（right）
     *   <li>使用二分查找找到 ≥ beginInclusive 的最小快照（left）
     *   <li>返回 [left, right] 范围的快照
     * </ol>
     *
     * <p><b>示例：</b>
     * <pre>
     * sortedTags = [快照5, 快照10, 快照15, 快照20, 快照25]
     * 过期范围 = [12, 23)
     *
     * right = findPreviousSnapshot(sortedTags, 23) = 快照20 (index=3)
     * left = findPreviousOrEqualSnapshot(sortedTags, 12) = 快照10 (index=1)
     * 返回：[快照10, 快照15, 快照20]
     * </pre>
     *
     * @param sortedTags 按 snapshot ID 升序排列的 Tag 快照列表
     * @param beginInclusive 过期范围起始 ID（包含）
     * @param endExclusive 过期范围结束 ID（不包含）
     * @return 需要保护的快照列表
     */
    public static List<Snapshot> findSkippingTags(
            List<Snapshot> sortedTags, long beginInclusive, long endExclusive) {
        List<Snapshot> overlappedSnapshots = new ArrayList<>();
        int right = findPreviousSnapshot(sortedTags, endExclusive);
        if (right >= 0) {
            int left = Math.max(findPreviousOrEqualSnapshot(sortedTags, beginInclusive), 0);
            for (int i = left; i <= right; i++) {
                overlappedSnapshots.add(sortedTags.get(i));
            }
        }
        return overlappedSnapshots;
    }
}
