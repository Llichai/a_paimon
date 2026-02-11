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
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.manifest.ExpireFileEntry;
import org.apache.paimon.operation.ChangelogDeletion;
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
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.paimon.table.ExpireSnapshotsImpl.findSkippingTags;

/**
 * Changelog 过期清理实现类
 *
 * <p>该类负责根据配置的保留策略，定期清理过期的 changelog 文件。
 *
 * <p><b>为什么需要清理 Changelog？</b>
 * <ul>
 *   <li>Changelog 文件会不断积累，占用存储空间
 *   <li>过多的 changelog 文件会影响查询性能
 *   <li>某些 changelog 可能已经不再需要（如已经被消费的旧数据）
 * </ul>
 *
 * <p><b>保留策略配置项：</b>
 * <ul>
 *   <li>changelog.num-retained.min：最少保留的 changelog 数量（默认继承 snapshot.num-retained.min）
 *   <li>changelog.num-retained.max：最多保留的 changelog 数量（默认继承 snapshot.num-retained.max）
 *   <li>changelog.time-retained：最长保留时间（默认继承 snapshot.time-retained）
 * </ul>
 *
 * <p><b>过期算法：</b>
 * <ol>
 *   <li>计算可以删除的 changelog 范围：[earliestChangelogId, maxExclusive)
 *   <li>maxExclusive 受以下因素限制：
 *       <ul>
 *         <li>num-retained.min：保证至少保留 min 个 changelog
 *         <li>num-retained.max：最多删除到 (latest - max + 1)
 *         <li>consumer 进度：不能删除正在被消费的 changelog
 *         <li>expire-limit：单次最多删除的数量限制
 *         <li>time-retained：不能删除未超过保留时间的 changelog
 *       </ul>
 *   <li>遍历范围内的每个 changelog，调用 {@link ChangelogDeletion} 删除文件
 *   <li>更新 earliest hint 文件，记录新的最早 changelog ID
 * </ol>
 *
 * <p><b>与 Snapshot 过期的关系：</b>
 * <ul>
 *   <li>Changelog 的生命周期独立于 Snapshot（通过 changelogLifecycleDecoupled 配置）
 *   <li>可以配置不同的保留策略（如 changelog 保留更久用于数据追溯）
 *   <li>Changelog ID 通常与 Snapshot ID 对应，但可以单独管理
 * </ul>
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li>定期清理：由 {@link org.apache.paimon.table.FileStoreTableImpl} 在写入时定期调用
 *   <li>手动清理：通过 ExpireChangelogsProcedure 存储过程手动触发
 * </ul>
 *
 * @see ChangelogDeletion
 * @see org.apache.paimon.utils.ChangelogManager
 * @see org.apache.paimon.Changelog
 */
public class ExpireChangelogImpl implements ExpireSnapshots {

    public static final Logger LOG = LoggerFactory.getLogger(ExpireChangelogImpl.class);

    /** Snapshot 管理器，用于查询 snapshot 信息 */
    private final SnapshotManager snapshotManager;

    /** Changelog 管理器，用于查询和删除 changelog */
    private final ChangelogManager changelogManager;

    /** Consumer 管理器，用于查询消费者进度，避免删除正在被消费的 changelog */
    private final ConsumerManager consumerManager;

    /** Changelog 文件删除器 */
    private final ChangelogDeletion changelogDeletion;

    /** Tag 管理器，用于保护被 tag 引用的文件 */
    private final TagManager tagManager;

    /** 过期配置（保留策略） */
    private ExpireConfig expireConfig;

    /**
     * 构造 ExpireChangelogImpl
     *
     * @param snapshotManager Snapshot 管理器
     * @param changelogManager Changelog 管理器
     * @param tagManager Tag 管理器
     * @param changelogDeletion Changelog 删除器
     */
    public ExpireChangelogImpl(
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            TagManager tagManager,
            ChangelogDeletion changelogDeletion) {
        this.snapshotManager = snapshotManager;
        this.changelogManager = changelogManager;
        this.tagManager = tagManager;
        this.consumerManager =
                new ConsumerManager(
                        snapshotManager.fileIO(),
                        snapshotManager.tablePath(),
                        snapshotManager.branch());
        this.changelogDeletion = changelogDeletion;
        this.expireConfig = ExpireConfig.builder().build();
    }

    /**
     * 配置过期策略
     *
     * @param expireConfig 过期配置对象
     * @return this
     */
    @Override
    public ExpireSnapshots config(ExpireConfig expireConfig) {
        this.expireConfig = expireConfig;
        return this;
    }

    /**
     * 执行 changelog 过期清理
     *
     * <p>核心算法：
     * <ol>
     *   <li>获取当前最新和最早的 changelog ID
     *   <li>计算允许删除的最小 ID (min)：
     *       min = max(latestSnapshotId - retainMax + 1, earliestChangelogId)
     *   <li>计算允许删除的最大 ID (maxExclusive)：
     *       <ul>
     *         <li>初始值：latestSnapshotId - retainMin + 1（保证至少保留 retainMin 个）
     *         <li>限制1：不能超过消费者进度（consumerManager.minNextSnapshot()）
     *         <li>限制2：单次最多删除 maxDeletes 个
     *         <li>限制3：不能超过 latestChangelogId（只删除 changelog 目录的文件）
     *       </ul>
     *   <li>遍历 [min, maxExclusive)，找到第一个未过期（时间未超过 timeRetain）的 changelog
     *   <li>调用 expireUntil 删除 [earliestId, endExclusiveId) 范围的 changelog
     * </ol>
     *
     * @return 实际删除的 changelog 数量
     */
    @Override
    public int expire() {
        // 1. 获取配置参数
        int retainMax = expireConfig.getChangelogRetainMax();
        int retainMin = expireConfig.getChangelogRetainMin();
        int maxDeletes = expireConfig.getChangelogMaxDeletes();
        long olderThanMills =
                System.currentTimeMillis() - expireConfig.getChangelogTimeRetain().toMillis();

        // 2. 获取当前最新的 snapshot ID
        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        if (latestSnapshotId == null) {
            // 没有 snapshot，无需过期
            return 0;
        }

        Long earliestSnapshotId = snapshotManager.earliestSnapshotId();
        if (earliestSnapshotId == null) {
            return 0;
        }

        // 3. 获取当前最新和最早的 changelog ID
        Long latestChangelogId = changelogManager.latestLongLivedChangelogId();
        if (latestChangelogId == null) {
            return 0;
        }
        Long earliestChangelogId = changelogManager.earliestLongLivedChangelogId();
        if (earliestChangelogId == null) {
            return 0;
        }

        // 4. 校验配置参数
        Preconditions.checkArgument(
                retainMax >= retainMin,
                String.format(
                        "retainMax (%s) must not be less than retainMin (%s).",
                        retainMax, retainMin));

        // 5. 计算允许删除的范围
        // min: 最小需要保留的 changelog ID（从 retainMax 角度）
        long min = Math.max(latestSnapshotId - retainMax + 1, earliestChangelogId);

        // maxExclusive: 最大可以删除到的 ID（不包含）
        // 受多个因素限制：retainMin、consumer 进度、maxDeletes、latestChangelogId
        long maxExclusive = latestSnapshotId - retainMin + 1;

        // 限制1：不能删除正在被消费的 changelog
        maxExclusive =
                Math.min(maxExclusive, consumerManager.minNextSnapshot().orElse(Long.MAX_VALUE));

        // 限制2：单次最多删除 maxDeletes 个
        maxExclusive = Math.min(maxExclusive, earliestChangelogId + maxDeletes);

        // 限制3：只清理 changelog 目录中的文件
        maxExclusive = Math.min(maxExclusive, latestChangelogId);

        // 6. 检查时间保留策略，找到第一个未过期的 changelog
        for (long id = min; id <= maxExclusive; id++) {
            if (changelogManager.longLivedChangelogExists(id)
                    && olderThanMills <= changelogManager.longLivedChangelog(id).timeMillis()) {
                // 找到第一个未过期的，删除到这里为止
                return expireUntil(earliestChangelogId, id);
            }
        }

        // 7. 所有 changelog 都已过期，删除到 maxExclusive
        return expireUntil(earliestChangelogId, maxExclusive);
    }

    /**
     * 删除指定范围的 changelog
     *
     * <p>删除步骤：
     * <ol>
     *   <li>查找被 tag 引用的 snapshot，构建需要跳过的文件集合
     *   <li>遍历 [earliestId, endExclusiveId) 范围的每个 changelog
     *   <li>对每个 changelog：
     *       <ul>
     *         <li>构建数据文件跳过器（保护被 tag 引用的文件）
     *         <li>删除未使用的数据文件
     *         <li>删除未使用的 manifest 文件
     *         <li>删除 changelog 元数据文件本身
     *       </ul>
     *   <li>清理空目录
     *   <li>更新 earliest hint 文件
     * </ol>
     *
     * @param earliestId 最早的 changelog ID（包含）
     * @param endExclusiveId 结束的 changelog ID（不包含）
     * @return 实际删除的 changelog 数量
     */
    public int expireUntil(long earliestId, long endExclusiveId) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Changelog expire range is [" + earliestId + ", " + endExclusiveId + ")");
        }

        // 1. 获取所有被 tag 标记的 snapshot（这些 snapshot 引用的文件不能删除）
        List<Snapshot> taggedSnapshots = tagManager.taggedSnapshots();

        // 2. 找到需要跳过的 snapshot（被 tag 引用 + endExclusiveId 本身）
        List<Snapshot> skippingSnapshots =
                findSkippingTags(taggedSnapshots, earliestId, endExclusiveId);
        skippingSnapshots.add(changelogManager.changelog(endExclusiveId));

        // 3. 构建需要跳过的 manifest 文件集合
        Set<String> manifestSkippSet = changelogDeletion.manifestSkippingSet(skippingSnapshots);

        // 4. 遍历删除每个 changelog
        for (long id = earliestId; id < endExclusiveId; id++) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ready to delete changelog files from changelog #" + id);
            }

            Changelog changelog = changelogManager.longLivedChangelog(id);

            // 4.1 构建数据文件跳过器（保护被 tag 引用的数据文件）
            Predicate<ExpireFileEntry> skipper;
            try {
                skipper = changelogDeletion.createDataFileSkipperForTags(taggedSnapshots, id);
            } catch (Exception e) {
                LOG.info(
                        String.format(
                                "Skip cleaning data files of changelog '%s' due to failed to build skipping set.",
                                id),
                        e);
                continue;
            }

            // 4.2 删除未使用的数据文件
            changelogDeletion.cleanUnusedDataFiles(changelog, skipper);

            // 4.3 删除未使用的 manifest 文件
            changelogDeletion.cleanUnusedManifests(changelog, manifestSkippSet);

            // 4.4 删除 changelog 元数据文件本身
            changelogManager.fileIO().deleteQuietly(changelogManager.longLivedChangelogPath(id));
        }

        // 5. 清理空目录
        changelogDeletion.cleanEmptyDirectories();

        // 6. 更新 earliest hint 文件
        writeEarliestHintFile(endExclusiveId);

        return (int) (endExclusiveId - earliestId);
    }

    /**
     * 过期所有分离的 changelog（仅由 ExpireChangelogsProcedure 使用）
     *
     * <p>该方法用于存储过程，强制删除所有 long-lived changelog 文件。
     *
     * <p>使用场景：
     * <ul>
     *   <li>当用户不再需要历史 changelog 时，可以通过存储过程一次性清理
     *   <li>适用于 changelog 与 snapshot 生命周期分离的场景
     * </ul>
     *
     * <p>前提条件：
     * <ul>
     *   <li>latestChangelogId < earliestSnapshotId
     *   <li>即所有 changelog 都早于最早的 snapshot
     * </ul>
     *
     * <p>清理范围：
     * <ul>
     *   <li>[earliestChangelogId, latestChangelogId]
     *   <li>保护被 tag 引用的文件
     *   <li>保护 earliestSnapshotId 引用的文件
     * </ul>
     */
    public void expireAll() {
        // 1. 获取最新和最早的 snapshot ID
        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        if (latestSnapshotId == null) {
            return;
        }

        Long earliestSnapshotId = snapshotManager.earliestSnapshotId();
        if (earliestSnapshotId == null) {
            return;
        }

        // 2. 获取最新和最早的 changelog ID
        Long latestChangelogId = changelogManager.latestLongLivedChangelogId();
        if (latestChangelogId == null) {
            return;
        }
        Long earliestChangelogId = changelogManager.earliestLongLivedChangelogId();
        if (earliestChangelogId == null) {
            return;
        }

        LOG.info(
                "Read earliest and latest changelog for expire all. earliestChangelogId is {}, latestChangelogId is {}",
                earliestChangelogId,
                latestChangelogId);

        // 3. 获取所有被 tag 标记的 snapshot
        List<Snapshot> taggedSnapshots = tagManager.taggedSnapshots();

        // 4. 校验：所有 changelog 必须早于最早的 snapshot
        Preconditions.checkArgument(
                latestChangelogId < earliestSnapshotId,
                "latest changelog id should be less than earliest snapshot id."
                        + "please check your table!");

        // 5. 构建需要跳过的 snapshot 集合
        List<Snapshot> skippingSnapshots =
                findSkippingTags(taggedSnapshots, earliestChangelogId, earliestSnapshotId);
        skippingSnapshots.add(snapshotManager.snapshot(earliestSnapshotId));

        // 6. 构建需要跳过的 manifest 文件集合
        Set<String> manifestSkippSet = changelogDeletion.manifestSkippingSet(skippingSnapshots);

        // 7. 遍历删除所有 changelog
        for (long id = earliestChangelogId; id <= latestChangelogId; id++) {
            LOG.info("Ready to delete changelog files from changelog #" + id);

            // 7.1 尝试获取 changelog（可能不存在）
            Changelog changelog;
            try {
                changelog = changelogManager.tryGetChangelog(id);
            } catch (FileNotFoundException e) {
                LOG.info("fail to get changelog #" + id);
                continue;
            }

            // 7.2 构建数据文件跳过器
            Predicate<ExpireFileEntry> skipper;
            try {
                skipper = changelogDeletion.createDataFileSkipperForTags(taggedSnapshots, id);
            } catch (Exception e) {
                LOG.info(
                        String.format(
                                "Skip cleaning data files of changelog '%s' due to failed to build skipping set.",
                                id),
                        e);
                continue;
            }

            // 7.3 删除数据文件
            changelogDeletion.cleanUnusedDataFiles(changelog, skipper);

            // 7.4 删除 manifest 文件
            changelogDeletion.cleanUnusedManifests(changelog, manifestSkippSet);

            // 7.5 删除 changelog 元数据文件
            changelogManager.fileIO().deleteQuietly(changelogManager.longLivedChangelogPath(id));
        }

        // 8. 删除 hint 文件
        try {
            changelogManager.deleteEarliestHint();
            changelogManager.deleteLatestHint();
        } catch (Exception e) {
            LOG.error("delete changelog hint file error.", e);
        }

        // 9. 清理空目录
        changelogDeletion.cleanEmptyDirectories();
    }

    /**
     * 写入 earliest hint 文件
     *
     * <p>Hint 文件用于快速查找最早的 changelog ID，避免每次都遍历所有文件。
     *
     * @param earliest 新的最早 changelog ID
     */
    private void writeEarliestHintFile(long earliest) {
        try {
            changelogManager.commitLongLivedChangelogEarliestHint(earliest);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
