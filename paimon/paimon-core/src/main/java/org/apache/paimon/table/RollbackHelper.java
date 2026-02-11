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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 回滚辅助类 - 用于 {@link Table#rollbackTo} 操作，清理快照并保证数据一致性
 *
 * <p>RollbackHelper 提供了回滚表到指定快照的能力，核心功能包括：
 * <ul>
 *   <li>清理目标快照之后的所有快照
 *   <li>清理目标快照之后的所有 Long-Lived Changelog
 *   <li>清理目标快照之后的所有 Tag
 *   <li>更新 latest hint 文件
 *   <li>必要时创建快照文件（用于 Tag 回滚）
 * </ul>
 *
 * <p><b>回滚场景：</b>
 * <ul>
 *   <li><b>数据错误恢复</b>：写入了错误数据，需要回滚到之前的正确状态
 *   <li><b>Tag 回滚</b>：回滚到某个 Tag 标记的快照
 *   <li><b>时间旅行</b>：恢复到某个历史时间点的数据
 *   <li><b>测试回退</b>：在测试环境中回滚到初始状态
 * </ul>
 *
 * <p><b>回滚流程：</b>
 * <ol>
 *   <li>验证目标快照存在
 *   <li>调用 {@link #cleanLargerThan(Snapshot)} 清理后续快照
 *   <li>清理步骤：
 *       <ul>
 *         <li>清理快照文件（{@link #cleanSnapshots}）
 *         <li>清理 Long-Lived Changelog（{@link #cleanLongLivedChangelogs}）
 *         <li>清理 Tag 文件（{@link #cleanTags}）
 *       </ul>
 *   <li>如果是 Tag 回滚且快照文件不存在，创建快照文件
 * </ol>
 *
 * <p><b>数据一致性保证：</b>
 * <ul>
 *   <li><b>原子性</b>：先更新 latest hint，再删除快照（失败时可重试）
 *   <li><b>顺序性</b>：从最新快照向旧快照逆序删除
 *   <li><b>容错性</b>：跳过不存在的快照（可能已被过期删除）
 *   <li><b>元数据优先</b>：先删除快照文件，数据文件由过期任务清理
 * </ul>
 *
 * <p><b>Tag 回滚的特殊处理：</b>
 * <ul>
 *   <li>Tag 可能指向已过期的快照（earliestSnapshot > tagSnapshot）
 *   <li>此时需要调用 {@link #createSnapshotFileIfNeeded} 重建快照文件
 *   <li>将 Tag 快照文件复制到 snapshot 目录
 *   <li>更新 earliest hint 指向该快照
 * </ul>
 *
 * <p><b>示例用法：</b>
 * <pre>{@code
 * // 1. 创建 RollbackHelper
 * RollbackHelper helper = new RollbackHelper(
 *     snapshotManager,
 *     changelogManager,
 *     tagManager,
 *     fileIO
 * );
 *
 * // 2. 回滚到指定快照
 * Snapshot targetSnapshot = snapshotManager.snapshot(100);
 * helper.cleanLargerThan(targetSnapshot);
 * // 现在表状态回滚到快照 100，快照 101、102、... 都被删除
 *
 * // 3. Tag 回滚（快照可能已过期）
 * Snapshot taggedSnapshot = tagManager.taggedSnapshot("v1.0");
 * helper.createSnapshotFileIfNeeded(taggedSnapshot);  // 重建快照文件
 * helper.cleanLargerThan(taggedSnapshot);
 * }</pre>
 *
 * <p><b>清理范围示例：</b>
 * <pre>
 * 当前快照：10, 11, 12, 13, 14, 15 (latest)
 * Tag: v1.0 → 快照 12
 * 回滚到快照 12：
 *   → 删除快照：13, 14, 15
 *   → 删除 Changelog：13, 14, 15
 *   → 删除 Tag：指向 13, 14, 15 的所有 Tag
 *   → 更新 latest hint：12
 * </pre>
 *
 * <p><b>Long-Lived Changelog 的清理：</b>
 * <ul>
 *   <li>如果 Changelog 生命周期与快照分离（lifecycle-decoupled）
 *   <li>需要同步清理对应的 Changelog 文件
 *   <li>更新 Changelog 的 latest hint
 *   <li>如果所有 Changelog 都被清理，latest hint 设置为 -1
 * </ul>
 *
 * <p><b>注意事项：</b>
 * <ul>
 *   <li>回滚操作不可逆（被删除的快照无法恢复）
 *   <li>建议在回滚前创建 Tag 或备份
 *   <li>回滚不会删除数据文件（由后续的 Expire 任务清理）
 *   <li>多个 Tag 指向同一快照时，删除一个 Tag 不影响其他 Tag
 * </ul>
 *
 * @see Table#rollbackTo(long)
 * @see Table#rollbackTo(String)
 * @see org.apache.paimon.utils.SnapshotManager
 * @see org.apache.paimon.utils.TagManager
 */
public class RollbackHelper {

    /** Snapshot 管理器（用于查询和删除快照） */
    private final SnapshotManager snapshotManager;

    /** Changelog 管理器（用于清理 Long-Lived Changelog） */
    private final ChangelogManager changelogManager;

    /** Tag 管理器（用于查询和删除 Tag） */
    private final TagManager tagManager;

    /** 文件系统访问接口 */
    private final FileIO fileIO;

    /**
     * 构造 RollbackHelper
     *
     * @param snapshotManager Snapshot 管理器
     * @param changelogManager Changelog 管理器
     * @param tagManager Tag 管理器
     * @param fileIO 文件系统访问接口
     */
    public RollbackHelper(
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            TagManager tagManager,
            FileIO fileIO) {
        this.snapshotManager = snapshotManager;
        this.changelogManager = changelogManager;
        this.tagManager = tagManager;
        this.fileIO = fileIO;
    }

    /**
     * 清理大于给定快照 ID 的所有快照、Changelog 和 Tag
     *
     * <p>这是回滚的核心方法，执行步骤：
     * <ol>
     *   <li>清理快照文件（{@link #cleanSnapshots}）
     *   <li>清理 Long-Lived Changelog（{@link #cleanLongLivedChangelogs}）
     *   <li>清理 Tag 文件（{@link #cleanTags}）
     * </ol>
     *
     * @param retainedSnapshot 要保留的快照（回滚目标）
     * @throws RuntimeException 如果发生 IO 异常
     */
    public void cleanLargerThan(Snapshot retainedSnapshot) {
        try {
            cleanSnapshots(retainedSnapshot);
            cleanLongLivedChangelogs(retainedSnapshot);
            cleanTags(retainedSnapshot);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 如果需要，创建快照文件（用于 Tag 回滚）
     *
     * <p>在 Tag 回滚场景中，Tag 指向的快照可能已经被过期删除。
     * 此时需要将 Tag 快照文件复制到 snapshot 目录，并更新 earliest hint。
     *
     * <p><b>场景示例：</b>
     * <pre>
     * 当前快照：20, 21, 22, 23, 24, 25
     * Tag v1.0 → 快照 15（已被过期删除）
     * 回滚到 Tag v1.0：
     *   1. 检测到快照 15 不存在
     *   2. 从 Tag 文件恢复快照 15
     *   3. 写入 snapshot-15
     *   4. 更新 earliest hint = 15
     *   5. 删除快照 16~25（cleanLargerThan）
     * </pre>
     *
     * @param taggedSnapshot Tag 指向的快照
     */
    public void createSnapshotFileIfNeeded(Snapshot taggedSnapshot) {
        // it is possible that the earliest snapshot is later than the rollback tag because of
        // snapshot expiration, in this case the `cleanLargerThan` method will delete all
        // snapshots, so we should write the tag file to snapshot directory and modify the
        // earliest hint
        if (!snapshotManager.snapshotExists(taggedSnapshot.id())) {
            try {
                fileIO.writeFile(
                        snapshotManager.snapshotPath(taggedSnapshot.id()),
                        taggedSnapshot.toJson(),
                        false);
                snapshotManager.commitEarliestHint(taggedSnapshot.id());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private void cleanSnapshots(Snapshot retainedSnapshot) throws IOException {
        long earliest =
                checkNotNull(
                        snapshotManager.earliestSnapshotId(), "Cannot find earliest snapshot.");
        long latest =
                checkNotNull(snapshotManager.latestSnapshotId(), "Cannot find latest snapshot.");

        // modify the latest hint
        snapshotManager.commitLatestHint(retainedSnapshot.id());

        // it is possible that some snapshots have been expired
        long to = Math.max(earliest, retainedSnapshot.id() + 1);
        for (long i = latest; i >= to; i--) {
            // Ignore the non-existent snapshots
            if (snapshotManager.snapshotExists(i)) {
                snapshotManager.deleteSnapshot(i);
            }
        }
    }

    private void cleanLongLivedChangelogs(Snapshot retainedSnapshot) throws IOException {
        Long earliest = changelogManager.earliestLongLivedChangelogId();
        Long latest = changelogManager.latestLongLivedChangelogId();
        if (earliest == null || latest == null) {
            return;
        }

        // it is possible that some snapshots have been expired
        List<Changelog> toBeCleaned = new ArrayList<>();
        long to = Math.max(earliest, retainedSnapshot.id() + 1);
        for (long i = latest; i >= to; i--) {
            toBeCleaned.add(changelogManager.changelog(i));
        }

        // modify the latest hint
        if (!toBeCleaned.isEmpty()) {
            if (to == earliest) {
                // all changelog has been cleaned, so we do not know the actual latest id
                // set to -1
                changelogManager.commitLongLivedChangelogLatestHint(-1);
            } else {
                changelogManager.commitLongLivedChangelogLatestHint(to - 1);
            }
        }

        // delete data files of changelog
        for (Changelog changelog : toBeCleaned) {
            fileIO.deleteQuietly(changelogManager.longLivedChangelogPath(changelog.id()));
        }
    }

    private void cleanTags(Snapshot retainedSnapshot) {
        SortedMap<Snapshot, List<String>> tags = tagManager.tags();
        if (tags.isEmpty()) {
            return;
        }

        List<Snapshot> taggedSnapshots = new ArrayList<>(tags.keySet());

        // delete tag files
        for (int i = taggedSnapshots.size() - 1; i >= 0; i--) {
            Snapshot tag = taggedSnapshots.get(i);
            if (tag.id() <= retainedSnapshot.id()) {
                break;
            }
            tags.get(tag).forEach(tagName -> fileIO.deleteQuietly(tagManager.tagPath(tagName)));
        }
    }
}
