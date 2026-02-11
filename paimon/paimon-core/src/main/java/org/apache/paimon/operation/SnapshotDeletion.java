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

package org.apache.paimon.operation;

import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.ExpireFileEntry;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * 快照文件删除器
 *
 * <p>该类负责删除过期的快照(Snapshot)及其关联的数据文件和元数据文件。
 *
 * <p><b>快照过期策略</b>：
 * <ol>
 *   <li><b>时间保留策略 (time-retained)</b>：保留最近一段时间内的快照
 *       <ul>
 *         <li>配置项：{@code snapshot.time-retained}，默认值 1小时
 *         <li>计算方式：当前时间 - snapshot.commitMillis > time-retained 时过期
 *         <li>示例：设置 '1 h' 表示保留最近1小时内的快照
 *       </ul>
 *   </li>
 *   <li><b>数量保留策略 (num-retained-min)</b>：至少保留最近 N 个快照
 *       <ul>
 *         <li>配置项：{@code snapshot.num-retained.min}，默认值 10
 *         <li>即使超过时间保留期，也会保留最近 N 个快照
 *         <li>优先级高于时间保留策略（防止误删除最近的快照）
 *       </ul>
 *   </li>
 *   <li><b>数量上限策略 (num-retained-max)</b>：最多保留最近 N 个快照
 *       <ul>
 *         <li>配置项：{@code snapshot.num-retained.max}，默认值 Integer.MAX_VALUE
 *         <li>即使在时间保留期内，超过此数量的旧快照也会被删除
 *         <li>示例：设置 100 表示最多保留最近100个快照
 *       </ul>
 *   </li>
 *   <li><b>批量删除限制 (max-deletes)</b>：单次操作最多删除的快照数量
 *       <ul>
 *         <li>配置项：{@code snapshot.expire.limit}，默认值 10
 *         <li>防止一次性删除过多快照导致系统压力
 *         <li>剩余的过期快照会在下次清理时继续删除
 *       </ul>
 *   </li>
 * </ol>
 *
 * <p><b>快照过期时间计算示例</b>：
 * <pre>
 * 假设配置：
 *   - snapshot.time-retained = 2 h
 *   - snapshot.num-retained.min = 5
 *   - snapshot.num-retained.max = 20
 *   - 当前时间：2024-01-10 12:00:00
 *
 * 快照列表（从新到旧）：
 *   Snapshot-10: 2024-01-10 11:50:00  ✓ 保留（最近5个）
 *   Snapshot-9:  2024-01-10 11:30:00  ✓ 保留（最近5个）
 *   Snapshot-8:  2024-01-10 11:00:00  ✓ 保留（最近5个）
 *   Snapshot-7:  2024-01-10 10:30:00  ✓ 保留（最近5个）
 *   Snapshot-6:  2024-01-10 10:00:00  ✓ 保留（最近5个，虽然超过2h）
 *   Snapshot-5:  2024-01-10 09:00:00  ✗ 删除（超过2h且不在最近5个）
 *   Snapshot-4:  2024-01-10 08:00:00  ✗ 删除
 *   ...
 * </pre>
 *
 * <p><b>批量删除流程</b>：
 * <ol>
 *   <li>遍历所有快照，计算哪些快照需要删除（根据上述策略）
 *   <li>过滤掉被 Tag 引用的快照（TagManager.taggedSnapshots()）
 *   <li>限制单次删除数量（max-deletes）
 *   <li>对每个待删除快照执行：
 *       <ul>
 *         <li>删除数据文件（cleanUnusedDataFiles）
 *         <li>删除 Manifest 文件（cleanUnusedManifests）
 *         <li>删除索引文件（cleanUnusedIndexManifests）
 *         <li>删除统计文件（cleanUnusedStatisticsManifests）
 *         <li>删除快照文件本身（snapshot-N）
 *       </ul>
 *   </li>
 *   <li>清理空目录（cleanEmptyDirectories）
 * </ol>
 *
 * <p><b>Changelog 解耦模式处理</b>：
 * <ul>
 *   <li>当启用 changelog-decoupled 模式时，changelog 文件由 {@link org.apache.paimon.table.ExpireChangelogImpl} 独立管理
 *   <li>此时 SnapshotDeletion 会跳过清理 APPEND 类型的数据文件（这些是 changelog 文件）
 *   <li>避免与 ChangelogDeletion 的清理逻辑冲突
 * </ul>
 *
 * <p><b>与其他删除器的协调</b>：
 * <ul>
 *   <li>{@link ChangelogDeletion}：删除过期的 changelog 文件
 *   <li>{@link TagDeletion}：删除过期的 tag 文件
 *   <li>{@link OrphanFilesClean}：删除孤儿文件（未被任何快照引用的文件）
 * </ul>
 *
 * <p><b>使用示例</b>：
 * <pre>
 * SnapshotDeletion deletion = new SnapshotDeletion(...);
 * List&lt;Snapshot&gt; expiredSnapshots = snapshotManager.findExpiredSnapshots();
 * for (Snapshot snapshot : expiredSnapshots) {
 *     deletion.cleanUnusedDataFiles(snapshot, skipper);
 *     deletion.cleanUnusedManifests(snapshot, skippingSet);
 * }
 * deletion.cleanEmptyDirectories();
 * </pre>
 *
 * @see org.apache.paimon.utils.SnapshotManager#expire(long) 快照过期的入口方法
 * @see org.apache.paimon.table.ExpireSnapshotsImpl 快照过期的实现类
 * @see FileDeletionBase 文件删除的基类
 */
public class SnapshotDeletion extends FileDeletionBase<Snapshot> {

    /** 是否生成 changelog（用于 changelog-decoupled 模式判断） */
    private final boolean produceChangelog;

    /**
     * 构造 SnapshotDeletion
     *
     * @param fileIO 文件 IO 接口
     * @param pathFactory 路径工厂，用于生成各种文件路径
     * @param manifestFile Manifest 文件读写器
     * @param manifestList Manifest List 文件读写器
     * @param indexFileHandler 索引文件处理器
     * @param statsFileHandler 统计文件处理器
     * @param produceChangelog 是否生成 changelog（changelog-decoupled 模式下为 false）
     * @param cleanEmptyDirectories 是否清理空目录
     * @param deleteFileThreadNum 删除文件的线程数（并发删除以提高性能）
     */
    public SnapshotDeletion(
            FileIO fileIO,
            FileStorePathFactory pathFactory,
            ManifestFile manifestFile,
            ManifestList manifestList,
            IndexFileHandler indexFileHandler,
            StatsFileHandler statsFileHandler,
            boolean produceChangelog,
            boolean cleanEmptyDirectories,
            int deleteFileThreadNum) {
        super(
                fileIO,
                pathFactory,
                manifestFile,
                manifestList,
                indexFileHandler,
                statsFileHandler,
                cleanEmptyDirectories,
                deleteFileThreadNum);
        this.produceChangelog = produceChangelog;
    }

    /**
     * 清理快照中未使用的数据文件
     *
     * <p><b>Changelog 解耦模式的特殊处理</b>：
     * <ul>
     *   <li>当 changelogDecoupled=true 且 produceChangelog=false 时，表示 changelog 独立管理
     *   <li>此时跳过 APPEND 类型的数据文件（这些文件是 changelog 的一部分）
     *   <li>原因：
     *       <ul>
     *         <li>APPEND 文件可能被 changelog 引用，删除可能导致 changelog 读取失败
     *         <li>这些文件由 {@link org.apache.paimon.table.ExpireChangelogImpl} 统一管理
     *       </ul>
     *   <li>对于旧版本表（无 fileSource 信息），默认视为 APPEND，也会跳过
     * </ul>
     *
     * <p><b>删除逻辑</b>：
     * <ol>
     *   <li>读取 snapshot.deltaManifestList 获取增量文件列表
     *   <li>对每个文件条目，检查是否需要跳过（通过 skipper 判断）
     *   <li>删除标记为 DELETE 且不需要跳过的文件
     *   <li>额外文件（extraFiles，如索引文件）也会一并删除
     * </ol>
     *
     * @param snapshot 待清理的快照
     * @param skipper 文件跳过器，返回 true 表示该文件需要保留（如被 Tag 引用）
     */
    @Override
    public void cleanUnusedDataFiles(Snapshot snapshot, Predicate<ExpireFileEntry> skipper) {
        // Changelog 解耦模式：跳过 APPEND 类型文件
        if (changelogDecoupled && !produceChangelog) {
            // 跳过清理 'APPEND' 数据文件。如果没有 fileSource 信息（如旧版本表文件），
            // 我们在这里跳过清理，让 ExpireChangelogImpl 来处理
            Predicate<ExpireFileEntry> enriched =
                    manifestEntry ->
                            skipper.test(manifestEntry)
                                    || (manifestEntry.fileSource().orElse(FileSource.APPEND)
                                            == FileSource.APPEND);
            cleanUnusedDataFiles(snapshot.deltaManifestList(), enriched);
        } else {
            // 正常模式：直接清理
            cleanUnusedDataFiles(snapshot.deltaManifestList(), skipper);
        }
    }

    /**
     * 清理快照中未使用的 Manifest 文件
     *
     * <p><b>Manifest 文件的层次结构</b>：
     * <pre>
     * Snapshot
     *   ├── baseManifestList     (base 数据的 manifest list)
     *   ├── deltaManifestList    (增量数据的 manifest list)
     *   ├── changelogManifestList (changelog 的 manifest list，可选)
     *   ├── indexManifest        (索引文件的 manifest)
     *   └── statistics           (统计信息文件)
     * </pre>
     *
     * <p><b>Changelog 解耦模式的延迟清理</b>：
     * <ul>
     *   <li>当 changelogDecoupled=true 时，changelog manifest 由 ExpireChangelogImpl 管理
     *   <li>此时延迟清理 base 和 delta manifest list（传递参数控制）：
     *       <ul>
     *         <li>cleanDataManifests = !changelogDecoupled || produceChangelog
     *         <li>cleanChangelog = !changelogDecoupled
     *       </ul>
     *   <li>原因：changelog 可能还在引用这些 manifest，需要协调清理
     * </ul>
     *
     * @param snapshot 待清理的快照
     * @param skippingSet 需要跳过的 manifest 文件名集合（被其他快照或 Tag 引用）
     */
    @Override
    public void cleanUnusedManifests(Snapshot snapshot, Set<String> skippingSet) {
        // 当启用 changelog 解耦时，延迟清理 base 和 delta manifest list
        cleanUnusedManifests(
                snapshot,
                skippingSet,
                !changelogDecoupled || produceChangelog, // 是否清理数据 manifest list
                !changelogDecoupled); // 是否清理 changelog manifest list
    }

    /**
     * 清理未使用的数据文件（测试方法）
     *
     * <p>此方法用于测试，直接根据文件日志删除数据文件，不进行任何跳过检查。
     *
     * @param dataFileLog 数据文件变更日志（包含 ADD 和 DELETE 操作）
     */
    @VisibleForTesting
    void cleanUnusedDataFile(List<ExpireFileEntry> dataFileLog) {
        // 构建待删除文件集合
        Map<Path, Pair<ExpireFileEntry, List<Path>>> dataFileToDelete = new HashMap<>();
        getDataFileToDelete(dataFileToDelete, dataFileLog);
        // 执行删除（不跳过任何文件）
        doCleanUnusedDataFile(dataFileToDelete, f -> false);
    }
}
