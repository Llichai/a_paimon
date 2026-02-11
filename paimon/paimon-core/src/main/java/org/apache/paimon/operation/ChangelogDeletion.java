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

import org.apache.paimon.Changelog;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.ExpireFileEntry;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.utils.FileStorePathFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Changelog 文件删除器
 *
 * <p>该类负责删除过期的 changelog 文件及其相关的元数据文件。
 *
 * <p>Changelog 是 Paimon 用于记录数据变更的机制，根据配置的不同模式（INPUT、FULL_COMPACTION、LOOKUP）
 * 会生成不同的 changelog 文件。这些文件随着时间推移会积累，需要定期清理。
 *
 * <p>清理内容包括：
 * <ul>
 *   <li>Changelog 数据文件：实际存储变更记录的文件（如 changelog-xxx.avro）
 *   <li>Manifest 文件：描述 changelog 数据文件元信息的清单文件
 *   <li>Manifest List 文件：描述 manifest 文件列表的文件
 *   <li>空目录：清理后留下的空目录（可选）
 * </ul>
 *
 * <p>与 {@link org.apache.paimon.operation.SnapshotDeletion} 的区别：
 * <ul>
 *   <li>SnapshotDeletion：删除 snapshot 快照相关文件（数据+元数据）
 *   <li>ChangelogDeletion：只删除 changelog 相关文件（变更日志）
 * </ul>
 *
 * <p>使用场景：
 * 当配置了 changelog 保留策略（如 changelog.num-retained.max、changelog.time-retained）后，
 * {@link org.apache.paimon.table.ExpireChangelogImpl} 会调用此类清理过期的 changelog。
 *
 * @see org.apache.paimon.table.ExpireChangelogImpl
 * @see org.apache.paimon.Changelog
 */
public class ChangelogDeletion extends FileDeletionBase<Changelog> {

    /**
     * 构造 ChangelogDeletion
     *
     * @param fileIO 文件 IO 接口
     * @param pathFactory 路径工厂，用于生成各种文件路径
     * @param manifestFile Manifest 文件读写器
     * @param manifestList Manifest List 文件读写器
     * @param indexFileHandler 索引文件处理器
     * @param statsFileHandler 统计文件处理器
     * @param cleanEmptyDirectories 是否清理空目录
     * @param deleteFileThreadNum 删除文件的线程数
     */
    public ChangelogDeletion(
            FileIO fileIO,
            FileStorePathFactory pathFactory,
            ManifestFile manifestFile,
            ManifestList manifestList,
            IndexFileHandler indexFileHandler,
            StatsFileHandler statsFileHandler,
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
    }

    /**
     * 清理 Changelog 中未使用的数据文件
     *
     * <p>Changelog 包含两部分数据文件：
     * <ul>
     *   <li>changelog 文件：FULL_COMPACTION/LOOKUP 模式生成的独立 changelog 文件
     *       （通过 changelogManifestList 引用）
     *   <li>delta 数据文件：INPUT 模式下，changelog 和数据文件是同一批文件
     *       （通过 deltaManifestList 引用）
     * </ul>
     *
     * <p>删除逻辑：
     * <ol>
     *   <li>如果有 changelogManifestList，删除其中标记为 ADD 的文件
     *   <li>如果有 deltaManifestList，使用 skipper 过滤需要保留的文件后，删除剩余文件
     * </ol>
     *
     * @param changelog 待清理的 changelog 对象
     * @param skipper 文件跳过器，返回 true 表示该文件需要保留（被 tag 引用等）
     */
    @Override
    public void cleanUnusedDataFiles(Changelog changelog, Predicate<ExpireFileEntry> skipper) {
        // 1. 清理独立的 changelog 文件（FULL_COMPACTION/LOOKUP 模式）
        if (changelog.changelogManifestList() != null) {
            deleteAddedDataFiles(changelog.changelogManifestList());
        }

        // 2. 清理 delta 数据文件（INPUT 模式，changelog 与数据文件是同一批）
        if (manifestList.exists(changelog.deltaManifestList())) {
            cleanUnusedDataFiles(changelog.deltaManifestList(), skipper);
        }
    }

    /**
     * 清理 Changelog 中未使用的 Manifest 文件
     *
     * <p>Manifest 文件的层次结构：
     * <pre>
     * Changelog
     *   ├── changelogManifestList  (指向 changelog manifest 文件列表)
     *   ├── deltaManifestList       (指向 delta manifest 文件列表)
     *   └── baseManifestList        (指向 base manifest 文件列表)
     * </pre>
     *
     * <p>清理时需要检查这些 manifest 文件是否被其他 changelog 或 snapshot 引用（通过 skippingSet）。
     * 如果没有被引用，则可以安全删除。
     *
     * @param changelog 待清理的 changelog 对象
     * @param skippingSet 需要跳过的 manifest 文件名集合（被其他 changelog/snapshot 引用）
     */
    @Override
    public void cleanUnusedManifests(Changelog changelog, Set<String> skippingSet) {
        // 1. 清理 changelog manifest list（如果存在）
        if (changelog.changelogManifestList() != null) {
            cleanUnusedManifestList(changelog.changelogManifestList(), skippingSet);
        }

        // 2. 清理 delta manifest list（如果存在）
        if (manifestList.exists(changelog.deltaManifestList())) {
            cleanUnusedManifestList(changelog.deltaManifestList(), skippingSet);
        }

        // 3. 清理 base manifest list（如果存在）
        if (manifestList.exists(changelog.baseManifestList())) {
            cleanUnusedManifestList(changelog.baseManifestList(), skippingSet);
        }

        // 注意：索引和统计信息的 manifest list 由 snapshot deletion 处理，这里不处理
    }

    /**
     * 构建需要跳过的 Manifest 文件集合
     *
     * <p>遍历所有需要跳过的 snapshot（通常是被 tag 引用的 snapshot），
     * 将它们引用的所有 manifest 文件加入到 skippingSet 中。
     *
     * <p>包括：
     * <ul>
     *   <li>Base manifests：基础数据的 manifest
     *   <li>Delta manifests：增量数据的 manifest
     *   <li>Index manifests：索引文件的 manifest
     *   <li>Statistics files：统计信息文件
     * </ul>
     *
     * <p>这些文件不能被删除，因为它们可能被正在使用的 snapshot 或 tag 引用。
     *
     * @param skippingSnapshots 需要跳过的 snapshot 列表（如被 tag 引用的 snapshot）
     * @return 需要跳过的 manifest 文件名集合
     */
    @Override
    public Set<String> manifestSkippingSet(List<Snapshot> skippingSnapshots) {
        Set<String> skippingSet = new HashSet<>();

        for (Snapshot skippingSnapshot : skippingSnapshots) {
            // 1. 添加 base manifests
            if (manifestList.exists(skippingSnapshot.baseManifestList())) {
                skippingSet.add(skippingSnapshot.baseManifestList());
                manifestList.read(skippingSnapshot.baseManifestList()).stream()
                        .map(ManifestFileMeta::fileName)
                        .forEach(skippingSet::add);
            }

            // 2. 添加 delta manifests
            if (manifestList.exists(skippingSnapshot.deltaManifestList())) {
                skippingSet.add(skippingSnapshot.deltaManifestList());
                manifestList.read(skippingSnapshot.deltaManifestList()).stream()
                        .map(ManifestFileMeta::fileName)
                        .forEach(skippingSet::add);
            }

            // 3. 添加 index manifests
            String indexManifest = skippingSnapshot.indexManifest();
            if (indexManifest != null && indexFileHandler.existsManifest(indexManifest)) {
                skippingSet.add(indexManifest);
                indexFileHandler.readManifest(indexManifest).stream()
                        .map(IndexManifestEntry::indexFile)
                        .map(IndexFileMeta::fileName)
                        .forEach(skippingSet::add);
            }

            // 4. 添加统计信息文件
            if (skippingSnapshot.statistics() != null) {
                skippingSet.add(skippingSnapshot.statistics());
            }
        }

        return skippingSet;
    }
}
