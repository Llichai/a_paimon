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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.ExpireFileEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.utils.DataFilePathFactories;
import org.apache.paimon.utils.FileStorePathFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Tag 文件删除器
 *
 * <p>该类负责删除过期的 Tag 及其关联的数据文件和元数据文件。
 *
 * <p><b>什么是 Tag？</b>
 * <ul>
 *   <li>Tag 是对特定 Snapshot 的命名引用，类似于 Git 中的 tag
 *   <li>作用：
 *       <ul>
 *         <li>保留重要的数据版本（如每日快照、月度备份）
 *         <li>防止重要快照被自动过期删除
 *         <li>方便后续查询和回滚到特定版本
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p><b>Tag 保留策略</b>：
 * <ul>
 *   <li><b>手动创建的 Tag</b>：
 *       <ul>
 *         <li>不会自动过期，需要手动删除
 *         <li>使用场景：重要的里程碑版本、测试基线等
 *       </ul>
 *   </li>
 *   <li><b>自动创建的 Tag</b>：
 *       <ul>
 *         <li>配置项：{@code tag.automatic-creation}（如 'daily', 'hourly'）
 *         <li>保留策略：
 *             <ul>
 *               <li>{@code tag.num-retained.daily}：保留最近 N 天的 daily tag
 *               <li>{@code tag.num-retained.hourly}：保留最近 N 小时的 hourly tag
 *             </ul>
 *         </li>
 *         <li>过期后自动删除
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p><b>Tag 删除流程</b>：
 * <ol>
 *   <li><b>读取 Tag 引用的 Snapshot</b>：
 *       <ul>
 *         <li>每个 Tag 文件（tag-xxx）内部存储一个 snapshot ID
 *         <li>读取该 snapshot ID 对应的 Snapshot 对象
 *       </ul>
 *   </li>
 *   <li><b>计算需要保留的数据文件</b>：
 *       <ul>
 *         <li>遍历其他未删除的 Snapshot，构建它们引用的数据文件集合
 *         <li>这些文件不能删除（可能被其他 Snapshot 或 Tag 使用）
 *         <li>使用 {@link #dataFileSkipper} 方法构建跳过器
 *       </ul>
 *   </li>
 *   <li><b>删除数据文件</b>：
 *       <ul>
 *         <li>读取 Tag 对应 Snapshot 的 base 和 delta manifest
 *         <li>合并获取所有引用的数据文件
 *         <li>应用 skipper 过滤，跳过需要保留的文件
 *         <li>删除剩余的数据文件和额外文件（extraFiles）
 *       </ul>
 *   </li>
 *   <li><b>删除 Manifest 文件</b>：
 *       <ul>
 *         <li>清理 base manifest list 和 delta manifest list
 *         <li>清理 index manifest（但不清理 changelog manifest，由 SnapshotDeletion 处理）
 *         <li>应用 skippingSet 过滤，跳过被其他快照引用的 manifest
 *       </ul>
 *   </li>
 *   <li><b>删除 Tag 文件本身</b>：
 *       <ul>
 *         <li>删除 tag-xxx 文件
 *       </ul>
 *   </li>
 * </ol>
 *
 * <p><b>与 SnapshotDeletion 的区别</b>：
 * <ul>
 *   <li><b>SnapshotDeletion</b>：
 *       <ul>
 *         <li>删除普通快照（未被 Tag 引用）
 *         <li>处理 changelog manifest（如果启用 changelog-decoupled）
 *         <li>从 deltaManifestList 中读取增量文件
 *       </ul>
 *   </li>
 *   <li><b>TagDeletion</b>：
 *       <ul>
 *         <li>删除 Tag 引用的快照
 *         <li>不处理 changelog manifest（由 SnapshotDeletion 统一处理）
 *         <li>从 baseManifestList + deltaManifestList 中读取所有文件（合并读取）
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p><b>数据文件的合并读取</b>：
 * <pre>
 * Tag 删除时需要读取完整的数据文件列表，包括：
 * - Base files: 基础数据文件（来自 compaction）
 * - Delta files: 增量数据文件（新写入的）
 *
 * 读取流程：
 * 1. 读取 baseManifestList，获取所有 base manifest 文件
 * 2. 读取 deltaManifestList，获取所有 delta manifest 文件
 * 3. 合并两部分 manifest，读取所有数据文件条目
 * 4. 调用 {@link #readMergedDataFiles} 合并 ADD/DELETE 操作
 * 5. 得到最终的数据文件列表
 * </pre>
 *
 * <p><b>Extra Files（额外文件）处理</b>：
 * <ul>
 *   <li>什么是 extra files：
 *       <ul>
 *         <li>与数据文件对齐的辅助文件（如 Bloom Filter 文件）
 *         <li>存储在 {@link org.apache.paimon.io.DataFileMeta#extraFiles()} 中
 *       </ul>
 *   </li>
 *   <li>删除逻辑：
 *       <ul>
 *         <li>删除数据文件时，同时删除其所有 extra files
 *         <li>使用 {@code dataFilePathFactory.toAlignedPath} 计算路径
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p><b>异常处理</b>：
 * <ul>
 *   <li>读取 Manifest 失败：记录日志，跳过该 Tag 的数据文件清理（避免误删）
 *   <li>删除文件失败：静默忽略（使用 fileIO.deleteQuietly），残留文件由孤儿文件清理处理
 * </ul>
 *
 * <p><b>使用示例</b>：
 * <pre>
 * TagDeletion deletion = new TagDeletion(...);
 * List&lt;Snapshot&gt; expiredTags = tagManager.findExpiredTags();
 * for (Snapshot taggedSnapshot : expiredTags) {
 *     // 构建 skipper（跳过其他快照引用的文件）
 *     Predicate&lt;ExpireFileEntry&gt; skipper =
 *         deletion.dataFileSkipper(otherSnapshots);
 *
 *     // 删除数据文件和 manifest 文件
 *     deletion.cleanUnusedDataFiles(taggedSnapshot, skipper);
 *     deletion.cleanUnusedManifests(taggedSnapshot, skippingSet);
 * }
 * </pre>
 *
 * @see org.apache.paimon.utils.TagManager Tag 管理器
 * @see org.apache.paimon.table.ExpireSnapshotsImpl Tag 过期实现
 * @see SnapshotDeletion 快照删除
 */
public class TagDeletion extends FileDeletionBase<Snapshot> {

    private static final Logger LOG = LoggerFactory.getLogger(TagDeletion.class);

    /**
     * 构造 TagDeletion
     *
     * @param fileIO 文件 IO 接口
     * @param pathFactory 路径工厂
     * @param manifestFile Manifest 文件读写器
     * @param manifestList Manifest List 文件读写器
     * @param indexFileHandler 索引文件处理器
     * @param statsFileHandler 统计文件处理器
     * @param cleanEmptyDirectories 是否清理空目录
     * @param deleteFileThreadNum 删除文件的线程数
     */
    public TagDeletion(
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
     * 清理 Tag 引用的快照中未使用的数据文件
     *
     * <p><b>与 SnapshotDeletion 的区别</b>：
     * <ul>
     *   <li>读取方式：合并读取 baseManifestList 和 deltaManifestList（完整数据）
     *   <li>SnapshotDeletion：只读取 deltaManifestList（增量数据）
     * </ul>
     *
     * <p><b>删除流程</b>：
     * <ol>
     *   <li>读取 base manifest 和 delta manifest，合并获取所有数据文件条目
     *   <li>对每个文件条目，调用 skipper 判断是否需要跳过
     *   <li>删除不需要跳过的数据文件和额外文件（extraFiles）
     *   <li>记录删除的 bucket，用于后续空目录清理
     * </ol>
     *
     * <p><b>异常处理</b>：
     * <ul>
     *   <li>如果读取 manifest 失败（IOException），记录日志并跳过该 Tag 的清理
     *   <li>原因：避免在不确定的情况下误删文件
     * </ul>
     *
     * @param taggedSnapshot Tag 引用的快照对象
     * @param skipper 文件跳过器，返回 true 表示该文件需要保留
     */
    @Override
    public void cleanUnusedDataFiles(Snapshot taggedSnapshot, Predicate<ExpireFileEntry> skipper) {
        Collection<ExpireFileEntry> manifestEntries;
        try {
            // 读取 base 和 delta manifest，合并获取所有文件条目
            List<ManifestFileMeta> manifests =
                    tryReadManifestList(taggedSnapshot.baseManifestList());
            manifests.addAll(tryReadManifestList(taggedSnapshot.deltaManifestList()));
            manifestEntries = readMergedDataFiles(manifests);
        } catch (IOException e) {
            LOG.info("Skip data file clean for the tag of id {}.", taggedSnapshot.id(), e);
            return;
        }

        // 构建待删除文件集合
        Set<Path> dataFileToDelete = new HashSet<>();
        DataFilePathFactories factories = new DataFilePathFactories(pathFactory);
        for (ExpireFileEntry entry : manifestEntries) {
            DataFilePathFactory dataFilePathFactory =
                    factories.get(entry.partition(), entry.bucket());
            if (!skipper.test(entry)) {
                // 删除数据文件
                dataFileToDelete.add(dataFilePathFactory.toPath(entry));
                // 删除额外文件（如 Bloom Filter 文件）
                for (String file : entry.extraFiles()) {
                    dataFileToDelete.add(dataFilePathFactory.toAlignedPath(file, entry));
                }

                recordDeletionBuckets(entry);
            }
        }
        // 并行删除文件
        deleteFiles(dataFileToDelete, fileIO::deleteQuietly);
    }

    /**
     * 清理 Tag 引用的快照中未使用的 Manifest 文件
     *
     * <p><b>清理内容</b>：
     * <ul>
     *   <li>Base manifest list 和对应的 manifest 文件
     *   <li>Delta manifest list 和对应的 manifest 文件
     *   <li>Index manifest 和对应的索引文件
     *   <li>Statistics 文件
     * </ul>
     *
     * <p><b>注意</b>：不清理 changelog manifest，因为它们由 SnapshotDeletion 统一处理。
     *
     * @param taggedSnapshot Tag 引用的快照对象
     * @param skippingSet 需要跳过的 manifest 文件名集合（被其他快照或 Tag 引用）
     */
    @Override
    public void cleanUnusedManifests(Snapshot taggedSnapshot, Set<String> skippingSet) {
        // 不清理 changelog 文件，因为它们由 SnapshotDeletion 处理
        cleanUnusedManifests(taggedSnapshot, skippingSet, true, false);
    }

    /**
     * 构建数据文件跳过器（用于判断文件是否需要保留）
     *
     * <p><b>功能</b>：构建一个 Predicate，用于判断某个数据文件是否被其他快照引用。
     *
     * <p><b>实现逻辑</b>：
     * <ol>
     *   <li>遍历所有需要保留的快照（fromSnapshots）
     *   <li>读取每个快照的数据文件列表，加入到 skipped 集合中
     *   <li>返回一个 Predicate：检查文件是否在 skipped 集合中
     * </ol>
     *
     * <p><b>数据结构</b>：
     * <pre>
     * skipped: Map&lt;BinaryRow, Map&lt;Integer, Set&lt;String&gt;&gt;&gt;
     *   - BinaryRow: 分区
     *   - Integer: bucket ID
     *   - Set&lt;String&gt;: 文件名集合
     *
     * 示例：
     * skipped = {
     *   partition1: {
     *     bucket0: ["data-1.orc", "data-2.orc"],
     *     bucket1: ["data-3.orc"]
     *   },
     *   partition2: {
     *     bucket0: ["data-4.orc"]
     *   }
     * }
     * </pre>
     *
     * @param fromSnapshots 需要保留的快照列表（这些快照引用的文件不能删除）
     * @return 文件跳过器（返回 true 表示文件需要保留）
     * @throws Exception 如果读取快照失败
     */
    public Predicate<ExpireFileEntry> dataFileSkipper(List<Snapshot> fromSnapshots)
            throws Exception {
        Map<BinaryRow, Map<Integer, Set<String>>> skipped = new HashMap<>();
        for (Snapshot snapshot : fromSnapshots) {
            addMergedDataFiles(skipped, snapshot);
        }
        return entry -> containsDataFile(skipped, entry);
    }
}
