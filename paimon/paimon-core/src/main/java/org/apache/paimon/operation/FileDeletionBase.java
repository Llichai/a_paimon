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
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.ExpireFileEntry;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileEntry.Identifier;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.utils.DataFilePathFactories;
import org.apache.paimon.utils.FileOperationThreadPool;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * 文件删除基类 - 提供清理数据文件、Manifest 文件和空目录的通用方法
 *
 * <p>该类是文件删除操作的基础实现，被以下子类继承：
 * <ul>
 *   <li>{@link SnapshotDeletion}：删除过期的 Snapshot 文件
 *   <li>{@link ChangelogDeletion}：删除过期的 Changelog 文件
 *   <li>{@link TagDeletion}：删除过期的 Tag 文件
 * </ul>
 *
 * <p><b>核心功能</b>：
 * <ol>
 *   <li><b>数据文件清理</b>：
 *       <ul>
 *         <li>读取 Manifest 文件获取数据文件列表
 *         <li>识别 ADD/DELETE 操作，构建待删除文件集合
 *         <li>应用 skipper 过滤器，跳过需要保留的文件（如被 Tag 引用）
 *         <li>并行删除文件（支持配置线程数）
 *       </ul>
 *   </li>
 *   <li><b>Manifest 文件清理</b>：
 *       <ul>
 *         <li>清理 Manifest List 文件（manifest-list-xxx）
 *         <li>清理 Manifest 文件（manifest-xxx）
 *         <li>清理索引 Manifest 文件（index-manifest-xxx）
 *         <li>应用 skippingSet 过滤器，跳过被其他快照引用的文件
 *       </ul>
 *   </li>
 *   <li><b>空目录清理</b>：
 *       <ul>
 *         <li>记录删除文件所在的 bucket 和 partition
 *         <li>尝试删除空的 bucket 目录
 *         <li>自底向上删除空的 partition 目录
 *       </ul>
 *   </li>
 * </ol>
 *
 * <p><b>文件删除的合并策略</b>：
 * <pre>
 * Manifest 中的文件条目包含 ADD 和 DELETE 两种操作：
 * - ADD: 表示文件被添加到表中（如新写入的数据文件）
 * - DELETE: 表示文件被标记为删除（如 compaction 后的旧文件）
 *
 * 合并规则：
 * 1. 如果同一个文件先 DELETE 后 ADD（文件被升级）：
 *    - DELETE 标记的文件从待删除集合中移除
 *    - 最终文件不会被删除
 *
 * 2. 如果同一个文件只有 DELETE（文件不再需要）：
 *    - 文件保留在待删除集合中
 *    - 最终文件会被删除
 *
 * 示例：
 * Manifest 中的条目序列：
 * - (DELETE, data-1.orc) -> 将 data-1.orc 加入待删除集合
 * - (ADD, data-1.orc)    -> 将 data-1.orc 从待删除集合移除（文件被升级）
 * - (DELETE, data-2.orc) -> 将 data-2.orc 加入待删除集合
 * 最终：只删除 data-2.orc，保留 data-1.orc
 * </pre>
 *
 * <p><b>Skipper 机制（文件保留过滤器）</b>：
 * <ul>
 *   <li>目的：防止删除仍在使用的文件
 *   <li>使用场景：
 *       <ul>
 *         <li>Tag 引用：Tag 可能引用了某些数据文件，这些文件不能删除
 *         <li>分支引用：不同分支可能共享某些数据文件
 *         <li>外部引用：如 CDC 任务可能正在读取某些文件
 *       </ul>
 *   </li>
 *   <li>实现方式：
 *       <ul>
 *         <li>子类实现 {@link #createDataFileSkipperForTags} 方法
 *         <li>返回一个 Predicate，判断文件是否需要保留
 *         <li>删除前对每个文件调用 skipper.test(file)
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p><b>SkippingSet 机制（Manifest 保留集合）</b>：
 * <ul>
 *   <li>目的：防止删除被其他快照引用的 Manifest 文件
 *   <li>构建方式：
 *       <ul>
 *         <li>遍历所有需要保留的快照（如被 Tag 引用的快照）
 *         <li>将它们引用的所有 Manifest 文件名加入 skippingSet
 *       </ul>
 *   </li>
 *   <li>使用方式：
 *       <ul>
 *         <li>删除 Manifest 前检查：if (!skippingSet.contains(manifestName))
 *         <li>在集合中的文件跳过删除
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p><b>Changelog Decoupled 模式</b>：
 * <ul>
 *   <li>常规模式：changelog 和数据共享同一批文件，统一管理
 *   <li>解耦模式：changelog 独立存储，由 {@link ChangelogDeletion} 单独管理
 *   <li>影响：
 *       <ul>
 *         <li>SnapshotDeletion 跳过 APPEND 类型文件（这些是 changelog 文件）
 *         <li>延迟清理 base 和 delta manifest list（避免与 changelog 冲突）
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p><b>并发删除优化</b>：
 * <ul>
 *   <li>使用 ExecutorService 并行删除文件
 *   <li>线程数通过 deleteFileThreadNum 配置（默认与 CPU 核数相关）
 *   <li>所有删除任务通过 CompletableFuture 管理，确保全部完成
 * </ul>
 *
 * <p><b>Tag 缓存机制</b>：
 * <ul>
 *   <li>问题：每次检查 skipper 都需要读取 Tag 的数据文件列表，开销大
 *   <li>优化：缓存上一个 Tag 的数据文件列表（cachedTagDataFiles）
 *   <li>策略：
 *       <ul>
 *         <li>如果当前 Tag 和缓存的 Tag 相同，直接使用缓存
 *         <li>如果不同，清空缓存并重新加载
 *       </ul>
 *   </li>
 * </ul>
 *
 * @param <T> Snapshot 或 Changelog 类型
 * @see SnapshotDeletion
 * @see ChangelogDeletion
 * @see TagDeletion
 */
public abstract class FileDeletionBase<T extends Snapshot> {

    private static final Logger LOG = LoggerFactory.getLogger(FileDeletionBase.class);

    protected final FileIO fileIO;
    protected final FileStorePathFactory pathFactory;
    protected final ManifestFile manifestFile;
    protected final ManifestList manifestList;
    protected final IndexFileHandler indexFileHandler;
    protected final StatsFileHandler statsFileHandler;
    private final boolean cleanEmptyDirectories;
    protected final Map<BinaryRow, Set<Integer>> deletionBuckets;

    private final Executor deleteFileExecutor;

    protected boolean changelogDecoupled;

    /** Used to record which tag is cached. */
    private long cachedTag = 0;

    /** Used to cache data files used by current tag. */
    private final Map<BinaryRow, Map<Integer, Set<String>>> cachedTagDataFiles = new HashMap<>();

    public FileDeletionBase(
            FileIO fileIO,
            FileStorePathFactory pathFactory,
            ManifestFile manifestFile,
            ManifestList manifestList,
            IndexFileHandler indexFileHandler,
            StatsFileHandler statsFileHandler,
            boolean cleanEmptyDirectories,
            int deleteFileThreadNum) {
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
        this.manifestFile = manifestFile;
        this.manifestList = manifestList;
        this.indexFileHandler = indexFileHandler;
        this.statsFileHandler = statsFileHandler;
        this.cleanEmptyDirectories = cleanEmptyDirectories;
        this.deletionBuckets = new HashMap<>();
        this.deleteFileExecutor = FileOperationThreadPool.getExecutorService(deleteFileThreadNum);
    }

    /**
     * Clean data files that will not be used anymore in the snapshot.
     *
     * @param snapshot {@link Snapshot} that will be cleaned
     * @param skipper if the test result of a data file is true, it will be skipped when deleting;
     *     else it will be deleted
     */
    public abstract void cleanUnusedDataFiles(T snapshot, Predicate<ExpireFileEntry> skipper);

    /**
     * Clean metadata files that will not be used anymore of a snapshot, including data manifests,
     * index manifests and manifest lists.
     *
     * @param snapshot {@link Snapshot} that will be cleaned
     * @param skippingSet manifests that should not be deleted
     */
    public abstract void cleanUnusedManifests(T snapshot, Set<String> skippingSet);

    public void setChangelogDecoupled(boolean changelogDecoupled) {
        this.changelogDecoupled = changelogDecoupled;
    }

    /** Try to delete data directories that may be empty after data file deletion. */
    public void cleanEmptyDirectories() {
        if (!cleanEmptyDirectories || deletionBuckets.isEmpty()) {
            return;
        }

        // All directory paths are deduplicated and sorted by hierarchy level
        Map<Integer, Set<Path>> deduplicate = new HashMap<>();
        for (Map.Entry<BinaryRow, Set<Integer>> entry : deletionBuckets.entrySet()) {
            List<Path> toDeleteEmptyDirectory = new ArrayList<>();
            // try to delete bucket directories
            for (Integer bucket : entry.getValue()) {
                toDeleteEmptyDirectory.add(pathFactory.bucketPath(entry.getKey(), bucket));
            }
            deleteFiles(toDeleteEmptyDirectory, this::tryDeleteEmptyDirectory);

            List<Path> hierarchicalPaths = pathFactory.getHierarchicalPartitionPath(entry.getKey());
            int hierarchies = hierarchicalPaths.size();
            if (hierarchies == 0) {
                continue;
            }

            if (tryDeleteEmptyDirectory(hierarchicalPaths.get(hierarchies - 1))) {
                // deduplicate high level partition directories
                for (int hierarchy = 0; hierarchy < hierarchies - 1; hierarchy++) {
                    Path path = hierarchicalPaths.get(hierarchy);
                    deduplicate.computeIfAbsent(hierarchy, i -> new HashSet<>()).add(path);
                }
            }
        }

        // from deepest to shallowest
        for (int hierarchy = deduplicate.size() - 1; hierarchy >= 0; hierarchy--) {
            deduplicate.get(hierarchy).forEach(this::tryDeleteEmptyDirectory);
        }

        deletionBuckets.clear();
    }

    protected void recordDeletionBuckets(ExpireFileEntry entry) {
        deletionBuckets
                .computeIfAbsent(entry.partition(), p -> new HashSet<>())
                .add(entry.bucket());
    }

    public void cleanUnusedDataFiles(String manifestList, Predicate<ExpireFileEntry> skipper) {
        // try read manifests
        List<ManifestFileMeta> manifests = tryReadManifestList(manifestList);
        List<ExpireFileEntry> manifestEntries;
        // data file path -> (original manifest entry, extra file paths)
        Map<Path, Pair<ExpireFileEntry, List<Path>>> dataFileToDelete = new HashMap<>();
        for (ManifestFileMeta manifest : manifests) {
            try {
                manifestEntries =
                        manifestFile.readExpireFileEntries(
                                manifest.fileName(), manifest.fileSize());
            } catch (Exception e) {
                // cancel deletion if any exception occurs
                LOG.warn("Failed to read some manifest files. Cancel deletion.", e);
                return;
            }

            getDataFileToDelete(dataFileToDelete, manifestEntries);
        }

        doCleanUnusedDataFile(dataFileToDelete, skipper);
    }

    protected void doCleanUnusedDataFile(
            Map<Path, Pair<ExpireFileEntry, List<Path>>> dataFileToDelete,
            Predicate<ExpireFileEntry> skipper) {
        List<Path> actualDataFileToDelete = new ArrayList<>();
        dataFileToDelete.forEach(
                (path, pair) -> {
                    ExpireFileEntry entry = pair.getLeft();
                    // check whether we should skip the data file
                    if (!skipper.test(entry)) {
                        // delete data files
                        actualDataFileToDelete.add(path);
                        actualDataFileToDelete.addAll(pair.getRight());

                        recordDeletionBuckets(entry);
                    }
                });
        deleteFiles(actualDataFileToDelete, fileIO::deleteQuietly);
    }

    protected void getDataFileToDelete(
            Map<Path, Pair<ExpireFileEntry, List<Path>>> dataFileToDelete,
            List<ExpireFileEntry> dataFileEntries) {
        // we cannot delete a data file directly when we meet a DELETE entry, because that
        // file might be upgraded
        DataFilePathFactories factories = new DataFilePathFactories(pathFactory);
        for (ExpireFileEntry entry : dataFileEntries) {
            DataFilePathFactory dataFilePathFactory =
                    factories.get(entry.partition(), entry.bucket());
            Path dataFilePath = dataFilePathFactory.toPath(entry);
            switch (entry.kind()) {
                case ADD:
                    dataFileToDelete.remove(dataFilePath);
                    break;
                case DELETE:
                    List<Path> extraFiles = new ArrayList<>(entry.extraFiles().size());
                    for (String file : entry.extraFiles()) {
                        extraFiles.add(dataFilePathFactory.toAlignedPath(file, entry));
                    }
                    dataFileToDelete.put(dataFilePath, Pair.of(entry, extraFiles));
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unknown value kind " + entry.kind().name());
            }
        }
    }

    /**
     * Delete added file in the manifest list files. Added files marked as "ADD" in manifests.
     *
     * @param manifestListName name of manifest list
     */
    public void deleteAddedDataFiles(String manifestListName) {
        List<ManifestFileMeta> manifests = tryReadManifestList(manifestListName);
        for (ManifestFileMeta manifest : manifests) {
            try {
                List<ExpireFileEntry> manifestEntries =
                        manifestFile.readExpireFileEntries(
                                manifest.fileName(), manifest.fileSize());
                deleteAddedDataFiles(manifestEntries);
            } catch (Exception e) {
                // We want to delete the data file, so just ignore the unavailable files
                LOG.info("Failed to read manifest " + manifest.fileName() + ". Ignore it.", e);
            }
        }
    }

    private void deleteAddedDataFiles(List<ExpireFileEntry> manifestEntries) {
        List<Path> dataFileToDelete = new ArrayList<>();
        DataFilePathFactories factories = new DataFilePathFactories(pathFactory);
        for (ExpireFileEntry entry : manifestEntries) {
            DataFilePathFactory dataFilePathFactory =
                    factories.get(entry.partition(), entry.bucket());
            if (entry.kind() == FileKind.ADD) {
                dataFileToDelete.add(dataFilePathFactory.toPath(entry));
                recordDeletionBuckets(entry);
            }
        }
        deleteFiles(dataFileToDelete, fileIO::deleteQuietly);
    }

    public void cleanUnusedStatisticsManifests(Snapshot snapshot, Set<String> skippingSet) {
        // clean statistics
        if (snapshot.statistics() != null && !skippingSet.contains(snapshot.statistics())) {
            statsFileHandler.deleteStats(snapshot.statistics());
        }
    }

    public void cleanUnusedIndexManifests(Snapshot snapshot, Set<String> skippingSet) {
        // clean index manifests
        String indexManifest = snapshot.indexManifest();
        // check exists, it may have been deleted by other snapshots
        if (indexManifest != null) {
            List<IndexManifestEntry> indexManifestEntries;
            try {
                indexManifestEntries = indexFileHandler.readManifestWithIOException(indexManifest);
            } catch (FileNotFoundException e) {
                return;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            indexManifestEntries.removeIf(
                    entry -> skippingSet.contains(entry.indexFile().fileName()));
            deleteFiles(indexManifestEntries, indexFileHandler::deleteIndexFile);

            if (!skippingSet.contains(indexManifest)) {
                indexFileHandler.deleteManifest(indexManifest);
            }
        }
    }

    public void cleanUnusedManifestList(String manifestName, Set<String> skippingSet) {
        List<String> toDeleteManifests = new ArrayList<>();
        List<ManifestFileMeta> toExpireManifests = tryReadManifestList(manifestName);
        for (ManifestFileMeta manifest : toExpireManifests) {
            String fileName = manifest.fileName();
            if (!skippingSet.contains(fileName)) {
                toDeleteManifests.add(fileName);
                // to avoid other snapshots trying to delete again
                skippingSet.add(fileName);
            }
        }
        if (!skippingSet.contains(manifestName)) {
            toDeleteManifests.add(manifestName);
        }

        deleteFiles(toDeleteManifests, manifestFile::delete);
    }

    protected void cleanUnusedManifests(
            Snapshot snapshot,
            Set<String> skippingSet,
            boolean deleteDataManifestLists,
            boolean deleteChangelog) {
        if (deleteDataManifestLists) {
            cleanUnusedManifestList(snapshot.baseManifestList(), skippingSet);
            cleanUnusedManifestList(snapshot.deltaManifestList(), skippingSet);
        }
        if (deleteChangelog && snapshot.changelogManifestList() != null) {
            cleanUnusedManifestList(snapshot.changelogManifestList(), skippingSet);
        }
        cleanUnusedIndexManifests(snapshot, skippingSet);
        cleanUnusedStatisticsManifests(snapshot, skippingSet);
    }

    public Predicate<ExpireFileEntry> createDataFileSkipperForTags(
            List<Snapshot> taggedSnapshots, long expiringSnapshotId) throws Exception {
        int index = SnapshotManager.findPreviousSnapshot(taggedSnapshots, expiringSnapshotId);
        // refresh tag data files
        if (index >= 0) {
            Snapshot previousTag = taggedSnapshots.get(index);
            if (previousTag.id() != cachedTag) {
                cachedTag = 0;
                cachedTagDataFiles.clear();
                addMergedDataFiles(cachedTagDataFiles, previousTag);
                // update cachedTag after read tag successfully
                cachedTag = previousTag.id();
            }
            return entry -> containsDataFile(cachedTagDataFiles, entry);
        }
        return entry -> false;
    }

    /**
     * It is possible that a job was killed during expiration and some manifest files have been
     * deleted, so if the clean methods need to get manifests of a snapshot to be cleaned, we should
     * try to read manifests and return empty list if failed instead of calling {@link
     * ManifestList#readDataManifests} directly.
     */
    protected List<ManifestFileMeta> tryReadManifestList(String manifestListName) {
        try {
            return manifestList.read(manifestListName);
        } catch (Exception e) {
            LOG.warn("Failed to read manifest list file {}", manifestListName, e);
            return Collections.emptyList();
        }
    }

    /**
     * NOTE: This method is used for building data file skipping set. If failed to read some
     * manifests, it will throw exception which callers must handle.
     */
    protected void addMergedDataFiles(
            Map<BinaryRow, Map<Integer, Set<String>>> dataFiles, Snapshot snapshot)
            throws IOException {
        for (ExpireFileEntry entry :
                readMergedDataFiles(manifestList.readDataManifests(snapshot))) {
            dataFiles
                    .computeIfAbsent(entry.partition(), p -> new HashMap<>())
                    .computeIfAbsent(entry.bucket(), b -> new HashSet<>())
                    .add(entry.fileName());
        }
    }

    protected Collection<ExpireFileEntry> readMergedDataFiles(List<ManifestFileMeta> manifests)
            throws IOException {
        Map<Identifier, ExpireFileEntry> map = new HashMap<>();
        for (ManifestFileMeta manifest : manifests) {
            List<ExpireFileEntry> entries =
                    manifestFile.readExpireFileEntries(manifest.fileName(), manifest.fileSize());
            FileEntry.mergeEntries(entries, map);
        }

        return map.values();
    }

    protected boolean containsDataFile(
            Map<BinaryRow, Map<Integer, Set<String>>> dataFiles, ExpireFileEntry entry) {
        Map<Integer, Set<String>> buckets = dataFiles.get(entry.partition());
        if (buckets != null) {
            Set<String> fileNames = buckets.get(entry.bucket());
            if (fileNames != null) {
                return fileNames.contains(entry.fileName());
            }
        }
        return false;
    }

    public Set<String> manifestSkippingSet(List<Snapshot> skippingSnapshots) {
        Set<String> skippingSet = new HashSet<>();

        for (Snapshot skippingSnapshot : skippingSnapshots) {
            // data manifests
            skippingSet.add(skippingSnapshot.baseManifestList());
            skippingSet.add(skippingSnapshot.deltaManifestList());
            manifestList.readDataManifests(skippingSnapshot).stream()
                    .map(ManifestFileMeta::fileName)
                    .forEach(skippingSet::add);

            // index manifests
            String indexManifest = skippingSnapshot.indexManifest();
            if (indexManifest != null) {
                skippingSet.add(indexManifest);
                indexFileHandler.readManifest(indexManifest).stream()
                        .map(IndexManifestEntry::indexFile)
                        .map(IndexFileMeta::fileName)
                        .forEach(skippingSet::add);
            }

            // statistics
            if (skippingSnapshot.statistics() != null) {
                skippingSet.add(skippingSnapshot.statistics());
            }
        }

        return skippingSet;
    }

    private boolean tryDeleteEmptyDirectory(Path path) {
        try {
            fileIO.delete(path, false);
            return true;
        } catch (IOException e) {
            LOG.debug("Failed to delete directory '{}'. Check whether it is empty.", path);
            return false;
        }
    }

    protected <F> void deleteFiles(Collection<F> files, Consumer<F> deletion) {
        if (files.isEmpty()) {
            return;
        }

        List<CompletableFuture<Void>> deletionFutures = new ArrayList<>(files.size());
        for (F file : files) {
            deletionFutures.add(
                    CompletableFuture.runAsync(() -> deletion.accept(file), deleteFileExecutor));
        }

        try {
            CompletableFuture.allOf(deletionFutures.toArray(new CompletableFuture[0])).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
