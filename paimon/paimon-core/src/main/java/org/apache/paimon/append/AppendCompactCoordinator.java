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

package org.apache.paimon.append;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.append.AppendDeleteFileMaintainer;
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Append-Only 表的压缩协调器
 *
 * <p>AppendCompactCoordinator 负责扫描快照中的文件,生成压缩任务,合并小文件。
 * 它是 Append-Only 表自动压缩的核心组件。
 *
 * <p>工作流程:
 * <pre>
 * 1. 扫描阶段(scan):
 *    - 从快照中读取 ManifestEntry
 *    - 按分区(partition)分组文件
 *    - 过滤需要压缩的文件(小于 compaction.file-size)
 *
 * 2. 规划阶段(compactPlan):
 *    - 为每个分区的文件打包(pack)
 *    - 生成 AppendCompactTask 列表
 *    - 清理空的或过期的 SubCoordinator
 *
 * 3. 打包策略(FileBin):
 *    - 按文件大小排序
 *    - 累计文件大小 >= targetFileSize * 2 时触发
 *    - 或文件数量 >= compaction.min-file-num 时触发
 * </pre>
 *
 * <p>老化机制(Aging):
 * 为了减少内存占用,协调器会老化单个文件:
 * <ul>
 *   <li>每次扫描后 age++
 *   <li>age > {@link #REMOVE_AGE}(10) 时,单文件分区被移除
 *   <li>age > {@link #COMPACT_AGE}(5) 时,强制压缩所有文件
 *   <li>有新文件加入时,age 重置为 0
 * </ul>
 *
 * <p>删除向量(Deletion Vector)支持:
 * 当表启用 Deletion Vector 时:
 * <ul>
 *   <li>根据索引文件(index file)对数据文件分组
 *   <li>同一组的文件必须一起压缩,避免重复删除文件
 *   <li>高删除率的文件(删除行数 > rowCount * {@code compaction.delete-ratio-threshold})强制压缩
 * </ul>
 *
 * <p>并发处理:
 * FilesIterator 支持增量和流式扫描:
 * <ul>
 *   <li>批处理模式:扫描所有快照后抛出 {@link EndOfScanException}
 *   <li>流式模式:持续扫描新快照,使用 {@link ScanMode#DELTA}
 * </ul>
 *
 * <p>故障恢复:
 * 如果第三方任务删除了最新快照中的文件(批量删除/更新/覆写):
 * <ul>
 *   <li>协调器中的文件仍然存在并参与压缩任务
 *   <li>压缩作业在提交阶段失败
 *   <li>故障转移后重新扫描最新快照中的文件
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建协调器
 * AppendCompactCoordinator coordinator =
 *     new AppendCompactCoordinator(table, true); // isStreaming
 *
 * // 运行压缩规划
 * List<AppendCompactTask> tasks = coordinator.run();
 *
 * // 执行压缩任务
 * for (AppendCompactTask task : tasks) {
 *     CommitMessage message = task.doCompact(table, write);
 *     // 提交 message
 * }
 * }</pre>
 *
 * @see AppendCompactTask 压缩任务
 * @see SubCoordinator 单分区协调器
 * @see FilesIterator 文件迭代器
 * @see DvMaintainerCache 删除向量维护器缓存
 */
public class AppendCompactCoordinator {

    private static final int FILES_BATCH = 100_000;

    protected static final int REMOVE_AGE = 10;
    protected static final int COMPACT_AGE = 5;

    private final SnapshotManager snapshotManager;
    private final long targetFileSize;
    private final long compactionFileSize;
    private final double deleteThreshold;
    private final long openFileCost;
    private final int minFileNum;
    private final DvMaintainerCache dvMaintainerCache;
    private final FilesIterator filesIterator;

    final Map<BinaryRow, SubCoordinator> subCoordinators = new HashMap<>();

    public AppendCompactCoordinator(FileStoreTable table, boolean isStreaming) {
        this(table, isStreaming, null);
    }

    public AppendCompactCoordinator(
            FileStoreTable table,
            boolean isStreaming,
            @Nullable PartitionPredicate partitionPredicate) {
        checkArgument(table.primaryKeys().isEmpty());
        this.snapshotManager = table.snapshotManager();
        CoreOptions options = table.coreOptions();
        this.targetFileSize = options.targetFileSize(false);
        this.compactionFileSize = options.compactionFileSize(false);
        this.deleteThreshold = options.compactionDeleteRatioThreshold();
        this.openFileCost = options.splitOpenFileCost();
        this.minFileNum = options.compactionMinFileNum();
        this.dvMaintainerCache =
                options.deletionVectorsEnabled()
                        ? new DvMaintainerCache(table.store().newIndexFileHandler())
                        : null;
        this.filesIterator = new FilesIterator(table, isStreaming, partitionPredicate);
    }

    public List<AppendCompactTask> run() {
        // scan files in snapshot
        if (scan()) {
            // do plan compact tasks
            return compactPlan();
        }

        return Collections.emptyList();
    }

    @VisibleForTesting
    boolean scan() {
        Map<BinaryRow, List<DataFileMeta>> files = new HashMap<>();
        for (int i = 0; i < FILES_BATCH; i++) {
            ManifestEntry entry;
            try {
                entry = filesIterator.next();
            } catch (EndOfScanException e) {
                if (!files.isEmpty()) {
                    files.forEach(this::notifyNewFiles);
                    return true;
                }
                throw e;
            }
            if (entry == null) {
                break;
            }
            BinaryRow partition = entry.partition();
            files.computeIfAbsent(partition, k -> new ArrayList<>()).add(entry.file());
        }

        if (files.isEmpty()) {
            return false;
        }

        files.forEach(this::notifyNewFiles);
        return true;
    }

    @VisibleForTesting
    FilesIterator filesIterator() {
        return filesIterator;
    }

    @VisibleForTesting
    void notifyNewFiles(BinaryRow partition, List<DataFileMeta> files) {
        List<DataFileMeta> toCompact =
                files.stream()
                        .filter(file -> shouldCompact(partition, file))
                        .collect(Collectors.toList());
        subCoordinators
                .computeIfAbsent(partition, pp -> new SubCoordinator(partition))
                .addFiles(toCompact);
    }

    @VisibleForTesting
    // generate compaction task to the next stage
    List<AppendCompactTask> compactPlan() {
        // first loop to found compaction tasks
        List<AppendCompactTask> tasks =
                subCoordinators.values().stream()
                        .flatMap(s -> s.plan().stream())
                        .collect(Collectors.toList());

        // second loop to eliminate empty or old(with only one file) coordinator
        new ArrayList<>(subCoordinators.values())
                .stream()
                        .filter(SubCoordinator::readyToRemove)
                        .map(SubCoordinator::partition)
                        .forEach(subCoordinators::remove);

        return tasks;
    }

    @VisibleForTesting
    HashSet<DataFileMeta> listRestoredFiles() {
        HashSet<DataFileMeta> result = new HashSet<>();
        subCoordinators.values().stream().map(SubCoordinator::toCompact).forEach(result::addAll);
        return result;
    }

    /** Coordinator for a single partition. */
    class SubCoordinator {

        private final BinaryRow partition;
        private final HashSet<DataFileMeta> toCompact = new HashSet<>();
        int age = 0;

        public SubCoordinator(BinaryRow partition) {
            this.partition = partition;
        }

        public List<AppendCompactTask> plan() {
            return pickCompact();
        }

        public BinaryRow partition() {
            return partition;
        }

        public HashSet<DataFileMeta> toCompact() {
            return toCompact;
        }

        private List<AppendCompactTask> pickCompact() {
            List<List<DataFileMeta>> waitCompact = agePack();
            return waitCompact.stream()
                    .map(files -> new AppendCompactTask(partition, files))
                    .collect(Collectors.toList());
        }

        public void addFiles(List<DataFileMeta> dataFileMetas) {
            // reset age
            age = 0;
            // add to compact
            toCompact.addAll(dataFileMetas);
        }

        public boolean readyToRemove() {
            return toCompact.isEmpty() || age > REMOVE_AGE;
        }

        private List<List<DataFileMeta>> agePack() {
            List<List<DataFileMeta>> packed;
            if (dvMaintainerCache == null) {
                packed = pack(toCompact);
            } else {
                packed = packInDeletionVectorVMode(toCompact);
            }
            if (packed.isEmpty()) {
                // non-packed, we need to grow up age, and check whether to compact once
                if (++age > COMPACT_AGE && toCompact.size() > 1) {
                    List<DataFileMeta> all = new ArrayList<>(toCompact);
                    // empty the restored files, wait to be removed
                    toCompact.clear();
                    packed = Collections.singletonList(all);
                }
            }

            return packed;
        }

        private List<List<DataFileMeta>> pack(Set<DataFileMeta> toCompact) {
            // we don't know how many parallel compact works there should be, so in order to pack
            // better, we will sort them first
            ArrayList<DataFileMeta> files = new ArrayList<>(toCompact);
            files.sort(Comparator.comparingLong(DataFileMeta::fileSize));

            List<List<DataFileMeta>> result = new ArrayList<>();
            FileBin fileBin = new FileBin();
            for (DataFileMeta fileMeta : files) {
                fileBin.addFile(fileMeta);
                if (fileBin.enoughContent()) {
                    result.add(fileBin.drain());
                }
            }

            if (fileBin.enoughInputFiles()) {
                result.add(fileBin.drain());
            }
            // else skip these small files that are too few

            return result;
        }

        private List<List<DataFileMeta>> packInDeletionVectorVMode(Set<DataFileMeta> toCompact) {
            // we group the data files by their related index files.
            // In the subsequent compact task, if any files with deletion vectors are compacted, we
            // need to rewrite their corresponding deleted files. To avoid duplicate deleted files,
            // we must group them according to the deleted files
            Map<String, List<DataFileMeta>> filesWithDV = new HashMap<>();
            Set<DataFileMeta> rest = new HashSet<>();
            for (DataFileMeta dataFile : toCompact) {
                String indexFile =
                        dvMaintainerCache
                                .dvMaintainer(partition)
                                .getIndexFilePath(dataFile.fileName());
                if (indexFile == null) {
                    rest.add(dataFile);
                } else {
                    filesWithDV.computeIfAbsent(indexFile, f -> new ArrayList<>()).add(dataFile);
                }
            }

            // To avoid too small a compact task, merge them
            List<List<DataFileMeta>> dvGroups = new ArrayList<>(filesWithDV.values());
            dvGroups.sort(Comparator.comparingLong(this::fileSizeOfList));

            List<List<DataFileMeta>> result = new ArrayList<>();
            FileBin fileBin = new FileBin();
            for (List<DataFileMeta> dvGroup : dvGroups) {
                fileBin.addFiles(dvGroup);
                if (fileBin.enoughContent()) {
                    result.add(fileBin.drain());
                }
            }

            // for file with deletion vectors, must do compaction
            if (!fileBin.bin.isEmpty()) {
                result.add(fileBin.drain());
            }

            if (rest.size() > 1) {
                result.addAll(pack(rest));
            }
            return result;
        }

        private long fileSizeOfList(List<DataFileMeta> list) {
            return list.stream().mapToLong(DataFileMeta::fileSize).sum();
        }

        /** A file bin for {@link SubCoordinator} determine whether ready to compact. */
        private class FileBin {
            List<DataFileMeta> bin = new ArrayList<>();
            long totalFileSize = 0;

            public List<DataFileMeta> drain() {
                List<DataFileMeta> result = new ArrayList<>(bin);
                bin.forEach(toCompact::remove);
                bin.clear();
                totalFileSize = 0;
                return result;
            }

            public void addFiles(List<DataFileMeta> files) {
                files.forEach(this::addFile);
            }

            public void addFile(DataFileMeta file) {
                totalFileSize += file.fileSize() + openFileCost;
                bin.add(file);
            }

            private boolean enoughContent() {
                return bin.size() > 1 && totalFileSize >= targetFileSize * 2;
            }

            private boolean enoughInputFiles() {
                return bin.size() >= minFileNum;
            }
        }
    }

    private class DvMaintainerCache {

        private final IndexFileHandler indexFileHandler;

        /** Should be thread safe, ManifestEntryFilter will be invoked in many threads. */
        private final Map<BinaryRow, AppendDeleteFileMaintainer> cache = new ConcurrentHashMap<>();

        private DvMaintainerCache(IndexFileHandler indexFileHandler) {
            this.indexFileHandler = indexFileHandler;
        }

        private void refresh() {
            this.cache.clear();
        }

        private AppendDeleteFileMaintainer dvMaintainer(BinaryRow partition) {
            AppendDeleteFileMaintainer maintainer = cache.get(partition);
            if (maintainer == null) {
                synchronized (this) {
                    maintainer =
                            BaseAppendDeleteFileMaintainer.forUnawareAppend(
                                    indexFileHandler, snapshotManager.latestSnapshot(), partition);
                }
                cache.put(partition, maintainer);
            }
            return maintainer;
        }
    }

    /** Iterator to read files. */
    class FilesIterator {

        private final SnapshotReader snapshotReader;
        private final boolean streamingMode;

        @Nullable private Long nextSnapshot = null;
        @Nullable private Iterator<ManifestEntry> currentIterator;

        public FilesIterator(
                FileStoreTable table,
                boolean isStreaming,
                @Nullable PartitionPredicate partitionPredicate) {
            this.snapshotReader = table.newSnapshotReader();
            if (partitionPredicate != null) {
                snapshotReader.withPartitionFilter(partitionPredicate);
            }
            // drop stats to reduce memory
            if (table.coreOptions().manifestDeleteFileDropStats()) {
                snapshotReader.dropStats();
            }
            this.streamingMode = isStreaming;
        }

        private void assignNewIterator() {
            currentIterator = null;
            if (nextSnapshot == null) {
                nextSnapshot = snapshotManager.latestSnapshotId();
                if (nextSnapshot == null) {
                    if (!streamingMode) {
                        throw new EndOfScanException();
                    }
                    return;
                }
                snapshotReader.withMode(ScanMode.ALL);
            } else {
                if (!streamingMode) {
                    throw new EndOfScanException();
                }
                snapshotReader.withMode(ScanMode.DELTA);
            }

            if (!snapshotManager.snapshotExists(nextSnapshot)) {
                return;
            }

            Snapshot snapshot = snapshotManager.snapshot(nextSnapshot);
            nextSnapshot++;

            if (dvMaintainerCache != null) {
                dvMaintainerCache.refresh();
            }
            currentIterator =
                    snapshotReader
                            .withManifestEntryFilter(
                                    entry -> shouldCompact(entry.partition(), entry.file()))
                            .withSnapshot(snapshot)
                            .readFileIterator();
        }

        @Nullable
        public ManifestEntry next() {
            while (true) {
                if (currentIterator == null) {
                    assignNewIterator();
                    if (currentIterator == null) {
                        return null;
                    }
                }

                if (currentIterator.hasNext()) {
                    ManifestEntry entry = currentIterator.next();
                    if (entry.kind() == FileKind.DELETE) {
                        continue;
                    } else {
                        return entry;
                    }
                }
                currentIterator = null;
            }
        }
    }

    private boolean shouldCompact(BinaryRow partition, DataFileMeta file) {
        return file.fileSize() < compactionFileSize || tooHighDeleteRatio(partition, file);
    }

    private boolean tooHighDeleteRatio(BinaryRow partition, DataFileMeta file) {
        if (dvMaintainerCache != null) {
            DeletionFile deletionFile =
                    dvMaintainerCache.dvMaintainer(partition).getDeletionFile(file.fileName());
            if (deletionFile != null) {
                Long cardinality = deletionFile.cardinality();
                long rowCount = file.rowCount();
                return cardinality == null || cardinality > rowCount * deleteThreshold;
            }
        }
        return false;
    }

    @VisibleForTesting
    AppendDeleteFileMaintainer dvMaintainer(BinaryRow partition) {
        return dvMaintainerCache.dvMaintainer(partition);
    }
}
