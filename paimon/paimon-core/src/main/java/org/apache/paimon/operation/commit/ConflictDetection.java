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

import org.apache.paimon.Snapshot;
import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.manifest.SimpleFileEntryWithDV;
import org.apache.paimon.operation.PartitionExpire;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RangeHelper;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.operation.commit.ManifestEntryChanges.changedPartitions;
import static org.apache.paimon.utils.InternalRowPartitionComputer.partToSimpleString;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * 提交冲突检测器
 *
 * <p>检测基线快照和增量变更之间的文件冲突。
 *
 * <h2>功能概述</h2>
 * <p>冲突检测是Paimon乐观并发控制的核心组件，确保多个作业同时写入时的数据一致性：
 * <ul>
 *   <li><b>文件删除冲突</b>：检测要删除的文件是否已被其他提交删除
 *   <li><b>键范围冲突</b>：检测LSM文件的键范围是否重叠
 *   <li><b>Bucket变更冲突</b>：检测分区的Bucket数量是否改变
 *   <li><b>行ID范围冲突</b>：检测数据演化表的行ID是否冲突
 * </ul>
 *
 * <h2>冲突检测场景</h2>
 * <table border="1">
 *   <tr>
 *     <th>冲突类型</th>
 *     <th>检测条件</th>
 *     <th>适用表类型</th>
 *   </tr>
 *   <tr>
 *     <td>文件删除冲突</td>
 *     <td>合并后仍有DELETE条目</td>
 *     <td>所有表</td>
 *   </tr>
 *   <tr>
 *     <td>键范围冲突</td>
 *     <td>同层级文件键范围重叠</td>
 *     <td>主键表</td>
 *   </tr>
 *   <tr>
 *     <td>Bucket数量冲突</td>
 *     <td>分区Bucket数量改变</td>
 *     <td>固定Bucket表</td>
 *   </tr>
 *   <tr>
 *     <td>行ID范围冲突</td>
 *     <td>多个操作更新相同行</td>
 *     <td>数据演化表</td>
 *   </tr>
 * </table>
 *
 * <h2>冲突处理流程</h2>
 * <ol>
 *   <li><b>合并条目</b>：
 *       <ul>
 *         <li>合并基线条目和增量条目
 *         <li>验证DELETE和ADD条目配对
 *       </ul>
 *   <li><b>检测冲突</b>：
 *       <ul>
 *         <li>检查是否有无法删除的文件
 *         <li>检查键范围是否重叠
 *         <li>检查Bucket数量是否一致
 *         <li>检查行ID范围是否冲突
 *       </ul>
 *   <li><b>处理结果</b>：
 *       <ul>
 *         <li>无冲突：返回 {@code Optional.empty()}
 *         <li>有冲突：返回详细的异常信息
 *       </ul>
 * </ol>
 *
 * <h2>删除向量支持</h2>
 * <p>对于启用删除向量的表，需要特殊处理：
 * <ul>
 *   <li>为文件条目附加删除向量名称
 *   <li>确保文件和删除向量同步删除
 *   <li>处理删除向量的创建和更新
 * </ul>
 *
 * <h2>分区过期处理</h2>
 * <p>支持分区过期检测：
 * <ul>
 *   <li>检查是否写入了过期分区
 *   <li>避免持续写入过期数据导致作业反复失败
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建冲突检测器
 * ConflictDetection detection = new ConflictDetection(
 *     tableName,
 *     commitUser,
 *     partitionType,
 *     pathFactory,
 *     keyComparator,
 *     bucketMode,
 *     deletionVectorsEnabled,
 *     dataEvolutionEnabled,
 *     indexFileHandler,
 *     snapshotManager,
 *     commitScanner
 * );
 *
 * // 检测冲突
 * Optional<RuntimeException> conflict = detection.checkConflicts(
 *     latestSnapshot,
 *     baseEntries,
 *     deltaEntries,
 *     deltaIndexEntries,
 *     commitKind
 * );
 *
 * if (conflict.isPresent()) {
 *     // 处理冲突，重试提交
 *     throw conflict.get();
 * }
 * }</pre>
 *
 * @see FileEntry 文件条目
 * @see SimpleFileEntry 简化文件条目
 * @see IndexManifestEntry 索引文件条目
 */
public class ConflictDetection {

    private static final Logger LOG = LoggerFactory.getLogger(ConflictDetection.class);

    /** 表名 */
    private final String tableName;

    /** 当前提交用户 */
    private final String commitUser;

    /** 分区类型 */
    private final RowType partitionType;

    /** 文件路径工厂 */
    private final FileStorePathFactory pathFactory;

    /** 键比较器，如果是APPEND表则为null */
    private final @Nullable Comparator<InternalRow> keyComparator;

    /** Bucket模式 */
    private final BucketMode bucketMode;

    /** 是否启用删除向量 */
    private final boolean deletionVectorsEnabled;

    /** 是否启用数据演化 */
    private final boolean dataEvolutionEnabled;

    /** 索引文件处理器 */
    private final IndexFileHandler indexFileHandler;

    /** 快照管理器 */
    private final SnapshotManager snapshotManager;

    /** 提交扫描器 */
    private final CommitScanner commitScanner;

    /** 分区过期策略 */
    private @Nullable PartitionExpire partitionExpire;

    /** 行ID检查起始快照 */
    private @Nullable Long rowIdCheckFromSnapshot = null;

    /**
     * 构造冲突检测器
     *
     * @param tableName 表名
     * @param commitUser 当前提交用户
     * @param partitionType 分区类型
     * @param pathFactory 文件路径工厂
     * @param keyComparator 键比较器，APPEND表为null
     * @param bucketMode Bucket模式
     * @param deletionVectorsEnabled 是否启用删除向量
     * @param dataEvolutionEnabled 是否启用数据演化
     * @param indexFileHandler 索引文件处理器
     * @param snapshotManager 快照管理器
     * @param commitScanner 提交扫描器
     */
    public ConflictDetection(
            String tableName,
            String commitUser,
            RowType partitionType,
            FileStorePathFactory pathFactory,
            @Nullable Comparator<InternalRow> keyComparator,
            BucketMode bucketMode,
            boolean deletionVectorsEnabled,
            boolean dataEvolutionEnabled,
            IndexFileHandler indexFileHandler,
            SnapshotManager snapshotManager,
            CommitScanner commitScanner) {
        this.tableName = tableName;
        this.commitUser = commitUser;
        this.partitionType = partitionType;
        this.pathFactory = pathFactory;
        this.keyComparator = keyComparator;
        this.bucketMode = bucketMode;
        this.deletionVectorsEnabled = deletionVectorsEnabled;
        this.dataEvolutionEnabled = dataEvolutionEnabled;
        this.indexFileHandler = indexFileHandler;
        this.snapshotManager = snapshotManager;
        this.commitScanner = commitScanner;
    }

    /**
     * 设置行ID检查起始快照
     *
     * <p>用于数据演化表的行ID冲突检测。
     *
     * @param rowIdCheckFromSnapshot 起始快照ID
     */
    public void setRowIdCheckFromSnapshot(@Nullable Long rowIdCheckFromSnapshot) {
        this.rowIdCheckFromSnapshot = rowIdCheckFromSnapshot;
    }

    /**
     * 获取键比较器
     *
     * @return 键比较器，APPEND表返回null
     */
    @Nullable
    public Comparator<InternalRow> keyComparator() {
        return keyComparator;
    }

    /**
     * 设置分区过期策略
     *
     * @param partitionExpire 分区过期策略
     */
    public void withPartitionExpire(PartitionExpire partitionExpire) {
        this.partitionExpire = partitionExpire;
    }

    /**
     * 判断是否应该使用覆盖提交
     *
     * <p>以下情况需要使用覆盖提交：
     * <ul>
     *   <li>追加的文件条目中包含DELETE类型
     *   <li>追加的索引文件中包含删除向量索引
     *   <li>设置了行ID检查起始快照（数据演化表）
     * </ul>
     *
     * @param appendFileEntries 追加的文件条目
     * @param appendIndexFiles 追加的索引文件
     * @return true表示应该使用覆盖提交
     */
    public <T extends FileEntry> boolean shouldBeOverwriteCommit(
            List<T> appendFileEntries, List<IndexManifestEntry> appendIndexFiles) {
        for (T appendFileEntry : appendFileEntries) {
            if (appendFileEntry.kind().equals(FileKind.DELETE)) {
                return true;
            }
        }
        for (IndexManifestEntry appendIndexFile : appendIndexFiles) {
            if (appendIndexFile.indexFile().indexType().equals(DELETION_VECTORS_INDEX)) {
                return true;
            }
        }
        return rowIdCheckFromSnapshot != null;
    }

    /**
     * 检测提交冲突
     *
     * <p>这是冲突检测的核心方法,通过比对基线快照和增量变更检测各种类型的冲突。
     *
     * <h3>检测流程</h3>
     * <ol>
     *   <li><b>删除向量处理</b>（如果启用）：
     *       <ul>
     *         <li>为基线条目附加删除向量名称
     *         <li>为增量条目附加删除向量名称
     *         <li>确保文件和删除向量同步操作
     *       </ul>
     *   <li><b>Bucket一致性检查</b>：
     *       <ul>
     *         <li>检查分区内Bucket数量是否保持一致
     *         <li>只对非OVERWRITE提交检查
     *       </ul>
     *   <li><b>合并条目</b>：
     *       <ul>
     *         <li>合并基线条目和增量条目
     *         <li>验证DELETE和ADD配对
     *       </ul>
     *   <li><b>文件删除检查</b>：
     *       <ul>
     *         <li>检查合并后是否有无法删除的文件
     *         <li>检查是否写入过期分区
     *       </ul>
     *   <li><b>键范围检查</b>（主键表）：
     *       <ul>
     *         <li>检查LSM层级文件键范围是否重叠
     *       </ul>
     *   <li><b>行ID范围检查</b>（数据演化表）：
     *       <ul>
     *         <li>检查COMPACT操作的行ID范围冲突
     *         <li>检查MERGE INTO操作的行ID范围冲突
     *       </ul>
     *   <li><b>历史快照检查</b>（数据演化表）：
     *       <ul>
     *         <li>检查从指定快照以来的行ID范围冲突
     *       </ul>
     * </ol>
     *
     * <h3>冲突类型</h3>
     * <ul>
     *   <li><b>文件删除冲突</b>：要删除的文件已被其他提交删除
     *   <li><b>键范围冲突</b>：同层级LSM文件键范围重叠
     *   <li><b>Bucket数量冲突</b>：分区的Bucket数量改变
     *   <li><b>行ID范围冲突</b>：多个操作更新相同的行
     *   <li><b>过期分区冲突</b>：写入已过期的分区
     * </ul>
     *
     * @param latestSnapshot 最新快照
     * @param baseEntries 基线文件条目（从最新快照读取）
     * @param deltaEntries 增量文件条目（本次提交的变更）
     * @param deltaIndexEntries 增量索引文件条目
     * @param commitKind 提交类型
     * @return 如果有冲突返回包含异常的Optional，否则返回empty
     */
    public Optional<RuntimeException> checkConflicts(
            Snapshot latestSnapshot,
            List<SimpleFileEntry> baseEntries,
            List<SimpleFileEntry> deltaEntries,
            List<IndexManifestEntry> deltaIndexEntries,
            CommitKind commitKind) {
        String baseCommitUser = latestSnapshot.commitUser();
        if (deletionVectorsEnabled && bucketMode.equals(BucketMode.BUCKET_UNAWARE)) {
            // Enrich dvName in fileEntry to checker for base ADD dv and delta DELETE dv.
            // For example:
            // If the base file is <ADD baseFile1, ADD dv1>,
            // then the delta file must be <DELETE deltaFile1, DELETE dv1>; and vice versa,
            // If the delta file is <DELETE deltaFile2, DELETE dv2>,
            // then the base file must be <ADD baseFile2, ADD dv2>.
            try {
                baseEntries =
                        buildBaseEntriesWithDV(
                                baseEntries,
                                latestSnapshot.indexManifest() == null
                                        ? Collections.emptyList()
                                        : indexFileHandler.readManifest(
                                                latestSnapshot.indexManifest()));
                deltaEntries =
                        buildDeltaEntriesWithDV(baseEntries, deltaEntries, deltaIndexEntries);
            } catch (Throwable e) {
                return Optional.of(
                        conflictException(commitUser, baseEntries, deltaEntries).apply(e));
            }
        }

        List<SimpleFileEntry> allEntries = new ArrayList<>(baseEntries);
        allEntries.addAll(deltaEntries);

        Optional<RuntimeException> exception =
                checkBucketKeepSame(
                        baseEntries, deltaEntries, commitKind, allEntries, baseCommitUser);
        if (exception.isPresent()) {
            return exception;
        }

        Function<Throwable, RuntimeException> conflictException =
                conflictException(baseCommitUser, baseEntries, deltaEntries);

        try {
            // check the delta, it is important not to delete and add the same file. Since scan
            // relies on map for deduplication, this may result in the loss of this file
            FileEntry.mergeEntries(deltaEntries);
        } catch (Throwable e) {
            throw conflictException.apply(e);
        }

        Collection<SimpleFileEntry> mergedEntries;
        try {
            // merge manifest entries and also check if the files we want to delete are still there
            mergedEntries = FileEntry.mergeEntries(allEntries);
        } catch (Throwable e) {
            return Optional.of(conflictException.apply(e));
        }

        exception = checkDeleteInEntries(mergedEntries, conflictException);
        if (exception.isPresent()) {
            return exception;
        }
        exception = checkKeyRange(baseEntries, deltaEntries, mergedEntries, baseCommitUser);
        if (exception.isPresent()) {
            return exception;
        }

        exception = checkRowIdRangeConflicts(commitKind, mergedEntries);
        if (exception.isPresent()) {
            return exception;
        }

        return checkForRowIdFromSnapshot(latestSnapshot, deltaEntries, deltaIndexEntries);
    }

    private Optional<RuntimeException> checkBucketKeepSame(
            List<SimpleFileEntry> baseEntries,
            List<SimpleFileEntry> deltaEntries,
            CommitKind commitKind,
            List<SimpleFileEntry> allEntries,
            String baseCommitUser) {
        if (commitKind == CommitKind.OVERWRITE) {
            return Optional.empty();
        }

        // total buckets within the same partition should remain the same
        Map<BinaryRow, Integer> totalBuckets = new HashMap<>();
        for (SimpleFileEntry entry : allEntries) {
            if (entry.totalBuckets() <= 0) {
                continue;
            }

            if (!totalBuckets.containsKey(entry.partition())) {
                totalBuckets.put(entry.partition(), entry.totalBuckets());
                continue;
            }

            int old = totalBuckets.get(entry.partition());
            if (old == entry.totalBuckets()) {
                continue;
            }

            Pair<RuntimeException, RuntimeException> conflictException =
                    createConflictException(
                            "Total buckets of partition "
                                    + entry.partition()
                                    + " changed from "
                                    + old
                                    + " to "
                                    + entry.totalBuckets()
                                    + " without overwrite. Give up committing.",
                            baseCommitUser,
                            baseEntries,
                            deltaEntries,
                            null);
            LOG.warn("", conflictException.getLeft());
            return Optional.of(conflictException.getRight());
        }
        return Optional.empty();
    }

    private Optional<RuntimeException> checkKeyRange(
            List<SimpleFileEntry> baseEntries,
            List<SimpleFileEntry> deltaEntries,
            Collection<SimpleFileEntry> mergedEntries,
            String baseCommitUser) {
        // fast exit for file store without keys
        if (keyComparator == null) {
            return Optional.empty();
        }

        // group entries by partitions, buckets and levels
        Map<LevelIdentifier, List<SimpleFileEntry>> levels = new HashMap<>();
        for (SimpleFileEntry entry : mergedEntries) {
            int level = entry.level();
            if (level >= 1) {
                levels.computeIfAbsent(
                                new LevelIdentifier(entry.partition(), entry.bucket(), level),
                                lv -> new ArrayList<>())
                        .add(entry);
            }
        }

        // check for all LSM level >= 1, key ranges of files do not intersect
        for (List<SimpleFileEntry> entries : levels.values()) {
            entries.sort((a, b) -> keyComparator.compare(a.minKey(), b.minKey()));
            for (int i = 0; i + 1 < entries.size(); i++) {
                SimpleFileEntry a = entries.get(i);
                SimpleFileEntry b = entries.get(i + 1);
                if (keyComparator.compare(a.maxKey(), b.minKey()) >= 0) {
                    Pair<RuntimeException, RuntimeException> conflictException =
                            createConflictException(
                                    "LSM conflicts detected! Give up committing. Conflict files are:\n"
                                            + a.identifier().toString(pathFactory)
                                            + "\n"
                                            + b.identifier().toString(pathFactory),
                                    baseCommitUser,
                                    baseEntries,
                                    deltaEntries,
                                    null);

                    LOG.warn("", conflictException.getLeft());
                    return Optional.of(conflictException.getRight());
                }
            }
        }
        return Optional.empty();
    }

    private Function<Throwable, RuntimeException> conflictException(
            String baseCommitUser,
            List<SimpleFileEntry> baseEntries,
            List<SimpleFileEntry> deltaEntries) {
        return e -> {
            Pair<RuntimeException, RuntimeException> conflictException =
                    createConflictException(
                            "File deletion conflicts detected! Give up committing.",
                            baseCommitUser,
                            baseEntries,
                            deltaEntries,
                            e);
            LOG.warn("", conflictException.getLeft());
            return conflictException.getRight();
        };
    }

    private Optional<RuntimeException> checkDeleteInEntries(
            Collection<SimpleFileEntry> mergedEntries,
            Function<Throwable, RuntimeException> exceptionFunction) {
        try {
            for (SimpleFileEntry entry : mergedEntries) {
                checkState(
                        entry.kind() != FileKind.DELETE,
                        "Trying to delete file %s for table %s which is not previously added.",
                        entry.fileName(),
                        tableName);
            }
        } catch (Throwable e) {
            Optional<RuntimeException> exception = checkConflictForPartitionExpire(mergedEntries);
            if (exception.isPresent()) {
                return exception;
            }
            return Optional.of(exceptionFunction.apply(e));
        }
        return Optional.empty();
    }

    private Optional<RuntimeException> checkConflictForPartitionExpire(
            Collection<SimpleFileEntry> mergedEntries) {
        if (partitionExpire != null && partitionExpire.isValueExpiration()) {
            Set<BinaryRow> deletedPartitions = new HashSet<>();
            for (SimpleFileEntry entry : mergedEntries) {
                if (entry.kind() == FileKind.DELETE) {
                    deletedPartitions.add(entry.partition());
                }
            }
            if (partitionExpire.isValueAllExpired(deletedPartitions)) {
                List<String> expiredPartitions =
                        deletedPartitions.stream()
                                .map(
                                        partition ->
                                                partToSimpleString(
                                                        partitionType, partition, "-", 200))
                                .collect(Collectors.toList());
                return Optional.of(
                        new RuntimeException(
                                "You are writing data to expired partitions, and you can filter this data to avoid job failover."
                                        + " Otherwise, continuous expired records will cause the job to failover restart continuously."
                                        + " Expired partitions are: "
                                        + expiredPartitions));
            }
        }
        return Optional.empty();
    }

    private Optional<RuntimeException> checkRowIdRangeConflicts(
            CommitKind commitKind, Collection<SimpleFileEntry> mergedEntries) {
        if (!dataEvolutionEnabled) {
            return Optional.empty();
        }
        if (rowIdCheckFromSnapshot == null && commitKind != CommitKind.COMPACT) {
            return Optional.empty();
        }

        List<SimpleFileEntry> entries =
                mergedEntries.stream()
                        .filter(file -> file.firstRowId() != null)
                        .collect(Collectors.toList());

        RangeHelper<SimpleFileEntry> rangeHelper =
                new RangeHelper<>(
                        SimpleFileEntry::nonNullFirstRowId,
                        f -> f.nonNullFirstRowId() + f.rowCount() - 1);
        List<List<SimpleFileEntry>> merged = rangeHelper.mergeOverlappingRanges(entries);
        for (List<SimpleFileEntry> group : merged) {
            List<SimpleFileEntry> dataFiles = new ArrayList<>();
            for (SimpleFileEntry f : group) {
                if (!isBlobFile(f.fileName())) {
                    dataFiles.add(f);
                }
            }
            if (!rangeHelper.areAllRangesSame(dataFiles)) {
                return Optional.of(
                        new RuntimeException(
                                "For Data Evolution table, multiple 'MERGE INTO' and 'COMPACT' operations "
                                        + "have encountered conflicts, data files: "
                                        + dataFiles));
            }
        }
        return Optional.empty();
    }

    private Optional<RuntimeException> checkForRowIdFromSnapshot(
            Snapshot latestSnapshot,
            List<SimpleFileEntry> deltaEntries,
            List<IndexManifestEntry> deltaIndexEntries) {
        if (!dataEvolutionEnabled) {
            return Optional.empty();
        }
        if (rowIdCheckFromSnapshot == null) {
            return Optional.empty();
        }

        List<BinaryRow> changedPartitions = changedPartitions(deltaEntries, deltaIndexEntries);
        // collect history row id ranges
        List<Range> historyIdRanges = new ArrayList<>();
        for (SimpleFileEntry entry : deltaEntries) {
            Long firstRowId = entry.firstRowId();
            long rowCount = entry.rowCount();
            if (firstRowId != null) {
                historyIdRanges.add(new Range(firstRowId, firstRowId + rowCount - 1));
            }
        }

        // check history row id ranges
        Long checkNextRowId = snapshotManager.snapshot(rowIdCheckFromSnapshot).nextRowId();
        checkState(
                checkNextRowId != null,
                "Next row id cannot be null for snapshot %s.",
                rowIdCheckFromSnapshot);
        for (long i = rowIdCheckFromSnapshot + 1; i <= latestSnapshot.id(); i++) {
            Snapshot snapshot = snapshotManager.snapshot(i);
            if (snapshot.commitKind() == CommitKind.COMPACT) {
                continue;
            }
            List<ManifestEntry> changes =
                    commitScanner.readIncrementalEntries(snapshot, changedPartitions);
            for (ManifestEntry entry : changes) {
                DataFileMeta file = entry.file();
                long firstRowId = file.nonNullFirstRowId();
                if (firstRowId < checkNextRowId) {
                    Range fileRange = new Range(firstRowId, firstRowId + file.rowCount() - 1);
                    for (Range range : historyIdRanges) {
                        if (range.hasIntersection(fileRange)) {
                            return Optional.of(
                                    new RuntimeException(
                                            "For Data Evolution table, multiple 'MERGE INTO' operations have encountered conflicts,"
                                                    + " updating the same file, which can render some updates ineffective."));
                        }
                    }
                }
            }
        }

        return Optional.empty();
    }

    /**
     * 为基线条目附加删除向量名称
     *
     * <p>为基线文件条目附加对应的删除向量文件名,用于冲突检测时同步检查文件和删除向量。
     *
     * <h3>处理逻辑</h3>
     * <ol>
     *   <li>从基线索引条目中提取删除向量索引
     *   <li>只处理ADD类型的索引（不包括DELETE类型）
     *   <li>建立数据文件名到删除向量文件名的映射
     *   <li>为每个基线文件条目附加对应的删除向量名称
     * </ol>
     *
     * <h3>约束检查</h3>
     * <ul>
     *   <li>一个数据文件只能对应一个删除向量条目
     *   <li>如果出现多对一的情况会抛出异常
     * </ul>
     *
     * @param baseEntries 基线文件条目列表
     * @param baseIndexEntries 基线索引条目列表
     * @return 附加删除向量名称后的文件条目列表
     * @throws IllegalStateException 如果一个文件对应多个删除向量
     */
    static List<SimpleFileEntry> buildBaseEntriesWithDV(
            List<SimpleFileEntry> baseEntries, List<IndexManifestEntry> baseIndexEntries) {
        if (baseEntries.isEmpty()) {
            return Collections.emptyList();
        }

        Map<String, String> fileNameToDVFileName = new HashMap<>();
        for (IndexManifestEntry indexManifestEntry : baseIndexEntries) {
            // Should not attach DELETE type dv index for base file.
            if (!indexManifestEntry.kind().equals(FileKind.DELETE)) {
                IndexFileMeta indexFile = indexManifestEntry.indexFile();
                LinkedHashMap<String, DeletionVectorMeta> dvRanges = indexFile.dvRanges();
                if (dvRanges != null) {
                    for (DeletionVectorMeta value : dvRanges.values()) {
                        checkState(
                                !fileNameToDVFileName.containsKey(value.dataFileName()),
                                "One file should correspond to only one dv entry.");
                        fileNameToDVFileName.put(value.dataFileName(), indexFile.fileName());
                    }
                }
            }
        }

        // Attach dv name to file entries.
        List<SimpleFileEntry> entriesWithDV = new ArrayList<>(baseEntries.size());
        for (SimpleFileEntry fileEntry : baseEntries) {
            entriesWithDV.add(
                    new SimpleFileEntryWithDV(
                            fileEntry, fileNameToDVFileName.get(fileEntry.fileName())));
        }
        return entriesWithDV;
    }

    /**
     * 为增量条目附加删除向量名称
     *
     * <p>为增量文件条目附加对应的删除向量文件名，处理比基线更复杂：
     * <ul>
     *   <li>支持同一文件对应多个删除向量条目（DELETE旧的，ADD新的）
     *   <li>支持纯删除向量更新（文件不在增量条目中）
     * </ul>
     *
     * <h3>处理场景</h3>
     * <ol>
     *   <li><b>增量文件 + 删除向量</b>：
     *       <ul>
     *         <li>文件同时在deltaEntries和deltaIndexEntries中
     *         <li>为文件条目附加删除向量名称
     *       </ul>
     *   <li><b>纯删除向量更新</b>：
     *       <ul>
     *         <li>删除向量在deltaIndexEntries中，但文件不在deltaEntries中
     *         <li>从基线条目中找到对应文件
     *         <li>根据删除向量的类型（ADD/DELETE）生成相应的文件条目
     *       </ul>
     * </ol>
     *
     * <h3>删除向量更新示例</h3>
     * <pre>
     * 场景：更新文件f1的删除向量
     * - 基线：ADD<f1, dv1>
     * - 增量索引：DELETE<dv1>, ADD<dv2>
     * - 生成条目：
     *   1. DELETE<f1, dv1> （删除旧关联）
     *   2. DELETE<f1, null> （删除旧文件状态）
     *   3. ADD<f1, dv2> （添加新关联）
     * </pre>
     *
     * @param baseEntries 基线文件条目列表（已附加删除向量）
     * @param deltaEntries 增量文件条目列表
     * @param deltaIndexEntries 增量索引条目列表
     * @return 附加删除向量名称后的增量条目列表
     * @throws IllegalStateException 如果增量条目对应多个删除向量
     */
    static List<SimpleFileEntry> buildDeltaEntriesWithDV(
            List<SimpleFileEntry> baseEntries,
            List<SimpleFileEntry> deltaEntries,
            List<IndexManifestEntry> deltaIndexEntries) {
        if (deltaEntries.isEmpty() && deltaIndexEntries.isEmpty()) {
            return Collections.emptyList();
        }

        List<SimpleFileEntry> entriesWithDV = new ArrayList<>(deltaEntries.size());

        // One file may correspond to more than one dv entries, for example, delete the old dv, and
        // create a new one.
        Map<String, List<IndexManifestEntry>> fileNameToDVEntry = new HashMap<>();
        for (IndexManifestEntry deltaIndexEntry : deltaIndexEntries) {
            LinkedHashMap<String, DeletionVectorMeta> dvRanges =
                    deltaIndexEntry.indexFile().dvRanges();
            if (dvRanges != null) {
                for (DeletionVectorMeta meta : dvRanges.values()) {
                    fileNameToDVEntry.putIfAbsent(meta.dataFileName(), new ArrayList<>());
                    fileNameToDVEntry.get(meta.dataFileName()).add(deltaIndexEntry);
                }
            }
        }

        Set<String> fileNotInDeltaEntries = new HashSet<>(fileNameToDVEntry.keySet());
        // 1. Attach dv name to delta file entries.
        for (SimpleFileEntry fileEntry : deltaEntries) {
            if (fileNameToDVEntry.containsKey(fileEntry.fileName())) {
                List<IndexManifestEntry> dvs = fileNameToDVEntry.get(fileEntry.fileName());
                checkState(dvs.size() == 1, "Delta entry only can have one dv file");
                entriesWithDV.add(
                        new SimpleFileEntryWithDV(fileEntry, dvs.get(0).indexFile().fileName()));
                fileNotInDeltaEntries.remove(fileEntry.fileName());
            } else {
                entriesWithDV.add(new SimpleFileEntryWithDV(fileEntry, null));
            }
        }

        // 2. For file not in delta entries, build entry with dv with baseEntries.
        if (!fileNotInDeltaEntries.isEmpty()) {
            Map<String, SimpleFileEntry> fileNameToFileEntry = new HashMap<>();
            for (SimpleFileEntry baseEntry : baseEntries) {
                if (baseEntry.kind().equals(FileKind.ADD)) {
                    fileNameToFileEntry.put(baseEntry.fileName(), baseEntry);
                }
            }

            for (String fileName : fileNotInDeltaEntries) {
                SimpleFileEntryWithDV simpleFileEntry =
                        (SimpleFileEntryWithDV) fileNameToFileEntry.get(fileName);
                checkState(
                        simpleFileEntry != null,
                        String.format(
                                "Trying to create deletion vector on file %s which is not previously added.",
                                fileName));
                List<IndexManifestEntry> dvEntries = fileNameToDVEntry.get(fileName);
                // If dv entry's type id DELETE, add DELETE<f, dv>
                // If dv entry's type id ADD, add ADD<f, dv>
                for (IndexManifestEntry dvEntry : dvEntries) {
                    entriesWithDV.add(
                            new SimpleFileEntryWithDV(
                                    dvEntry.kind().equals(FileKind.ADD)
                                            ? simpleFileEntry
                                            : simpleFileEntry.toDelete(),
                                    dvEntry.indexFile().fileName()));
                }

                // If one file correspond to only one dv entry and the type is ADD,
                // we need to add a DELETE<f, null>.
                // This happens when create a dv for a file that doesn't have dv before.
                if (dvEntries.size() == 1 && dvEntries.get(0).kind().equals(FileKind.ADD)) {
                    entriesWithDV.add(new SimpleFileEntryWithDV(simpleFileEntry.toDelete(), null));
                }
            }
        }

        return entriesWithDV;
    }

    /**
     * Construct detailed conflict exception. The returned exception is formed of (full exception,
     * simplified exception), The simplified exception is generated when the entry length is larger
     * than the max limit.
     */
    private Pair<RuntimeException, RuntimeException> createConflictException(
            String message,
            String baseCommitUser,
            List<SimpleFileEntry> baseEntries,
            List<SimpleFileEntry> changes,
            Throwable cause) {
        String possibleCauses =
                String.join(
                        "\n",
                        "Don't panic!",
                        "Conflicts during commits are normal and this failure is intended to resolve the conflicts.",
                        "Conflicts are mainly caused by the following scenarios:",
                        "1. Multiple jobs are writing into the same partition at the same time, "
                                + "or you use STATEMENT SET to execute multiple INSERT statements into the same Paimon table.",
                        "   You'll probably see different base commit user and current commit user below.",
                        "   You can use "
                                + "https://paimon.apache.org/docs/master/maintenance/dedicated-compaction#dedicated-compaction-job"
                                + " to support multiple writing.",
                        "2. You're recovering from an old savepoint, or you're creating multiple jobs from a savepoint.",
                        "   The job will fail continuously in this scenario to protect metadata from corruption.",
                        "   You can either recover from the latest savepoint, "
                                + "or you can revert the table to the snapshot corresponding to the old savepoint.");
        String commitUserString =
                "Base commit user is: "
                        + baseCommitUser
                        + "; Current commit user is: "
                        + commitUser;
        String baseEntriesString =
                "Base entries are:\n"
                        + baseEntries.stream()
                                .map(Object::toString)
                                .collect(Collectors.joining("\n"));
        String changesString =
                "Changes are:\n"
                        + changes.stream().map(Object::toString).collect(Collectors.joining("\n"));

        RuntimeException fullException =
                new RuntimeException(
                        message
                                + "\n\n"
                                + possibleCauses
                                + "\n\n"
                                + commitUserString
                                + "\n\n"
                                + baseEntriesString
                                + "\n\n"
                                + changesString,
                        cause);

        RuntimeException simplifiedException;
        int maxEntry = 50;
        if (baseEntries.size() > maxEntry || changes.size() > maxEntry) {
            baseEntriesString =
                    "Base entries are:\n"
                            + baseEntries.subList(0, Math.min(baseEntries.size(), maxEntry))
                                    .stream()
                                    .map(Object::toString)
                                    .collect(Collectors.joining("\n"));
            changesString =
                    "Changes are:\n"
                            + changes.subList(0, Math.min(changes.size(), maxEntry)).stream()
                                    .map(Object::toString)
                                    .collect(Collectors.joining("\n"));
            simplifiedException =
                    new RuntimeException(
                            message
                                    + "\n\n"
                                    + possibleCauses
                                    + "\n\n"
                                    + commitUserString
                                    + "\n\n"
                                    + baseEntriesString
                                    + "\n\n"
                                    + changesString
                                    + "\n\n"
                                    + "The entry list above are not fully displayed, please refer to taskmanager.log for more information.",
                            cause);
            return Pair.of(fullException, simplifiedException);
        } else {
            return Pair.of(fullException, fullException);
        }
    }

    /**
     * LSM层级标识符
     *
     * <p>用于唯一标识一个LSM层级，确保同一层级内文件键范围不重叠。
     *
     * <h3>组成部分</h3>
     * <ul>
     *   <li><b>partition</b>：分区
     *   <li><b>bucket</b>：Bucket编号
     *   <li><b>level</b>：LSM层级（>= 1的层级需要检查键范围）
     * </ul>
     */
    private static class LevelIdentifier {

        /** 分区 */
        private final BinaryRow partition;

        /** Bucket编号 */
        private final int bucket;

        /** LSM层级 */
        private final int level;

        /**
         * 构造层级标识符
         *
         * @param partition 分区
         * @param bucket Bucket编号
         * @param level LSM层级
         */
        private LevelIdentifier(BinaryRow partition, int bucket, int level) {
            this.partition = partition;
            this.bucket = bucket;
            this.level = level;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof LevelIdentifier)) {
                return false;
            }
            LevelIdentifier that = (LevelIdentifier) o;
            return Objects.equals(partition, that.partition)
                    && bucket == that.bucket
                    && level == that.level;
        }

        @Override
        public int hashCode() {
            return Objects.hash(partition, bucket, level);
        }
    }

    /**
     * 冲突检测器工厂接口
     *
     * <p>用于创建冲突检测器实例的工厂接口。
     *
     * <h3>使用场景</h3>
     * <p>该接口允许根据不同的扫描器创建冲突检测器：
     * <ul>
     *   <li>支持延迟初始化扫描器
     *   <li>允许为不同提交使用不同的扫描配置
     * </ul>
     */
    public interface Factory {
        /**
         * 创建冲突检测器
         *
         * @param scanner 提交扫描器
         * @return 冲突检测器实例
         */
        ConflictDetection create(CommitScanner scanner);
    }
}
