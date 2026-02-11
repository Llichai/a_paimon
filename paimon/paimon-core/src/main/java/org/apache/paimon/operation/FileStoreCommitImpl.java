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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.commit.CommitChanges;
import org.apache.paimon.operation.commit.CommitChangesProvider;
import org.apache.paimon.operation.commit.CommitCleaner;
import org.apache.paimon.operation.commit.CommitResult;
import org.apache.paimon.operation.commit.CommitRollback;
import org.apache.paimon.operation.commit.CommitScanner;
import org.apache.paimon.operation.commit.ConflictDetection;
import org.apache.paimon.operation.commit.ManifestEntryChanges;
import org.apache.paimon.operation.commit.RetryCommitResult;
import org.apache.paimon.operation.commit.RetryCommitResult.CommitFailRetryResult;
import org.apache.paimon.operation.commit.RowTrackingCommitUtils.RowTrackingAssigned;
import org.apache.paimon.operation.commit.StrictModeChecker;
import org.apache.paimon.operation.commit.SuccessCommitResult;
import org.apache.paimon.operation.metrics.CommitMetrics;
import org.apache.paimon.operation.metrics.CommitStats;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DataFilePathFactories;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.RetryWaiter;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.manifest.ManifestEntry.nullableRecordCount;
import static org.apache.paimon.manifest.ManifestEntry.recordCountAdd;
import static org.apache.paimon.manifest.ManifestEntry.recordCountDelete;
import static org.apache.paimon.operation.commit.ManifestEntryChanges.changedPartitions;
import static org.apache.paimon.operation.commit.RowTrackingCommitUtils.assignRowTracking;
import static org.apache.paimon.partition.PartitionPredicate.createBinaryPartitions;
import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * {@link FileStoreCommit} 的默认实现
 *
 * <p>这个类为用户提供了原子提交方法。
 *
 * <h2>提交流程（两阶段提交协议）</h2>
 * <ol>
 *   <li><b>预处理阶段</b>：
 *       <ul>
 *         <li>调用 {@link #filterCommitted} 过滤已提交的数据（用户可选）
 *         <li>收集所有变更（新增文件、删除文件、索引文件等）
 *       </ul>
 *   <li><b>冲突检测阶段</b>：
 *       <ul>
 *         <li>检查所有待删除文件是否仍然存在
 *         <li>检查修改文件的 Key 范围是否与现有文件重叠
 *         <li>根据冲突检测策略进行验证
 *       </ul>
 *   <li><b>原子提交阶段</b>：
 *       <ul>
 *         <li>使用外部 {@link SnapshotCommit}（如果提供）
 *         <li>或使用文件系统的原子重命名确保原子性
 *       </ul>
 *   <li><b>异常处理</b>：
 *       <ul>
 *         <li>如果因冲突或异常提交失败，尽力清理并中止
 *         <li>如果原子重命名失败，从第 2 步开始重试
 *       </ul>
 * </ol>
 *
 * <h2>冲突检测机制</h2>
 * <p>支持三种冲突检测策略（通过 {@link ConflictDetection} 实现）：
 * <ul>
 *   <li><b>无冲突检测</b>：适用于无并发写入的场景
 *   <li><b>追加文件冲突检测</b>：检查文件是否被删除
 *   <li><b>键范围冲突检测</b>：检查键范围是否重叠（适用于 Primary Key 表）
 * </ul>
 *
 * <h2>快照管理</h2>
 * <ul>
 *   <li>每次提交生成新的快照（Snapshot）
 *   <li>维护 Manifest List 和 Manifest 文件
 *   <li>支持增量快照和完整快照
 *   <li>支持快照过期和清理
 * </ul>
 *
 * <h2>提交类型（CommitKind）</h2>
 * <ul>
 *   <li><b>APPEND</b>：普通追加提交
 *   <li><b>COMPACT</b>：压缩提交
 *   <li><b>OVERWRITE</b>：覆盖提交（分区覆盖）
 *   <li><b>ANALYZE</b>：统计信息提交
 * </ul>
 *
 * <h2>重要说明</h2>
 * <p>注意：如果要修改此类，提交过程中的任何异常都不能被忽略。必须抛出异常以重启作业。
 * 建议运行 FileStoreCommitTest 数千次以确保您的更改是正确的。
 *
 * @see FileStoreCommit 提交接口定义
 * @see ConflictDetection 冲突检测策略
 * @see SnapshotManager 快照管理器
 */
public class FileStoreCommitImpl implements FileStoreCommit {

    private static final Logger LOG = LoggerFactory.getLogger(FileStoreCommitImpl.class);

    private final SnapshotCommit snapshotCommit;
    private final FileIO fileIO;
    private final SchemaManager schemaManager;
    private final String tableName;
    private final String commitUser;
    private final RowType partitionType;
    private final CoreOptions options;
    private final String partitionDefaultName;
    private final FileStorePathFactory pathFactory;
    private final SnapshotManager snapshotManager;
    private final ManifestFile manifestFile;
    private final ManifestList manifestList;
    private final IndexManifestFile indexManifestFile;
    @Nullable private final CommitRollback rollback;
    private final CommitScanner scanner;
    private final int numBucket;
    private final MemorySize manifestTargetSize;
    private final MemorySize manifestFullCompactionSize;
    private final int manifestMergeMinCount;
    private final boolean dynamicPartitionOverwrite;
    private final String branchName;
    @Nullable private final Integer manifestReadParallelism;
    private final List<CommitCallback> commitCallbacks;
    private final StatsFileHandler statsFileHandler;
    private final BucketMode bucketMode;
    private final long commitTimeout;
    private final RetryWaiter retryWaiter;
    private final int commitMaxRetries;
    private final InternalRowPartitionComputer partitionComputer;
    private final boolean rowTrackingEnabled;
    private final boolean discardDuplicateFiles;
    @Nullable private final StrictModeChecker strictModeChecker;
    private final ConflictDetection conflictDetection;
    private final CommitCleaner commitCleaner;

    private boolean ignoreEmptyCommit;
    private CommitMetrics commitMetrics;
    private boolean appendCommitCheckConflict = false;

    public FileStoreCommitImpl(
            SnapshotCommit snapshotCommit,
            FileIO fileIO,
            SchemaManager schemaManager,
            String tableName,
            String commitUser,
            RowType partitionType,
            CoreOptions options,
            String partitionDefaultName,
            FileStorePathFactory pathFactory,
            SnapshotManager snapshotManager,
            ManifestFile.Factory manifestFileFactory,
            ManifestList.Factory manifestListFactory,
            IndexManifestFile.Factory indexManifestFileFactory,
            FileStoreScan scan,
            int numBucket,
            MemorySize manifestTargetSize,
            MemorySize manifestFullCompactionSize,
            int manifestMergeMinCount,
            boolean dynamicPartitionOverwrite,
            String branchName,
            StatsFileHandler statsFileHandler,
            BucketMode bucketMode,
            @Nullable Integer manifestReadParallelism,
            List<CommitCallback> commitCallbacks,
            int commitMaxRetries,
            long commitTimeout,
            long commitMinRetryWait,
            long commitMaxRetryWait,
            boolean rowTrackingEnabled,
            boolean discardDuplicateFiles,
            ConflictDetection.Factory conflictDetectFactory,
            @Nullable StrictModeChecker strictModeChecker,
            @Nullable CommitRollback rollback) {
        this.snapshotCommit = snapshotCommit;
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.tableName = tableName;
        this.commitUser = commitUser;
        this.partitionType = partitionType;
        this.options = options;
        this.partitionDefaultName = partitionDefaultName;
        this.pathFactory = pathFactory;
        this.snapshotManager = snapshotManager;
        this.manifestFile = manifestFileFactory.create();
        this.manifestList = manifestListFactory.create();
        this.indexManifestFile = indexManifestFileFactory.create();
        this.rollback = rollback;
        this.scanner = new CommitScanner(scan, indexManifestFile, options);
        this.numBucket = numBucket;
        this.manifestTargetSize = manifestTargetSize;
        this.manifestFullCompactionSize = manifestFullCompactionSize;
        this.manifestMergeMinCount = manifestMergeMinCount;
        this.dynamicPartitionOverwrite = dynamicPartitionOverwrite;
        this.branchName = branchName;
        this.manifestReadParallelism = manifestReadParallelism;
        this.commitCallbacks = commitCallbacks;
        this.commitMaxRetries = commitMaxRetries;
        this.commitTimeout = commitTimeout;
        this.retryWaiter = new RetryWaiter(commitMinRetryWait, commitMaxRetryWait);
        this.partitionComputer =
                new InternalRowPartitionComputer(
                        options.partitionDefaultName(),
                        partitionType,
                        partitionType.getFieldNames().toArray(new String[0]),
                        options.legacyPartitionName());
        this.ignoreEmptyCommit = true;
        this.commitMetrics = null;
        this.statsFileHandler = statsFileHandler;
        this.bucketMode = bucketMode;
        this.rowTrackingEnabled = rowTrackingEnabled;
        this.discardDuplicateFiles = discardDuplicateFiles;
        this.strictModeChecker = strictModeChecker;
        this.conflictDetection = conflictDetectFactory.create(scanner);
        this.commitCleaner = new CommitCleaner(manifestList, manifestFile, indexManifestFile);
    }

    /**
     * 设置是否忽略空提交
     *
     * <p>当没有数据变更时，可以选择忽略提交以避免生成空快照。
     *
     * @param ignoreEmptyCommit true 表示忽略空提交，false 表示即使没有变更也生成快照
     * @return 当前提交对象，支持链式调用
     */
    @Override
    public FileStoreCommit ignoreEmptyCommit(boolean ignoreEmptyCommit) {
        this.ignoreEmptyCommit = ignoreEmptyCommit;
        return this;
    }

    /**
     * 设置分区过期策略
     *
     * <p>将分区过期策略传递给冲突检测器，用于在提交前检查分区过期情况。
     *
     * @param partitionExpire 分区过期策略
     * @return 当前提交对象，支持链式调用
     */
    @Override
    public FileStoreCommit withPartitionExpire(PartitionExpire partitionExpire) {
        this.conflictDetection.withPartitionExpire(partitionExpire);
        return this;
    }

    /**
     * 设置追加提交是否检查冲突
     *
     * <p>默认情况下追加提交不检查冲突，但在某些场景下需要开启冲突检测。
     *
     * @param appendCommitCheckConflict true 表示追加提交时检查冲突
     * @return 当前提交对象，支持链式调用
     */
    @Override
    public FileStoreCommit appendCommitCheckConflict(boolean appendCommitCheckConflict) {
        this.appendCommitCheckConflict = appendCommitCheckConflict;
        return this;
    }

    /**
     * 设置行ID冲突检查的起始快照
     *
     * <p>用于行级跟踪场景下的冲突检测。
     *
     * @param rowIdCheckFromSnapshot 开始检查行ID冲突的快照ID，null 表示不检查
     * @return 当前提交对象，支持链式调用
     */
    @Override
    public FileStoreCommit rowIdCheckConflict(@Nullable Long rowIdCheckFromSnapshot) {
        this.conflictDetection.setRowIdCheckFromSnapshot(rowIdCheckFromSnapshot);
        return this;
    }

    /**
     * 过滤已提交的可提交对象
     *
     * <p>该方法用于去重，过滤掉已经成功提交的数据，避免重复提交。
     *
     * <h3>过滤逻辑</h3>
     * <ol>
     *   <li>检查用户的最新快照
     *   <li>比较可提交对象的标识符与快照的提交标识符
     *   <li>保留标识符大于快照提交标识符的可提交对象
     *   <li>对于已提交的对象，触发回调的 retry 方法
     * </ol>
     *
     * <h3>前置条件</h3>
     * <p>可提交对象必须按照标识符升序排列，否则会抛出异常。
     *
     * @param committables 待过滤的可提交对象列表，必须按标识符升序排列
     * @return 过滤后的可提交对象列表，只包含未提交的对象
     */
    @Override
    public List<ManifestCommittable> filterCommitted(List<ManifestCommittable> committables) {
        // 快速退出：如果列表为空，直接返回
        if (committables.isEmpty()) {
            return committables;
        }

        // 验证输入：确保可提交对象按标识符排序
        for (int i = 1; i < committables.size(); i++) {
            checkArgument(
                    committables.get(i).identifier() > committables.get(i - 1).identifier(),
                    "Committables must be sorted according to identifiers before filtering. This is unexpected.");
        }

        // 获取用户的最新快照
        Optional<Snapshot> latestSnapshot = snapshotManager.latestSnapshotOfUser(commitUser);
        if (latestSnapshot.isPresent()) {
            List<ManifestCommittable> result = new ArrayList<>();
            for (ManifestCommittable committable : committables) {
                // 如果可提交对象比最新快照更新，则它还没有被提交
                if (committable.identifier() > latestSnapshot.get().commitIdentifier()) {
                    result.add(committable);
                } else {
                    // 已提交的对象，触发回调的重试方法
                    commitCallbacks.forEach(callback -> callback.retry(committable));
                }
            }
            return result;
        } else {
            // 如果没有之前的快照，则不需要过滤任何内容
            return committables;
        }
    }

    /**
     * 提交数据变更到表中
     *
     * <p>这是 Paimon 中最核心的提交方法，实现了完整的两阶段提交协议。
     *
     * <h2>提交流程详解</h2>
     *
     * <h3>第一阶段：变更收集和分类</h3>
     * <ol>
     *   <li><b>收集变更</b>：从 {@link CommitMessage} 中收集所有文件变更
     *   <li><b>分类变更</b>：将变更分为追加文件、压缩文件和索引文件
     *   <li><b>分离变更</b>：区分表文件和 Changelog 文件
     * </ol>
     *
     * <h3>第二阶段：追加提交</h3>
     * <p>如果存在追加变更（appendTableFiles 或 appendChangelog 或 appendIndexFiles）：
     * <ol>
     *   <li><b>确定提交类型</b>：
     *       <ul>
     *         <li>默认为 APPEND 提交
     *         <li>如果包含文件删除或删除向量，且满足覆盖条件，升级为 OVERWRITE 提交
     *       </ul>
     *   <li><b>冲突检测</b>：
     *       <ul>
     *         <li>OVERWRITE 提交始终检查冲突
     *         <li>如果 appendCommitCheckConflict=true，APPEND 提交也检查冲突
     *       </ul>
     *   <li><b>执行提交</b>：调用 {@link #tryCommit} 方法，带重试机制
     *   <li><b>回滚支持</b>：OVERWRITE 提交支持回滚（allowRollback=true）
     * </ol>
     *
     * <h3>第三阶段：压缩提交</h3>
     * <p>如果存在压缩变更（compactTableFiles 或 compactChangelog 或 compactIndexFiles）：
     * <ol>
     *   <li><b>提交类型</b>：固定为 COMPACT 提交
     *   <li><b>冲突检测</b>：压缩提交始终检查冲突（checkAppendFiles=true）
     *   <li><b>执行提交</b>：调用 {@link #tryCommit} 方法
     *   <li><b>独立提交</b>：压缩提交独立于追加提交，生成独立的快照
     * </ol>
     *
     * <h3>第四阶段：指标上报</h3>
     * <ol>
     *   <li>记录提交耗时
     *   <li>统计生成的快照数量
     *   <li>统计重试次数
     *   <li>上报到监控系统
     * </ol>
     *
     * <h2>提交类型说明</h2>
     * <ul>
     *   <li><b>APPEND</b>：普通追加提交，不删除现有数据
     *   <li><b>OVERWRITE</b>：覆盖提交，会删除部分现有数据
     *   <li><b>COMPACT</b>：压缩提交，合并小文件
     * </ul>
     *
     * <h2>空提交处理</h2>
     * <ul>
     *   <li>如果 ignoreEmptyCommit=true 且没有任何变更，则不生成快照
     *   <li>如果 ignoreEmptyCommit=false，即使没有变更也会生成快照
     * </ul>
     *
     * <h2>并发控制</h2>
     * <ul>
     *   <li>使用乐观锁机制：通过快照ID版本号检测并发修改
     *   <li>冲突时自动重试，最多重试 commitMaxRetries 次
     *   <li>超时时间为 commitTimeout 毫秒
     * </ul>
     *
     * <h2>异常处理</h2>
     * <ul>
     *   <li>提交失败时会自动清理临时文件
     *   <li>重试期间会等待一定时间（指数退避）
     *   <li>超过最大重试次数或超时后抛出异常
     * </ul>
     *
     * @param committable 要提交的可提交对象，包含所有文件变更和元数据
     * @param checkAppendFiles 是否检查追加文件的冲突，true 表示进行冲突检测
     * @return 生成的快照数量，通常为 0、1 或 2（追加+压缩）
     * @throws RuntimeException 如果提交失败且超过重试次数或超时
     */
    @Override
    public int commit(ManifestCommittable committable, boolean checkAppendFiles) {
        LOG.info(
                "Ready to commit to table {}, number of commit messages: {}",
                tableName,
                committable.fileCommittables().size());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Ready to commit\n{}", committable);
        }

        long started = System.nanoTime();
        int generatedSnapshot = 0;
        int attempts = 0;

        // 第一阶段：收集所有变更
        ManifestEntryChanges changes = collectChanges(committable.fileCommittables());
        try {
            // 将表文件转换为简单文件条目
            List<SimpleFileEntry> appendSimpleEntries =
                    SimpleFileEntry.from(changes.appendTableFiles);
            // 第二阶段：处理追加提交
            if (!ignoreEmptyCommit
                    || !changes.appendTableFiles.isEmpty()
                    || !changes.appendChangelog.isEmpty()
                    || !changes.appendIndexFiles.isEmpty()) {
                CommitKind commitKind = CommitKind.APPEND;
                // 检查是否需要启用冲突检测
                if (appendCommitCheckConflict) {
                    checkAppendFiles = true;
                }

                boolean allowRollback = false;
                // 检查是否应该升级为覆盖提交
                if (conflictDetection.shouldBeOverwriteCommit(
                        appendSimpleEntries, changes.appendIndexFiles)) {
                    commitKind = CommitKind.OVERWRITE;
                    checkAppendFiles = true;
                    allowRollback = true;
                }

                // 执行追加提交（带重试）
                attempts +=
                        tryCommit(
                                CommitChangesProvider.provider(
                                        changes.appendTableFiles,
                                        changes.appendChangelog,
                                        changes.appendIndexFiles),
                                committable.identifier(),
                                committable.watermark(),
                                committable.properties(),
                                commitKind,
                                allowRollback,
                                checkAppendFiles,
                                null);
                generatedSnapshot += 1;
            }

            // 第三阶段：处理压缩提交
            if (!changes.compactTableFiles.isEmpty()
                    || !changes.compactChangelog.isEmpty()
                    || !changes.compactIndexFiles.isEmpty()) {
                // 执行压缩提交（带重试）
                attempts +=
                        tryCommit(
                                CommitChangesProvider.provider(
                                        changes.compactTableFiles,
                                        changes.compactChangelog,
                                        changes.compactIndexFiles),
                                committable.identifier(),
                                committable.watermark(),
                                committable.properties(),
                                CommitKind.COMPACT,
                                false,
                                true,
                                null);
                generatedSnapshot += 1;
            }
        } finally {
            // 第四阶段：上报指标
            long commitDuration = (System.nanoTime() - started) / 1_000_000;
            LOG.info(
                    "Finished (Uncertain of success) commit to table {}, duration {} ms",
                    tableName,
                    commitDuration);
            if (this.commitMetrics != null) {
                reportCommit(
                        changes.appendTableFiles,
                        changes.appendChangelog,
                        changes.compactTableFiles,
                        changes.compactChangelog,
                        commitDuration,
                        generatedSnapshot,
                        attempts);
            }
        }
        return generatedSnapshot;
    }

    /**
     * 上报提交统计指标
     *
     * <p>将提交过程中的各项统计信息上报到监控系统。
     *
     * @param appendTableFiles 追加的表文件列表
     * @param appendChangelogFiles 追加的 Changelog 文件列表
     * @param compactTableFiles 压缩的表文件列表
     * @param compactChangelogFiles 压缩的 Changelog 文件列表
     * @param commitDuration 提交耗时（毫秒）
     * @param generatedSnapshots 生成的快照数量
     * @param attempts 尝试次数（包含重试）
     */
    private void reportCommit(
            List<ManifestEntry> appendTableFiles,
            List<ManifestEntry> appendChangelogFiles,
            List<ManifestEntry> compactTableFiles,
            List<ManifestEntry> compactChangelogFiles,
            long commitDuration,
            int generatedSnapshots,
            int attempts) {
        CommitStats commitStats =
                new CommitStats(
                        appendTableFiles,
                        appendChangelogFiles,
                        compactTableFiles,
                        compactChangelogFiles,
                        commitDuration,
                        generatedSnapshots,
                        attempts);
        commitMetrics.reportCommit(commitStats);
    }

    /**
     * 检查文件变更列表中是否包含文件删除或删除向量
     *
     * <p>用于判断提交是否应该升级为覆盖提交类型。
     *
     * @param appendFileEntries 追加的文件条目列表
     * @param appendIndexFiles 追加的索引文件列表
     * @return true 表示包含删除操作或删除向量
     */
    private <T extends FileEntry> boolean containsFileDeletionOrDeletionVectors(
            List<T> appendFileEntries, List<IndexManifestEntry> appendIndexFiles) {
        // 检查文件条目中是否有删除操作
        for (T appendFileEntry : appendFileEntries) {
            if (appendFileEntry.kind().equals(FileKind.DELETE)) {
                return true;
            }
        }
        // 检查索引文件中是否有删除向量
        for (IndexManifestEntry appendIndexFile : appendIndexFiles) {
            if (appendIndexFile.indexFile().indexType().equals(DELETION_VECTORS_INDEX)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 覆盖指定分区的数据
     *
     * <p>该方法实现分区级别的覆盖写入，支持静态和动态分区覆盖两种模式。
     *
     * <h2>覆盖模式</h2>
     * <ul>
     *   <li><b>静态覆盖</b>：覆盖用户指定的分区（通过 partition 参数）
     *   <li><b>动态覆盖</b>：覆盖变更数据中涉及的所有分区（自动推断）
     * </ul>
     *
     * <h2>静态覆盖流程</h2>
     * <ol>
     *   <li>根据 partition 参数创建分区过滤器
     *   <li>验证所有变更都在指定分区内
     *   <li>删除分区内的所有现有数据
     *   <li>写入新的数据文件
     * </ol>
     *
     * <h2>动态覆盖流程</h2>
     * <ol>
     *   <li>从变更数据中提取涉及的分区列表
     *   <li>为这些分区创建过滤器
     *   <li>只删除涉及分区内的数据
     *   <li>写入新的数据文件
     *   <li>如果变更为空，跳过覆盖操作
     * </ol>
     *
     * <h2>Changelog 处理</h2>
     * <p>覆盖模式不支持 Changelog，如果包含 Changelog 会记录警告并忽略。
     * 这是因为覆盖操作会导致流式读取器的状态不一致。
     *
     * <h2>压缩处理</h2>
     * <p>如果存在压缩变更：
     * <ul>
     *   <li>先执行覆盖提交
     *   <li>再执行独立的压缩提交
     *   <li>可能生成两个快照
     * </ul>
     *
     * <h2>文件升级</h2>
     * <p>如果配置了 overwrite-upgrade，会尝试将 Level 0 文件直接升级到最高 Level，
     * 以减少后续的压缩开销。
     *
     * @param partition 要覆盖的分区键值对，null 表示覆盖整个表
     * @param committable 要提交的可提交对象
     * @param properties 提交属性
     * @return 生成的快照数量
     */
    @Override
    public int overwritePartition(
            Map<String, String> partition,
            ManifestCommittable committable,
            Map<String, String> properties) {
        LOG.info(
                "Ready to overwrite to table {}, number of commit messages: {}",
                tableName,
                committable.fileCommittables().size());
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Ready to overwrite partition {}\nManifestCommittable: {}\nProperties: {}",
                    partition,
                    committable,
                    properties);
        }

        long started = System.nanoTime();
        int generatedSnapshot = 0;
        int attempts = 0;

        ManifestEntryChanges changes = collectChanges(committable.fileCommittables());
        // 检查并警告：覆盖模式不支持 Changelog
        if (!changes.appendChangelog.isEmpty() || !changes.compactChangelog.isEmpty()) {
            StringBuilder warnMessage =
                    new StringBuilder(
                            "Overwrite mode currently does not commit any changelog.\n"
                                    + "Please make sure that the partition you're overwriting "
                                    + "is not being consumed by a streaming reader.\n"
                                    + "Ignored changelog files are:\n");
            for (ManifestEntry entry : changes.appendChangelog) {
                warnMessage.append("  * ").append(entry.toString()).append("\n");
            }
            for (ManifestEntry entry : changes.compactChangelog) {
                warnMessage.append("  * ").append(entry.toString()).append("\n");
            }
            LOG.warn(warnMessage.toString());
        }

        try {
            boolean skipOverwrite = false;
            // 根据静态或动态分区模式构建分区过滤器
            PartitionPredicate partitionFilter = null;
            if (dynamicPartitionOverwrite) {
                // 动态分区覆盖：从变更数据中提取分区
                if (changes.appendTableFiles.isEmpty()) {
                    // 动态模式下，如果没有变更则不删除任何数据
                    skipOverwrite = true;
                } else {
                    Set<BinaryRow> partitions =
                            changes.appendTableFiles.stream()
                                    .map(ManifestEntry::partition)
                                    .collect(Collectors.toSet());
                    partitionFilter = PartitionPredicate.fromMultiple(partitionType, partitions);
                }
            } else {
                // 静态分区覆盖：使用用户指定的分区
                // 分区可能是部分字段，所以这里必须使用谓词方式
                Predicate partitionPredicate =
                        createPartitionPredicate(partition, partitionType, partitionDefaultName);
                partitionFilter =
                        PartitionPredicate.fromPredicate(partitionType, partitionPredicate);
                // 安全性检查：所有变更必须在指定分区内
                if (partitionFilter != null) {
                    for (ManifestEntry entry : changes.appendTableFiles) {
                        if (!partitionFilter.test(entry.partition())) {
                            throw new IllegalArgumentException(
                                    "Trying to overwrite partition "
                                            + partition
                                            + ", but the changes in "
                                            + pathFactory.getPartitionString(entry.partition())
                                            + " does not belong to this partition");
                        }
                    }
                }
            }

            boolean withCompact =
                    !changes.compactTableFiles.isEmpty() || !changes.compactIndexFiles.isEmpty();

            // 如果没有压缩，尝试升级文件
            if (!withCompact) {
                changes.appendTableFiles = tryUpgrade(changes.appendTableFiles);
            }

            // 执行覆盖写入
            if (!skipOverwrite) {
                attempts +=
                        tryOverwritePartition(
                                partitionFilter,
                                changes.appendTableFiles,
                                changes.appendIndexFiles,
                                committable.identifier(),
                                committable.watermark(),
                                committable.properties());
                generatedSnapshot += 1;
            }

            // 如果有压缩变更，执行独立的压缩提交
            if (withCompact) {
                attempts +=
                        tryCommit(
                                CommitChangesProvider.provider(
                                        changes.compactTableFiles,
                                        emptyList(),
                                        changes.compactIndexFiles),
                                committable.identifier(),
                                committable.watermark(),
                                committable.properties(),
                                CommitKind.COMPACT,
                                false,
                                true,
                                null);
                generatedSnapshot += 1;
            }
        } finally {
            long commitDuration = (System.nanoTime() - started) / 1_000_000;
            LOG.info("Finished overwrite to table {}, duration {} ms", tableName, commitDuration);
            if (this.commitMetrics != null) {
                reportCommit(
                        changes.appendTableFiles,
                        emptyList(),
                        changes.compactTableFiles,
                        emptyList(),
                        commitDuration,
                        generatedSnapshot,
                        attempts);
            }
        }
        return generatedSnapshot;
    }

    /**
     * 尝试升级文件到更高层级
     *
     * <p>该方法用于优化覆盖提交：如果所有文件都在 Level 0 且键范围不重叠，
     * 可以直接将它们升级到最高层级，避免后续的压缩开销。
     *
     * <h3>升级条件</h3>
     * <ol>
     *   <li>配置了 overwrite-upgrade 选项
     *   <li>存在键比较器（仅 Primary Key 表支持）
     *   <li>所有文件都在 Level 0
     *   <li>所有文件都是普通桶（bucket >= 0）
     *   <li>同一桶内的文件键范围不重叠
     * </ol>
     *
     * <h3>升级策略</h3>
     * <ul>
     *   <li>按桶分组文件
     *   <li>对每个桶内的文件按最小键排序
     *   <li>检查相邻文件的键范围是否重叠
     *   <li>如果不重叠，升级到最高层级（numLevels - 1）
     *   <li>如果有重叠，保持原样
     * </ul>
     *
     * @param appendFiles 要升级的文件列表
     * @return 升级后的文件列表，如果不满足升级条件则返回原列表
     */
    private List<ManifestEntry> tryUpgrade(List<ManifestEntry> appendFiles) {
        // 检查是否配置了升级选项
        if (!options.overwriteUpgrade()) {
            return appendFiles;
        }
        // 检查是否存在键比较器（仅 Primary Key 表支持）
        Comparator<InternalRow> keyComparator = conflictDetection.keyComparator();
        if (keyComparator == null) {
            return appendFiles;
        }
        // 检查所有文件是否都在 Level 0 且是普通桶
        for (ManifestEntry entry : appendFiles) {
            if (entry.level() > 0 || entry.bucket() < 0) {
                return appendFiles;
            }
        }

        // 按桶分组文件
        Map<Pair<BinaryRow, Integer>, List<ManifestEntry>> buckets = new HashMap<>();
        for (ManifestEntry entry : appendFiles) {
            buckets.computeIfAbsent(
                            Pair.of(entry.partition(), entry.bucket()), k -> new ArrayList<>())
                    .add(entry);
        }

        // 对每个桶的文件进行检查和升级
        List<ManifestEntry> results = new ArrayList<>();
        int maxLevel = options.numLevels() - 1;
        outer:
        for (List<ManifestEntry> entries : buckets.values()) {
            // 按最小键排序
            List<ManifestEntry> newEntries = new ArrayList<>(entries);
            newEntries.sort((a, b) -> keyComparator.compare(a.minKey(), b.minKey()));
            // 检查相邻文件的键范围是否重叠
            for (int i = 0; i + 1 < newEntries.size(); i++) {
                ManifestEntry a = newEntries.get(i);
                ManifestEntry b = newEntries.get(i + 1);
                if (keyComparator.compare(a.maxKey(), b.minKey()) >= 0) {
                    // 有重叠，保持原样
                    results.addAll(entries);
                    continue outer;
                }
            }
            // 没有重叠，升级到最高层级
            LOG.info("Upgraded for overwrite commit.");
            for (ManifestEntry entry : newEntries) {
                results.add(entry.upgrade(maxLevel));
            }
        }

        return results;
    }

    /**
     * 删除指定的分区
     *
     * <p>该方法支持批量删除多个分区，可以是完整分区或部分分区字段。
     *
     * @param partitions 要删除的分区列表，每个分区用 Map 表示键值对
     * @param commitIdentifier 提交标识符
     * @throws IllegalArgumentException 如果分区列表为空
     */
    @Override
    public void dropPartitions(List<Map<String, String>> partitions, long commitIdentifier) {
        checkArgument(!partitions.isEmpty(), "Partitions list cannot be empty.");

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Ready to drop partitions {}",
                    partitions.stream().map(Objects::toString).collect(Collectors.joining(",")));
        }

        // 判断是否所有分区都是完整的（包含所有分区字段）
        boolean fullMode =
                partitions.stream().allMatch(part -> part.size() == partitionType.getFieldCount());
        PartitionPredicate partitionFilter;
        if (fullMode) {
            // 完整分区模式：直接构建二进制分区列表
            List<BinaryRow> binaryPartitions =
                    createBinaryPartitions(partitions, partitionType, partitionDefaultName);
            partitionFilter = PartitionPredicate.fromMultiple(partitionType, binaryPartitions);
        } else {
            // 部分分区模式：使用谓词方式
            Predicate predicate =
                    partitions.stream()
                            .map(
                                    partition ->
                                            createPartitionPredicate(
                                                    partition, partitionType, partitionDefaultName))
                            .reduce(PredicateBuilder::or)
                            .orElseThrow(
                                    () -> new RuntimeException("Failed to get partition filter."));
            partitionFilter = PartitionPredicate.fromPredicate(partitionType, predicate);
        }

        // 执行覆盖提交，传入空的变更列表以删除分区
        tryOverwritePartition(
                partitionFilter, emptyList(), emptyList(), commitIdentifier, null, new HashMap<>());
    }

    /**
     * 清空表的所有数据
     *
     * <p>该方法会删除表中的所有分区数据。
     *
     * @param commitIdentifier 提交标识符
     */
    @Override
    public void truncateTable(long commitIdentifier) {
        // 传入 null 作为分区过滤器表示覆盖整个表
        tryOverwritePartition(
                null, emptyList(), emptyList(), commitIdentifier, null, new HashMap<>());
    }

    /**
     * 中止提交并清理临时文件
     *
     * <p>当提交失败或被取消时，该方法会清理所有未提交的临时文件。
     *
     * @param commitMessages 要中止的提交消息列表
     */
    @Override
    public void abort(List<CommitMessage> commitMessages) {
        DataFilePathFactories factories = new DataFilePathFactories(pathFactory);
        for (CommitMessage message : commitMessages) {
            DataFilePathFactory pathFactory = factories.get(message.partition(), message.bucket());
            CommitMessageImpl commitMessage = (CommitMessageImpl) message;
            // 收集所有需要删除的临时文件
            List<DataFileMeta> toDelete = new ArrayList<>();
            toDelete.addAll(commitMessage.newFilesIncrement().newFiles());
            toDelete.addAll(commitMessage.newFilesIncrement().changelogFiles());
            toDelete.addAll(commitMessage.compactIncrement().compactAfter());
            toDelete.addAll(commitMessage.compactIncrement().changelogFiles());

            // 静默删除文件（忽略异常）
            for (DataFileMeta file : toDelete) {
                fileIO.deleteQuietly(pathFactory.toPath(file));
            }
        }
    }

    /**
     * 设置提交指标收集器
     *
     * @param metrics 提交指标收集器
     * @return 当前提交对象，支持链式调用
     */
    @Override
    public FileStoreCommit withMetrics(CommitMetrics metrics) {
        this.commitMetrics = metrics;
        return this;
    }

    /**
     * 提交统计信息
     *
     * <p>将表的统计信息作为独立的快照提交。
     *
     * @param stats 统计信息对象
     * @param commitIdentifier 提交标识符
     */
    @Override
    public void commitStatistics(Statistics stats, long commitIdentifier) {
        // 写入统计信息文件
        String statsFileName = statsFileHandler.writeStats(stats);
        // 提交一个只包含统计信息的快照
        tryCommit(
                CommitChangesProvider.provider(emptyList(), emptyList(), emptyList()),
                commitIdentifier,
                null,
                Collections.emptyMap(),
                CommitKind.ANALYZE,
                false,
                false,
                statsFileName);
    }

    /**
     * 获取文件存储路径工厂
     *
     * @return 路径工厂对象
     */
    @Override
    public FileStorePathFactory pathFactory() {
        return pathFactory;
    }

    /**
     * 获取文件I/O对象
     *
     * @return 文件I/O对象
     */
    @Override
    public FileIO fileIO() {
        return fileIO;
    }

    /**
     * 收集提交消息中的所有变更
     *
     * <p>该方法将所有提交消息中的文件变更收集并分类。
     *
     * @param commitMessages 提交消息列表
     * @return 变更集合对象
     */
    private ManifestEntryChanges collectChanges(List<CommitMessage> commitMessages) {
        ManifestEntryChanges changes = new ManifestEntryChanges(numBucket);
        commitMessages.forEach(changes::collect);
        LOG.info("Finished collecting changes, including: {}", changes);
        return changes;
    }

    /**
     * 尝试提交变更（带重试机制）
     *
     * <p>该方法是提交的核心实现，包含完整的重试逻辑和冲突检测。
     *
     * <h3>重试策略</h3>
     * <ul>
     *   <li>最大重试次数：commitMaxRetries
     *   <li>超时时间：commitTimeout 毫秒
     *   <li>重试等待：指数退避策略
     *   <li>失败条件：超时或达到最大重试次数
     * </ul>
     *
     * <h3>提交流程</h3>
     * <ol>
     *   <li>获取最新快照（每次重试都重新获取）
     *   <li>根据最新快照生成变更集
     *   <li>调用 tryCommitOnce 执行单次提交
     *   <li>如果成功则退出，否则重试
     *   <li>如果失败，等待一段时间后重试
     * </ol>
     *
     * @param changesProvider 变更提供者，用于根据最新快照生成变更
     * @param identifier 提交标识符
     * @param watermark 水位线（可选）
     * @param properties 提交属性
     * @param commitKind 提交类型
     * @param allowRollback 是否允许回滚
     * @param detectConflicts 是否检测冲突
     * @param statsFileName 统计文件名（可选）
     * @return 总尝试次数（包含重试）
     * @throws RuntimeException 如果超时或超过最大重试次数
     */
    private int tryCommit(
            CommitChangesProvider changesProvider,
            long identifier,
            @Nullable Long watermark,
            Map<String, String> properties,
            CommitKind commitKind,
            boolean allowRollback,
            boolean detectConflicts,
            @Nullable String statsFileName) {
        int retryCount = 0;
        RetryCommitResult retryResult = null;
        long startMillis = System.currentTimeMillis();
        while (true) {
            // 获取最新快照
            Snapshot latestSnapshot = snapshotManager.latestSnapshot();
            // 根据最新快照生成变更
            CommitChanges changes = changesProvider.provide(latestSnapshot);
            // 执行单次提交
            CommitResult result =
                    tryCommitOnce(
                            retryResult,
                            changes.tableFiles,
                            changes.changelogFiles,
                            changes.indexFiles,
                            identifier,
                            watermark,
                            properties,
                            commitKind,
                            allowRollback,
                            latestSnapshot,
                            detectConflicts,
                            statsFileName);

            // 检查是否成功
            if (result.isSuccess()) {
                break;
            }

            retryResult = (RetryCommitResult) result;

            // 检查是否超时或超过最大重试次数
            if (System.currentTimeMillis() - startMillis > commitTimeout
                    || retryCount >= commitMaxRetries) {
                String message =
                        String.format(
                                "Commit failed after %s millis with %s retries, there maybe exist commit conflicts between multiple jobs.",
                                commitTimeout, retryCount);
                throw new RuntimeException(message, retryResult.exception);
            }

            // 等待后重试
            retryWaiter.retryWait(retryCount);
            retryCount++;
        }
        return retryCount + 1;
    }

    /**
     * 尝试覆盖分区
     *
     * <p>该方法通过读取要删除的数据和追加新数据来实现分区覆盖。
     *
     * @param partitionFilter 分区过滤器，null 表示覆盖整个表
     * @param changes 要追加的变更
     * @param indexFiles 索引文件
     * @param identifier 提交标识符
     * @param watermark 水位线（可选）
     * @param properties 提交属性
     * @return 总尝试次数（包含重试）
     */
    private int tryOverwritePartition(
            @Nullable PartitionPredicate partitionFilter,
            List<ManifestEntry> changes,
            List<IndexManifestEntry> indexFiles,
            long identifier,
            @Nullable Long watermark,
            Map<String, String> properties) {
        return tryCommit(
                latestSnapshot ->
                        scanner.readOverwriteChanges(
                                numBucket, changes, indexFiles, latestSnapshot, partitionFilter),
                identifier,
                watermark,
                properties,
                CommitKind.OVERWRITE,
                false,
                true,
                null);
    }

    @VisibleForTesting
    CommitResult tryCommitOnce(
            @Nullable RetryCommitResult retryResult,
            List<ManifestEntry> deltaFiles,
            List<ManifestEntry> changelogFiles,
            List<IndexManifestEntry> indexFiles,
            long identifier,
            @Nullable Long watermark,
            Map<String, String> properties,
            CommitKind commitKind,
            boolean allowRollback,
            @Nullable Snapshot latestSnapshot,
            boolean detectConflicts,
            @Nullable String newStatsFileName) {
        long startMillis = System.currentTimeMillis();

        // Check if the commit has been completed. At this point, there will be no more repeated
        // commits and just return success
        if (retryResult instanceof CommitFailRetryResult && latestSnapshot != null) {
            CommitFailRetryResult commitFailRetry = (CommitFailRetryResult) retryResult;
            Map<Long, Snapshot> snapshotCache = new HashMap<>();
            snapshotCache.put(latestSnapshot.id(), latestSnapshot);
            long startCheckSnapshot = Snapshot.FIRST_SNAPSHOT_ID;
            if (commitFailRetry.latestSnapshot != null) {
                snapshotCache.put(
                        commitFailRetry.latestSnapshot.id(), commitFailRetry.latestSnapshot);
                startCheckSnapshot = commitFailRetry.latestSnapshot.id() + 1;
            }
            for (long i = startCheckSnapshot; i <= latestSnapshot.id(); i++) {
                Snapshot snapshot = snapshotCache.computeIfAbsent(i, snapshotManager::snapshot);
                if (snapshot.commitUser().equals(commitUser)
                        && snapshot.commitIdentifier() == identifier
                        && snapshot.commitKind() == commitKind) {
                    return new SuccessCommitResult();
                }
            }
        }

        long newSnapshotId = Snapshot.FIRST_SNAPSHOT_ID;
        long firstRowIdStart = 0;
        if (latestSnapshot != null) {
            newSnapshotId = latestSnapshot.id() + 1;
            Long nextRowId = latestSnapshot.nextRowId();
            if (nextRowId != null) {
                firstRowIdStart = nextRowId;
            }
        }

        if (strictModeChecker != null) {
            strictModeChecker.check(newSnapshotId, commitKind);
            strictModeChecker.update(newSnapshotId - 1);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Ready to commit table files to snapshot {}", newSnapshotId);
            for (ManifestEntry entry : deltaFiles) {
                LOG.debug("  * {}", entry);
            }
            LOG.debug("Ready to commit changelog to snapshot {}", newSnapshotId);
            for (ManifestEntry entry : changelogFiles) {
                LOG.debug("  * {}", entry);
            }
        }

        List<SimpleFileEntry> baseDataFiles = new ArrayList<>();
        boolean discardDuplicate = discardDuplicateFiles && commitKind == CommitKind.APPEND;
        if (latestSnapshot != null && (discardDuplicate || detectConflicts)) {
            // latestSnapshotId is different from the snapshot id we've checked for conflicts,
            // so we have to check again
            List<BinaryRow> changedPartitions = changedPartitions(deltaFiles, indexFiles);
            CommitFailRetryResult commitFailRetry =
                    retryResult instanceof CommitFailRetryResult
                            ? (CommitFailRetryResult) retryResult
                            : null;
            if (commitFailRetry != null
                    && commitFailRetry.latestSnapshot != null
                    && commitFailRetry.baseDataFiles != null) {
                baseDataFiles = new ArrayList<>(commitFailRetry.baseDataFiles);
                List<SimpleFileEntry> incremental =
                        scanner.readIncrementalChanges(
                                commitFailRetry.latestSnapshot, latestSnapshot, changedPartitions);
                if (!incremental.isEmpty()) {
                    baseDataFiles.addAll(incremental);
                    baseDataFiles = new ArrayList<>(FileEntry.mergeEntries(baseDataFiles));
                }
            } else {
                baseDataFiles =
                        scanner.readAllEntriesFromChangedPartitions(
                                latestSnapshot, changedPartitions);
            }
            if (discardDuplicate) {
                Set<FileEntry.Identifier> baseIdentifiers =
                        baseDataFiles.stream()
                                .map(FileEntry::identifier)
                                .collect(Collectors.toSet());
                deltaFiles =
                        deltaFiles.stream()
                                .filter(entry -> !baseIdentifiers.contains(entry.identifier()))
                                .collect(Collectors.toList());
            }
            Optional<RuntimeException> exception =
                    conflictDetection.checkConflicts(
                            latestSnapshot,
                            baseDataFiles,
                            SimpleFileEntry.from(deltaFiles),
                            indexFiles,
                            commitKind);
            if (exception.isPresent()) {
                if (allowRollback && rollback != null) {
                    if (rollback.tryToRollback(latestSnapshot)) {
                        return RetryCommitResult.forRollback(exception.get());
                    }
                }
                throw exception.get();
            }
        }

        Snapshot newSnapshot;
        Pair<String, Long> baseManifestList = null;
        Pair<String, Long> deltaManifestList = null;
        List<PartitionEntry> deltaStatistics;
        Pair<String, Long> changelogManifestList = null;
        String oldIndexManifest = null;
        String indexManifest = null;
        List<ManifestFileMeta> mergeBeforeManifests = new ArrayList<>();
        List<ManifestFileMeta> mergeAfterManifests = new ArrayList<>();
        long nextRowIdStart = firstRowIdStart;
        try {
            long previousTotalRecordCount = 0L;
            Long currentWatermark = watermark;
            if (latestSnapshot != null) {
                previousTotalRecordCount = latestSnapshot.totalRecordCount();
                // read all previous manifest files
                mergeBeforeManifests = manifestList.readDataManifests(latestSnapshot);
                Long latestWatermark = latestSnapshot.watermark();
                if (latestWatermark != null) {
                    currentWatermark =
                            currentWatermark == null
                                    ? latestWatermark
                                    : Math.max(currentWatermark, latestWatermark);
                }
                oldIndexManifest = latestSnapshot.indexManifest();
            }

            // try to merge old manifest files to create base manifest list
            mergeAfterManifests =
                    ManifestFileMerger.merge(
                            mergeBeforeManifests,
                            manifestFile,
                            manifestTargetSize.getBytes(),
                            manifestMergeMinCount,
                            manifestFullCompactionSize.getBytes(),
                            partitionType,
                            manifestReadParallelism);
            baseManifestList = manifestList.write(mergeAfterManifests);

            if (rowTrackingEnabled) {
                RowTrackingAssigned assigned =
                        assignRowTracking(newSnapshotId, firstRowIdStart, deltaFiles);
                nextRowIdStart = assigned.nextRowIdStart;
                deltaFiles = assigned.assignedEntries;
            }

            // the added records subtract the deleted records from
            long deltaRecordCount = recordCountAdd(deltaFiles) - recordCountDelete(deltaFiles);
            long totalRecordCount = previousTotalRecordCount + deltaRecordCount;

            // write new delta files into manifest files
            deltaStatistics = new ArrayList<>(PartitionEntry.merge(deltaFiles));
            deltaManifestList = manifestList.write(manifestFile.write(deltaFiles));

            // write changelog into manifest files
            if (!changelogFiles.isEmpty()) {
                changelogManifestList = manifestList.write(manifestFile.write(changelogFiles));
            }

            indexManifest =
                    indexManifestFile.writeIndexFiles(oldIndexManifest, indexFiles, bucketMode);

            long latestSchemaId =
                    schemaManager
                            .latestOrThrow("Cannot get latest schema for table " + tableName)
                            .id();

            // write new stats or inherit from the previous snapshot
            String statsFileName = null;
            if (newStatsFileName != null) {
                statsFileName = newStatsFileName;
            } else if (latestSnapshot != null) {
                Optional<Statistics> previousStatistic = statsFileHandler.readStats(latestSnapshot);
                if (previousStatistic.isPresent()) {
                    if (previousStatistic.get().schemaId() != latestSchemaId) {
                        LOG.warn("Schema changed, stats will not be inherited");
                    } else {
                        statsFileName = latestSnapshot.statistics();
                    }
                }
            }

            // prepare snapshot file
            newSnapshot =
                    new Snapshot(
                            newSnapshotId,
                            latestSchemaId,
                            baseManifestList.getLeft(),
                            baseManifestList.getRight(),
                            deltaManifestList.getKey(),
                            deltaManifestList.getRight(),
                            changelogManifestList == null ? null : changelogManifestList.getKey(),
                            changelogManifestList == null ? null : changelogManifestList.getRight(),
                            indexManifest,
                            commitUser,
                            identifier,
                            commitKind,
                            System.currentTimeMillis(),
                            totalRecordCount,
                            deltaRecordCount,
                            nullableRecordCount(changelogFiles),
                            currentWatermark,
                            statsFileName,
                            // if empty properties, just set to null
                            properties.isEmpty() ? null : properties,
                            nextRowIdStart);
        } catch (Throwable e) {
            // fails when preparing for commit, we should clean up
            commitCleaner.cleanUpReuseTmpManifests(
                    deltaManifestList, changelogManifestList, oldIndexManifest, indexManifest);
            commitCleaner.cleanUpNoReuseTmpManifests(
                    baseManifestList, mergeBeforeManifests, mergeAfterManifests);
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when preparing snapshot #%d by user %s "
                                    + "with hash %s and kind %s. Clean up.",
                            newSnapshotId, commitUser, identifier, commitKind.name()),
                    e);
        }

        boolean success;
        try {
            success = commitSnapshotImpl(newSnapshot, deltaStatistics);
        } catch (Exception e) {
            // commit exception, not sure about the situation and should not clean up the files
            LOG.warn("Retry commit for exception.", e);
            return RetryCommitResult.forCommitFail(latestSnapshot, baseDataFiles, e);
        }

        if (!success) {
            // commit fails, should clean up the files
            long commitTime = (System.currentTimeMillis() - startMillis) / 1000;
            LOG.warn(
                    "Atomic commit failed for snapshot #{} by user {} "
                            + "with identifier {} and kind {} after {} seconds. "
                            + "Clean up and try again.",
                    newSnapshotId,
                    commitUser,
                    identifier,
                    commitKind.name(),
                    commitTime);
            commitCleaner.cleanUpNoReuseTmpManifests(
                    baseManifestList, mergeBeforeManifests, mergeAfterManifests);
            return RetryCommitResult.forCommitFail(latestSnapshot, baseDataFiles, null);
        }

        LOG.info(
                "Successfully commit snapshot {} to table {} by user {} "
                        + "with identifier {} and kind {}.",
                newSnapshotId,
                tableName,
                commitUser,
                identifier,
                commitKind.name());
        if (strictModeChecker != null) {
            strictModeChecker.update(newSnapshotId);
        }
        final List<SimpleFileEntry> finalBaseFiles = baseDataFiles;
        final List<ManifestEntry> finalDeltaFiles = deltaFiles;
        commitCallbacks.forEach(
                callback ->
                        callback.call(finalBaseFiles, finalDeltaFiles, indexFiles, newSnapshot));
        return new SuccessCommitResult();
    }

    public boolean replaceManifestList(
            Snapshot latest,
            long totalRecordCount,
            Pair<String, Long> baseManifestList,
            Pair<String, Long> deltaManifestList) {
        Snapshot newSnapshot =
                new Snapshot(
                        latest.id() + 1,
                        latest.schemaId(),
                        baseManifestList.getLeft(),
                        baseManifestList.getRight(),
                        deltaManifestList.getKey(),
                        deltaManifestList.getRight(),
                        null,
                        null,
                        latest.indexManifest(),
                        commitUser,
                        Long.MAX_VALUE,
                        CommitKind.OVERWRITE,
                        System.currentTimeMillis(),
                        totalRecordCount,
                        0L,
                        null,
                        latest.watermark(),
                        latest.statistics(),
                        // if empty properties, just set to null
                        latest.properties(),
                        latest.nextRowId());

        return commitSnapshotImpl(newSnapshot, emptyList());
    }

    public void compactManifest() {
        int retryCount = 0;
        long startMillis = System.currentTimeMillis();
        while (true) {
            boolean success = compactManifestOnce();
            if (success) {
                break;
            }

            if (System.currentTimeMillis() - startMillis > commitTimeout
                    || retryCount >= commitMaxRetries) {
                throw new RuntimeException(
                        String.format(
                                "Commit failed after %s millis with %s retries, there maybe exist commit conflicts between multiple jobs.",
                                commitTimeout, retryCount));
            }

            retryWaiter.retryWait(retryCount);
            retryCount++;
        }
    }

    private boolean compactManifestOnce() {
        Snapshot latestSnapshot = snapshotManager.latestSnapshot();

        if (latestSnapshot == null) {
            return true;
        }

        List<ManifestFileMeta> mergeBeforeManifests =
                manifestList.readDataManifests(latestSnapshot);
        List<ManifestFileMeta> mergeAfterManifests;

        // the fist trial
        mergeAfterManifests =
                ManifestFileMerger.merge(
                        mergeBeforeManifests,
                        manifestFile,
                        manifestTargetSize.getBytes(),
                        1,
                        1,
                        partitionType,
                        manifestReadParallelism);

        if (new HashSet<>(mergeBeforeManifests).equals(new HashSet<>(mergeAfterManifests))) {
            // no need to commit this snapshot, because no compact were happened
            return true;
        }

        Pair<String, Long> baseManifestList = manifestList.write(mergeAfterManifests);
        Pair<String, Long> deltaManifestList = manifestList.write(emptyList());

        // prepare snapshot file
        Snapshot newSnapshot =
                new Snapshot(
                        latestSnapshot.id() + 1,
                        latestSnapshot.schemaId(),
                        baseManifestList.getLeft(),
                        baseManifestList.getRight(),
                        deltaManifestList.getLeft(),
                        deltaManifestList.getRight(),
                        null,
                        null,
                        latestSnapshot.indexManifest(),
                        commitUser,
                        Long.MAX_VALUE,
                        CommitKind.COMPACT,
                        System.currentTimeMillis(),
                        latestSnapshot.totalRecordCount(),
                        0L,
                        null,
                        latestSnapshot.watermark(),
                        latestSnapshot.statistics(),
                        latestSnapshot.properties(),
                        latestSnapshot.nextRowId());

        return commitSnapshotImpl(newSnapshot, emptyList());
    }

    private boolean commitSnapshotImpl(Snapshot newSnapshot, List<PartitionEntry> deltaStatistics) {
        try {
            List<PartitionStatistics> statistics = new ArrayList<>(deltaStatistics.size());
            for (PartitionEntry entry : deltaStatistics) {
                statistics.add(entry.toPartitionStatistics(partitionComputer));
            }
            return snapshotCommit.commit(newSnapshot, branchName, statistics);
        } catch (Throwable e) {
            // exception when performing the atomic rename,
            // we cannot clean up because we can't determine the success
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when committing snapshot #%d by user %s "
                                    + "with identifier %s and kind %s. "
                                    + "Cannot clean up because we can't determine the success.",
                            newSnapshot.id(),
                            newSnapshot.commitUser(),
                            newSnapshot.commitIdentifier(),
                            newSnapshot.commitKind().name()),
                    e);
        }
    }

    @Override
    public void close() {
        IOUtils.closeAllQuietly(commitCallbacks);
        IOUtils.closeQuietly(snapshotCommit);
    }
}
