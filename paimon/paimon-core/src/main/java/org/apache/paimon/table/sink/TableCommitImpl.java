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

package org.apache.paimon.table.sink;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.operation.PartitionExpire;
import org.apache.paimon.operation.metrics.CommitMetrics;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.tag.TagAutoCreation;
import org.apache.paimon.tag.TagAutoManager;
import org.apache.paimon.tag.TagTimeExpire;
import org.apache.paimon.utils.CompactedChangelogPathResolver;
import org.apache.paimon.utils.DataFilePathFactories;
import org.apache.paimon.utils.ExecutorThreadFactory;
import org.apache.paimon.utils.FileOperationThreadPool;
import org.apache.paimon.utils.IndexFilePathFactories;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.paimon.CoreOptions.ExpireExecutionMode;
import static org.apache.paimon.table.sink.BatchWriteBuilder.COMMIT_IDENTIFIER;
import static org.apache.paimon.utils.Preconditions.checkState;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyExecuteSequentialReturn;

/**
 * {@link FileStoreCommit} 之上的抽象层，提供快照提交和过期功能。
 *
 * <p>这是 Table 层提交的核心实现，封装了底层 {@link FileStoreCommit} 并提供了额外的维护功能。
 *
 * <p>主要职责：
 * <ul>
 *     <li><b>快照提交</b>：将 {@link CommitMessage} 转换为 {@link ManifestCommittable} 并提交
 *     <li><b>快照过期</b>：定期清理过期的快照
 *     <li><b>分区过期</b>：根据分区策略删除过期的分区
 *     <li><b>标签管理</b>：自动创建和过期标签
 *     <li><b>消费者过期</b>：清理过期的消费者信息
 *     <li><b>覆写提交</b>：支持 INSERT OVERWRITE 语义
 *     <li><b>文件检查</b>：在恢复时检查文件是否存在
 * </ul>
 *
 * <p>与底层 FileStoreCommit 的关系：
 * <pre>
 * TableCommitImpl (Table 层实现)
 *   ├─ FileStoreCommit (底层提交)
 *   │   ├─ 提交 Manifest
 *   │   ├─ 更新快照
 *   │   └─ 冲突检测
 *   ├─ SnapshotExpire (快照过期)
 *   ├─ PartitionExpire (分区过期)
 *   ├─ TagAutoManager (标签管理)
 *   │   ├─ TagAutoCreation (自动创建标签)
 *   │   └─ TagTimeExpire (标签过期)
 *   └─ ConsumerManager (消费者管理)
 * </pre>
 *
 * <p>维护任务的执行模式：
 * <ul>
 *     <li><b>SYNC</b>: 同步执行，阻塞提交直到维护完成
 *     <li><b>ASYNC</b>: 异步执行，提交后在后台线程执行维护
 * </ul>
 *
 * <p>提交流程：
 * <ol>
 *     <li>收集所有 CommitMessage
 *     <li>转换为 ManifestCommittable
 *     <li>检查文件是否存在（恢复场景）
 *     <li>调用 FileStoreCommit 提交
 *     <li>执行维护任务（快照过期、分区过期等）
 * </ol>
 */
public class TableCommitImpl implements InnerTableCommit {

    private static final Logger LOG = LoggerFactory.getLogger(TableCommitImpl.class);

    /** 底层文件存储提交器 */
    private final FileStoreCommit commit;

    /** 快照过期任务（可选） */
    @Nullable private final Runnable expireSnapshots;

    /** 分区过期管理器（可选） */
    @Nullable private final PartitionExpire partitionExpire;

    /** 标签自动管理器，包含自动创建和过期功能（可选） */
    @Nullable private final TagAutoManager tagAutoManager;

    /** 消费者过期时间（可选） */
    @Nullable private final Duration consumerExpireTime;

    /** 消费者管理器 */
    private final ConsumerManager consumerManager;

    /** 维护任务执行器（同步或异步） */
    private final ExecutorService maintainExecutor;

    /** 维护任务错误引用，用于在下次提交时抛出异步维护的错误 */
    private final AtomicReference<Throwable> maintainError;

    /** 表名，用于日志和指标 */
    private final String tableName;

    /** 是否强制创建快照，即使没有数据变更 */
    private final boolean forceCreatingSnapshot;

    /** 文件检查执行器，用于并行检查文件是否存在 */
    private final ThreadPoolExecutor fileCheckExecutor;

    /** 覆写分区配置，用于 INSERT OVERWRITE 场景 */
    @Nullable private Map<String, String> overwritePartition = null;

    /** 批量提交标志，确保批量提交只执行一次 */
    private boolean batchCommitted = false;

    /** 空提交是否执行过期任务 */
    private boolean expireForEmptyCommit = true;

    /**
     * 构造函数。
     *
     * @param commit 底层文件存储提交器
     * @param expireSnapshots 快照过期任务（可选）
     * @param partitionExpire 分区过期管理器（可选）
     * @param tagAutoManager 标签自动管理器（可选）
     * @param consumerExpireTime 消费者过期时间（可选）
     * @param consumerManager 消费者管理器
     * @param expireExecutionMode 过期执行模式（同步或异步）
     * @param tableName 表名
     * @param forceCreatingSnapshot 是否强制创建快照
     * @param threadNum 文件检查线程数
     */
    public TableCommitImpl(
            FileStoreCommit commit,
            @Nullable Runnable expireSnapshots,
            @Nullable PartitionExpire partitionExpire,
            @Nullable TagAutoManager tagAutoManager,
            @Nullable Duration consumerExpireTime,
            ConsumerManager consumerManager,
            ExpireExecutionMode expireExecutionMode,
            String tableName,
            boolean forceCreatingSnapshot,
            int threadNum) {
        // 将分区过期管理器注入到底层提交器
        if (partitionExpire != null) {
            commit.withPartitionExpire(partitionExpire);
        }

        this.commit = commit;
        this.expireSnapshots = expireSnapshots;
        this.partitionExpire = partitionExpire;
        this.tagAutoManager = tagAutoManager;

        this.consumerExpireTime = consumerExpireTime;
        this.consumerManager = consumerManager;

        // 根据执行模式创建维护任务执行器
        this.maintainExecutor =
                expireExecutionMode == ExpireExecutionMode.SYNC
                        ? MoreExecutors.newDirectExecutorService()  // 同步执行
                        : Executors.newSingleThreadExecutor(  // 异步执行
                                new ExecutorThreadFactory(
                                        Thread.currentThread().getName() + "expire-main-thread"));
        this.maintainError = new AtomicReference<>(null);

        this.tableName = tableName;
        this.forceCreatingSnapshot = forceCreatingSnapshot;
        this.fileCheckExecutor = FileOperationThreadPool.getExecutorService(threadNum);
    }

    /**
     * 判断是否需要强制创建快照。
     *
     * <p>以下情况需要强制创建快照：
     * <ul>
     *     <li>配置了 forceCreatingSnapshot
     *     <li>使用了 INSERT OVERWRITE（overwritePartition != null）
     *     <li>标签自动创建配置了强制创建快照
     * </ul>
     *
     * @return true 如果需要强制创建快照
     */
    public boolean forceCreatingSnapshot() {
        if (this.forceCreatingSnapshot) {
            return true;
        }
        if (overwritePartition != null) {
            return true;
        }
        return tagAutoManager != null
                && tagAutoManager.getTagAutoCreation() != null
                && tagAutoManager.getTagAutoCreation().forceCreatingSnapshot();
    }

    @Override
    public TableCommitImpl withOverwrite(@Nullable Map<String, String> overwritePartitions) {
        this.overwritePartition = overwritePartitions;
        return this;
    }

    @Override
    public TableCommitImpl ignoreEmptyCommit(boolean ignoreEmptyCommit) {
        commit.ignoreEmptyCommit(ignoreEmptyCommit);
        return this;
    }

    @Override
    public TableCommitImpl expireForEmptyCommit(boolean expireForEmptyCommit) {
        this.expireForEmptyCommit = expireForEmptyCommit;
        return this;
    }

    @Override
    public TableCommitImpl appendCommitCheckConflict(boolean appendCommitCheckConflict) {
        commit.appendCommitCheckConflict(appendCommitCheckConflict);
        return this;
    }

    @Override
    public TableCommitImpl rowIdCheckConflict(@Nullable Long rowIdCheckFromSnapshot) {
        commit.rowIdCheckConflict(rowIdCheckFromSnapshot);
        return this;
    }

    @Override
    public InnerTableCommit withMetricRegistry(MetricRegistry registry) {
        commit.withMetrics(new CommitMetrics(registry, tableName));
        return this;
    }

    @Override
    public void commit(List<CommitMessage> commitMessages) {
        checkCommitted();
        commit(COMMIT_IDENTIFIER, commitMessages);
    }

    @Override
    public void truncateTable() {
        checkCommitted();
        commit.truncateTable(COMMIT_IDENTIFIER);
    }

    @Override
    public void truncatePartitions(List<Map<String, String>> partitionSpecs) {
        commit.dropPartitions(partitionSpecs, COMMIT_IDENTIFIER);
    }

    @Override
    public void updateStatistics(Statistics statistics) {
        commit.commitStatistics(statistics, COMMIT_IDENTIFIER);
    }

    @Override
    public void compactManifests() {
        commit.compactManifest();
    }

    /**
     * 检查是否已经提交。
     *
     * <p>批量提交只支持一次性提交，多次提交会抛出异常。
     */
    private void checkCommitted() {
        checkState(!batchCommitted, "BatchTableCommit only support one-time committing.");
        batchCommitted = true;
    }

    @Override
    public void commit(long identifier, List<CommitMessage> commitMessages) {
        commit(createManifestCommittable(identifier, commitMessages));
    }

    @Override
    public int filterAndCommit(Map<Long, List<CommitMessage>> commitIdentifiersAndMessages) {
        return filterAndCommitMultiple(
                commitIdentifiersAndMessages.entrySet().stream()
                        .map(e -> createManifestCommittable(e.getKey(), e.getValue()))
                        .collect(Collectors.toList()));
    }

    /**
     * 创建 ManifestCommittable。
     *
     * <p>将 Table 层的 CommitMessage 列表转换为 Operation 层的 ManifestCommittable。
     *
     * @param identifier 提交标识符
     * @param commitMessages 提交消息列表
     * @return ManifestCommittable
     */
    private ManifestCommittable createManifestCommittable(
            long identifier, List<CommitMessage> commitMessages) {
        ManifestCommittable committable = new ManifestCommittable(identifier);
        for (CommitMessage commitMessage : commitMessages) {
            committable.addFileCommittable(commitMessage);
        }
        return committable;
    }

    /**
     * 提交单个 ManifestCommittable。
     *
     * @param committable 待提交的 ManifestCommittable
     */
    public void commit(ManifestCommittable committable) {
        commitMultiple(singletonList(committable), false);
    }

    /**
     * 提交多个 ManifestCommittable。
     *
     * <p>提交流程：
     * <ol>
     *     <li>如果是普通提交（非覆写）：
     *         <ul>
     *             <li>依次提交所有 committable
     *             <li>记录新快照数量
     *             <li>执行维护任务
     *         </ul>
     *     <li>如果是覆写提交（INSERT OVERWRITE）：
     *         <ul>
     *             <li>只允许单个 committable
     *             <li>调用 overwritePartition 方法
     *             <li>执行维护任务
     *         </ul>
     * </ol>
     *
     * @param committables 待提交的 ManifestCommittable 列表
     * @param checkAppendFiles 是否检查追加文件（用于冲突检测）
     */
    public void commitMultiple(List<ManifestCommittable> committables, boolean checkAppendFiles) {
        if (overwritePartition == null) {
            // 普通提交模式
            int newSnapshots = 0;
            for (ManifestCommittable committable : committables) {
                newSnapshots += commit.commit(committable, checkAppendFiles);
            }
            if (!committables.isEmpty()) {
                // 执行维护任务（使用最后一个 committable 的标识符）
                maintain(
                        committables.get(committables.size() - 1).identifier(),
                        maintainExecutor,
                        newSnapshots > 0 || expireForEmptyCommit);
            }
        } else {
            // 覆写提交模式
            ManifestCommittable committable;
            if (committables.size() > 1) {
                throw new RuntimeException(
                        "Multiple committables appear in overwrite mode, this may be a bug, please report it: "
                                + committables);
            } else if (committables.size() == 1) {
                committable = committables.get(0);
            } else {
                // 创建一个空的 committable
                // identifier 是 Long.MAX_VALUE，来自批量任务
                committable = new ManifestCommittable(Long.MAX_VALUE);
            }
            int newSnapshots =
                    commit.overwritePartition(
                            overwritePartition, committable, Collections.emptyMap());
            maintain(
                    committable.identifier(),
                    maintainExecutor,
                    newSnapshots > 0 || expireForEmptyCommit);
        }
    }

    /**
     * 过滤并提交多个 ManifestCommittable。
     *
     * @param committables 待提交的 ManifestCommittable 列表
     * @return 实际提交的数量
     */
    public int filterAndCommitMultiple(List<ManifestCommittable> committables) {
        return filterAndCommitMultiple(committables, true);
    }

    /**
     * 过滤并提交多个 ManifestCommittable。
     *
     * <p>此方法用于故障恢复场景：
     * <ol>
     *     <li>将 committable 按标识符排序（必须递增）
     *     <li>调用 filterCommitted 过滤已提交的 committable
     *     <li>检查待提交文件是否存在
     *     <li>提交剩余的 committable
     * </ol>
     *
     * @param committables 待提交的 ManifestCommittable 列表
     * @param checkAppendFiles 是否检查追加文件
     * @return 实际提交的数量
     */
    public int filterAndCommitMultiple(
            List<ManifestCommittable> committables, boolean checkAppendFiles) {
        // 按标识符排序（必须递增）
        List<ManifestCommittable> sortedCommittables =
                committables.stream()
                        .sorted(Comparator.comparingLong(ManifestCommittable::identifier))
                        .collect(Collectors.toList());
        // 过滤已提交的 committable
        List<ManifestCommittable> retryCommittables = commit.filterCommitted(sortedCommittables);

        if (!retryCommittables.isEmpty()) {
            // 检查文件是否存在
            checkFilesExistence(retryCommittables);
            // 提交剩余的 committable
            commitMultiple(retryCommittables, checkAppendFiles);
        }
        return retryCommittables.size();
    }

    /**
     * 检查文件是否存在。
     *
     * <p>在故障恢复时，需要检查待提交的文件是否仍然存在。
     * 如果文件已被删除（例如从非常旧的 savepoint 恢复），则抛出异常。
     *
     * <p>检查范围：
     * <ul>
     *     <li>新增的数据文件
     *     <li>新增的 changelog 文件
     *     <li>新增的索引文件
     *     <li>压缩后的文件
     *     <li>压缩后的索引文件
     * </ul>
     *
     * @param committables 待检查的 ManifestCommittable 列表
     * @throws RuntimeException 如果有文件不存在
     */
    private void checkFilesExistence(List<ManifestCommittable> committables) {
        List<Path> files = new ArrayList<>();
        DataFilePathFactories factories = new DataFilePathFactories(commit.pathFactory());
        IndexFilePathFactories indexFactories = new IndexFilePathFactories(commit.pathFactory());

        // 收集所有需要检查的文件路径
        for (ManifestCommittable committable : committables) {
            for (CommitMessage message : committable.fileCommittables()) {
                CommitMessageImpl msg = (CommitMessageImpl) message;
                DataFilePathFactory pathFactory =
                        factories.get(message.partition(), message.bucket());
                IndexPathFactory indexFileFactory =
                        indexFactories.get(message.partition(), message.bucket());
                Consumer<DataFileMeta> collector = f -> files.addAll(f.collectFiles(pathFactory));

                // 收集新增文件
                msg.newFilesIncrement().newFiles().forEach(collector);
                msg.newFilesIncrement().changelogFiles().forEach(collector);
                msg.newFilesIncrement().newIndexFiles().stream()
                        .map(indexFileFactory::toPath)
                        .forEach(files::add);

                // 收集压缩后文件
                msg.compactIncrement().compactAfter().forEach(collector);
                msg.compactIncrement().newIndexFiles().stream()
                        .map(indexFileFactory::toPath)
                        .forEach(files::add);

                // 跳过压缩前文件和删除的索引文件（这些文件可能已被删除）
            }
        }

        // 解析压缩的 changelog 文件到实际文件路径
        List<Path> resolvedFiles = new ArrayList<>();
        for (Path file : files) {
            resolvedFiles.add(CompactedChangelogPathResolver.resolveCompactedChangelogPath(file));
        }
        // 去重（多个压缩的 changelog 引用可能解析到同一个物理文件）
        resolvedFiles = resolvedFiles.stream().distinct().collect(Collectors.toList());

        // 定义文件不存在的判断逻辑
        Predicate<Path> nonExists =
                p -> {
                    try {
                        return !commit.fileIO().exists(p);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                };

        // 并行检查文件是否存在
        List<Path> nonExistFiles =
                Lists.newArrayList(
                        randomlyExecuteSequentialReturn(
                                fileCheckExecutor,
                                f -> nonExists.test(f) ? singletonList(f) : emptyList(),
                                resolvedFiles));

        // 如果有文件不存在，抛出异常
        if (!nonExistFiles.isEmpty()) {
            String message =
                    String.join(
                            "\n",
                            "Cannot recover from this checkpoint because some files in the snapshot that"
                                    + " need to be resubmitted have been deleted:",
                            "    "
                                    + nonExistFiles.stream()
                                            .map(Object::toString)
                                            .collect(Collectors.joining(",")),
                            "    The most likely reason is because you are recovering from a very old savepoint that"
                                    + " contains some uncommitted files that have already been deleted.");
            throw new RuntimeException(message);
        }
    }

    /**
     * 执行维护任务的入口方法。
     *
     * <p>根据执行模式选择同步或异步执行：
     * <ul>
     *     <li>批量提交：同步执行
     *     <li>流式提交：根据配置同步或异步执行
     * </ul>
     *
     * @param identifier 提交标识符
     * @param executor 维护任务执行器
     * @param doExpire 是否执行过期任务
     */
    private void maintain(long identifier, ExecutorService executor, boolean doExpire) {
        // 检查是否有异步维护任务的错误
        if (maintainError.get() != null) {
            throw new RuntimeException(maintainError.get());
        }

        if (batchCommitted) {
            // 批量提交同步执行维护
            maintain(identifier, doExpire);
        } else {
            // 流式提交异步执行维护
            executor.execute(
                    () -> {
                        try {
                            maintain(identifier, doExpire);
                        } catch (Throwable t) {
                            LOG.error("Executing maintain encountered an error.", t);
                            maintainError.compareAndSet(null, t);
                        }
                    });
        }
    }

    /**
     * 执行维护任务。
     *
     * <p>维护任务的执行顺序（按耗时从少到多）：
     * <ol>
     *     <li>标签自动创建
     *     <li>标签过期
     *     <li>分区过期
     *     <li>消费者过期
     *     <li>快照过期（通常最耗时，放在最后）
     * </ol>
     *
     * @param identifier 提交标识符
     * @param doExpire 是否执行过期任务
     */
    private void maintain(long identifier, boolean doExpire) {
        // 1. 标签自动创建和过期
        if (tagAutoManager != null) {
            TagAutoCreation tagAutoCreation = tagAutoManager.getTagAutoCreation();
            if (tagAutoCreation != null) {
                tagAutoCreation.run();
            }

            TagTimeExpire tagTimeExpire = tagAutoManager.getTagTimeExpire();
            if (doExpire && tagTimeExpire != null) {
                tagTimeExpire.expire();
            }
        }

        // 2. 分区过期
        if (doExpire && partitionExpire != null) {
            partitionExpire.expire(identifier);
        }

        // 3. 消费者过期（在快照过期之前，避免阻止快照过期）
        if (doExpire && consumerExpireTime != null) {
            consumerManager.expire(LocalDateTime.now().minus(consumerExpireTime));
        }

        // 4. 快照过期（通常最耗时，放在最后）
        if (doExpire && expireSnapshots != null) {
            expireSnapshots.run();
        }
    }

    /**
     * 手动触发快照过期。
     *
     * <p>此方法可在外部手动调用，无需等待提交。
     */
    public void expireSnapshots() {
        if (expireSnapshots != null) {
            expireSnapshots.run();
        }
    }

    @Override
    public void close() throws Exception {
        commit.close();
        maintainExecutor.shutdownNow();
    }

    @Override
    public void abort(List<CommitMessage> commitMessages) {
        commit.abort(commitMessages);
    }

    /**
     * 获取维护任务执行器（测试用）。
     *
     * @return 维护任务执行器
     */
    @VisibleForTesting
    public ExecutorService getMaintainExecutor() {
        return maintainExecutor;
    }
}
