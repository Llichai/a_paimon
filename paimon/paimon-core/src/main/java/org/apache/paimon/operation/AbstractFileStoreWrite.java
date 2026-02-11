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
import org.apache.paimon.KeyValue;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BlobConsumer;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.index.DynamicBucketIndexMaintainer;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.ExecutorThreadFactory;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static org.apache.paimon.CoreOptions.PARTITION_DEFAULT_NAME;
import static org.apache.paimon.io.DataFileMeta.getMaxSequenceNumber;
import static org.apache.paimon.shade.guava30.com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.paimon.utils.FileStorePathFactory.getPartitionComputer;

/**
 * 抽象文件存储写入实现 - 管理 Writer 容器、内存和压缩触发
 *
 * <p>该类是所有 FileStoreWrite 实现的基类，提供以下核心功能：
 * <ol>
 *   <li><b>Writer 容器管理</b>：
 *       <ul>
 *         <li>为每个 (partition, bucket) 创建并缓存 Writer
 *         <li>管理 Writer 的生命周期（创建、使用、关闭）
 *         <li>记录每个 Writer 的状态（最后修改时间、base snapshot ID 等）
 *       </ul>
 *   </li>
 *   <li><b>内存管理和溢写机制</b>：
 *       <ul>
 *         <li>限制同时活跃的 Writer 数量（writerNumberMax）
 *         <li>当 Writer 数量超过阈值时，触发 buffer spill（溢写到磁盘）
 *         <li>避免内存溢出（OOM）
 *       </ul>
 *   </li>
 *   <li><b>压缩触发逻辑</b>：
 *       <ul>
 *         <li>根据文件数量、大小等条件触发 compaction
 *         <li>支持同步和异步压缩（通过 compactExecutor）
 *         <li>管理压缩任务的并发执行
 *       </ul>
 *   </li>
 * </ol>
 *
 * <p><b>Writer 容器结构</b>：
 * <pre>
 * writers: Map&lt;BinaryRow, Map&lt;Integer, WriterContainer&lt;T&gt;&gt;&gt;
 *   - BinaryRow: 分区（如 {date=2024-01-01, region=US}）
 *   - Integer: bucket ID（如 0, 1, 2, ...）
 *   - WriterContainer: Writer 的包装类，包含：
 *       - writer: 实际的记录写入器
 *       - totalBuckets: 该分区的总 bucket 数
 *       - dynamicBucketMaintainer: 动态分桶索引维护器（可选）
 *       - deletionVectorsMaintainer: 删除向量维护器（可选）
 *       - baseSnapshotId: Writer 创建时的 snapshot ID
 *       - lastModifiedCommitIdentifier: 最后修改的 commit 标识
 *
 * 示例结构：
 * writers = {
 *   partition{date=2024-01-01}: {
 *     0: WriterContainer{writer=..., baseSnapshotId=100, lastModified=1001},
 *     1: WriterContainer{writer=..., baseSnapshotId=100, lastModified=1001}
 *   },
 *   partition{date=2024-01-02}: {
 *     0: WriterContainer{writer=..., baseSnapshotId=101, lastModified=1002}
 *   }
 * }
 * </pre>
 *
 * <p><b>内存管理和溢写机制详解</b>：
 * <ul>
 *   <li><b>问题</b>：
 *       <ul>
 *         <li>批处理模式下，可能同时写入大量分区和 bucket
 *         <li>每个 Writer 都有自己的内存缓冲区（write buffer）
 *         <li>如果 Writer 数量过多，总内存占用会超过 JVM 堆大小，导致 OOM
 *       </ul>
 *   </li>
 *   <li><b>解决方案</b>：
 *       <ul>
 *         <li>配置项：{@code write.max-writers-to-spill}（默认值基于内存大小）
 *         <li>当 Writer 数量达到阈值时，调用 {@link #forceBufferSpill()}
 *         <li>forceBufferSpill() 通知所有 Writer 将内存数据溢写到磁盘
 *         <li>溢写后，内存占用降低，可以继续创建新的 Writer
 *       </ul>
 *   </li>
 *   <li><b>溢写触发时机</b>：
 *       <pre>
 *       创建新 Writer 时：
 *       1. 检查当前 Writer 数量：writerNumber()
 *       2. 如果 writerNumber >= writerNumberMax：
 *          a. 调用 forceBufferSpill()
 *          b. 等待所有 Writer 完成溢写
 *          c. 继续创建新 Writer
 *       3. 否则直接创建 Writer
 *       </pre>
 *   </li>
 *   <li><b>示例</b>：
 *       <pre>
 *       假设 writerNumberMax = 100
 *       当前有 99 个 Writer：
 *       - 创建第 100 个 Writer -> 正常创建
 *       - 创建第 101 个 Writer -> 触发 forceBufferSpill()
 *           - 所有 Writer 将内存数据写入临时文件
 *           - 内存占用大幅降低
 *           - 继续创建第 101 个 Writer
 *       </pre>
 *   </li>
 * </ul>
 *
 * <p><b>压缩触发逻辑</b>：
 * <ul>
 *   <li><b>异步压缩</b>（推荐）：
 *       <ul>
 *         <li>每个 Writer 有独立的 compactExecutor（单线程）
 *         <li>当文件数量达到阈值时，提交压缩任务到 executor
 *         <li>写入和压缩并行执行，互不阻塞
 *         <li>配置：{@code compaction.async-enabled = true}
 *       </ul>
 *   </li>
 *   <li><b>同步压缩</b>：
 *       <ul>
 *         <li>在 prepareCommit() 时等待所有压缩任务完成
 *         <li>阻塞写入流程，适合小数据量场景
 *         <li>配置：{@code compaction.async-enabled = false}
 *       </ul>
 *   </li>
 *   <li><b>强制压缩</b>：
 *       <ul>
 *         <li>配置：{@code commit.force-compact = true}
 *         <li>每次提交前强制执行压缩，确保数据文件数量最少
 *         <li>性能开销大，仅用于对查询性能要求极高的场景
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p><b>Writer 清理机制</b>：
 * <ul>
 *   <li><b>问题</b>：不再使用的 Writer 占用内存和资源
 *   <li><b>解决方案</b>：在 prepareCommit() 时清理空闲 Writer
 *   <li><b>清理条件</b>：
 *       <ul>
 *         <li>Writer 没有新数据（committable.isEmpty()）
 *         <li>Writer 的最后修改已提交（writerCleanChecker.apply(...)）
 *         <li>Writer 没有进行中的压缩任务（!writer.compactNotCompleted()）
 *       </ul>
 *   </li>
 *   <li><b>清理流程</b>：
 *       <pre>
 *       prepareCommit(commitIdentifier=1002):
 *       1. 遍历所有 Writer
 *       2. 对每个 Writer：
 *          a. 调用 writer.prepareCommit() 获取 increment
 *          b. 如果 increment.isEmpty() && lastModified < 1002:
 *             - 关闭 Writer
 *             - 从 writers map 中移除
 *       3. 返回非空的 CommitMessage 列表
 *       </pre>
 *   </li>
 *   <li><b>示例</b>：
 *       <pre>
 *       假设有 3 个 Writer：
 *       - Writer1: lastModified=1000, isEmpty=true -> 清理
 *       - Writer2: lastModified=1001, isEmpty=false -> 保留
 *       - Writer3: lastModified=1002, isEmpty=true -> 保留（最近修改）
 *       </pre>
 *   </li>
 * </ul>
 *
 * <p><b>Dynamic Bucket 模式</b>：
 * <ul>
 *   <li>传统 Fixed Bucket：bucket 数量固定，无法扩展
 *   <li>Dynamic Bucket：根据数据量动态分配 bucket
 *   <li>实现：
 *       <ul>
 *         <li>DynamicBucketIndexMaintainer：维护 key -> bucket 的映射索引
 *         <li>写入时：根据 key 查找或分配 bucket
 *         <li>索引文件在 commit 时一并提交
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p><b>Deletion Vectors（删除向量）</b>：
 * <ul>
 *   <li>用于标记数据文件中哪些行已被删除（逻辑删除）
 *   <li>避免重写整个文件（物理删除）
 *   <li>实现：
 *       <ul>
 *         <li>BucketedDvMaintainer：维护每个 bucket 的 deletion vector
 *         <li>读取时应用 DV 过滤已删除的行
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p><b>状态恢复（Checkpoint/Restore）</b>：
 * <ul>
 *   <li>checkpoint()：将所有 Writer 的状态序列化为 State 对象
 *   <li>restore()：从 State 对象恢复 Writer
 *   <li>用于 Flink 等流式引擎的 failover 恢复
 * </ul>
 *
 * @param <T> 记录类型（如 KeyValue、InternalRow）
 * @see KeyValueFileStoreWrite 主键表的写入实现
 * @see AppendOnlyFileStoreWrite Append-Only 表的写入实现
 */
public abstract class AbstractFileStoreWrite<T> implements FileStoreWrite<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractFileStoreWrite.class);

    private final int writerNumberMax;
    @Nullable private final DynamicBucketIndexMaintainer.Factory dbMaintainerFactory;
    @Nullable private final BucketedDvMaintainer.Factory dvMaintainerFactory;
    private final int numBuckets;
    private final RowType partitionType;

    @Nullable protected IOManager ioManager;

    protected final Map<BinaryRow, Map<Integer, WriterContainer<T>>> writers;

    protected WriteRestore restore;
    private ExecutorService lazyCompactExecutor;
    private boolean closeCompactExecutorWhenLeaving = true;
    private boolean ignorePreviousFiles = false;
    private boolean ignoreNumBucketCheck = false;

    protected CompactionMetrics compactionMetrics = null;
    protected final String tableName;
    private final boolean legacyPartitionName;

    protected AbstractFileStoreWrite(
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            @Nullable DynamicBucketIndexMaintainer.Factory dbMaintainerFactory,
            @Nullable BucketedDvMaintainer.Factory dvMaintainerFactory,
            String tableName,
            CoreOptions options,
            RowType partitionType) {
        IndexFileHandler indexFileHandler = null;
        if (dbMaintainerFactory != null) {
            indexFileHandler = dbMaintainerFactory.indexFileHandler();
        } else if (dvMaintainerFactory != null) {
            indexFileHandler = dvMaintainerFactory.indexFileHandler();
        }
        this.restore = new FileSystemWriteRestore(options, snapshotManager, scan, indexFileHandler);
        this.dbMaintainerFactory = dbMaintainerFactory;
        this.dvMaintainerFactory = dvMaintainerFactory;
        this.numBuckets = options.bucket();
        this.partitionType = partitionType;
        this.writers = new HashMap<>();
        this.tableName = tableName;
        this.writerNumberMax = options.writeMaxWritersToSpill();
        this.legacyPartitionName = options.legacyPartitionName();
    }

    @Override
    public FileStoreWrite<T> withWriteRestore(WriteRestore writeRestore) {
        this.restore = writeRestore;
        return this;
    }

    @Override
    public FileStoreWrite<T> withIOManager(IOManager ioManager) {
        this.ioManager = ioManager;
        return this;
    }

    @Override
    public FileStoreWrite<T> withMemoryPoolFactory(MemoryPoolFactory memoryPoolFactory) {
        return this;
    }

    @Override
    public FileStoreWrite<T> withBlobConsumer(BlobConsumer blobConsumer) {
        return this;
    }

    @Override
    public void withIgnorePreviousFiles(boolean ignorePreviousFiles) {
        this.ignorePreviousFiles = ignorePreviousFiles;
    }

    @Override
    public void withIgnoreNumBucketCheck(boolean ignoreNumBucketCheck) {
        this.ignoreNumBucketCheck = ignoreNumBucketCheck;
    }

    @Override
    public void withCompactExecutor(ExecutorService compactExecutor) {
        this.lazyCompactExecutor = compactExecutor;
        this.closeCompactExecutorWhenLeaving = false;
    }

    @Override
    public void write(BinaryRow partition, int bucket, T data) throws Exception {
        WriterContainer<T> container = getWriterWrapper(partition, bucket);
        container.writer.write(data);
        if (container.dynamicBucketMaintainer != null) {
            container.dynamicBucketMaintainer.notifyNewRecord((KeyValue) data);
        }
    }

    @Override
    public void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception {
        getWriterWrapper(partition, bucket).writer.compact(fullCompaction);
    }

    @Override
    public void notifyNewFiles(
            long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files) {
        WriterContainer<T> writerContainer = getWriterWrapper(partition, bucket);
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Get extra compact files for partition {}, bucket {}. Extra snapshot {}, base snapshot {}.\nFiles: {}",
                    partition,
                    bucket,
                    snapshotId,
                    writerContainer.baseSnapshotId,
                    files);
        }
        if (snapshotId > writerContainer.baseSnapshotId) {
            writerContainer.writer.addNewFiles(files);
        }
    }

    @Override
    public List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier)
            throws Exception {
        Function<WriterContainer<T>, Boolean> writerCleanChecker;
        if (writers.values().stream()
                        .map(Map::values)
                        .flatMap(Collection::stream)
                        .mapToLong(w -> w.lastModifiedCommitIdentifier)
                        .max()
                        .orElse(Long.MIN_VALUE)
                == Long.MIN_VALUE) {
            // If this is the first commit, no writer should be cleaned.
            writerCleanChecker = writerContainer -> false;
        } else {
            writerCleanChecker = createWriterCleanChecker();
        }

        List<CommitMessage> result = new ArrayList<>();

        Iterator<Map.Entry<BinaryRow, Map<Integer, WriterContainer<T>>>> partIter =
                writers.entrySet().iterator();
        while (partIter.hasNext()) {
            Map.Entry<BinaryRow, Map<Integer, WriterContainer<T>>> partEntry = partIter.next();
            BinaryRow partition = partEntry.getKey();
            Iterator<Map.Entry<Integer, WriterContainer<T>>> bucketIter =
                    partEntry.getValue().entrySet().iterator();
            while (bucketIter.hasNext()) {
                Map.Entry<Integer, WriterContainer<T>> entry = bucketIter.next();
                int bucket = entry.getKey();
                WriterContainer<T> writerContainer = entry.getValue();

                CommitIncrement increment = writerContainer.writer.prepareCommit(waitCompaction);
                DataIncrement newFilesIncrement = increment.newFilesIncrement();
                CompactIncrement compactIncrement = increment.compactIncrement();
                if (writerContainer.dynamicBucketMaintainer != null) {
                    newFilesIncrement
                            .newIndexFiles()
                            .addAll(writerContainer.dynamicBucketMaintainer.prepareCommit());
                }
                CompactDeletionFile compactDeletionFile = increment.compactDeletionFile();
                if (compactDeletionFile != null) {
                    compactDeletionFile
                            .getOrCompute()
                            .ifPresent(compactIncrement.newIndexFiles()::add);
                }
                CommitMessageImpl committable =
                        new CommitMessageImpl(
                                partition,
                                bucket,
                                writerContainer.totalBuckets,
                                newFilesIncrement,
                                compactIncrement);
                result.add(committable);

                if (committable.isEmpty()) {
                    if (writerCleanChecker.apply(writerContainer)) {
                        // Clear writer if no update, and if its latest modification has committed.
                        //
                        // We need a mechanism to clear writers, otherwise there will be more and
                        // more such as yesterday's partition that no longer needs to be written.
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                    "Closing writer for partition {}, bucket {}. "
                                            + "Writer's last modified identifier is {}, "
                                            + "while current commit identifier is {}.",
                                    partition,
                                    bucket,
                                    writerContainer.lastModifiedCommitIdentifier,
                                    commitIdentifier);
                        }
                        writerContainer.writer.close();
                        bucketIter.remove();
                    }
                } else {
                    writerContainer.lastModifiedCommitIdentifier = commitIdentifier;
                }
            }

            if (partEntry.getValue().isEmpty()) {
                partIter.remove();
            }
        }

        return result;
    }

    // This abstract function returns a whole function (instead of just a boolean value),
    // because we do not want to introduce `commitUser` into this base class.
    //
    // For writers with no conflicts, `commitUser` might be some random value.
    protected abstract Function<WriterContainer<T>, Boolean> createWriterCleanChecker();

    protected static <T>
            Function<WriterContainer<T>, Boolean> createConflictAwareWriterCleanChecker(
                    String commitUser, WriteRestore restore) {
        long latestCommittedIdentifier = restore.latestCommittedIdentifier(commitUser);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Latest committed identifier is {}", latestCommittedIdentifier);
        }

        // Condition 1: There is no more record waiting to be committed. Note that the
        // condition is < (instead of <=), because each commit identifier may have
        // multiple snapshots. We must make sure all snapshots of this identifier are
        // committed.
        //
        // Condition 2: No compaction is in progress. That is, no more changelog will be
        // produced.
        //
        // Condition 3: The writer has no postponed compaction like gentle lookup compaction.
        return writerContainer ->
                writerContainer.lastModifiedCommitIdentifier < latestCommittedIdentifier
                        && !writerContainer.writer.compactNotCompleted();
    }

    protected static <T>
            Function<WriterContainer<T>, Boolean> createNoConflictAwareWriterCleanChecker() {
        return writerContainer -> true;
    }

    @Override
    public void close() throws Exception {
        for (Map<Integer, WriterContainer<T>> bucketWriters : writers.values()) {
            for (WriterContainer<T> writerContainer : bucketWriters.values()) {
                writerContainer.writer.close();
            }
        }
        writers.clear();
        if (lazyCompactExecutor != null && closeCompactExecutorWhenLeaving) {
            lazyCompactExecutor.shutdownNow();
        }
        if (compactionMetrics != null) {
            compactionMetrics.close();
        }
    }

    @Override
    public List<State<T>> checkpoint() {
        List<State<T>> result = new ArrayList<>();

        for (Map.Entry<BinaryRow, Map<Integer, WriterContainer<T>>> partitionEntry :
                writers.entrySet()) {
            BinaryRow partition = partitionEntry.getKey();
            for (Map.Entry<Integer, WriterContainer<T>> bucketEntry :
                    partitionEntry.getValue().entrySet()) {
                int bucket = bucketEntry.getKey();
                WriterContainer<T> writerContainer = bucketEntry.getValue();

                CommitIncrement increment;
                try {
                    increment = writerContainer.writer.prepareCommit(false);
                } catch (Exception e) {
                    throw new RuntimeException(
                            "Failed to extract state from writer of partition "
                                    + partition
                                    + " bucket "
                                    + bucket,
                            e);
                }
                // writer.allFiles() must be fetched after writer.prepareCommit(), because
                // compaction result might be updated during prepareCommit
                Collection<DataFileMeta> dataFiles = writerContainer.writer.dataFiles();
                result.add(
                        new State<>(
                                partition,
                                bucket,
                                writerContainer.totalBuckets,
                                writerContainer.baseSnapshotId,
                                writerContainer.lastModifiedCommitIdentifier,
                                dataFiles,
                                writerContainer.writer.maxSequenceNumber(),
                                writerContainer.dynamicBucketMaintainer,
                                writerContainer.deletionVectorsMaintainer,
                                increment));
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Extracted state " + result);
        }
        return result;
    }

    @Override
    public void restore(List<State<T>> states) {
        for (State<T> state : states) {
            RecordWriter<T> writer =
                    createWriter(
                            state.partition,
                            state.bucket,
                            state.dataFiles,
                            state.maxSequenceNumber,
                            state.commitIncrement,
                            compactExecutor(),
                            state.deletionVectorsMaintainer);
            notifyNewWriter(writer);
            WriterContainer<T> writerContainer =
                    new WriterContainer<>(
                            writer,
                            state.totalBuckets,
                            state.indexMaintainer,
                            state.deletionVectorsMaintainer,
                            state.baseSnapshotId);
            writerContainer.lastModifiedCommitIdentifier = state.lastModifiedCommitIdentifier;
            writers.computeIfAbsent(state.partition, k -> new HashMap<>())
                    .put(state.bucket, writerContainer);
        }
    }

    public Map<BinaryRow, List<Integer>> getActiveBuckets() {
        Map<BinaryRow, List<Integer>> result = new HashMap<>();
        for (Map.Entry<BinaryRow, Map<Integer, WriterContainer<T>>> partitions :
                writers.entrySet()) {
            result.put(partitions.getKey(), new ArrayList<>(partitions.getValue().keySet()));
        }
        return result;
    }

    protected WriterContainer<T> getWriterWrapper(BinaryRow partition, int bucket) {
        Map<Integer, WriterContainer<T>> buckets = writers.get(partition);
        if (buckets == null) {
            buckets = new HashMap<>();
            writers.put(partition.copy(), buckets);
        }
        return buckets.computeIfAbsent(
                bucket, k -> createWriterContainer(partition.copy(), bucket));
    }

    public RecordWriter<T> createWriter(BinaryRow partition, int bucket) {
        return createWriterContainer(partition, bucket).writer;
    }

    public WriterContainer<T> createWriterContainer(BinaryRow partition, int bucket) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating writer for partition {}, bucket {}", partition, bucket);
        }

        if (writerNumber() >= writerNumberMax) {
            try {
                forceBufferSpill();
            } catch (Exception e) {
                throw new RuntimeException("Error happens while force buffer spill", e);
            }
        }

        RestoreFiles restored = RestoreFiles.empty();
        if (!ignorePreviousFiles) {
            restored = scanExistingFileMetas(partition, bucket);
        }

        DynamicBucketIndexMaintainer indexMaintainer =
                dbMaintainerFactory == null
                        ? null
                        : dbMaintainerFactory.create(
                                partition, bucket, restored.dynamicBucketIndex());
        BucketedDvMaintainer dvMaintainer =
                dvMaintainerFactory == null
                        ? null
                        : dvMaintainerFactory.create(
                                partition, bucket, restored.deleteVectorsIndex());

        List<DataFileMeta> restoreFiles = restored.dataFiles();
        if (restoreFiles == null) {
            restoreFiles = new ArrayList<>();
        }
        RecordWriter<T> writer =
                createWriter(
                        partition.copy(),
                        bucket,
                        restoreFiles,
                        getMaxSequenceNumber(restoreFiles),
                        null,
                        compactExecutor(),
                        dvMaintainer);
        notifyNewWriter(writer);

        Snapshot previousSnapshot = restored.snapshot();
        return new WriterContainer<>(
                writer,
                firstNonNull(restored.totalBuckets(), numBuckets),
                indexMaintainer,
                dvMaintainer,
                previousSnapshot == null ? null : previousSnapshot.id());
    }

    private long writerNumber() {
        return writers.values().stream().mapToLong(Map::size).sum();
    }

    @Override
    public FileStoreWrite<T> withMetricRegistry(MetricRegistry metricRegistry) {
        this.compactionMetrics = new CompactionMetrics(metricRegistry, tableName);
        return this;
    }

    private RestoreFiles scanExistingFileMetas(BinaryRow partition, int bucket) {
        RestoreFiles restored =
                restore.restoreFiles(
                        partition,
                        bucket,
                        dbMaintainerFactory != null,
                        dvMaintainerFactory != null);
        Integer restoredTotalBuckets = restored.totalBuckets();
        int totalBuckets = numBuckets;
        if (restoredTotalBuckets != null) {
            totalBuckets = restoredTotalBuckets;
        }
        if (!ignoreNumBucketCheck && totalBuckets != numBuckets) {
            String partInfo =
                    partitionType.getFieldCount() > 0
                            ? "partition "
                                    + getPartitionComputer(
                                                    partitionType,
                                                    PARTITION_DEFAULT_NAME.defaultValue(),
                                                    legacyPartitionName)
                                            .generatePartValues(partition)
                            : "table";
            throw new RuntimeException(
                    String.format(
                            "Try to write %s with a new bucket num %d, but the previous bucket num is %d. "
                                    + "Please switch to batch mode, and perform INSERT OVERWRITE to rescale current data layout first.",
                            partInfo, numBuckets, totalBuckets));
        }
        return restored;
    }

    private ExecutorService compactExecutor() {
        if (lazyCompactExecutor == null) {
            lazyCompactExecutor =
                    Executors.newSingleThreadScheduledExecutor(
                            new ExecutorThreadFactory(
                                    Thread.currentThread().getName() + "-compaction"));
        }
        return lazyCompactExecutor;
    }

    @VisibleForTesting
    public ExecutorService getCompactExecutor() {
        return lazyCompactExecutor;
    }

    protected void notifyNewWriter(RecordWriter<T> writer) {}

    protected abstract RecordWriter<T> createWriter(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> restoreFiles,
            long restoredMaxSeqNumber,
            @Nullable CommitIncrement restoreIncrement,
            ExecutorService compactExecutor,
            @Nullable BucketedDvMaintainer deletionVectorsMaintainer);

    // force buffer spill to avoid out of memory in batch mode
    protected void forceBufferSpill() throws Exception {}

    /**
     * {@link RecordWriter} with the snapshot id it is created upon and the identifier of its last
     * modified commit.
     */
    @VisibleForTesting
    public static class WriterContainer<T> {
        public final RecordWriter<T> writer;
        public final int totalBuckets;
        @Nullable public final DynamicBucketIndexMaintainer dynamicBucketMaintainer;
        @Nullable public final BucketedDvMaintainer deletionVectorsMaintainer;
        protected final long baseSnapshotId;
        protected long lastModifiedCommitIdentifier;

        protected WriterContainer(
                RecordWriter<T> writer,
                int totalBuckets,
                @Nullable DynamicBucketIndexMaintainer dynamicBucketMaintainer,
                @Nullable BucketedDvMaintainer deletionVectorsMaintainer,
                Long baseSnapshotId) {
            this.writer = writer;
            this.totalBuckets = totalBuckets;
            this.dynamicBucketMaintainer = dynamicBucketMaintainer;
            this.deletionVectorsMaintainer = deletionVectorsMaintainer;
            this.baseSnapshotId =
                    baseSnapshotId == null ? Snapshot.FIRST_SNAPSHOT_ID - 1 : baseSnapshotId;
            this.lastModifiedCommitIdentifier = Long.MIN_VALUE;
        }
    }

    @VisibleForTesting
    public Map<BinaryRow, Map<Integer, WriterContainer<T>>> writers() {
        return writers;
    }

    @VisibleForTesting
    public CompactionMetrics compactionMetrics() {
        return compactionMetrics;
    }
}
