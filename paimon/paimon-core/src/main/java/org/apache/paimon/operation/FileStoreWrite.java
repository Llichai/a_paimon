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

import org.apache.paimon.FileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BlobConsumer;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.index.DynamicBucketIndexMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.SinkRecord;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.Restorable;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * 文件存储写入操作接口，提供 {@link RecordWriter} 的创建和将 {@link SinkRecord} 写入 {@link FileStore} 的功能。
 *
 * <p>写入流程包括：
 * <ol>
 *   <li><b>数据路由</b>：根据分区和桶将记录路由到对应的 Writer</li>
 *   <li><b>内存缓冲</b>：数据先写入内存缓冲区（WriteBuffer），达到阈值后溢写到磁盘</li>
 *   <li><b>文件写入</b>：将缓冲区数据刷写成数据文件</li>
 *   <li><b>自动压缩</b>：根据策略触发后台压缩任务，合并小文件</li>
 *   <li><b>准备提交</b>：等待所有异步操作完成，生成 CommitMessage</li>
 * </ol>
 *
 * <p>内存管理：
 * <ul>
 *   <li>每个 Writer 从 MemoryPool 申请内存用于缓冲</li>
 *   <li>当内存不足时会触发溢写操作</li>
 *   <li>支持通过 MemoryPoolFactory 自定义内存分配策略</li>
 * </ul>
 *
 * <p>该接口实现了 {@link Restorable}，支持从故障中恢复写入状态。
 *
 * @param <T> 要写入的记录类型
 */
public interface FileStoreWrite<T> extends Restorable<List<FileStoreWrite.State<T>>> {

    /** 设置写入恢复器，用于从故障中恢复。 */
    FileStoreWrite<T> withWriteRestore(WriteRestore writeRestore);

    /** 设置 IO 管理器，用于管理临时文件和溢写操作。 */
    FileStoreWrite<T> withIOManager(IOManager ioManager);

    /**
     * 指定写入的行类型。
     *
     * @param writeType 写入行类型
     */
    default void withWriteType(RowType writeType) {
        throw new UnsupportedOperationException();
    }

    /**
     * 设置内存池工厂。
     *
     * <p>内存池用于管理写入缓冲区的内存分配，控制内存使用上限，
     * 避免 OOM 并提供内存反压机制。
     *
     * @param memoryPoolFactory 内存池工厂
     */
    FileStoreWrite<T> withMemoryPoolFactory(MemoryPoolFactory memoryPoolFactory);

    /**
     * 设置 Blob 消费者，用于处理大对象。
     *
     * @param blobConsumer Blob 消费者
     */
    FileStoreWrite<T> withBlobConsumer(BlobConsumer blobConsumer);

    /**
     * 设置是否忽略之前存储的文件。
     *
     * <p>当设置为 true 时，写入操作将不会读取已有的数据文件，
     * 这在某些场景下可以提高性能（如全量覆盖写入）。
     *
     * @param ignorePreviousFiles 是否忽略之前的文件
     */
    void withIgnorePreviousFiles(boolean ignorePreviousFiles);

    /**
     * 忽略桶数量检查。
     *
     * <p>正常情况下，写入的分区必须与表选项中的桶数量一致。
     * 设置此选项后将跳过该检查。
     *
     * @param ignoreNumBucketCheck 是否忽略桶数量检查
     */
    void withIgnoreNumBucketCheck(boolean ignoreNumBucketCheck);

    /**
     * 设置指标注册器，用于监控压缩相关的指标。
     *
     * @param metricRegistry 指标注册器
     */
    FileStoreWrite<T> withMetricRegistry(MetricRegistry metricRegistry);

    /**
     * 设置压缩执行器。
     *
     * <p>压缩操作在后台异步执行，此执行器用于提交压缩任务。
     *
     * @param compactExecutor 压缩执行器
     */
    void withCompactExecutor(ExecutorService compactExecutor);

    /**
     * 将数据写入指定分区和桶。
     *
     * <p>写入流程：
     * <ol>
     *   <li>根据分区和桶找到或创建对应的 Writer</li>
     *   <li>将数据写入 Writer 的内存缓冲区</li>
     *   <li>如果缓冲区满，触发溢写到磁盘</li>
     *   <li>根据压缩策略可能触发异步压缩</li>
     * </ol>
     *
     * @param partition 数据的分区
     * @param bucket 数据的桶 ID
     * @param data 要写入的数据
     * @throws Exception 写入记录时抛出的异常
     */
    void write(BinaryRow partition, int bucket, T data) throws Exception;

    /**
     * 压缩指定分区和桶中存储的数据。
     *
     * <p>注意：压缩过程仅被提交，方法返回时可能尚未完成。
     * 压缩在后台异步执行，需要在 prepareCommit 时等待完成。
     *
     * @param partition 要压缩的分区
     * @param bucket 要压缩的桶
     * @param fullCompaction 是否触发全量压缩（false 为普通压缩）
     * @throws Exception 压缩记录时抛出的异常
     */
    void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception;

    /**
     * 通知在给定快照的指定桶中创建了新文件。
     *
     * <p>这些文件很可能是由其他作业创建的。目前此方法仅用于
     * 专用压缩作业查看写入作业创建的文件。
     *
     * @param snapshotId 创建新文件的快照 ID
     * @param partition 创建新文件的分区
     * @param bucket 创建新文件的桶
     * @param files 新文件本身
     */
    void notifyNewFiles(long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files);

    /**
     * 准备提交写入。
     *
     * <p>此方法将：
     * <ol>
     *   <li>刷新所有内存中的数据到磁盘</li>
     *   <li>如果 waitCompaction 为 true，等待所有异步压缩完成</li>
     *   <li>收集所有新生成的文件信息</li>
     *   <li>生成 CommitMessage 列表，包含本次提交的所有变更</li>
     * </ol>
     *
     * @param waitCompaction 是否等待当前压缩完成
     * @param commitIdentifier 正在准备的提交标识符
     * @return 文件可提交列表（CommitMessage）
     * @throws Exception 抛出的异常
     */
    List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier)
            throws Exception;

    /**
     * 关闭写入器。
     *
     * <p>释放所有资源，包括内存、文件句柄、线程池等。
     *
     * @throws Exception 抛出的异常
     */
    void close() throws Exception;

    /**
     * {@link FileStoreWrite} 的可恢复状态。
     *
     * <p>用于故障恢复，保存了 Writer 的完整状态信息，
     * 包括当前写入的文件、序列号、索引等。
     */
    class State<T> {

        /** 分区信息 */
        protected final BinaryRow partition;
        /** 桶号 */
        protected final int bucket;
        /** 总桶数 */
        protected final int totalBuckets;

        /** 基础快照 ID */
        protected final long baseSnapshotId;
        /** 最后修改的提交标识符 */
        protected final long lastModifiedCommitIdentifier;
        /** 当前已有的数据文件列表 */
        protected final List<DataFileMeta> dataFiles;
        /** 最大序列号 */
        protected final long maxSequenceNumber;
        /** 动态桶索引维护器（可选） */
        @Nullable protected final DynamicBucketIndexMaintainer indexMaintainer;
        /** 删除向量维护器（可选） */
        @Nullable protected final BucketedDvMaintainer deletionVectorsMaintainer;
        /** 提交增量信息 */
        protected final CommitIncrement commitIncrement;

        protected State(
                BinaryRow partition,
                int bucket,
                int totalBuckets,
                long baseSnapshotId,
                long lastModifiedCommitIdentifier,
                Collection<DataFileMeta> dataFiles,
                long maxSequenceNumber,
                @Nullable DynamicBucketIndexMaintainer indexMaintainer,
                @Nullable BucketedDvMaintainer deletionVectorsMaintainer,
                CommitIncrement commitIncrement) {
            this.partition = partition;
            this.bucket = bucket;
            this.totalBuckets = totalBuckets;
            this.baseSnapshotId = baseSnapshotId;
            this.lastModifiedCommitIdentifier = lastModifiedCommitIdentifier;
            this.dataFiles = new ArrayList<>(dataFiles);
            this.maxSequenceNumber = maxSequenceNumber;
            this.indexMaintainer = indexMaintainer;
            this.deletionVectorsMaintainer = deletionVectorsMaintainer;
            this.commitIncrement = commitIncrement;
        }

        @Override
        public String toString() {
            return String.format(
                    "{%s, %d, %d, %d, %d, %s, %d, %s, %s, %s}",
                    partition,
                    bucket,
                    totalBuckets,
                    baseSnapshotId,
                    lastModifiedCommitIdentifier,
                    dataFiles,
                    maxSequenceNumber,
                    indexMaintainer,
                    deletionVectorsMaintainer,
                    commitIncrement);
        }
    }
}
