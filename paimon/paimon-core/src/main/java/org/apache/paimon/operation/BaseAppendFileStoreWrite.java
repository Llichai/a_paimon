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

import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.AppendOnlyWriter;
import org.apache.paimon.append.cluster.Sorter;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BlobConsumer;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.RowDataRollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.IOExceptionSupplier;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.MutableObjectIterator;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.StatsCollectorFactories;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.paimon.format.FileFormat.fileFormat;
import static org.apache.paimon.types.DataTypeRoot.BLOB;
import static org.apache.paimon.utils.StatsCollectorFactories.createStatsFactories;

/**
 * 基础追加写入实现 - 处理 Append-Only 表的无序写入和滚动文件策略
 *
 * <p>该类是所有 Append-Only 表写入实现的基类，相比主键表（KeyValue表）有以下特点：
 * <ul>
 *   <li><b>无序写入</b>：不需要按照主键排序，直接追加写入
 *   <li><b>无合并操作</b>：不需要 merge function，没有 +I/-U/-D 等操作语义
 *   <li><b>支持部分列写入</b>：可以只写入表的部分列（通过 writeCols 配置）
 *   <li><b>BLOB 字段支持</b>：对 BLOB 大字段有特殊处理（独立文件存储）
 * </ul>
 *
 * <p><b>滚动文件策略（Rolling File）</b>：
 * <ul>
 *   <li><b>目标文件大小</b>：{@code file.target-file-size}（默认 128MB）
 *   <li><b>滚动条件</b>：
 *       <ul>
 *         <li>当前文件大小达到目标大小 -> 关闭当前文件，创建新文件
 *         <li>强制 flush -> 关闭当前文件
 *         <li>Checkpoint -> 关闭所有打开的文件
 *       </ul>
 *   </li>
 *   <li><b>BLOB 文件单独滚动</b>：
 *       <ul>
 *         <li>BLOB 字段存储在独立文件中（blob-target-file-size）
 *         <li>普通列和 BLOB 列使用不同的文件大小阈值
 *         <li>避免 BLOB 大字段影响普通数据的文件大小控制
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p><b>无序写入处理</b>：
 * <ul>
 *   <li><b>问题</b>：
 *       <ul>
 *         <li>批处理作业可能乱序写入不同分区
 *         <li>需要为每个分区维护独立的 Writer
 *         <li>Writer 数量可能非常多（分区数 × bucket 数）
 *       </ul>
 *   </li>
 *   <li><b>解决方案</b>：
 *       <ul>
 *         <li>使用 write buffer + spillable 机制
 *         <li>当 Writer 数量超过阈值，将内存数据溢写到磁盘
 *         <li>配置项：
 *             <ul>
 *               <li>{@code write.buffer-for-append}：是否启用写缓冲（默认 false）
 *               <li>{@code write.buffer-spillable}：缓冲区是否可溢写（默认 false）
 *             </ul>
 *         </li>
 *       </ul>
 *   </li>
 *   <li><b>强制溢写模式</b>：
 *       <ul>
 *         <li>当 Writer 数量超过 {@code write.max-writers-to-spill} 时自动触发
 *         <li>调用 {@link #forceBufferSpill()} 强制所有 Writer 使用可溢写缓冲区
 *         <li>避免内存溢出（OOM）
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p><b>压缩（Compaction）机制</b>：
 * <ul>
 *   <li>虽然是 Append-Only 表，但仍然需要压缩以控制文件数量
 *   <li>压缩类型：
 *       <ul>
 *         <li><b>文件合并</b>：将多个小文件合并为大文件（{@link #compactRewrite}）
 *         <li><b>排序聚簇</b>：对文件内容按照聚簇键排序（{@link #clusterRewrite}）
 *       </ul>
 *   </li>
 *   <li>压缩管理器：
 *       <ul>
 *         <li>BucketedAppendCompactManager：普通压缩（合并小文件）
 *         <li>BucketedAppendClusterManager：聚簇压缩（排序 + 合并）
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p><b>部分列写入（Partial Column Write）</b>：
 * <ul>
 *   <li><b>使用场景</b>：
 *       <ul>
 *         <li>只写入表的部分列（如 INSERT INTO t(a, b) VALUES ...）
 *         <li>其他列保持 NULL 或默认值
 *       </ul>
 *   </li>
 *   <li><b>实现方式</b>：
 *       <ul>
 *         <li>通过 {@link #withWriteType(RowType)} 指定要写入的列
 *         <li>writeType 包含实际写入的列，rowType 包含表的所有列
 *         <li>Writer 只序列化 writeType 中的列
 *       </ul>
 *   </li>
 *   <li><b>优化</b>：
 *       <ul>
 *         <li>如果 writeType 包含所有列，则 writeCols = null（避免额外开销）
 *         <li>如果 writeType 是部分列，则 writeCols 记录列名列表
 *       </ul>
 *   </li>
 *   <li><b>示例</b>：
 *       <pre>
 *       表定义：t(id INT, name STRING, age INT, address STRING)
 *       写入语句：INSERT INTO t(id, name) VALUES (1, 'Alice')
 *       - rowType = [id, name, age, address]
 *       - writeType = [id, name]
 *       - writeCols = ['id', 'name']
 *       - 文件中只存储 id 和 name，age 和 address 为 NULL
 *       </pre>
 *   </li>
 * </ul>
 *
 * <p><b>BLOB 字段处理</b>：
 * <ul>
 *   <li><b>检测 BLOB 列</b>：
 *       <pre>
 *       withBlob = rowType.getFieldTypes().stream().anyMatch(t -> t.is(BLOB))
 *       </pre>
 *   </li>
 *   <li><b>BLOB 文件存储</b>：
 *       <ul>
 *         <li>BLOB 数据不内联存储在主数据文件中
 *         <li>单独写入 blob 文件（blob-xxx.bin）
 *         <li>主数据文件中存储 BLOB 文件的引用（路径）
 *       </ul>
 *   </li>
 *   <li><b>BLOB 文件大小控制</b>：
 *       <ul>
 *         <li>配置项：{@code file.blob-target-file-size}（默认 1GB）
 *         <li>独立于普通文件的大小控制
 *       </ul>
 *   </li>
 *   <li><b>BLOB 消费器</b>：
 *       <ul>
 *         <li>通过 {@link #withBlobConsumer(BlobConsumer)} 设置
 *         <li>用于外部系统消费 BLOB 数据（如发送到对象存储）
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p><b>压缩重写（Compact Rewrite）</b>：
 * <ul>
 *   <li><b>{@link #compactRewrite}</b>：合并多个文件
 *       <ul>
 *         <li>读取所有待压缩文件的数据
 *         <li>应用 Deletion Vector（如果有）过滤删除的行
 *         <li>按照目标文件大小重新写入
 *         <li>返回新生成的文件列表
 *       </ul>
 *   </li>
 *   <li><b>{@link #clusterRewrite}</b>：排序 + 合并
 *       <ul>
 *         <li>读取所有待压缩文件的数据
 *         <li>使用外部排序器（Sorter）对数据排序
 *         <li>按照排序后的顺序重新写入
 *         <li>提高读取性能（顺序扫描更高效）
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p><b>强制缓冲区溢写</b>：
 * <ul>
 *   <li>当 Writer 数量超过阈值时，调用 {@link #forceBufferSpill()}
 *   <li>实现：
 *       <pre>
 *       forceBufferSpill():
 *       1. 设置 forceBufferSpill = true
 *       2. 遍历所有 Writer
 *       3. 调用 writer.toBufferedWriter() 切换到可溢写模式
 *       4. Writer 将内存数据写入临时文件
 *       5. 继续写入新数据到临时文件
 *       </pre>
 *   </li>
 *   <li>注意：
 *       <ul>
 *         <li>如果 ioManager == null，无法溢写（需要磁盘支持）
 *         <li>如果包含 BLOB 字段，不支持溢写（BLOB 文件已经是磁盘文件）
 *         <li>只在首次触发时生效（forceBufferSpill 标记防止重复调用）
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p><b>Bundle 写入（批量写入优化）</b>：
 * <ul>
 *   <li>通过 {@link BundleFileStoreWriter#writeBundle} 接口支持
 *   <li>批量提交一组记录，减少函数调用开销
 *   <li>适用于高吞吐场景（如实时数仓）
 * </ul>
 *
 * @see AppendOnlyWriter 实际的追加写入器
 * @see BucketedAppendFileStoreWrite 分桶追加写入（Fixed Bucket）
 * @see UnAwareBucketAppendFileStoreWrite 无分桶追加写入（Unaware Bucket）
 */
public abstract class BaseAppendFileStoreWrite extends MemoryFileStoreWrite<InternalRow>
        implements BundleFileStoreWriter {

    private static final Logger LOG = LoggerFactory.getLogger(BaseAppendFileStoreWrite.class);

    private final FileIO fileIO;
    private final RawFileSplitRead readForCompact;
    private final long schemaId;
    private final FileFormat fileFormat;
    private final FileStorePathFactory pathFactory;
    private final FileIndexOptions fileIndexOptions;
    private final RowType rowType;

    private RowType writeType;
    private @Nullable List<String> writeCols;
    private boolean forceBufferSpill = false;
    private boolean withBlob;
    private @Nullable BlobConsumer blobConsumer;

    public BaseAppendFileStoreWrite(
            FileIO fileIO,
            RawFileSplitRead readForCompact,
            long schemaId,
            RowType rowType,
            RowType partitionType,
            FileStorePathFactory pathFactory,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            CoreOptions options,
            @Nullable BucketedDvMaintainer.Factory dvMaintainerFactory,
            String tableName) {
        super(snapshotManager, scan, options, partitionType, null, dvMaintainerFactory, tableName);
        this.fileIO = fileIO;
        this.readForCompact = readForCompact;
        this.schemaId = schemaId;
        this.rowType = rowType;
        this.writeType = rowType;
        this.writeCols = null;
        this.fileFormat = fileFormat(options);
        this.pathFactory = pathFactory;
        this.withBlob = rowType.getFieldTypes().stream().anyMatch(t -> t.is(BLOB));

        this.fileIndexOptions = options.indexColumnsOptions();
    }

    @Override
    public BaseAppendFileStoreWrite withBlobConsumer(BlobConsumer blobConsumer) {
        this.blobConsumer = blobConsumer;
        return this;
    }

    @Override
    protected RecordWriter<InternalRow> createWriter(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> restoredFiles,
            long restoredMaxSeqNumber,
            @Nullable CommitIncrement restoreIncrement,
            ExecutorService compactExecutor,
            @Nullable BucketedDvMaintainer dvMaintainer) {
        return new AppendOnlyWriter(
                fileIO,
                ioManager,
                schemaId,
                fileFormat,
                options.targetFileSize(false),
                options.blobTargetFileSize(),
                writeType,
                writeCols,
                restoredMaxSeqNumber,
                getCompactManager(partition, bucket, restoredFiles, compactExecutor, dvMaintainer),
                // it is only for new files, no dv
                files -> createFilesIterator(partition, bucket, files, null),
                options.commitForceCompact(),
                pathFactory.createDataFilePathFactory(partition, bucket),
                restoreIncrement,
                options.useWriteBufferForAppend() || forceBufferSpill,
                options.writeBufferSpillable() || forceBufferSpill,
                options.fileCompression(),
                options.spillCompressOptions(),
                new StatsCollectorFactories(options),
                options.writeBufferSpillDiskSize(),
                fileIndexOptions,
                options.asyncFileWrite(),
                options.statsDenseStore(),
                blobConsumer,
                options.dataEvolutionEnabled());
    }

    @Override
    public void withWriteType(RowType writeType) {
        this.writeType = writeType;
        this.withBlob = writeType.getFieldTypes().stream().anyMatch(t -> t.is(BLOB));
        int fullCount = rowType.getFieldCount();
        List<String> fullNames = rowType.getFieldNames();
        this.writeCols = writeType.getFieldNames();
        // optimize writeCols to null in following cases:
        // writeType contains all columns (without _ROW_ID and _SEQUENCE_NUMBER)
        if (writeCols.equals(fullNames)) {
            writeCols = null;
        }
    }

    private SimpleColStatsCollector.Factory[] statsCollectors() {
        return createStatsFactories(options.statsMode(), options, writeType.getFieldNames());
    }

    protected abstract CompactManager getCompactManager(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> restoredFiles,
            ExecutorService compactExecutor,
            @Nullable BucketedDvMaintainer dvMaintainer);

    public List<DataFileMeta> compactRewrite(
            BinaryRow partition,
            int bucket,
            @Nullable Function<String, DeletionVector> dvFactory,
            List<DataFileMeta> toCompact)
            throws Exception {
        if (toCompact.isEmpty()) {
            return Collections.emptyList();
        }
        Exception collectedExceptions = null;
        RowDataRollingFileWriter rewriter =
                createRollingFileWriter(
                        partition,
                        bucket,
                        () -> new LongCounter(toCompact.get(0).minSequenceNumber()));
        Map<String, IOExceptionSupplier<DeletionVector>> dvFactories = null;
        if (dvFactory != null) {
            dvFactories = new HashMap<>();
            for (DataFileMeta file : toCompact) {
                dvFactories.put(file.fileName(), () -> dvFactory.apply(file.fileName()));
            }
        }
        try {
            rewriter.write(createFilesIterator(partition, bucket, toCompact, dvFactories));
        } catch (Exception e) {
            collectedExceptions = e;
        } finally {
            try {
                rewriter.close();
            } catch (Exception e) {
                collectedExceptions = ExceptionUtils.firstOrSuppressed(e, collectedExceptions);
            }
        }
        if (collectedExceptions != null) {
            throw collectedExceptions;
        }
        return rewriter.result();
    }

    public List<DataFileMeta> clusterRewrite(
            BinaryRow partition, int bucket, List<DataFileMeta> toCluster) throws Exception {
        RecordReaderIterator<InternalRow> reader =
                createFilesIterator(partition, bucket, toCluster, null);

        // sort and rewrite
        Exception collectedExceptions = null;
        Sorter sorter = Sorter.getSorter(reader, ioManager, rowType, options);
        RowDataRollingFileWriter rewriter =
                createRollingFileWriter(
                        partition,
                        bucket,
                        () -> new LongCounter(toCluster.get(0).minSequenceNumber()));
        try {
            MutableObjectIterator<BinaryRow> sorted = sorter.sort();
            BinaryRow binaryRow = new BinaryRow(sorter.arity());
            while ((binaryRow = sorted.next(binaryRow)) != null) {
                InternalRow rowRemovedKey = sorter.removeSortKey(binaryRow);
                rewriter.write(rowRemovedKey);
            }
        } catch (Exception e) {
            collectedExceptions = e;
        } finally {
            try {
                rewriter.close();
                sorter.close();
            } catch (Exception e) {
                collectedExceptions = ExceptionUtils.firstOrSuppressed(e, collectedExceptions);
            }
        }

        if (collectedExceptions != null) {
            throw collectedExceptions;
        }

        return rewriter.result();
    }

    private RowDataRollingFileWriter createRollingFileWriter(
            BinaryRow partition, int bucket, Supplier<LongCounter> seqNumCounterSupplier) {
        return new RowDataRollingFileWriter(
                fileIO,
                schemaId,
                fileFormat,
                options.targetFileSize(false),
                writeType,
                pathFactory.createDataFilePathFactory(partition, bucket),
                seqNumCounterSupplier,
                options.fileCompression(),
                statsCollectors(),
                fileIndexOptions,
                FileSource.COMPACT,
                options.asyncFileWrite(),
                options.statsDenseStore(),
                rowType.equals(writeType) ? null : writeType.getFieldNames());
    }

    private RecordReaderIterator<InternalRow> createFilesIterator(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            @Nullable Map<String, IOExceptionSupplier<DeletionVector>> dvFactories)
            throws IOException {
        return new RecordReaderIterator<>(
                readForCompact.createReader(partition, bucket, files, dvFactories));
    }

    @Override
    protected void forceBufferSpill() throws Exception {
        if (ioManager == null) {
            return;
        }
        if (withBlob) {
            return;
        }
        if (forceBufferSpill) {
            return;
        }
        forceBufferSpill = true;
        LOG.info(
                "Force buffer spill for append-only file store write, writer number is: {}",
                writers.size());
        for (Map<Integer, WriterContainer<InternalRow>> bucketWriters : writers.values()) {
            for (WriterContainer<InternalRow> writerContainer : bucketWriters.values()) {
                ((AppendOnlyWriter) writerContainer.writer).toBufferedWriter();
            }
        }
    }

    @Override
    public void writeBundle(BinaryRow partition, int bucket, BundleRecords bundle)
            throws Exception {
        WriterContainer<InternalRow> container = getWriterWrapper(partition, bucket);
        ((AppendOnlyWriter) container.writer).writeBundle(bundle);
    }
}
