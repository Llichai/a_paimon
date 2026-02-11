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

import org.apache.paimon.FileStore;
import org.apache.paimon.casting.DefaultValueRow;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BlobConsumer;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.BundleFileStoreWriter;
import org.apache.paimon.operation.FileStoreWrite;
import org.apache.paimon.operation.FileStoreWrite.State;
import org.apache.paimon.operation.WriteRestore;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Restorable;
import org.apache.paimon.utils.RowKindFilter;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * {@link TableWrite} 的实现类。
 *
 * <p>这是 Table 层写入的核心实现，封装了底层 {@link FileStoreWrite} 并提供了更高层的 API。
 *
 * <p>主要职责：
 * <ul>
 *     <li><b>行键提取</b>：通过 {@link KeyAndBucketExtractor} 从行数据中提取分区键、分桶键和主键
 *     <li><b>记录转换</b>：通过 {@link RecordExtractor} 将 {@link SinkRecord} 转换为底层存储格式
 *     <li><b>行类型生成</b>：通过 {@link RowKindGenerator} 生成行类型（INSERT、UPDATE、DELETE）
 *     <li><b>行类型过滤</b>：通过 {@link RowKindFilter} 过滤特定的行类型
 *     <li><b>数据写入</b>：调用底层 FileStoreWrite 完成实际的文件写入
 * </ul>
 *
 * <p>架构层次：
 * <pre>
 * TableWriteImpl (Table 层实现)
 *   ├─ KeyAndBucketExtractor (提取分区、分桶、主键)
 *   ├─ RecordExtractor (转换为底层格式)
 *   ├─ RowKindGenerator (生成行类型)
 *   ├─ RowKindFilter (过滤行类型)
 *   └─ FileStoreWrite (底层文件写入)
 *       ├─ KeyValueFileStoreWrite (Primary Key 表)
 *       └─ AppendOnlyFileStoreWrite (Append-Only 表)
 * </pre>
 *
 * <p>泛型参数 {@code <T>} 的含义：
 * <ul>
 *     <li>对于 Primary Key 表：T = {@link org.apache.paimon.data.KeyValue}
 *     <li>对于 Append-Only 表：T = {@link InternalRow}
 * </ul>
 *
 * @param <T> 写入 {@link FileStore} 的记录类型
 */
public class TableWriteImpl<T> implements InnerTableWrite, Restorable<List<State<T>>> {

    /** 底层文件存储写入器 */
    private final FileStoreWrite<T> write;

    /** 行键和分桶提取器，用于从行数据中提取分区、分桶、主键 */
    private final KeyAndBucketExtractor<InternalRow> keyAndBucketExtractor;

    /** 记录提取器，用于将 SinkRecord 转换为底层存储格式 */
    private final RecordExtractor<T> recordExtractor;

    /** 行类型生成器，用于生成行类型（可选） */
    @Nullable private final RowKindGenerator rowKindGenerator;

    /** 行类型过滤器，用于过滤特定的行类型（可选） */
    @Nullable private final RowKindFilter rowKindFilter;

    /** 批量提交标志，确保批量写入只提交一次 */
    private boolean batchCommitted = false;

    /** 写入的行类型 */
    private RowType writeType;

    /** 非空字段的索引数组，用于空值检查 */
    private int[] notNullFieldIndex;

    /** 默认值行包装器，用于处理默认值（可选） */
    private final @Nullable DefaultValueRow defaultValueRow;

    /**
     * 构造函数。
     *
     * @param rowType 行类型
     * @param write 底层文件存储写入器
     * @param keyAndBucketExtractor 行键和分桶提取器
     * @param recordExtractor 记录提取器
     * @param rowKindGenerator 行类型生成器（可选）
     * @param rowKindFilter 行类型过滤器（可选）
     */
    public TableWriteImpl(
            RowType rowType,
            FileStoreWrite<T> write,
            KeyAndBucketExtractor<InternalRow> keyAndBucketExtractor,
            RecordExtractor<T> recordExtractor,
            @Nullable RowKindGenerator rowKindGenerator,
            @Nullable RowKindFilter rowKindFilter) {
        this.writeType = rowType;
        this.write = write;
        this.keyAndBucketExtractor = keyAndBucketExtractor;
        this.recordExtractor = recordExtractor;
        this.rowKindGenerator = rowKindGenerator;
        this.rowKindFilter = rowKindFilter;

        // 提取非空字段的索引，用于后续的空值检查
        List<String> notNullColumnNames =
                rowType.getFields().stream()
                        .filter(field -> !field.type().isNullable())
                        .map(DataField::name)
                        .collect(Collectors.toList());
        this.notNullFieldIndex = rowType.getFieldIndices(notNullColumnNames);
        this.defaultValueRow = DefaultValueRow.create(rowType);
    }

    /**
     * 获取底层文件存储写入器。
     *
     * @return FileStoreWrite 实例
     */
    public FileStoreWrite<T> fileStoreWrite() {
        return write;
    }

    @Override
    public InnerTableWrite withWriteRestore(WriteRestore writeRestore) {
        this.write.withWriteRestore(writeRestore);
        return this;
    }

    @Override
    public TableWriteImpl<T> withIgnorePreviousFiles(boolean ignorePreviousFiles) {
        write.withIgnorePreviousFiles(ignorePreviousFiles);
        return this;
    }

    @Override
    public TableWriteImpl<T> withIOManager(IOManager ioManager) {
        write.withIOManager(ioManager);
        return this;
    }

    @Override
    public TableWriteImpl<T> withWriteType(RowType writeType) {
        write.withWriteType(writeType);
        this.writeType = writeType;
        // 重新计算非空字段索引
        List<String> notNullColumnNames =
                writeType.getFields().stream()
                        .filter(field -> !field.type().isNullable())
                        .map(DataField::name)
                        .collect(Collectors.toList());
        this.notNullFieldIndex = writeType.getFieldIndices(notNullColumnNames);
        return this;
    }

    @Override
    public TableWriteImpl<T> withMemoryPoolFactory(MemoryPoolFactory memoryPoolFactory) {
        write.withMemoryPoolFactory(memoryPoolFactory);
        return this;
    }

    @Override
    public TableWrite withBlobConsumer(BlobConsumer blobConsumer) {
        write.withBlobConsumer(blobConsumer);
        return this;
    }

    /**
     * 设置压缩执行器，用于异步执行压缩任务。
     *
     * @param compactExecutor 压缩执行器
     * @return 当前 TableWriteImpl 实例
     */
    public TableWriteImpl<T> withCompactExecutor(ExecutorService compactExecutor) {
        write.withCompactExecutor(compactExecutor);
        return this;
    }

    @Override
    public BinaryRow getPartition(InternalRow row) {
        // 设置当前记录到提取器
        keyAndBucketExtractor.setRecord(row);
        // 提取分区键
        return keyAndBucketExtractor.partition();
    }

    @Override
    public int getBucket(InternalRow row) {
        // 设置当前记录到提取器
        keyAndBucketExtractor.setRecord(row);
        // 提取分桶号
        return keyAndBucketExtractor.bucket();
    }

    @Override
    public void write(InternalRow row) throws Exception {
        writeAndReturn(row);
    }

    @Override
    public void write(InternalRow row, int bucket) throws Exception {
        writeAndReturn(row, bucket);
    }

    @Override
    public void writeBundle(BinaryRow partition, int bucket, BundleRecords bundle)
            throws Exception {
        // 如果底层写入器支持批量写入，直接调用
        if (write instanceof BundleFileStoreWriter) {
            ((BundleFileStoreWriter) write).writeBundle(partition, bucket, bundle);
        } else {
            // 否则逐行写入
            for (InternalRow row : bundle) {
                write(row, bucket);
            }
        }
    }

    /**
     * 写入一行数据并返回 SinkRecord。
     *
     * <p>此方法会自动计算分桶号。
     *
     * @param row 待写入的行数据
     * @return SinkRecord，如果被过滤则返回 null
     * @throws Exception 写入过程中的异常
     */
    @Nullable
    public SinkRecord writeAndReturn(InternalRow row) throws Exception {
        return writeAndReturn(row, -1);
    }

    /**
     * 写入一行数据并返回 SinkRecord。
     *
     * <p>写入流程：
     * <ol>
     *     <li>检查非空字段的空值
     *     <li>包装默认值
     *     <li>生成行类型（INSERT、UPDATE、DELETE）
     *     <li>过滤不需要的行类型
     *     <li>转换为 SinkRecord
     *     <li>调用底层 FileStoreWrite 写入
     * </ol>
     *
     * @param row 待写入的行数据
     * @param bucket 分桶号，-1 表示自动计算
     * @return SinkRecord，如果被过滤则返回 null
     * @throws Exception 写入过程中的异常
     */
    @Nullable
    public SinkRecord writeAndReturn(InternalRow row, int bucket) throws Exception {
        // 1. 检查非空字段
        checkNullability(row);
        // 2. 包装默认值
        row = wrapDefaultValue(row);
        // 3. 生成行类型
        RowKind rowKind = RowKindGenerator.getRowKind(rowKindGenerator, row);
        // 4. 过滤行类型
        if (rowKindFilter != null && !rowKindFilter.test(rowKind)) {
            return null;
        }
        // 5. 转换为 SinkRecord
        SinkRecord record = bucket == -1 ? toSinkRecord(row) : toSinkRecord(row, bucket);
        // 6. 调用底层 FileStoreWrite 写入
        write.write(record.partition(), record.bucket(), recordExtractor.extract(record, rowKind));
        return record;
    }

    /**
     * 检查非空字段的空值。
     *
     * @param row 待检查的行数据
     * @throws RuntimeException 如果非空字段为空
     */
    private void checkNullability(InternalRow row) {
        for (int idx : notNullFieldIndex) {
            if (row.isNullAt(idx)) {
                String columnName = writeType.getFields().get(idx).name();
                throw new RuntimeException(
                        String.format("Cannot write null to non-null column(%s)", columnName));
            }
        }
    }

    /**
     * 包装默认值。
     *
     * <p>如果字段定义了默认值，且当前值为空，则使用默认值。
     *
     * @param row 原始行数据
     * @return 包装后的行数据
     */
    private InternalRow wrapDefaultValue(InternalRow row) {
        return defaultValueRow == null ? row : defaultValueRow.replaceRow(row);
    }

    /**
     * 将行数据转换为 SinkRecord，自动计算分桶。
     *
     * @param row 行数据
     * @return SinkRecord
     */
    private SinkRecord toSinkRecord(InternalRow row) {
        keyAndBucketExtractor.setRecord(row);
        return new SinkRecord(
                keyAndBucketExtractor.partition(),
                keyAndBucketExtractor.bucket(),
                keyAndBucketExtractor.trimmedPrimaryKey(),
                row);
    }

    /**
     * 将行数据转换为 SinkRecord，使用指定的分桶。
     *
     * @param row 行数据
     * @param bucket 分桶号
     * @return SinkRecord
     */
    private SinkRecord toSinkRecord(InternalRow row, int bucket) {
        keyAndBucketExtractor.setRecord(row);
        return new SinkRecord(
                keyAndBucketExtractor.partition(),
                bucket,
                keyAndBucketExtractor.trimmedPrimaryKey(),
                row);
    }

    @Override
    public void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception {
        write.compact(partition, bucket, fullCompaction);
    }

    @Override
    public TableWriteImpl<T> withMetricRegistry(MetricRegistry metricRegistry) {
        write.withMetricRegistry(metricRegistry);
        return this;
    }

    /**
     * 通知新文件的创建。
     *
     * <p>用于通知在给定快照的给定分桶中创建了新文件。
     * 通常这些文件是由其他任务创建的。当前主要用于专用的压缩任务，
     * 以便感知写入任务创建的文件。
     *
     * @param snapshotId 快照 ID
     * @param partition 分区
     * @param bucket 分桶号
     * @param files 新创建的文件列表
     */
    public void notifyNewFiles(
            long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files) {
        write.notifyNewFiles(snapshotId, partition, bucket, files);
    }

    @Override
    public List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier)
            throws Exception {
        return write.prepareCommit(waitCompaction, commitIdentifier);
    }

    @Override
    public List<CommitMessage> prepareCommit() throws Exception {
        checkState(!batchCommitted, "BatchTableWrite only support one-time committing.");
        batchCommitted = true;
        // 批量写入使用固定的提交标识符，等待压缩完成
        return prepareCommit(true, BatchWriteBuilder.COMMIT_IDENTIFIER);
    }

    @Override
    public void close() throws Exception {
        write.close();
    }

    @Override
    public List<State<T>> checkpoint() {
        return write.checkpoint();
    }

    @Override
    public void restore(List<State<T>> state) {
        write.restore(state);
    }

    /**
     * 获取底层写入器。
     *
     * @return FileStoreWrite 实例
     */
    public FileStoreWrite<T> getWrite() {
        return write;
    }

    /**
     * 记录提取器接口，用于从 {@link SinkRecord} 中提取底层存储格式的记录。
     *
     * <p>不同的表类型有不同的提取器实现：
     * <ul>
     *     <li>Primary Key 表：提取 KeyValue（包含 key 和 value）
     *     <li>Append-Only 表：直接使用 InternalRow
     * </ul>
     *
     * @param <T> 提取的记录类型
     */
    public interface RecordExtractor<T> {

        /**
         * 从 SinkRecord 中提取记录。
         *
         * @param record Sink 记录
         * @param rowKind 行类型
         * @return 提取的记录
         */
        T extract(SinkRecord record, RowKind rowKind);
    }
}

    public TableWriteImpl(
            RowType rowType,
            FileStoreWrite<T> write,
            KeyAndBucketExtractor<InternalRow> keyAndBucketExtractor,
            RecordExtractor<T> recordExtractor,
            @Nullable RowKindGenerator rowKindGenerator,
            @Nullable RowKindFilter rowKindFilter) {
        this.writeType = rowType;
        this.write = write;
        this.keyAndBucketExtractor = keyAndBucketExtractor;
        this.recordExtractor = recordExtractor;
        this.rowKindGenerator = rowKindGenerator;
        this.rowKindFilter = rowKindFilter;

        List<String> notNullColumnNames =
                rowType.getFields().stream()
                        .filter(field -> !field.type().isNullable())
                        .map(DataField::name)
                        .collect(Collectors.toList());
        this.notNullFieldIndex = rowType.getFieldIndices(notNullColumnNames);
        this.defaultValueRow = DefaultValueRow.create(rowType);
    }

    public FileStoreWrite<T> fileStoreWrite() {
        return write;
    }

    @Override
    public InnerTableWrite withWriteRestore(WriteRestore writeRestore) {
        this.write.withWriteRestore(writeRestore);
        return this;
    }

    @Override
    public TableWriteImpl<T> withIgnorePreviousFiles(boolean ignorePreviousFiles) {
        write.withIgnorePreviousFiles(ignorePreviousFiles);
        return this;
    }

    @Override
    public TableWriteImpl<T> withIOManager(IOManager ioManager) {
        write.withIOManager(ioManager);
        return this;
    }

    @Override
    public TableWriteImpl<T> withWriteType(RowType writeType) {
        write.withWriteType(writeType);
        this.writeType = writeType;
        List<String> notNullColumnNames =
                writeType.getFields().stream()
                        .filter(field -> !field.type().isNullable())
                        .map(DataField::name)
                        .collect(Collectors.toList());
        this.notNullFieldIndex = writeType.getFieldIndices(notNullColumnNames);
        return this;
    }

    @Override
    public TableWriteImpl<T> withMemoryPoolFactory(MemoryPoolFactory memoryPoolFactory) {
        write.withMemoryPoolFactory(memoryPoolFactory);
        return this;
    }

    @Override
    public TableWrite withBlobConsumer(BlobConsumer blobConsumer) {
        write.withBlobConsumer(blobConsumer);
        return this;
    }

    public TableWriteImpl<T> withCompactExecutor(ExecutorService compactExecutor) {
        write.withCompactExecutor(compactExecutor);
        return this;
    }

    @Override
    public BinaryRow getPartition(InternalRow row) {
        keyAndBucketExtractor.setRecord(row);
        return keyAndBucketExtractor.partition();
    }

    @Override
    public int getBucket(InternalRow row) {
        keyAndBucketExtractor.setRecord(row);
        return keyAndBucketExtractor.bucket();
    }

    @Override
    public void write(InternalRow row) throws Exception {
        writeAndReturn(row);
    }

    @Override
    public void write(InternalRow row, int bucket) throws Exception {
        writeAndReturn(row, bucket);
    }

    @Override
    public void writeBundle(BinaryRow partition, int bucket, BundleRecords bundle)
            throws Exception {
        if (write instanceof BundleFileStoreWriter) {
            ((BundleFileStoreWriter) write).writeBundle(partition, bucket, bundle);
        } else {
            for (InternalRow row : bundle) {
                write(row, bucket);
            }
        }
    }

    @Nullable
    public SinkRecord writeAndReturn(InternalRow row) throws Exception {
        return writeAndReturn(row, -1);
    }

    @Nullable
    public SinkRecord writeAndReturn(InternalRow row, int bucket) throws Exception {
        checkNullability(row);
        row = wrapDefaultValue(row);
        RowKind rowKind = RowKindGenerator.getRowKind(rowKindGenerator, row);
        if (rowKindFilter != null && !rowKindFilter.test(rowKind)) {
            return null;
        }
        SinkRecord record = bucket == -1 ? toSinkRecord(row) : toSinkRecord(row, bucket);
        write.write(record.partition(), record.bucket(), recordExtractor.extract(record, rowKind));
        return record;
    }

    private void checkNullability(InternalRow row) {
        for (int idx : notNullFieldIndex) {
            if (row.isNullAt(idx)) {
                String columnName = writeType.getFields().get(idx).name();
                throw new RuntimeException(
                        String.format("Cannot write null to non-null column(%s)", columnName));
            }
        }
    }

    private InternalRow wrapDefaultValue(InternalRow row) {
        return defaultValueRow == null ? row : defaultValueRow.replaceRow(row);
    }

    private SinkRecord toSinkRecord(InternalRow row) {
        keyAndBucketExtractor.setRecord(row);
        return new SinkRecord(
                keyAndBucketExtractor.partition(),
                keyAndBucketExtractor.bucket(),
                keyAndBucketExtractor.trimmedPrimaryKey(),
                row);
    }

    private SinkRecord toSinkRecord(InternalRow row, int bucket) {
        keyAndBucketExtractor.setRecord(row);
        return new SinkRecord(
                keyAndBucketExtractor.partition(),
                bucket,
                keyAndBucketExtractor.trimmedPrimaryKey(),
                row);
    }

    @Override
    public void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception {
        write.compact(partition, bucket, fullCompaction);
    }

    @Override
    public TableWriteImpl<T> withMetricRegistry(MetricRegistry metricRegistry) {
        write.withMetricRegistry(metricRegistry);
        return this;
    }

    /**
     * Notify that some new files are created at given snapshot in given bucket.
     *
     * <p>Most probably, these files are created by another job. Currently this method is only used
     * by the dedicated compact job to see files created by writer jobs.
     */
    public void notifyNewFiles(
            long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files) {
        write.notifyNewFiles(snapshotId, partition, bucket, files);
    }

    @Override
    public List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier)
            throws Exception {
        return write.prepareCommit(waitCompaction, commitIdentifier);
    }

    @Override
    public List<CommitMessage> prepareCommit() throws Exception {
        checkState(!batchCommitted, "BatchTableWrite only support one-time committing.");
        batchCommitted = true;
        return prepareCommit(true, BatchWriteBuilder.COMMIT_IDENTIFIER);
    }

    @Override
    public void close() throws Exception {
        write.close();
    }

    @Override
    public List<State<T>> checkpoint() {
        return write.checkpoint();
    }

    @Override
    public void restore(List<State<T>> state) {
        write.restore(state);
    }

    public FileStoreWrite<T> getWrite() {
        return write;
    }

    /** Extractor to extract {@link T} from the {@link SinkRecord}. */
    public interface RecordExtractor<T> {

        T extract(SinkRecord record, RowKind rowKind);
    }
}
