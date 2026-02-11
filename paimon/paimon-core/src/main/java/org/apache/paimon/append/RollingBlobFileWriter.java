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

import org.apache.paimon.data.BlobConsumer;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.FileWriterAbortExecutor;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.io.RowDataFileWriter;
import org.apache.paimon.io.SingleFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.types.BlobType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StatsCollectorFactories;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * 滚动 Blob 文件写入器
 *
 * <p>RollingBlobFileWriter 处理同时包含普通字段和 BLOB 字段的行数据,
 * 将它们分别写入不同的文件,并确保行对应关系的一致性。
 *
 * <p>文件分离策略:
 * <pre>
 * 表Schema: id INT, name STRING, image BLOB, video BLOB
 *
 * 分离为:
 * 1. 普通文件(Normal File): id, name
 *    → data-{uuid}.parquet
 *
 * 2. BLOB文件(Blob Files): image, video
 *    → data-{uuid}-image.blob
 *    → data-{uuid}-video.blob
 * </pre>
 *
 * <p>滚动机制:
 * 当普通文件达到目标大小时,同时滚动普通文件和所有 BLOB 文件:
 * <pre>
 * 文件对应关系:
 * Normal file1: f1.parquet (1000 rows)
 *   ├── image1.blob (100 rows)
 *   ├── image2.blob (400 rows)
 *   ├── image3.blob (500 rows)
 *   ├── video1.blob (200 rows)
 *   └── video2.blob (800 rows)
 *
 * Normal file2: f2.parquet (800 rows)
 *   ├── image4.blob (600 rows)
 *   ├── image5.blob (200 rows)
 *   └── video3.blob (800 rows)
 * </pre>
 *
 * <p>一致性校验:
 * 确保普通文件和 BLOB 文件的行数一致:
 * <ul>
 *   <li>普通文件行数 = 1000
 *   <li>每个 BLOB 字段的总行数 = 1000 (image1+image2+image3 = 100+400+500)
 *   <li>如果行数不匹配,抛出 {@code IllegalStateException}
 * </ul>
 *
 * <p>写入流程:
 * <pre>
 * 1. write(row):
 *    - currentWriter 写入普通字段(id, name)
 *    - blobWriter 写入 BLOB 字段(image, video)
 *    - 每 CHECK_ROLLING_RECORD_CNT(1000) 行检查是否滚动
 *
 * 2. rollingFile():
 *    - 检查 currentWriter 是否达到 targetFileSize
 *    - 如果是,调用 closeCurrentWriter()
 *
 * 3. closeCurrentWriter():
 *    - 关闭 currentWriter,获取 mainDataFileMeta
 *    - 关闭 blobWriter,获取 blobMetas 列表
 *    - 校验行数一致性
 *    - 将所有文件元数据添加到 results
 * </pre>
 *
 * <p>使用场景:
 * 适用于包含大字段(BLOB)的 Append-Only 表:
 * <ul>
 *   <li>日志表:普通字段(时间戳、级别、消息) + BLOB(堆栈跟踪、附件)
 *   <li>媒体表:普通字段(ID、标题、描述) + BLOB(图片、视频)
 *   <li>文档表:普通字段(元数据) + BLOB(PDF、Word 文档)
 * </ul>
 *
 * <p>配置参数:
 * <ul>
 *   <li>{@code targetFileSize}: 普通文件的目标大小,控制滚动频率
 *   <li>{@code blobTargetFileSize}: BLOB 文件的目标大小,独立控制
 * </ul>
 *
 * <p>异常处理:
 * <ul>
 *   <li>写入失败时自动调用 {@code abort()},清理所有临时文件
 *   <li>关闭失败时记录警告日志并清理资源
 * </ul>
 *
 * @see MultipleBlobFileWriter 多 Blob 文件写入器
 * @see ProjectedFileWriter 投影文件写入器
 * @see RollingFileWriter 滚动文件写入器接口
 */
public class RollingBlobFileWriter implements RollingFileWriter<InternalRow, DataFileMeta> {

    private static final Logger LOG = LoggerFactory.getLogger(RollingBlobFileWriter.class);

    /** Constant for checking rolling condition periodically. */
    private static final long CHECK_ROLLING_RECORD_CNT = 1000L;

    // Core components
    private final Supplier<
                    ProjectedFileWriter<SingleFileWriter<InternalRow, DataFileMeta>, DataFileMeta>>
            writerFactory;
    private final Supplier<MultipleBlobFileWriter> blobWriterFactory;
    private final long targetFileSize;

    // State management
    private final List<FileWriterAbortExecutor> closedWriters;
    private final List<DataFileMeta> results;

    private ProjectedFileWriter<SingleFileWriter<InternalRow, DataFileMeta>, DataFileMeta>
            currentWriter;
    private MultipleBlobFileWriter blobWriter;
    private long recordCount = 0;
    private boolean closed = false;

    public RollingBlobFileWriter(
            FileIO fileIO,
            long schemaId,
            FileFormat fileFormat,
            long targetFileSize,
            long blobTargetFileSize,
            RowType writeSchema,
            DataFilePathFactory pathFactory,
            Supplier<LongCounter> seqNumCounterSupplier,
            String fileCompression,
            StatsCollectorFactories statsCollectorFactories,
            FileIndexOptions fileIndexOptions,
            FileSource fileSource,
            boolean asyncFileWrite,
            boolean statsDenseStore,
            @Nullable BlobConsumer blobConsumer) {
        // Initialize basic fields
        this.targetFileSize = targetFileSize;
        this.results = new ArrayList<>();
        this.closedWriters = new ArrayList<>();

        // Initialize writer factory for normal data
        this.writerFactory =
                createNormalWriterFactory(
                        fileIO,
                        schemaId,
                        fileFormat,
                        BlobType.splitBlob(writeSchema).getLeft(),
                        writeSchema,
                        pathFactory,
                        seqNumCounterSupplier,
                        fileCompression,
                        statsCollectorFactories,
                        fileIndexOptions,
                        fileSource,
                        asyncFileWrite,
                        statsDenseStore);

        // Initialize blob writer
        this.blobWriterFactory =
                () ->
                        new MultipleBlobFileWriter(
                                fileIO,
                                schemaId,
                                writeSchema,
                                pathFactory,
                                seqNumCounterSupplier,
                                fileSource,
                                asyncFileWrite,
                                statsDenseStore,
                                blobTargetFileSize,
                                blobConsumer);
    }

    /** Creates a factory for normal data writers. */
    private static Supplier<
                    ProjectedFileWriter<SingleFileWriter<InternalRow, DataFileMeta>, DataFileMeta>>
            createNormalWriterFactory(
                    FileIO fileIO,
                    long schemaId,
                    FileFormat fileFormat,
                    RowType normalRowType,
                    RowType writeSchema,
                    DataFilePathFactory pathFactory,
                    Supplier<LongCounter> seqNumCounterSupplier,
                    String fileCompression,
                    StatsCollectorFactories statsCollectorFactories,
                    FileIndexOptions fileIndexOptions,
                    FileSource fileSource,
                    boolean asyncFileWrite,
                    boolean statsDenseStore) {

        List<String> normalColumnNames = normalRowType.getFieldNames();
        int[] projectionNormalFields = writeSchema.projectIndexes(normalColumnNames);

        return () -> {
            RowDataFileWriter rowDataFileWriter =
                    new RowDataFileWriter(
                            fileIO,
                            RollingFileWriter.createFileWriterContext(
                                    fileFormat,
                                    normalRowType,
                                    statsCollectorFactories.statsCollectors(normalColumnNames),
                                    fileCompression),
                            pathFactory.newPath(),
                            normalRowType,
                            schemaId,
                            seqNumCounterSupplier,
                            fileIndexOptions,
                            fileSource,
                            asyncFileWrite,
                            statsDenseStore,
                            pathFactory.isExternalPath(),
                            normalColumnNames);
            return new ProjectedFileWriter<>(rowDataFileWriter, projectionNormalFields);
        };
    }

    /**
     * Writes a single row to both normal and blob writers. Automatically handles file rolling when
     * target size is reached.
     *
     * @param row The row to write
     * @throws IOException if writing fails
     */
    @Override
    public void write(InternalRow row) throws IOException {
        try {
            if (currentWriter == null) {
                currentWriter = writerFactory.get();
            }
            if (blobWriter == null) {
                blobWriter = blobWriterFactory.get();
            }
            currentWriter.write(row);
            blobWriter.write(row);
            recordCount++;

            if (rollingFile()) {
                closeCurrentWriter();
            }
        } catch (Throwable e) {
            handleWriteException(e);
            throw e;
        }
    }

    /** Handles write exceptions by logging and cleaning up resources. */
    private void handleWriteException(Throwable e) {
        String filePath = (currentWriter == null) ? null : currentWriter.writer().path().toString();
        LOG.warn("Exception occurs when writing file {}. Cleaning up.", filePath, e);
        abort();
    }

    /**
     * Writes a bundle of records by iterating through each row.
     *
     * @param bundle The bundle of records to write
     * @throws IOException if writing fails
     */
    @Override
    public void writeBundle(BundleRecords bundle) throws IOException {
        // TODO: support bundle projection
        for (InternalRow row : bundle) {
            write(row);
        }
    }

    /**
     * Returns the total number of records written.
     *
     * @return the record count
     */
    @Override
    public long recordCount() {
        return recordCount;
    }

    /**
     * Aborts all writers and cleans up resources. This method should be called when an error occurs
     * during writing.
     */
    @Override
    public void abort() {
        if (currentWriter != null) {
            currentWriter.abort();
            currentWriter = null;
        }
        for (FileWriterAbortExecutor abortExecutor : closedWriters) {
            abortExecutor.abort();
        }
        if (blobWriter != null) {
            blobWriter.abort();
            blobWriter = null;
        }
    }

    /** Checks if the current file should be rolled based on size and record count. */
    private boolean rollingFile() throws IOException {
        return currentWriter
                .writer()
                .reachTargetSize(recordCount % CHECK_ROLLING_RECORD_CNT == 0, targetFileSize);
    }

    /**
     * Closes the current writer and processes the results. Validates consistency between main and
     * blob files.
     *
     * @throws IOException if closing fails
     */
    private void closeCurrentWriter() throws IOException {
        if (currentWriter == null) {
            return;
        }

        // Close main writer and get metadata
        DataFileMeta mainDataFileMeta = closeMainWriter();

        // Close blob writer and process blob metadata
        List<DataFileMeta> blobMetas = closeBlobWriter();

        // Validate consistency between main and blob files
        validateFileConsistency(mainDataFileMeta, blobMetas);

        // Add results to the results list
        results.add(mainDataFileMeta);
        results.addAll(blobMetas);

        // Reset current writer
        currentWriter = null;
    }

    /** Closes the main writer and returns its metadata. */
    private DataFileMeta closeMainWriter() throws IOException {
        currentWriter.close();
        currentWriter.writer().abortExecutor().ifPresent(closedWriters::add);
        return currentWriter.result();
    }

    /** Closes the blob writer and processes blob metadata with appropriate tags. */
    private List<DataFileMeta> closeBlobWriter() throws IOException {
        if (blobWriter == null) {
            return Collections.emptyList();
        }
        blobWriter.close();
        List<DataFileMeta> results = blobWriter.result();
        blobWriter = null;
        return results;
    }

    /** Validates that the row counts match between main and blob files. */
    private void validateFileConsistency(
            DataFileMeta mainDataFileMeta, List<DataFileMeta> blobTaggedMetas) {
        long mainRowCount = mainDataFileMeta.rowCount();

        Map<String, Long> blobRowCounts = new HashMap<>();
        for (DataFileMeta file : blobTaggedMetas) {
            long count = file.rowCount();
            blobRowCounts.compute(file.writeCols().get(0), (k, v) -> v == null ? count : v + count);
        }
        for (String blobFieldName : blobRowCounts.keySet()) {
            long blobRowCount = blobRowCounts.get(blobFieldName);
            if (mainRowCount != blobRowCount) {
                throw new IllegalStateException(
                        String.format(
                                "This is a bug: The row count of main file and blob file does not match. "
                                        + "Main file: %s (row count: %d), blob field name: %s (row count: %d)",
                                mainDataFileMeta, mainRowCount, blobFieldName, blobRowCount));
            }
        }
    }

    /**
     * Returns the list of file metadata for all written files. This method can only be called after
     * the writer has been closed.
     *
     * @return list of file metadata
     * @throws IllegalStateException if the writer is not closed
     */
    @Override
    public List<DataFileMeta> result() {
        Preconditions.checkState(closed, "Cannot access the results unless close all writers.");
        return results;
    }

    /**
     * Closes the writer and finalizes all files. This method ensures proper cleanup and validation
     * of written data.
     *
     * @throws IOException if closing fails
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        try {
            closeCurrentWriter();
        } catch (IOException e) {
            handleCloseException(e);
            throw e;
        } finally {
            closed = true;
        }
    }

    /** Handles exceptions that occur during closing. */
    private void handleCloseException(IOException e) {
        String filePath = (currentWriter == null) ? null : currentWriter.writer().path().toString();
        LOG.warn("Exception occurs when writing file {}. Cleaning up.", filePath, e);
        abort();
    }
}
