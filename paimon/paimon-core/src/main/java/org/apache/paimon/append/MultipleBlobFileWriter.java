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
import org.apache.paimon.format.blob.BlobFileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.io.RollingFileWriterImpl;
import org.apache.paimon.io.RowDataFileWriter;
import org.apache.paimon.io.SingleFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.statistics.NoneSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.BlobType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

/**
 * 多 Blob 文件写入器
 *
 * <p>MultipleBlobFileWriter 负责写入表中的多个 BLOB 类型字段,为每个 BLOB 字段创建独立的文件。
 *
 * <p>设计目的:
 * BLOB 字段(如图片、视频、大文本)通常:
 * <ul>
 *   <li>体积很大,不适合与普通字段混合存储
 *   <li>访问模式不同,通常按需加载
 *   <li>需要单独的压缩和编码策略
 * </ul>
 *
 * <p>工作原理:
 * <pre>
 * 1. 初始化:
 *    - 通过 BlobType.splitBlob 分离出所有 BLOB 字段
 *    - 为每个 BLOB 字段创建一个 BlobProjectedFileWriter
 *    - 使用 BlobFileFormat 处理 BLOB 数据
 *
 * 2. 写入过程:
 *    - 每次 write(row) 调用时,遍历所有 blobWriters
 *    - 每个 writer 投影(project)出对应的 BLOB 字段
 *    - 写入到独立的 *.blob 文件
 *
 * 3. 结果收集:
 *    - result() 返回所有 BLOB 文件的 DataFileMeta
 *    - 每个文件包含 writeCols 字段,标识 BLOB 字段名
 * </pre>
 *
 * <p>文件命名:
 * Blob 文件使用 {@link DataFilePathFactory#newBlobPath} 生成路径,格式为:
 * <ul>
 *   <li>{@code data-{uuid}.blob} (独立存储,与普通数据文件分离)
 * </ul>
 *
 * <p>投影机制:
 * 使用 {@link ProjectedFileWriter} 只写入特定 BLOB 字段:
 * <ul>
 *   <li>减少不必要的序列化开销
 *   <li>每个 BLOB 文件只包含一个字段的数据
 *   <li>支持独立的文件滚动(rolling)策略
 * </ul>
 *
 * <p>配置参数:
 * <ul>
 *   <li>{@code targetFileSize}: 目标文件大小,控制何时滚动文件
 *   <li>{@code blobConsumer}: 可选的 BLOB 消费者,用于自定义处理逻辑
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 假设表有字段: id INT, name STRING, image BLOB, video BLOB
 * MultipleBlobFileWriter blobWriter = new MultipleBlobFileWriter(
 *     fileIO,
 *     schemaId,
 *     rowType, // 包含 image 和 video 字段
 *     pathFactory,
 *     seqNumCounter,
 *     FileSource.APPEND,
 *     asyncFileWrite,
 *     statsDenseStore,
 *     blobTargetFileSize,
 *     blobConsumer
 * );
 *
 * // 写入数据
 * for (InternalRow row : rows) {
 *     blobWriter.write(row); // 同时写入 image.blob 和 video.blob
 * }
 *
 * // 关闭并获取结果
 * blobWriter.close();
 * List<DataFileMeta> blobFiles = blobWriter.result();
 * // blobFiles[0].writeCols() = ["image"]
 * // blobFiles[1].writeCols() = ["video"]
 * }</pre>
 *
 * @see RollingBlobFileWriter Blob 文件滚动写入器
 * @see BlobProjectedFileWriter Blob 投影文件写入器
 * @see ProjectedFileWriter 投影文件写入器基类
 */
public class MultipleBlobFileWriter implements Closeable {

    private final List<BlobProjectedFileWriter> blobWriters;

    public MultipleBlobFileWriter(
            FileIO fileIO,
            long schemaId,
            RowType writeSchema,
            DataFilePathFactory pathFactory,
            Supplier<LongCounter> seqNumCounterSupplier,
            FileSource fileSource,
            boolean asyncFileWrite,
            boolean statsDenseStore,
            long targetFileSize,
            @Nullable BlobConsumer blobConsumer) {
        RowType blobRowType = BlobType.splitBlob(writeSchema).getRight();
        this.blobWriters = new ArrayList<>();
        for (String blobFieldName : blobRowType.getFieldNames()) {
            BlobFileFormat blobFileFormat = new BlobFileFormat();
            blobFileFormat.setWriteConsumer(blobConsumer);
            blobWriters.add(
                    new BlobProjectedFileWriter(
                            () ->
                                    new RowDataFileWriter(
                                            fileIO,
                                            RollingFileWriter.createFileWriterContext(
                                                    blobFileFormat,
                                                    writeSchema.project(blobFieldName),
                                                    new SimpleColStatsCollector.Factory[] {
                                                        NoneSimpleColStatsCollector::new
                                                    },
                                                    "none"),
                                            pathFactory.newBlobPath(),
                                            writeSchema.project(blobFieldName),
                                            schemaId,
                                            seqNumCounterSupplier,
                                            new FileIndexOptions(),
                                            fileSource,
                                            asyncFileWrite,
                                            statsDenseStore,
                                            pathFactory.isExternalPath(),
                                            singletonList(blobFieldName)),
                            targetFileSize,
                            writeSchema.projectIndexes(singletonList(blobFieldName))));
        }
    }

    public void write(InternalRow row) throws IOException {
        for (BlobProjectedFileWriter blobWriter : blobWriters) {
            blobWriter.write(row);
        }
    }

    public void abort() {
        for (BlobProjectedFileWriter blobWriter : blobWriters) {
            blobWriter.abort();
        }
    }

    @Override
    public void close() throws IOException {
        for (BlobProjectedFileWriter blobWriter : blobWriters) {
            blobWriter.close();
        }
    }

    public List<DataFileMeta> result() throws IOException {
        List<DataFileMeta> results = new ArrayList<>();
        for (BlobProjectedFileWriter blobWriter : blobWriters) {
            results.addAll(blobWriter.result());
        }
        return results;
    }

    private static class BlobProjectedFileWriter
            extends ProjectedFileWriter<
                    RollingFileWriterImpl<InternalRow, DataFileMeta>, List<DataFileMeta>> {
        public BlobProjectedFileWriter(
                Supplier<? extends SingleFileWriter<InternalRow, DataFileMeta>> writerFactory,
                long targetFileSize,
                int[] projection) {
            super(new RollingFileWriterImpl<>(writerFactory, targetFileSize), projection);
        }
    }
}
