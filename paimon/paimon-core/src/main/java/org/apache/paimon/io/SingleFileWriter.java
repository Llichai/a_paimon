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

package org.apache.paimon.io;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.BundleFormatWriter;
import org.apache.paimon.format.FileAwareFormatWriter;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SupportsDirectWrite;
import org.apache.paimon.fs.AsyncPositionOutputStream;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.function.Function;

/**
 * 单文件写入器。
 *
 * <p>用于写入单个数据文件的基础写入器实现。支持以下功能:
 * <ul>
 *   <li>记录转换: 将业务记录转换为内部行格式</li>
 *   <li>格式写入: 支持多种文件格式(Parquet、ORC、Avro等)</li>
 *   <li>异步写入: 可选的异步写入优化</li>
 *   <li>直接写入: 某些格式支持直接写入文件系统</li>
 *   <li>错误处理: 写入失败时自动清理文件</li>
 *   <li>大小控制: 监控文件大小,支持滚动写入</li>
 * </ul>
 *
 * @param <T> 写入的记录类型
 * @param <R> 写入完成后返回的结果类型
 */
public abstract class SingleFileWriter<T, R> implements FileWriter<T, R> {

    private static final Logger LOG = LoggerFactory.getLogger(SingleFileWriter.class);

    protected final FileIO fileIO;
    /** 文件路径 */
    protected final Path path;
    /** 记录转换器,将业务记录转换为内部行 */
    private final Function<T, InternalRow> converter;

    /** 是否在中止时删除文件 */
    private boolean deleteFileUponAbort;
    /** 格式写入器 */
    private FormatWriter writer;
    /** 输出流(某些格式不需要) */
    @Nullable private PositionOutputStream out;

    /** 输出字节数(缓存) */
    @Nullable private Long outputBytes;
    /** 已写入的记录数 */
    private long recordCount;
    /** 写入器是否已关闭 */
    protected boolean closed;

    /**
     * 构造单文件写入器。
     *
     * @param fileIO 文件I/O接口
     * @param factory 格式写入器工厂
     * @param path 文件路径
     * @param converter 记录转换器
     * @param compression 压缩格式
     * @param asyncWrite 是否使用异步写入
     */
    public SingleFileWriter(
            FileIO fileIO,
            FormatWriterFactory factory,
            Path path,
            Function<T, InternalRow> converter,
            String compression,
            boolean asyncWrite) {
        this.fileIO = fileIO;
        this.path = path;
        this.converter = converter;
        // 初始设置为 true 以便在异常时清理文件
        this.deleteFileUponAbort = true;

        try {
            // 某些格式支持直接写入文件系统
            if (factory instanceof SupportsDirectWrite) {
                writer = ((SupportsDirectWrite) factory).create(fileIO, path, compression);
            } else {
                // 大多数格式需要通过输出流写入
                out = fileIO.newOutputStream(path, false);
                if (asyncWrite) {
                    // 使用异步输出流提高写入性能
                    out = new AsyncPositionOutputStream(out);
                }
                writer = factory.create(out, compression);
            }

            // 某些格式需要知道文件路径和是否支持中止时删除
            if (writer instanceof FileAwareFormatWriter) {
                FileAwareFormatWriter fileAwareFormatWriter = (FileAwareFormatWriter) writer;
                fileAwareFormatWriter.setFile(path);
                deleteFileUponAbort = fileAwareFormatWriter.deleteFileUponAbort();
            }
        } catch (IOException e) {
            LOG.warn(
                    "Failed to open the bulk writer, closing the output stream and throw the error.",
                    e);
            if (out != null) {
                abort();
            }
            throw new UncheckedIOException(e);
        }

        this.recordCount = 0;
        this.closed = false;
    }

    /**
     * 获取文件路径。
     *
     * @return 文件路径
     */
    public Path path() {
        return path;
    }

    /**
     * 写入单条记录。
     *
     * @param record 要写入的记录
     * @throws IOException 如果写入过程中发生I/O错误
     */
    @Override
    public void write(T record) throws IOException {
        writeImpl(record);
    }

    /**
     * 批量写入记录。
     *
     * <p>某些格式(如 ORC)支持批量写入以提高性能。
     *
     * @param bundle 要写入的记录束
     * @throws IOException 如果写入过程中发生I/O错误
     */
    public void writeBundle(BundleRecords bundle) throws IOException {
        if (closed) {
            throw new RuntimeException("Writer has already closed!");
        }

        try {
            if (writer instanceof BundleFormatWriter) {
                ((BundleFormatWriter) writer).writeBundle(bundle);
            } else {
                for (InternalRow row : bundle) {
                    writer.addElement(row);
                }
            }
            recordCount += bundle.rowCount();
        } catch (Throwable e) {
            LOG.warn("Exception occurs when writing file {}. Cleaning up.", path, e);
            abort();
            throw e;
        }
    }

    protected InternalRow writeImpl(T record) throws IOException {
        if (closed) {
            throw new RuntimeException("Writer has already closed!");
        }

        try {
            InternalRow rowData = converter.apply(record);
            writer.addElement(rowData);
            recordCount++;
            return rowData;
        } catch (Throwable e) {
            LOG.warn("Exception occurs when writing file {}. Cleaning up.", path, e);
            abort();
            throw e;
        }
    }

    @Override
    public long recordCount() {
        return recordCount;
    }

    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        return writer.reachTargetSize(suggestedCheck, targetSize);
    }

    @Override
    public void abort() {
        if (writer != null) {
            IOUtils.closeQuietly(writer);
            writer = null;
        }
        if (out != null) {
            IOUtils.closeQuietly(out);
            out = null;
        }
        abortExecutor().ifPresent(FileWriterAbortExecutor::abort);
    }

    public Optional<FileWriterAbortExecutor> abortExecutor() {
        return deleteFileUponAbort
                ? Optional.of(new FileWriterAbortExecutor(fileIO, path))
                : Optional.empty();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Closing file {}", path);
        }

        try {
            if (writer != null) {
                writer.close();
                writer = null;
            }
            if (out != null) {
                out.flush();
                outputBytes = out.getPos();
                out.close();
                out = null;
            }
        } catch (IOException e) {
            LOG.warn("Exception occurs when closing file {}. Cleaning up.", path, e);
            abort();
            throw e;
        } finally {
            closed = true;
        }
    }

    protected long outputBytes() throws IOException {
        if (outputBytes == null) {
            outputBytes = fileIO.getFileSize(path);
        }
        return outputBytes;
    }
}
