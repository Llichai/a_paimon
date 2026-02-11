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
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.types.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * 格式表滚动文件写入器,当文件大小超过目标值时自动滚动到新文件。
 *
 * <p>滚动机制:
 * <ul>
 *   <li>每写入1000条记录检查一次文件大小
 *   <li>达到targetFileSize时关闭当前文件,创建新文件
 *   <li>保存所有已关闭文件的提交器和中止执行器
 * </ul>
 *
 * <p>工作流程:
 * <ol>
 *   <li>创建第一个FormatTableSingleFileWriter
 *   <li>写入数据,定期检查大小
 *   <li>达到目标大小时:
 *       <ul>
 *         <li>关闭当前写入器
 *         <li>保存提交器和中止执行器
 *         <li>创建新的写入器
 *       </ul>
 *   <li>出错时中止所有写入器
 *   <li>成功时返回所有提交器
 * </ol>
 *
 * <p>使用示例:
 * <pre>{@code
 * FormatTableRollingFileWriter writer = new FormatTableRollingFileWriter(
 *     fileIO, fileFormat, targetFileSize, writeSchema,
 *     pathFactory, compression);
 *
 * try {
 *     // 写入大量数据,自动滚动
 *     for (InternalRow row : rows) {
 *         writer.write(row);
 *     }
 *     writer.close();
 *
 *     // 提交所有文件
 *     List<Committer> committers = writer.committers();
 *     for (Committer c : committers) {
 *         c.commit(fileIO);
 *     }
 * } catch (Exception e) {
 *     // 中止所有文件
 *     writer.abort();
 *     throw e;
 * }
 * }</pre>
 *
 * @see FormatTableSingleFileWriter 单文件写入器
 */
public class FormatTableRollingFileWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(FormatTableRollingFileWriter.class);

    private static final int CHECK_ROLLING_RECORD_CNT = 1000;

    private final Supplier<FormatTableSingleFileWriter> writerFactory;
    private final long targetFileSize;
    private final List<FileWriterAbortExecutor> closedWriters;
    private final List<TwoPhaseOutputStream.Committer> committers;

    private FormatTableSingleFileWriter currentWriter = null;
    private long recordCount = 0;
    private boolean closed = false;

    public FormatTableRollingFileWriter(
            FileIO fileIO,
            FileFormat fileFormat,
            long targetFileSize,
            RowType writeSchema,
            DataFilePathFactory pathFactory,
            String fileCompression) {
        this.writerFactory =
                () ->
                        new FormatTableSingleFileWriter(
                                fileIO,
                                fileFormat.createWriterFactory(writeSchema),
                                pathFactory.newPath(),
                                fileCompression);
        this.targetFileSize = targetFileSize;
        this.closedWriters = new ArrayList<>();
        this.committers = new ArrayList<>();
    }

    /**
     * 获取目标文件大小。
     *
     * @return 目标文件大小(字节)
     */
    public long targetFileSize() {
        return targetFileSize;
    }

    /**
     * 写入一行数据。
     *
     * <p>自动滚动逻辑:
     * <ul>
     *   <li>如果当前写入器为null,创建新写入器
     *   <li>写入数据到当前写入器
     *   <li>每1000条记录检查一次文件大小
     *   <li>达到targetFileSize时关闭当前写入器并创建新写入器
     * </ul>
     *
     * @param row 要写入的数据行
     * @throws IOException 写入失败
     */
    public void write(InternalRow row) throws IOException {
        try {
            if (currentWriter == null) {
                currentWriter = writerFactory.get();
            }

            currentWriter.write(row);
            recordCount += 1;
            boolean needRolling =
                    currentWriter.reachTargetSize(
                            recordCount % CHECK_ROLLING_RECORD_CNT == 0, targetFileSize);
            if (needRolling) {
                closeCurrentWriter();
            }
        } catch (Throwable e) {
            LOG.warn(
                    "Exception occurs when writing file {}. Cleaning up.",
                    currentWriter == null ? null : currentWriter.path(),
                    e);
            abort();
            throw e;
        }
    }

    /**
     * 关闭当前写入器并保存其提交器。
     *
     * @throws IOException 关闭失败
     */
    private void closeCurrentWriter() throws IOException {
        if (currentWriter == null) {
            return;
        }

        currentWriter.close();
        closedWriters.add(currentWriter.abortExecutor());
        if (currentWriter.committers() != null) {
            committers.addAll(currentWriter.committers());
        }

        currentWriter = null;
    }

    /**
     * 中止所有写入器,删除所有已写入的文件。
     */
    public void abort() {
        if (currentWriter != null) {
            currentWriter.abort();
        }
        for (FileWriterAbortExecutor abortExecutor : closedWriters) {
            abortExecutor.abort();
        }
    }

    /**
     * 获取所有提交器列表。
     *
     * @return 两阶段提交器列表
     */
    public List<TwoPhaseOutputStream.Committer> committers() {
        return committers;
    }

    /**
     * 关闭写入器,关闭当前写入器(如果有)。
     *
     * @throws IOException 关闭失败
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        try {
            closeCurrentWriter();
        } catch (IOException e) {
            LOG.warn(
                    "Exception occurs when writing file {}. Cleaning up.", currentWriter.path(), e);
            abort();
            throw e;
        } finally {
            closed = true;
        }
    }
}
