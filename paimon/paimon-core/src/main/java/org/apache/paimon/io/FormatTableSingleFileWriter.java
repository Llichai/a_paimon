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
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SupportsDirectWrite;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.utils.IOUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * 格式表单文件写入器,用于生成单个格式化文件。
 *
 * <p>主要用途:
 * <ul>
 *   <li>Format Table的数据写入
 *   <li>直接生成格式文件(Parquet、ORC等)
 *   <li>不维护Paimon元数据
 * </ul>
 *
 * <p>与数据文件写入器的区别:
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>FormatTableSingleFileWriter</th>
 *     <th>DataFileWriter</th>
 *   </tr>
 *   <tr>
 *     <td>元数据</td>
 *     <td>不生成DataFileMeta</td>
 *     <td>生成DataFileMeta</td>
 *   </tr>
 *   <tr>
 *     <td>统计信息</td>
 *     <td>不收集</td>
 *     <td>收集min/max/nullCount</td>
 *   </tr>
 *   <tr>
 *     <td>文件索引</td>
 *     <td>不支持</td>
 *     <td>支持Bloom Filter等</td>
 *   </tr>
 *   <tr>
 *     <td>提交方式</td>
 *     <td>TwoPhaseCommit</td>
 *     <td>直接写入</td>
 *   </tr>
 * </table>
 *
 * <p>两阶段提交:
 * <ol>
 *   <li>写入阶段: 写入数据到临时位置
 *   <li>关闭阶段: closeForCommit(),准备提交
 *   <li>提交阶段: committer.commit(),移动到最终位置
 *   <li>中止阶段: committer.discard(),删除临时文件
 * </ol>
 *
 * <p>使用示例:
 * <pre>{@code
 * FormatTableSingleFileWriter writer = new FormatTableSingleFileWriter(
 *     fileIO, writerFactory, path, compression);
 *
 * // 写入数据
 * writer.write(row1);
 * writer.write(row2);
 *
 * // 关闭并获取提交器
 * writer.close();
 * List<Committer> committers = writer.committers();
 *
 * // 提交或中止
 * if (success) {
 *     for (Committer c : committers) c.commit(fileIO);
 * } else {
 *     writer.abort();
 * }
 * }</pre>
 *
 * @see FormatTableRollingFileWriter 滚动写入器版本
 */
public class FormatTableSingleFileWriter {

    private static final Logger LOG = LoggerFactory.getLogger(FormatTableSingleFileWriter.class);

    protected final FileIO fileIO;
    protected final Path path;

    private FormatWriter writer;
    private PositionOutputStream out;
    private TwoPhaseOutputStream.Committer committer;

    protected long outputBytes;
    protected boolean closed;

    public FormatTableSingleFileWriter(
            FileIO fileIO, FormatWriterFactory factory, Path path, String compression) {
        this.fileIO = fileIO;
        this.path = path;

        try {
            if (factory instanceof SupportsDirectWrite) {
                throw new UnsupportedOperationException("Does not support SupportsDirectWrite.");
            } else {
                out = fileIO.newTwoPhaseOutputStream(path, false);
                writer = factory.create(out, compression);
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
     * 写入一条记录。
     *
     * @param record 要写入的记录
     * @throws IOException 写入失败
     * @throws RuntimeException 如果写入器已关闭
     */
    public void write(InternalRow record) throws IOException {
        if (closed) {
            throw new RuntimeException("Writer has already closed!");
        }

        try {
            writer.addElement(record);
        } catch (Throwable e) {
            LOG.warn("Exception occurs when writing file {}. Cleaning up.", path, e);
            abort();
            throw e;
        }
    }

    /**
     * 检查是否达到目标文件大小。
     *
     * @param suggestedCheck 是否建议检查
     * @param targetSize 目标文件大小
     * @return true表示达到目标大小
     * @throws IOException 检查失败
     */
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        return writer.reachTargetSize(suggestedCheck, targetSize);
    }

    /**
     * 中止写入,清理所有资源和临时文件。
     *
     * <p>清理步骤:
     * <ol>
     *   <li>关闭格式写入器
     *   <li>关闭输出流并获取提交器
     *   <li>丢弃提交器(删除临时文件)
     *   <li>删除目标文件(如果存在)
     * </ol>
     */
    public void abort() {
        if (writer != null) {
            IOUtils.closeQuietly(writer);
            writer = null;
        }
        if (out != null) {
            try {
                committer = ((TwoPhaseOutputStream) out).closeForCommit();
            } catch (Throwable e) {
                LOG.warn("Exception occurs when close for commit out {}", committer, e);
            }
            out = null;
        }
        if (committer != null) {
            try {
                committer.discard(this.fileIO);
            } catch (Throwable e) {
                LOG.warn("Exception occurs when close out {}", committer, e);
            }
        }
        fileIO.deleteQuietly(path);
    }

    /**
     * 获取两阶段提交的提交器列表。
     *
     * @return 提交器列表
     * @throws RuntimeException 如果写入器未关闭
     */
    public List<TwoPhaseOutputStream.Committer> committers() {
        if (!closed) {
            throw new RuntimeException("Writer should be closed before getting committer!");
        }
        return Lists.newArrayList(committer);
    }

    /**
     * 获取中止执行器。
     *
     * @return 中止执行器实例
     * @throws RuntimeException 如果写入器未关闭
     */
    public FileWriterAbortExecutor abortExecutor() {
        if (!closed) {
            throw new RuntimeException("Writer should be closed!");
        }

        return new FileWriterAbortExecutor(fileIO, path);
    }

    /**
     * 关闭写入器并准备提交。
     *
     * <p>执行步骤:
     * <ol>
     *   <li>关闭格式写入器
     *   <li>刷新输出流
     *   <li>记录输出字节数
     *   <li>closeForCommit获取提交器
     * </ol>
     *
     * @throws IOException 关闭失败
     */
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
                committer = ((TwoPhaseOutputStream) out).closeForCommit();
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
}
