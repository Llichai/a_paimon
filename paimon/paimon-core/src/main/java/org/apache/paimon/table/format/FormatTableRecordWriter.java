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

package org.apache.paimon.table.format;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.FormatTableRollingFileWriter;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.List;

/**
 * FormatTable 的记录写入器。
 *
 * <p>FormatTableRecordWriter 负责将数据记录写入外部格式文件，支持滚动写入（当文件达到目标大小时自动创建新文件）。
 *
 * <h3>主要功能：</h3>
 * <ul>
 *   <li>滚动写入：使用 {@link FormatTableRollingFileWriter} 实现文件滚动
 *   <li>两阶段提交：返回 {@link org.apache.paimon.fs.TwoPhaseOutputStream.Committer} 列表
 *   <li>懒创建：第一次写入时才创建底层写入器
 * </ul>
 *
 * <h3>写入流程：</h3>
 * <ol>
 *   <li>write(row)：写入数据记录
 *   <li>closeAndGetCommitters()：关闭写入器并获取提交器
 *   <li>提交器执行提交：将临时文件重命名为最终文件
 * </ol>
 *
 * @see FormatTableFileWriter
 * @see FormatTableRollingFileWriter
 */
public class FormatTableRecordWriter implements AutoCloseable {

    /** 文件 I/O */
    private final FileIO fileIO;

    /** 数据文件路径工厂 */
    private final DataFilePathFactory pathFactory;

    /** 写入的行类型 */
    private final RowType writeSchema;

    /** 文件压缩格式 */
    private final String fileCompression;

    /** 文件格式 */
    private final FileFormat fileFormat;

    /** 目标文件大小 */
    private final long targetFileSize;

    /** 滚动文件写入器（懒创建） */
    private FormatTableRollingFileWriter writer;

    /**
     * 构造 FormatTableRecordWriter。
     *
     * @param fileIO 文件 I/O
     * @param fileFormat 文件格式
     * @param targetFileSize 目标文件大小
     * @param pathFactory 数据文件路径工厂
     * @param writeSchema 写入的行类型
     * @param fileCompression 文件压缩格式
     */
    public FormatTableRecordWriter(
            FileIO fileIO,
            FileFormat fileFormat,
            long targetFileSize,
            DataFilePathFactory pathFactory,
            RowType writeSchema,
            String fileCompression) {
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
        this.fileCompression = fileCompression;
        this.writeSchema = writeSchema;
        this.fileFormat = fileFormat;
        this.targetFileSize = targetFileSize;
    }

    /**
     * 写入一行数据。
     *
     * <p>懒创建：第一次写入时创建底层的滚动文件写入器。
     *
     * @param data 数据行
     * @throws Exception 如果写入失败
     */
    public void write(InternalRow data) throws Exception {
        if (writer == null) {
            writer = createRollingRowWriter();
        }
        writer.write(data);
    }

    /**
     * 关闭写入器并获取提交器。
     *
     * <p>这个方法用于准备两阶段提交：
     * <ol>
     *   <li>关闭底层写入器（刷新缓冲区）
     *   <li>获取所有文件的提交器
     *   <li>清空写入器引用
     * </ol>
     *
     * @return 提交器列表
     * @throws Exception 如果关闭失败
     */
    public List<TwoPhaseOutputStream.Committer> closeAndGetCommitters() throws Exception {
        List<TwoPhaseOutputStream.Committer> commits = new ArrayList<>();
        if (writer != null) {
            writer.close();
            commits.addAll(writer.committers());
            writer = null;
        }
        return commits;
    }

    /**
     * 关闭写入器（回滚未提交的数据）。
     *
     * <p>如果写入器未提交就关闭，会删除临时文件。
     *
     * @throws Exception 如果关闭失败
     */
    @Override
    public void close() throws Exception {
        if (writer != null) {
            writer.abort();
            writer = null;
        }
    }

    /**
     * 创建滚动文件写入器。
     *
     * @return 滚动文件写入器
     */
    private FormatTableRollingFileWriter createRollingRowWriter() {
        return new FormatTableRollingFileWriter(
                fileIO, fileFormat, targetFileSize, writeSchema, pathFactory, fileCompression);
    }
}
