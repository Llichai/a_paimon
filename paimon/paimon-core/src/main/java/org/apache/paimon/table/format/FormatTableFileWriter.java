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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.format.FileFormat.fileFormat;
import static org.apache.paimon.utils.PartitionPathUtils.generatePartitionPathUtil;

/**
 * FormatTable 的文件写入器。
 *
 * <p>FormatTableFileWriter 管理多个分区的文件写入，为每个分区维护一个 {@link FormatTableRecordWriter}。
 *
 * <h3>主要功能：</h3>
 * <ul>
 *   <li>分区路由：根据分区值路由到对应的记录写入器
 *   <li>写入器管理：为每个分区懒创建记录写入器
 *   <li>两阶段提交：收集所有分区的提交器并返回提交消息
 * </ul>
 *
 * <h3>写入流程：</h3>
 * <ol>
 *   <li>write(partition, row)：将数据写入指定分区
 *   <li>prepareCommit()：关闭所有写入器，返回提交消息
 *   <li>提交消息包含 {@link TwoPhaseOutputStream.Committer}
 * </ol>
 *
 * @see FormatTableWrite
 * @see FormatTableRecordWriter
 */
public class FormatTableFileWriter {

    /** 文件 I/O */
    private final FileIO fileIO;

    /** 写入的行类型（不包含分区列） */
    private RowType writeRowType;

    /** 文件格式 */
    private final FileFormat fileFormat;

    /** 文件存储路径工厂 */
    private final FileStorePathFactory pathFactory;

    /** 分区到记录写入器的映射 */
    protected final Map<BinaryRow, FormatTableRecordWriter> writers;

    /** 核心配置选项 */
    protected final CoreOptions options;

    /**
     * 构造 FormatTableFileWriter。
     *
     * @param fileIO 文件 I/O
     * @param writeRowType 写入的行类型（不包含分区列）
     * @param options 核心配置选项
     * @param partitionType 分区类型
     */
    public FormatTableFileWriter(
            FileIO fileIO, RowType writeRowType, CoreOptions options, RowType partitionType) {
        this.fileIO = fileIO;
        this.writeRowType = writeRowType;
        this.fileFormat = fileFormat(options);
        this.writers = new HashMap<>();
        this.options = options;
        this.pathFactory =
                new FileStorePathFactory(
                        options.path(),
                        partitionType,
                        options.partitionDefaultName(),
                        options.fileFormatString(),
                        options.dataFilePrefix(),
                        options.changelogFilePrefix(),
                        options.legacyPartitionName(),
                        options.fileSuffixIncludeCompression(),
                        options.formatTableFileCompression(),
                        options.dataFilePathDirectory(),
                        null,
                        CoreOptions.ExternalPathStrategy.NONE,
                        options.indexFileInDataFileDir(),
                        null);
    }

    /**
     * 设置写入类型。
     *
     * @param writeType 写入类型
     */
    public void withWriteType(RowType writeType) {
        this.writeRowType = writeType;
    }

    /**
     * 写入数据到指定分区。
     *
     * <p>为每个分区懒创建记录写入器，将数据路由到对应的写入器。
     *
     * @param partition 分区值
     * @param data 数据行
     * @throws Exception 如果写入失败
     */
    public void write(BinaryRow partition, InternalRow data) throws Exception {
        FormatTableRecordWriter writer = writers.get(partition);
        if (writer == null) {
            writer = createWriter(partition.copy());
            writers.put(partition.copy(), writer);
        }
        writer.write(data);
    }

    /**
     * 关闭所有写入器。
     *
     * @throws Exception 如果关闭失败
     */
    public void close() throws Exception {
        writers.clear();
    }

    /**
     * 准备提交。
     *
     * <p>关闭所有分区的记录写入器，收集所有提交器并封装为 {@link TwoPhaseCommitMessage}。
     *
     * @return 提交消息列表
     * @throws Exception 如果准备提交失败
     */
    public List<CommitMessage> prepareCommit() throws Exception {
        List<CommitMessage> commitMessages = new ArrayList<>();
        for (FormatTableRecordWriter writer : writers.values()) {
            List<TwoPhaseOutputStream.Committer> commiters = writer.closeAndGetCommitters();
            for (TwoPhaseOutputStream.Committer committer : commiters) {
                TwoPhaseCommitMessage twoPhaseCommitMessage = new TwoPhaseCommitMessage(committer);
                commitMessages.add(twoPhaseCommitMessage);
            }
        }
        return commitMessages;
    }

    /**
     * 为指定分区创建记录写入器。
     *
     * <p>根据分区值计算分区路径，创建 {@link FormatTableRecordWriter}。
     *
     * @param partition 分区值
     * @return 记录写入器
     */
    private FormatTableRecordWriter createWriter(BinaryRow partition) {
        // 计算分区路径
        Path parent = pathFactory.root();
        if (partition.getFieldCount() > 0) {
            LinkedHashMap<String, String> partValues =
                    pathFactory.partitionComputer().generatePartValues(partition);
            parent =
                    new Path(
                            parent,
                            generatePartitionPathUtil(
                                    partValues, options.formatTablePartitionOnlyValueInPath()));
        }

        // 创建记录写入器
        return new FormatTableRecordWriter(
                fileIO,
                fileFormat,
                options.targetFileSize(false),
                pathFactory.createDataFilePathFactory(parent, null),
                writeRowType,
                options.formatTableFileCompression());
    }
}
