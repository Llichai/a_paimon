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
import org.apache.paimon.casting.DefaultValueRow;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BlobConsumer;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.FormatTableRowPartitionKeyExtractor;
import org.apache.paimon.table.sink.TableWrite;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ProjectedRow;

import javax.annotation.Nullable;

import java.util.List;
import java.util.stream.Collectors;

/**
 * FormatTable 的写入实现。
 *
 * <p>FormatTableWrite 为 {@link FormatTable} 提供简化的写入功能，直接写入外部格式文件，不需要 MergeTree 的复杂逻辑。
 *
 * <h3>与 TableWriteImpl 的区别：</h3>
 * <ul>
 *   <li><b>无 MergeTree</b>：不需要 LSM-Tree 的层级结构和合并逻辑
 *   <li><b>无 Compaction</b>：不需要压缩，每次写入都是新文件
 *   <li><b>无 Bucket</b>：不使用分桶，数据按分区组织
 *   <li><b>简单写入</b>：只需处理分区路由、非空检查、默认值填充
 * </ul>
 *
 * <h3>主要功能：</h3>
 * <ul>
 *   <li>分区路由：根据数据的分区列值路由到对应的分区写入器
 *   <li>非空检查：验证非空列不能写入 null 值
 *   <li>默认值填充：为空值填充默认值
 *   <li>列投影：从完整行中投影出数据列（排除分区列）
 *   <li>滚动写入：使用 {@link FormatTableFileWriter} 实现文件滚动
 * </ul>
 *
 * @see FormatTable
 * @see FormatTableFileWriter
 */
public class FormatTableWrite implements BatchTableWrite {

    /** 表的行类型 */
    private final RowType rowType;

    /** 文件写入器 */
    private final FormatTableFileWriter write;

    /** 分区键提取器 */
    private final FormatTableRowPartitionKeyExtractor partitionKeyExtractor;

    /** 非空字段索引 */
    private final int[] notNullFieldIndex;

    /** 默认值行（用于填充空值） */
    private final @Nullable DefaultValueRow defaultValueRow;

    /** 投影行（用于排除分区列） */
    private final ProjectedRow projectedRow;

    /**
     * 构造 FormatTableWrite。
     *
     * @param fileIO 文件 I/O
     * @param rowType 行类型
     * @param options 核心配置选项
     * @param partitionType 分区类型
     * @param partitionKeys 分区列名列表
     */
    public FormatTableWrite(
            FileIO fileIO,
            RowType rowType,
            CoreOptions options,
            RowType partitionType,
            List<String> partitionKeys) {
        this.rowType = rowType;
        this.partitionKeyExtractor =
                new FormatTableRowPartitionKeyExtractor(rowType, partitionKeys);

        // 收集非空列的索引
        List<String> notNullColumnNames =
                rowType.getFields().stream()
                        .filter(field -> !field.type().isNullable())
                        .map(DataField::name)
                        .collect(Collectors.toList());
        this.notNullFieldIndex = rowType.getFieldIndices(notNullColumnNames);

        // 创建默认值行（如果有默认值）
        this.defaultValueRow = DefaultValueRow.create(rowType);

        // 创建投影行（排除分区列）
        RowType writeRowType =
                rowType.project(
                        rowType.getFieldNames().stream()
                                .filter(name -> !partitionType.getFieldNames().contains(name))
                                .collect(Collectors.toList()));
        this.projectedRow = ProjectedRow.from(writeRowType, rowType);

        // 创建文件写入器
        this.write = new FormatTableFileWriter(fileIO, writeRowType, options, partitionType);
    }

    /**
     * 获取数据行的分区值。
     *
     * @param row 数据行
     * @return 分区值（二进制行）
     */
    @Override
    public BinaryRow getPartition(InternalRow row) {
        return partitionKeyExtractor.partition(row);
    }

    /**
     * 写入一行数据。
     *
     * <p>执行以下步骤：
     * <ol>
     *   <li>检查非空列：验证非空列不能为 null
     *   <li>填充默认值：为空值填充默认值（如果有）
     *   <li>提取分区值：从行中提取分区列的值
     *   <li>投影数据列：排除分区列，只保留数据列
     *   <li>路由写入：将数据路由到对应分区的文件写入器
     * </ol>
     *
     * @param row 数据行
     * @throws Exception 如果写入失败或数据不合法
     */
    @Override
    public void write(InternalRow row) throws Exception {
        // 检查非空列
        for (int idx : notNullFieldIndex) {
            if (row.isNullAt(idx)) {
                String columnName = rowType.getFields().get(idx).name();
                throw new RuntimeException(
                        String.format("Cannot write null to non-null column(%s)", columnName));
            }
        }

        // 填充默认值
        row = defaultValueRow == null ? row : defaultValueRow.replaceRow(row);

        // 提取分区值
        BinaryRow partition = partitionKeyExtractor.partition(row);

        // 投影数据列并写入
        write.write(partition, projectedRow.replaceRow(row));
    }

    /**
     * 准备提交。
     *
     * <p>关闭所有文件写入器，返回两阶段提交消息列表。
     *
     * @return 提交消息列表
     * @throws Exception 如果准备提交失败
     */
    @Override
    public List<CommitMessage> prepareCommit() throws Exception {
        return write.prepareCommit();
    }

    /**
     * 关闭写入器。
     *
     * @throws Exception 如果关闭失败
     */
    @Override
    public void close() throws Exception {
        write.close();
    }

    /**
     * 获取数据行的 Bucket（FormatTable 不使用分桶，返回 0）。
     *
     * @param row 数据行
     * @return 0
     */
    @Override
    public int getBucket(InternalRow row) {
        return 0;
    }

    /**
     * 设置内存池工厂（FormatTable 不支持，返回自身）。
     *
     * @param memoryPoolFactory 内存池工厂
     * @return 当前写入器
     */
    @Override
    public TableWrite withMemoryPoolFactory(MemoryPoolFactory memoryPoolFactory) {
        return this;
    }

    /**
     * 设置 Blob 消费者（FormatTable 不支持）。
     *
     * @throws UnsupportedOperationException 始终抛出异常
     */
    @Override
    public TableWrite withBlobConsumer(BlobConsumer blobConsumer) {
        throw new UnsupportedOperationException();
    }

    /**
     * 设置 I/O 管理器（FormatTable 不支持，返回自身）。
     *
     * @param ioManager I/O 管理器
     * @return 当前写入器
     */
    @Override
    public BatchTableWrite withIOManager(IOManager ioManager) {
        return this;
    }

    /**
     * 设置写入类型（FormatTable 不支持）。
     *
     * @throws UnsupportedOperationException 始终抛出异常
     */
    @Override
    public BatchTableWrite withWriteType(RowType writeType) {
        throw new UnsupportedOperationException();
    }

    /**
     * 写入指定 Bucket 的数据（FormatTable 不支持）。
     *
     * @throws UnsupportedOperationException 始终抛出异常
     */
    @Override
    public void write(InternalRow row, int bucket) throws Exception {
        throw new UnsupportedOperationException();
    }

    /**
     * 批量写入（FormatTable 不支持）。
     *
     * @throws UnsupportedOperationException 始终抛出异常
     */
    @Override
    public void writeBundle(BinaryRow partition, int bucket, BundleRecords bundle)
            throws Exception {
        throw new UnsupportedOperationException();
    }

    /**
     * 触发压缩（FormatTable 不支持）。
     *
     * @throws UnsupportedOperationException 始终抛出异常
     */
    @Override
    public void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception {
        throw new UnsupportedOperationException();
    }

    /**
     * 设置度量注册器（FormatTable 不支持，返回自身）。
     *
     * @param registry 度量注册器
     * @return 当前写入器
     */
    @Override
    public TableWrite withMetricRegistry(MetricRegistry registry) {
        return this;
    }
}
