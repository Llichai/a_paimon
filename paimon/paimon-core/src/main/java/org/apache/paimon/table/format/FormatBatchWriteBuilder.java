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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.WriteSelector;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * FormatTable 的批量写入构建器。
 *
 * <p>FormatBatchWriteBuilder 为 {@link FormatTable} 提供批量写入功能，用于写入外部格式文件（ORC、Parquet、CSV、JSON、TEXT）。
 *
 * <h3>与 TableWriteImpl 的区别：</h3>
 * <ul>
 *   <li><b>无 MergeTree</b>：不需要合并逻辑，直接写入文件
 *   <li><b>无 Compaction</b>：不需要压缩，每次写入都是新文件
 *   <li><b>两阶段提交</b>：使用 {@link TwoPhaseCommitMessage} 实现事务性写入
 *   <li><b>支持覆盖</b>：支持分区级别的覆盖写入
 * </ul>
 *
 * <h3>主要功能：</h3>
 * <ul>
 *   <li>创建写入器：{@link FormatTableWrite}
 *   <li>创建提交器：{@link FormatTableCommit}
 *   <li>支持覆盖写入（分区级别）
 *   <li>验证静态分区规格
 * </ul>
 *
 * @see FormatTable
 * @see FormatTableWrite
 * @see FormatTableCommit
 */
public class FormatBatchWriteBuilder implements BatchWriteBuilder {

    private static final long serialVersionUID = 1L;

    /** FormatTable 实例 */
    private final FormatTable table;

    /** 核心配置选项 */
    protected final CoreOptions options;

    /** 静态分区规格（用于覆盖写入） */
    private Map<String, String> staticPartition;

    /** 是否是覆盖写入 */
    private boolean overwrite = false;

    /**
     * 构造 FormatBatchWriteBuilder。
     *
     * @param table FormatTable 实例
     */
    public FormatBatchWriteBuilder(FormatTable table) {
        this.table = table;
        this.options = new CoreOptions(table.options());
    }

    /**
     * 获取表名。
     *
     * @return 表名
     */
    @Override
    public String tableName() {
        return table.name();
    }

    /**
     * 获取行类型。
     *
     * @return 行类型
     */
    @Override
    public RowType rowType() {
        return table.rowType();
    }

    /**
     * 创建写入选择器（FormatTable 不支持，返回空）。
     *
     * @return Optional.empty()
     */
    @Override
    public Optional<WriteSelector> newWriteSelector() {
        return table.newBatchWriteBuilder().newWriteSelector();
    }

    /**
     * 创建批量写入器。
     *
     * <p>创建 {@link FormatTableWrite}，用于将数据写入外部格式文件。
     *
     * @return 批量写入器
     */
    @Override
    public BatchTableWrite newWrite() {
        return new FormatTableWrite(
                table.fileIO(),
                rowType(),
                this.options,
                table.partitionType(),
                table.partitionKeys());
    }

    /**
     * 创建批量提交器。
     *
     * <p>创建 {@link FormatTableCommit}，用于提交两阶段写入的文件。
     *
     * @return 批量提交器
     */
    @Override
    public BatchTableCommit newCommit() {
        CoreOptions options = new CoreOptions(table.options());
        boolean formatTablePartitionOnlyValueInPath = options.formatTablePartitionOnlyValueInPath();
        String syncHiveUri = options.formatTableCommitSyncPartitionHiveUri();
        return new FormatTableCommit(
                table.location(),
                table.partitionKeys(),
                table.fileIO(),
                formatTablePartitionOnlyValueInPath,
                overwrite,
                Identifier.fromString(table.fullName()),
                staticPartition,
                syncHiveUri,
                table.catalogContext());
    }

    /**
     * 设置覆盖写入模式。
     *
     * <p>覆盖写入会在提交前删除目标分区的旧数据文件。
     * 如果指定了静态分区，只覆盖该分区；否则覆盖所有写入的分区。
     *
     * @param staticPartition 静态分区规格（可选）
     * @return 当前构建器
     */
    @Override
    public BatchWriteBuilder withOverwrite(@Nullable Map<String, String> staticPartition) {
        this.overwrite = true;
        validateStaticPartition(staticPartition, table.partitionKeys());
        this.staticPartition = staticPartition;
        return this;
    }

    /**
     * 验证静态分区规格。
     *
     * <p>静态分区规格必须满足以下条件：
     * <ul>
     *   <li>分区列必须是表的分区列
     *   <li>分区列必须是连续的前导列（不能有间隔）
     * </ul>
     *
     * <h4>有效示例：</h4>
     * <pre>{@code
     * 分区列: [year, month, day]
     * 有效: {year=2023}
     * 有效: {year=2023, month=01}
     * 有效: {year=2023, month=01, day=15}
     * }</pre>
     *
     * <h4>无效示例：</h4>
     * <pre>{@code
     * 分区列: [year, month, day]
     * 无效: {year=2023, day=15}  // 缺少 month
     * 无效: {month=01}           // 缺少前导的 year
     * }</pre>
     *
     * @param staticPartition 静态分区规格
     * @param partitionKeys 分区列名列表
     * @throws IllegalArgumentException 如果分区规格无效
     */
    protected static void validateStaticPartition(
            Map<String, String> staticPartition, List<String> partitionKeys) {
        if (staticPartition != null && !staticPartition.isEmpty()) {
            if (partitionKeys == null || partitionKeys.isEmpty()) {
                throw new IllegalArgumentException(
                        "Format table is not partitioned, static partition values are not allowed.");
            }

            boolean missingLeadingKey = false;
            for (String partitionKey : partitionKeys) {
                boolean contains = staticPartition.containsKey(partitionKey);
                if (missingLeadingKey && contains) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Static partition column '%s' cannot be specified without its leading partition.",
                                    partitionKey));
                }
                if (!contains) {
                    missingLeadingKey = true;
                }
            }

            for (String key : staticPartition.keySet()) {
                if (!partitionKeys.contains(key)) {
                    throw new IllegalArgumentException(
                            String.format("Unknown static partition column '%s'.", key));
                }
            }
        }
    }
}
