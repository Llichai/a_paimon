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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.CoreOptions.createCommitUser;

/**
 * {@link WriteBuilder} 的批量写入实现。
 *
 * <p>此实现用于批处理场景，例如：
 * <ul>
 *     <li>Spark Batch 任务
 *     <li>Flink Batch 任务
 *     <li>Hive 导入任务
 * </ul>
 *
 * <p>与流式写入的区别：
 * <pre>
 * 特性                  | 批量写入                | 流式写入
 * ---------------------|------------------------|------------------------
 * CommitUser           | 自动生成                | 需要手动设置
 * CommitIdentifier     | 固定（Long.MAX_VALUE）  | 递增（Checkpoint ID）
 * Overwrite 支持       | 是                      | 否
 * 状态恢复             | 不支持                  | 支持
 * 空提交检查           | 默认忽略                | 默认不忽略
 * </pre>
 *
 * <p>此类是可序列化的，可以在分布式环境中传输。
 */
public class BatchWriteBuilderImpl implements BatchWriteBuilder {

    private static final long serialVersionUID = 1L;

    /** 内部表引用 */
    private final InnerTable table;

    /** 提交用户标识 */
    private final String commitUser;

    /** 静态分区配置，用于 INSERT OVERWRITE PARTITION */
    private Map<String, String> staticPartition;

    /** 是否检查追加提交的冲突 */
    private boolean appendCommitCheckConflict = false;

    /** Row ID 冲突检查的起始快照，null 表示不检查 */
    private @Nullable Long rowIdCheckFromSnapshot = null;

    /**
     * 构造函数。
     *
     * <p>CommitUser 从表选项中自动创建。
     *
     * @param table 内部表
     */
    public BatchWriteBuilderImpl(InnerTable table) {
        this.table = table;
        this.commitUser = createCommitUser(new Options(table.options()));
    }

    /**
     * 私有构造函数，用于复制构建器。
     *
     * @param table 内部表
     * @param commitUser 提交用户标识
     * @param staticPartition 静态分区配置
     */
    private BatchWriteBuilderImpl(
            InnerTable table, String commitUser, @Nullable Map<String, String> staticPartition) {
        this.table = table;
        this.commitUser = commitUser;
        this.staticPartition = staticPartition;
    }

    @Override
    public String tableName() {
        return table.name();
    }

    @Override
    public RowType rowType() {
        return table.rowType();
    }

    @Override
    public Optional<WriteSelector> newWriteSelector() {
        return table.newWriteSelector();
    }

    @Override
    public BatchWriteBuilder withOverwrite(@Nullable Map<String, String> staticPartition) {
        this.staticPartition = staticPartition;
        return this;
    }

    @Override
    public BatchTableWrite newWrite() {
        // 如果配置了静态分区（覆写模式），则忽略现有文件
        return table.newWrite(commitUser).withIgnorePreviousFiles(staticPartition != null);
    }

    @Override
    public BatchTableCommit newCommit() {
        InnerTableCommit commit =
                table.newCommit(commitUser)
                        .withOverwrite(staticPartition)
                        .appendCommitCheckConflict(appendCommitCheckConflict)
                        .rowIdCheckConflict(rowIdCheckFromSnapshot);

        // 批量提交默认忽略空提交，除非配置了不忽略
        commit.ignoreEmptyCommit(
                Options.fromMap(table.options())
                        .getOptional(CoreOptions.SNAPSHOT_IGNORE_EMPTY_COMMIT)
                        .orElse(true));
        return commit;
    }

    /**
     * 使用新表复制构建器。
     *
     * <p>用于表更新场景，保留原有配置。
     *
     * @param newTable 新表
     * @return 新的 BatchWriteBuilderImpl 实例
     */
    public BatchWriteBuilderImpl copyWithNewTable(Table newTable) {
        return new BatchWriteBuilderImpl((InnerTable) newTable, commitUser, staticPartition);
    }

    /**
     * 设置是否检查追加提交的冲突。
     *
     * <p>用于追加表的并发写入场景。
     *
     * @param appendCommitCheckConflict 是否检查冲突
     * @return 当前构建器实例
     */
    public BatchWriteBuilderImpl appendCommitCheckConflict(boolean appendCommitCheckConflict) {
        this.appendCommitCheckConflict = appendCommitCheckConflict;
        return this;
    }

    /**
     * 设置 Row ID 冲突检查的起始快照。
     *
     * <p>用于检查 Row ID 分配的冲突。
     *
     * @param rowIdCheckFromSnapshot 起始快照 ID，null 表示不检查
     * @return 当前构建器实例
     */
    public BatchWriteBuilderImpl rowIdCheckConflict(@Nullable Long rowIdCheckFromSnapshot) {
        this.rowIdCheckFromSnapshot = rowIdCheckFromSnapshot;
        return this;
    }
}
