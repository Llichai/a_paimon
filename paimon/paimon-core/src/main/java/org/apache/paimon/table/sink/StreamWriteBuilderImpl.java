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

import org.apache.paimon.options.Options;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.types.RowType;

import java.util.Optional;

import static org.apache.paimon.CoreOptions.createCommitUser;

/**
 * {@link WriteBuilder} 的流式写入实现。
 *
 * <p>此实现用于流处理场景，例如：
 * <ul>
 *     <li>Flink Streaming 任务
 *     <li>Spark Streaming 任务
 *     <li>实时 CDC 同步任务
 * </ul>
 *
 * <p>与批量写入的区别：
 * <pre>
 * 特性                  | 流式写入                | 批量写入
 * ---------------------|------------------------|------------------------
 * CommitUser           | 需要手动设置            | 自动生成
 * CommitIdentifier     | 递增（Checkpoint ID）   | 固定（Long.MAX_VALUE）
 * Overwrite 支持       | 否                      | 是
 * 状态恢复             | 支持                    | 不支持
 * 空提交检查           | 默认不忽略              | 默认忽略
 * </pre>
 *
 * <p>Exactly-Once 保证：
 * <ul>
 *     <li>使用唯一的 CommitUser 标识应用
 *     <li>使用递增的 CommitIdentifier 标识每次提交
 *     <li>支持从 Checkpoint 恢复状态
 *     <li>过滤已提交的消息以避免重复提交
 * </ul>
 *
 * <p>此类是可序列化的，可以在分布式环境中传输。
 */
public class StreamWriteBuilderImpl implements StreamWriteBuilder {

    private static final long serialVersionUID = 1L;

    /** 内部表引用 */
    private final InnerTable table;

    /** 提交用户标识，用于标识流式应用 */
    private String commitUser;

    /**
     * 构造函数。
     *
     * <p>CommitUser 从表选项中自动创建（通常是 UUID），
     * 但建议在生产环境中使用固定的值。
     *
     * @param table 内部表
     */
    public StreamWriteBuilderImpl(InnerTable table) {
        this.table = table;
        this.commitUser = createCommitUser(new Options(table.options()));
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
    public String commitUser() {
        return commitUser;
    }

    @Override
    public StreamWriteBuilder withCommitUser(String commitUser) {
        this.commitUser = commitUser;
        return this;
    }

    @Override
    public StreamTableWrite newWrite() {
        return table.newWrite(commitUser);
    }

    @Override
    public StreamTableCommit newCommit() {
        // 流式提交默认不忽略空提交，以便定期创建快照
        return table.newCommit(commitUser).ignoreEmptyCommit(false);
    }
}
