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

import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;

import java.io.Serializable;
import java.util.Optional;

/**
 * 写入构建器接口，用于构建 {@link TableWrite} 和 {@link TableCommit}。
 *
 * <p>这是所有写入构建器的基础接口，定义了创建写入和提交组件的标准方法。
 * 具体的实现有：
 * <ul>
 *     <li>{@link BatchWriteBuilder}: 批量写入构建器，用于 Batch 任务
 *     <li>{@link StreamWriteBuilder}: 流式写入构建器，用于 Streaming 任务
 * </ul>
 *
 * <p>WriteBuilder 是可序列化的，可以在分布式计算环境中传输：
 * <ul>
 *     <li>在 Driver 节点创建 WriteBuilder
 *     <li>序列化并分发到各个 Worker 节点
 *     <li>在 Worker 节点上创建 TableWrite 和 TableCommit
 * </ul>
 *
 * <p>架构层次：
 * <pre>
 * Table (用户层)
 *   └─ WriteBuilder (构建器接口)
 *       ├─ TableWrite (写入接口)
 *       └─ TableCommit (提交接口)
 *           └─ FileStoreWrite/FileStoreCommit (底层实现)
 * </pre>
 *
 * @since 0.4.0
 */
@Public
public interface WriteBuilder extends Serializable {

    /**
     * 返回表的标识名称。
     *
     * <p>用于日志记录、监控指标命名等场景。
     *
     * @return 表名
     */
    String tableName();

    /**
     * 返回表的行类型（Schema）。
     *
     * <p>定义了表的所有字段及其类型，用于：
     * <ul>
     *     <li>数据验证
     *     <li>序列化/反序列化
     *     <li>类型转换
     * </ul>
     *
     * @return 表的行类型
     */
    RowType rowType();

    /**
     * 创建一个 {@link WriteSelector}，用于在写入器之前对记录进行分区。
     *
     * <p>WriteSelector 用于数据分发和负载均衡，例如：
     * <ul>
     *     <li>根据分区键将数据路由到不同的写入任务
     *     <li>根据分桶键将数据分配到不同的写入器
     *     <li>实现自定义的数据分布策略
     * </ul>
     *
     * @return 如果不需要数据分发，返回 empty；否则返回 WriteSelector
     * @throws UnsupportedOperationException 如果此表模式不支持通过上层 API 构建写入
     */
    @Experimental
    Optional<WriteSelector> newWriteSelector();

    /**
     * 创建一个 {@link TableWrite} 用于写入 {@link InternalRow}。
     *
     * <p>TableWrite 负责：
     * <ul>
     *     <li>接收行数据并写入文件
     *     <li>管理写入缓冲区
     *     <li>触发压缩
     *     <li>生成 CommitMessage
     * </ul>
     *
     * @return TableWrite 实例
     */
    TableWrite newWrite();

    /**
     * 创建一个 {@link TableCommit} 用于提交 {@link CommitMessage}。
     *
     * <p>TableCommit 负责：
     * <ul>
     *     <li>收集所有 CommitMessage
     *     <li>创建新的快照
     *     <li>原子性地更新元数据
     *     <li>处理提交冲突
     * </ul>
     *
     * @return TableCommit 实例
     */
    TableCommit newCommit();
}
