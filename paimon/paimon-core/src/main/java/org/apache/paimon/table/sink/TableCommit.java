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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.table.Table;

import java.util.List;

/**
 * 表提交接口，用于创建和提交表的快照（Snapshot）。
 *
 * <p>快照是从 {@link CommitMessage} 生成的，而 CommitMessage 本身由 {@link TableWrite} 生成。
 *
 * <p>提交流程：
 * <ol>
 *     <li>TableWrite 写入数据并生成 CommitMessage（包含新写入的文件信息）
 *     <li>将所有 CommitMessage 收集到一起
 *     <li>TableCommit 根据 CommitMessage 创建新的快照
 *     <li>原子性地更新快照文件，完成提交
 * </ol>
 *
 * <p>提交可能会失败并抛出异常。在提交之前，会首先检查冲突，
 * 即检查所有要删除的文件当前是否仍然存在。这是乐观并发控制的一部分。
 *
 * <p>与底层 FileStoreCommit 的关系：
 * <ul>
 *     <li>TableCommit 是 Table 层的封装，提供面向用户的 API
 *     <li>FileStoreCommit 是 Operation 层的实现，处理实际的快照提交
 *     <li>TableCommitImpl 负责将 Table 层的 CommitMessage 转换为 Operation 层的 CommitMessage
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public interface TableCommit extends AutoCloseable {

    /**
     * 设置指标注册器，用于监控表提交的性能指标。
     *
     * @param registry 指标注册器
     * @return 当前 TableCommit 实例
     */
    TableCommit withMetricRegistry(MetricRegistry registry);

    /**
     * 中止一次不成功的提交。
     *
     * <p>此方法会删除与这些 CommitMessage 相关的数据文件，用于清理失败提交留下的临时文件。
     *
     * <p>使用场景：
     * <ul>
     *     <li>提交失败后的清理
     *     <li>任务取消后的资源回收
     *     <li>异常恢复时的文件清理
     * </ul>
     *
     * @param commitMessages 要中止的提交消息列表
     */
    void abort(List<CommitMessage> commitMessages);
}
