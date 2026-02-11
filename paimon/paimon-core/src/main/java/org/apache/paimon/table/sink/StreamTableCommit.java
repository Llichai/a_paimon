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

import java.util.List;
import java.util.Map;

/**
 * 流处理的 {@link TableCommit} 接口。支持多次提交。
 *
 * <p>流式提交的特点：
 * <ul>
 *     <li><b>多次提交</b>：可以多次调用 commit 方法，每次对应一个 Checkpoint
 *     <li><b>递增标识符</b>：使用递增的 commitIdentifier 确保 exactly-once
 *     <li><b>过滤重复</b>：支持过滤已提交的消息，用于故障恢复
 *     <li><b>自动过期</b>：提交后自动触发快照和分区的过期策略
 * </ul>
 *
 * <p>提交后的维护策略：
 * <ol>
 *     <li><b>快照过期</b>：根据以下配置清理过期快照：
 *         <ul>
 *             <li>'snapshot.time-retained': 保留已完成快照的最长时间
 *             <li>'snapshot.num-retained.min': 保留已完成快照的最小数量
 *             <li>'snapshot.num-retained.max': 保留已完成快照的最大数量
 *         </ul>
 *     <li><b>分区过期</b>：根据 'partition.expiration-time' 删除过期分区。
 *         分区检查开销较大，不是每次提交都检查，检查频率由
 *         'partition.expiration-check-interval' 控制。分区过期会创建一个
 *         'OVERWRITE' 类型的快照。
 * </ol>
 *
 * <p>使用示例（Flink Streaming）：
 * <pre>{@code
 * // 1. 创建流式提交
 * StreamTableCommit commit = builder.newCommit();
 *
 * // 2. 正常提交（快速路径）
 * commit.commit(checkpointId, messages);
 *
 * // 3. 故障恢复时使用 filterAndCommit
 * Map<Long, List<CommitMessage>> pending = restoreFromState();
 * int committed = commit.filterAndCommit(pending);
 *
 * // 4. 关闭
 * commit.close();
 * }</pre>
 *
 * @since 0.4.0
 * @see StreamWriteBuilder
 */
@Public
public interface StreamTableCommit extends TableCommit {

    /**
     * 创建一个新的提交。
     *
     * <p>一次提交可能生成最多两个快照：
     * <ul>
     *     <li>一个用于添加新文件（CommitKind.APPEND）
     *     <li>另一个用于压缩（CommitKind.COMPACT）
     * </ul>
     *
     * <p>提交后会执行过期策略，详见类文档。
     *
     * <p>与 {@link #filterAndCommit} 相比，此方法不检查 {@code commitIdentifier}
     * 是否已经提交，因此速度更快。请仅在确保 {@code commitIdentifier} 之前未提交时使用此方法。
     *
     * <p>使用场景：
     * <ul>
     *     <li>正常的 Checkpoint 提交（快速路径）
     *     <li>确定没有重复提交的场景
     * </ul>
     *
     * @param commitIdentifier 提交的事务 ID，可以从 0 开始。如果有多次提交，请递增此 ID
     * @param commitMessages 来自 TableWrite 的提交消息
     * @see StreamTableWrite#prepareCommit
     */
    void commit(long commitIdentifier, List<CommitMessage> commitMessages);

    /**
     * 过滤掉所有已提交的 {@code List<CommitMessage>}，并提交剩余的。
     *
     * <p>与 {@link #commit} 相比，此方法会首先检查 {@code commitIdentifier}
     * 是否已经提交，因此速度较慢。此方法的常见用途是在失败后重试提交过程。
     *
     * <p>工作原理：
     * <ol>
     *     <li>查询已提交的快照，获取已提交的 commitIdentifier 列表
     *     <li>过滤掉已提交的 commitIdentifier 对应的 CommitMessage
     *     <li>提交剩余的 CommitMessage
     *     <li>返回实际提交的数量
     * </ol>
     *
     * <p>使用场景：
     * <ul>
     *     <li>从检查点或保存点恢复
     *     <li>任务失败后重试
     *     <li>不确定是否重复提交的场景
     * </ul>
     *
     * <p>示例：
     * <pre>{@code
     * // 恢复时可能包含已提交和未提交的消息
     * Map<Long, List<CommitMessage>> pending = new HashMap<>();
     * pending.put(100L, messages1);  // 可能已提交
     * pending.put(101L, messages2);  // 可能已提交
     * pending.put(102L, messages3);  // 未提交
     *
     * // filterAndCommit 会过滤已提交的，只提交未提交的
     * int committed = commit.filterAndCommit(pending);
     * // committed = 1 (只提交了 102)
     * }</pre>
     *
     * @param commitIdentifiersAndMessages 包含所有待提交的 {@link CommitMessage} 的映射。
     *                                     键是 {@code commitIdentifier}
     * @return 实际提交的 {@code List<CommitMessage>} 数量
     */
    int filterAndCommit(Map<Long, List<CommitMessage>> commitIdentifiersAndMessages);
}
