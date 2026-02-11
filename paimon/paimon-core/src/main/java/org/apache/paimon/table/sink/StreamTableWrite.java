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

/**
 * 流处理的 {@link TableWrite} 接口。支持多次提交。
 *
 * <p>流式写入的特点：
 * <ul>
 *     <li><b>多次提交</b>：可以多次调用 {@link #prepareCommit}，每次对应一个 Checkpoint
 *     <li><b>递增标识符</b>：使用递增的 commitIdentifier（通常是 Checkpoint ID）
 *     <li><b>状态恢复</b>：支持从检查点恢复写入状态
 *     <li><b>可选压缩等待</b>：可以选择不等待压缩完成，提高提交速度
 * </ul>
 *
 * <p>使用示例（Flink Streaming）：
 * <pre>{@code
 * // 1. 创建流式写入
 * StreamTableWrite write = builder.newWrite();
 *
 * // 2. 写入数据（持续进行）
 * while (running) {
 *     InternalRow row = source.next();
 *     write.write(row);
 *
 *     // 3. 在 Checkpoint 时准备提交
 *     if (checkpoint) {
 *         long checkpointId = context.getCheckpointId();
 *         List<CommitMessage> messages = write.prepareCommit(false, checkpointId);
 *         // 将 messages 发送到提交节点
 *     }
 * }
 *
 * // 4. 关闭
 * write.close();
 * }</pre>
 *
 * <p>与批量写入的区别：
 * <pre>
 * 特性              | 流式写入                | 批量写入
 * ------------------|------------------------|------------------------
 * 提交次数          | 多次                   | 一次
 * 提交标识符        | 递增（Checkpoint ID）  | 固定
 * 等待压缩          | 可选（性能优先）       | 是（完整性优先）
 * 状态恢复          | 支持                   | 不支持
 * 使用场景          | 实时数据接入           | ETL、批量导入
 * </pre>
 *
 * @since 0.4.0
 * @see StreamWriteBuilder
 */
@Public
public interface StreamTableWrite extends TableWrite {

    /**
     * 为 {@link TableCommit} 准备提交。收集本次写入的增量文件。
     *
     * <p>此方法会：
     * <ol>
     *     <li>如果 waitCompaction 为 true，等待所有后台压缩任务完成
     *     <li>刷新所有写入器的缓冲区
     *     <li>收集自上次提交以来的所有文件变更
     *     <li>生成 CommitMessage 列表
     *     <li>保存当前状态以支持恢复
     * </ol>
     *
     * <p>提交标识符必须满足：
     * <ul>
     *     <li>从 0 开始（或任意起始值）
     *     <li>每次提交递增
     *     <li>与 {@link StreamTableCommit#commit} 使用相同的值
     * </ul>
     *
     * <p>性能考虑：
     * <ul>
     *     <li>waitCompaction=false: 提交更快，但可能产生更多小文件
     *     <li>waitCompaction=true: 提交较慢，但文件更少更大
     * </ul>
     *
     * @param waitCompaction 是否等待后台压缩任务结束
     * @param commitIdentifier 提交的事务 ID，可以从 0 开始。如果有多次提交，请递增此 ID
     * @return 提交消息列表，包含所有文件变更信息
     * @throws Exception 准备提交过程中的异常
     * @see StreamTableCommit#commit
     */
    List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier)
            throws Exception;
}
