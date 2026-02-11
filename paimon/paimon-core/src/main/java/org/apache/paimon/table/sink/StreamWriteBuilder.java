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
import org.apache.paimon.data.InternalRow;

/**
 * 流式写入构建器接口,用于构建 {@link StreamTableWrite} 和 {@link StreamTableCommit}。
 *
 * <p>流式写入适用于实时流处理场景，例如：
 * <ul>
 *     <li>实时数据接入（Kafka、Pulsar 等）
 *     <li>CDC 数据同步
 *     <li>实时数据转换和聚合
 * </ul>
 *
 * <p>实现 exactly-once 一致性的关键点：
 *
 * <ul>
 *   <li><b>CommitUser</b>: 代表一个用户（应用）。一个用户可以提交多次。
 *       在分布式处理中，所有节点应使用相同的 commitUser。
 *   <li><b>不同的应用需要使用不同的 commitUser</b>，以避免提交冲突。
 *   <li><b>CommitIdentifier</b>: {@link StreamTableWrite} 和 {@link StreamTableCommit}
 *       的 commitIdentifier 需要保持一致，并且在下次提交时递增。
 *   <li><b>失败恢复</b>: 当发生故障时，如果还有未提交的 {@link CommitMessage}，
 *       请使用 {@link StreamTableCommit#filterAndCommit} 重试，该方法会根据
 *       commitIdentifier 过滤掉已经提交的消息。
 * </ul>
 *
 * <p>流式写入的特点：
 * <ul>
 *     <li>使用递增的 commitIdentifier（通常来自 Checkpoint ID）
 *     <li>支持从检查点恢复
 *     <li>需要状态管理（存储未提交的 CommitMessage）
 *     <li>定期提交（每个 Checkpoint 一次）
 * </ul>
 *
 * <p>与批量写入的区别：
 * <pre>
 * 特性               | 批量写入              | 流式写入
 * -------------------|----------------------|----------------------
 * CommitIdentifier   | 固定值（Long.MAX）    | 递增值（Checkpoint ID）
 * CommitUser         | 不需要                | 必需（唯一标识）
 * 状态恢复           | 不支持                | 支持
 * 提交频率           | 一次性                | 定期（Checkpoint）
 * </pre>
 *
 * @since 0.4.0
 */
@Public
public interface StreamWriteBuilder extends WriteBuilder {

    /**
     * 获取提交用户标识，由 {@link #withCommitUser} 设置。
     *
     * <p>CommitUser 用于标识提交者，确保不同应用之间的提交不会冲突。
     *
     * @return 提交用户标识
     */
    String commitUser();

    /**
     * 设置提交用户标识，默认值为随机 UUID。
     *
     * <p>{@link TableWrite} 和 {@link TableCommit} 使用的 commitUser 必须相同，
     * 否则会导致提交冲突。
     *
     * <p>使用建议：
     * <ul>
     *     <li>生产环境应使用固定的 commitUser（例如应用名称）
     *     <li>不同的应用使用不同的 commitUser
     *     <li>同一个应用的所有实例使用相同的 commitUser
     * </ul>
     *
     * @param commitUser 提交用户标识
     * @return 当前 StreamWriteBuilder 实例
     */
    StreamWriteBuilder withCommitUser(String commitUser);

    /**
     * 创建一个 {@link TableWrite} 用于写入 {@link InternalRow}。
     *
     * @return StreamTableWrite 实例
     */
    @Override
    StreamTableWrite newWrite();

    /**
     * 创建一个 {@link TableCommit} 用于提交 {@link CommitMessage}。
     *
     * @return StreamTableCommit 实例
     */
    @Override
    StreamTableCommit newCommit();
}
