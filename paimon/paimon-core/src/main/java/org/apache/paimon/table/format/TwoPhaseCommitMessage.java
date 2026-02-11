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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.table.sink.CommitMessage;

import javax.annotation.Nullable;

/**
 * FormatTable 的两阶段提交消息。
 *
 * <p>TwoPhaseCommitMessage 封装了 {@link org.apache.paimon.fs.TwoPhaseOutputStream.Committer}，
 * 用于实现外部格式文件的事务性写入。
 *
 * <h3>两阶段提交流程：</h3>
 * <ol>
 *   <li><b>准备阶段（prepareCommit）</b>：
 *     <ul>
 *       <li>写入器将数据写入临时文件（如 .part-xxx.tmp）
 *       <li>创建 Committer 并返回 TwoPhaseCommitMessage
 *     </ul>
 *   <li><b>提交阶段（commit）</b>：
 *     <ul>
 *       <li>FormatTableCommit 调用 Committer.commit()
 *       <li>将临时文件重命名为最终文件（原子操作）
 *     </ul>
 *   <li><b>清理阶段</b>：
 *     <ul>
 *       <li>调用 Committer.clean() 删除临时文件
 *     </ul>
 * </ol>
 *
 * <h3>与 CommitMessageImpl 的区别：</h3>
 * <ul>
 *   <li><b>无 DataFile</b>：不包含 DataFileMeta，只有文件路径
 *   <li><b>无 Bucket</b>：FormatTable 不使用分桶
 *   <li><b>轻量级</b>：只存储 Committer 引用，不存储复杂的元数据
 * </ul>
 *
 * @see FormatTableCommit
 * @see FormatTableFileWriter
 */
public class TwoPhaseCommitMessage implements CommitMessage {

    /** 两阶段提交器 */
    private final TwoPhaseOutputStream.Committer committer;

    /**
     * 构造 TwoPhaseCommitMessage。
     *
     * @param committer 两阶段提交器
     */
    public TwoPhaseCommitMessage(TwoPhaseOutputStream.Committer committer) {
        this.committer = committer;
    }

    /**
     * 获取分区（FormatTable 在提交消息中不存储分区，返回 null）。
     *
     * @return null
     */
    @Override
    public BinaryRow partition() {
        return null;
    }

    /**
     * 获取 Bucket（FormatTable 不使用分桶，返回 0）。
     *
     * @return 0
     */
    @Override
    public int bucket() {
        return 0;
    }

    /**
     * 获取总 Bucket 数（FormatTable 不使用分桶，返回 0）。
     *
     * @return 0
     */
    @Override
    public @Nullable Integer totalBuckets() {
        return 0;
    }

    /**
     * 获取两阶段提交器。
     *
     * @return 提交器
     */
    public TwoPhaseOutputStream.Committer getCommitter() {
        return committer;
    }
}
