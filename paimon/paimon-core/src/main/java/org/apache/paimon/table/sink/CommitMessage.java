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
import org.apache.paimon.data.BinaryRow;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * 分区和分桶的提交消息接口。
 *
 * <p>CommitMessage 是 Table 层写入和提交之间的桥梁：
 * <ul>
 *     <li>{@link TableWrite} 在 prepareCommit 时生成 CommitMessage
 *     <li>{@link TableCommit} 收集 CommitMessage 并创建快照
 * </ul>
 *
 * <p>CommitMessage 包含的信息：
 * <ul>
 *     <li><b>分区</b>：数据所属的分区
 *     <li><b>分桶</b>：数据所属的分桶
 *     <li><b>总分桶数</b>：分区的总分桶数（动态分桶时使用）
 *     <li><b>文件变更</b>：新增、删除、压缩的文件列表（在实现类中）
 * </ul>
 *
 * <p>实现类：
 * <ul>
 *     <li>{@link CommitMessageImpl}：Table 层的完整实现
 *     <li>包含 {@link DataIncrement} 和 {@link CompactIncrement}
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public interface CommitMessage extends Serializable {

    /**
     * 获取此提交消息的分区。
     *
     * @return 分区的二进制表示
     */
    BinaryRow partition();

    /**
     * 获取此提交消息的分桶号。
     *
     * @return 分桶号
     */
    int bucket();

    /**
     * 获取分区的总分桶数。
     *
     * <p>对于动态分桶表，此值会随着数据写入而增长。
     * 对于固定分桶表，此值等于表配置的分桶数。
     *
     * @return 总分桶数，如果不适用则返回 null
     */
    @Nullable
    Integer totalBuckets();
}
