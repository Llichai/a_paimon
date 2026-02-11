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

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;

/**
 * 批量写入构建器接口，用于构建 {@link BatchTableWrite} 和 {@link BatchTableCommit}。
 *
 * <p>批量写入适用于离线批处理场景，例如：
 * <ul>
 *     <li>ETL 批量导入
 *     <li>历史数据回填
 *     <li>定期批量更新
 * </ul>
 *
 * <p>分布式批量写入示例：
 *
 * <pre>{@code
 * // 1. 创建 WriteBuilder（可序列化）
 * Table table = catalog.getTable(...);
 * WriteBuilder builder = table.newWriteBuilder();
 *
 * // 2. 在分布式任务中写入记录
 * BatchTableWrite write = builder.newWrite();
 * write.write(...);
 * write.write(...);
 * write.write(...);
 * List<CommitMessage> messages = write.prepareCommit();
 *
 * // 3. 将所有 CommitMessage 收集到全局节点并提交
 * BatchTableCommit commit = builder.newCommit();
 * commit.commit(allCommitMessages());
 * }</pre>
 *
 * <p>批量写入的特点：
 * <ul>
 *     <li>使用固定的 commitIdentifier（{@link #COMMIT_IDENTIFIER}）
 *     <li>支持 OVERWRITE 语义（INSERT OVERWRITE）
 *     <li>不需要状态恢复（无检查点）
 *     <li>所有写入任务完成后一次性提交
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public interface BatchWriteBuilder extends WriteBuilder {

    /**
     * 批量写入的固定提交标识符。
     *
     * <p>批量写入不需要递增的提交标识符，使用固定的 Long.MAX_VALUE。
     * 这与流式写入不同，流式写入需要递增的标识符来实现 exactly-once。
     */
    long COMMIT_IDENTIFIER = Long.MAX_VALUE;

    /**
     * 启用覆写模式，等同于 SQL 的 'INSERT OVERWRITE' 语义。
     *
     * <p>覆写整个表的所有数据。
     *
     * @return 当前 BatchWriteBuilder 实例
     */
    default BatchWriteBuilder withOverwrite() {
        withOverwrite(Collections.emptyMap());
        return this;
    }

    /**
     * 启用覆写模式，等同于 SQL 的 'INSERT OVERWRITE T PARTITION (...)' 语义。
     *
     * <p>仅覆写指定的静态分区。
     *
     * <p>示例：
     * <pre>{@code
     * // 覆写 dt=2024-01-01 分区
     * Map<String, String> staticPartition = new HashMap<>();
     * staticPartition.put("dt", "2024-01-01");
     * builder.withOverwrite(staticPartition);
     * }</pre>
     *
     * @param staticPartition 静态分区键值对，如果为 null 或空，则覆写整个表
     * @return 当前 BatchWriteBuilder 实例
     */
    BatchWriteBuilder withOverwrite(@Nullable Map<String, String> staticPartition);

    /**
     * 创建一个 {@link TableWrite} 用于写入 {@link InternalRow}。
     *
     * @return BatchTableWrite 实例
     */
    @Override
    BatchTableWrite newWrite();

    /**
     * 创建一个 {@link TableCommit} 用于提交 {@link CommitMessage}。
     *
     * @return BatchTableCommit 实例
     */
    @Override
    BatchTableCommit newCommit();
}
