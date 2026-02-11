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

import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.annotation.Public;
import org.apache.paimon.stats.Statistics;

import java.util.List;
import java.util.Map;

/**
 * 批量处理的 {@link TableCommit} 接口。建议用于一次性提交场景。
 *
 * <p>批量提交的特点：
 * <ul>
 *     <li><b>一次性提交</b>：收集所有 CommitMessage 后一次性提交
 *     <li><b>完整性保证</b>：提交前确保所有文件已写入完成
 *     <li><b>自动过期</b>：提交后自动触发快照和分区的过期策略
 *     <li><b>支持覆写</b>：支持 INSERT OVERWRITE 语义
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
 * <p>使用示例：
 * <pre>{@code
 * // 1. 创建批量提交
 * BatchTableCommit commit = builder.newCommit();
 *
 * // 2. 收集所有提交消息
 * List<CommitMessage> messages = collectAllMessages();
 *
 * // 3. 提交
 * commit.commit(messages);
 *
 * // 4. 关闭
 * commit.close();
 * }</pre>
 *
 * @since 0.4.0
 */
@Public
public interface BatchTableCommit extends TableCommit {

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
     * @param commitMessages 来自 TableWrite 的提交消息
     */
    void commit(List<CommitMessage> commitMessages);

    /**
     * 清空表，类似于 SQL 的 TRUNCATE TABLE 语句。
     *
     * <p>与正常的 {@link #commit} 类似，文件不会立即删除，
     * 只是逻辑删除，会在快照过期后被物理删除。
     *
     * <p>此操作会创建一个 'OVERWRITE' 类型的快照。
     */
    void truncateTable();

    /**
     * 清空指定分区，类似于 SQL 的 TRUNCATE TABLE PARTITION 语句。
     *
     * <p>与正常的 {@link #commit} 类似，文件不会立即删除，
     * 只是逻辑删除，会在快照过期后被物理删除。
     *
     * <p>此操作会创建一个 'OVERWRITE' 类型的快照。
     *
     * @param partitionSpecs 要清空的分区规格列表，每个分区用 Map 表示
     */
    void truncatePartitions(List<Map<String, String>> partitionSpecs);

    /**
     * 提交新的统计信息。
     *
     * <p>生成一个 {@link CommitKind#ANALYZE} 类型的快照。
     *
     * <p>统计信息用于查询优化，例如：
     * <ul>
     *     <li>行数估算
     *     <li>数据分布统计
     *     <li>列值范围统计
     * </ul>
     *
     * @param statistics 新的统计信息
     */
    void updateStatistics(Statistics statistics);

    /**
     * 压缩 Manifest 条目。
     *
     * <p>生成一个 {@link CommitKind#COMPACT} 类型的快照。
     *
     * <p>Manifest 压缩会合并多个 Manifest 文件，减少元数据文件数量，
     * 提高读取性能。
     */
    void compactManifests();
}
