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

package org.apache.paimon.append;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;

import java.util.List;
import java.util.Objects;

/**
 * 多表追加压缩任务
 *
 * <p>MultiTableAppendCompactTask 扩展了 {@link AppendCompactTask},
 * 添加了表标识符({@link Identifier}),用于在多表压缩场景中区分不同的表。
 *
 * <p>使用场景:
 * <ul>
 *   <li><b>多表压缩作业</b>:一个压缩作业处理多个表的压缩任务
 *   <li><b>数据库级压缩</b>:批量压缩数据库中的所有 Append-Only 表
 *   <li><b>资源共享</b>:多个表共享同一个压缩线程池和资源
 * </ul>
 *
 * <p>与 {@link AppendCompactTask} 的区别:
 * <ul>
 *   <li>{@link AppendCompactTask}: 单表压缩,不包含表标识
 *   <li>{@link MultiTableAppendCompactTask}: 多表压缩,包含 tableIdentifier
 * </ul>
 *
 * <p>表标识符:
 * {@link Identifier} 包含:
 * <ul>
 *   <li>数据库名(database)
 *   <li>表名(table)
 *   <li>完整标识: {@code database.table}
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建多表压缩任务
 * Identifier tableId = Identifier.create("default_db", "user_events");
 * MultiTableAppendCompactTask task = new MultiTableAppendCompactTask(
 *     partition,
 *     filesToCompact,
 *     tableId
 * );
 *
 * // 执行压缩
 * FileStoreTable table = catalog.getTable(task.tableIdentifier());
 * CommitMessage message = task.doCompact(table, write);
 *
 * // 提交结果
 * table.newCommit().commit(Collections.singletonList(message));
 * }</pre>
 *
 * <p>任务调度:
 * 在多表压缩作业中,可以根据 tableIdentifier 将任务分组:
 * <pre>{@code
 * // 按表分组任务
 * Map<Identifier, List<MultiTableAppendCompactTask>> tasksByTable =
 *     tasks.stream().collect(Collectors.groupingBy(
 *         MultiTableAppendCompactTask::tableIdentifier
 *     ));
 *
 * // 并行压缩不同的表
 * tasksByTable.forEach((tableId, tableTasks) -> {
 *     executor.submit(() -> compactTable(tableId, tableTasks));
 * });
 * }</pre>
 *
 * @see AppendCompactTask 单表压缩任务
 * @see Identifier 表标识符
 */
public class MultiTableAppendCompactTask extends AppendCompactTask {

    private final Identifier tableIdentifier;

    public MultiTableAppendCompactTask(
            BinaryRow partition, List<DataFileMeta> files, Identifier identifier) {
        super(partition, files);
        this.tableIdentifier = identifier;
    }

    public Identifier tableIdentifier() {
        return tableIdentifier;
    }

    public int hashCode() {
        return Objects.hash(partition(), compactBefore(), compactAfter(), tableIdentifier);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MultiTableAppendCompactTask that = (MultiTableAppendCompactTask) o;
        return Objects.equals(partition(), that.partition())
                && Objects.equals(compactBefore(), that.compactBefore())
                && Objects.equals(compactAfter(), that.compactAfter())
                && Objects.equals(tableIdentifier, that.tableIdentifier);
    }
}
