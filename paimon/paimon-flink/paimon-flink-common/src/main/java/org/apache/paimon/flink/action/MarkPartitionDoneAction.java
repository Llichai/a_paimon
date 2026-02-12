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

package org.apache.paimon.flink.action;

import org.apache.paimon.partition.actions.PartitionMarkDoneAction;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.PartitionPathUtils;

import java.util.List;
import java.util.Map;

import static org.apache.paimon.flink.sink.listener.PartitionMarkDoneListener.markDone;

/**
 * 标记分区完成操作。
 *
 * <p>用于在 Paimon 表中标记指定的分区已处理完成。在数据湖场景中，某些数据来自于外部系统，
 * 这些数据需要被标记为"已完成"，以通知下游系统可以开始处理该分区数据。
 *
 * <p>主要特性：
 * <ul>
 *   <li>支持标记单个或多个分区为完成状态</li>
 *   <li>触发分区级别的事件通知机制</li>
 *   <li>支持自定义分区标记处理逻辑</li>
 *   <li>本地执行操作，无需 Flink 集群</li>
 *   <li>通常与外部数据导入流程集成</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 *     // 标记单个分区完成
 *     Map<String, String> partition = new HashMap<>();
 *     partition.put("year", "2024");
 *     partition.put("month", "01");
 *
 *     MarkPartitionDoneAction action = new MarkPartitionDoneAction(
 *         "mydb",
 *         "mytable",
 *         Collections.singletonList(partition),
 *         catalogConfig
 *     );
 *     action.run();
 *
 *     // 标记多个分区完成
 *     List<Map<String, String>> partitions = Arrays.asList(
 *         createPartition("2024", "01"),
 *         createPartition("2024", "02"),
 *         createPartition("2024", "03")
 *     );
 *     MarkPartitionDoneAction action2 = new MarkPartitionDoneAction(
 *         "mydb",
 *         "mytable",
 *         partitions,
 *         catalogConfig
 *     );
 *     action2.run();
 * }</pre>
 *
 * <p>工作原理：
 * <ul>
 *   <li>解析指定的分区信息</li>
 *   <li>调用存储引擎的分区标记处理器</li>
 *   <li>将分区标记为完成状态</li>
 *   <li>触发相关的事件监听器</li>
 * </ul>
 *
 * <p>注意事项：
 * <ul>
 *   <li>仅支持 FileStoreTable，其他表类型不支持</li>
 *   <li>分区信息必须与表的分区列完全匹配</li>
 *   <li>通常与数据导入工作流协同使用</li>
 * </ul>
 *
 * @see PartitionMarkDoneAction
 * @see TableActionBase
 * @since 0.1
 */
public class MarkPartitionDoneAction extends TableActionBase implements LocalAction {

    private final FileStoreTable fileStoreTable;
    private final List<Map<String, String>> partitions;

    public MarkPartitionDoneAction(
            String databaseName,
            String tableName,
            List<Map<String, String>> partitions,
            Map<String, String> catalogConfig) {
        super(databaseName, tableName, catalogConfig);
        if (!(table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Only FileStoreTable supports mark_partition_done action. The table type is '%s'.",
                            table.getClass().getName()));
        }

        this.fileStoreTable = (FileStoreTable) table;
        this.partitions = partitions;
    }

    @Override
    public void executeLocally() throws Exception {
        List<PartitionMarkDoneAction> actions =
                PartitionMarkDoneAction.createActions(
                        getClass().getClassLoader(), fileStoreTable, fileStoreTable.coreOptions());

        List<String> partitionPaths =
                PartitionPathUtils.generatePartitionPaths(
                        partitions, fileStoreTable.store().partitionType());

        markDone(partitionPaths, actions);

        IOUtils.closeAllQuietly(actions);
    }
}
