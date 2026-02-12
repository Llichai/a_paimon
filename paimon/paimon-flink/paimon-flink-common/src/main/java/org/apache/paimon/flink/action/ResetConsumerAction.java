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

import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.table.FileStoreTable;

import java.util.Map;
import java.util.Objects;

/**
 * 重置消费者操作。
 *
 * <p>用于重置 Paimon 表中某个消费者的消费进度。消费者是订阅表数据变化的客户端，每个消费者都维护独立的消费进度。
 * 当消费进度需要回溯到过去的某个快照，或者需要重新消费历史数据时，可以使用本操作。
 *
 * <p>主要特性：
 * <ul>
 *   <li>支持将消费者进度重置到指定的快照</li>
 *   <li>支持删除消费者（nextSnapshotId 为 null）</li>
 *   <li>本地执行操作，无需 Flink 集群</li>
 *   <li>支持重新消费历史数据</li>
 *   <li>精确控制消费进度</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 *     // 重置消费者的消费进度到指定的快照
 *     ResetConsumerAction action = new ResetConsumerAction(
 *         "mydb",
 *         "mytable",
 *         catalogConfig,
 *         "my_consumer"
 *     )
 *     .withNextSnapshotIds(1000L);  // 重置到快照 ID 1000
 *     action.run();
 *
 *     // 删除消费者
 *     ResetConsumerAction action2 = new ResetConsumerAction(
 *         "mydb",
 *         "mytable",
 *         catalogConfig,
 *         "my_consumer"
 *     );
 *     // 不设置 nextSnapshotId，则删除该消费者
 *     action2.run();
 * }</pre>
 *
 * <p>参数说明：
 * <ul>
 *   <li>databaseName: 目标数据库名称</li>
 *   <li>tableName: 目标表名称</li>
 *   <li>consumerId: 消费者 ID，用于标识消费者</li>
 *   <li>nextSnapshotId: 重置到的目标快照 ID（可选）
 *       <ul>
 *         <li>如果指定，消费者进度被重置到该快照</li>
 *         <li>如果为 null，消费者被删除</li>
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p>重置过程：
 * <ul>
 *   <li>验证目标快照的有效性</li>
 *   <li>更新消费者的消费进度</li>
 *   <li>保存消费者的新状态</li>
 * </ul>
 *
 * <p>典型应用场景：
 * <ul>
 *   <li>消费者处理出错，需要回溯重新处理</li>
 *   <li>业务需要重新消费历史数据</li>
 *   <li>消费端数据损坏，需要从某个已知的好快照重新开始</li>
 *   <li>清理不再使用的消费者</li>
 * </ul>
 *
 * <p>注意事项：
 * <ul>
 *   <li>目标快照必须存在且有效</li>
 *   <li>重置后，消费者需要重新开始从该快照消费数据</li>
 *   <li>重置到很旧的快照时，可能需要较长的数据重放时间</li>
 *   <li>消费者 ID 必须已存在，否则操作可能失败</li>
 * </ul>
 *
 * @see ConsumerManager
 * @see TableActionBase
 * @since 0.1
 */
public class ResetConsumerAction extends TableActionBase implements LocalAction {

    private final String consumerId;
    private Long nextSnapshotId;

    protected ResetConsumerAction(
            String databaseName,
            String tableName,
            Map<String, String> catalogConfig,
            String consumerId) {
        super(databaseName, tableName, catalogConfig);
        this.consumerId = consumerId;
    }

    public ResetConsumerAction withNextSnapshotIds(Long nextSnapshotId) {
        this.nextSnapshotId = nextSnapshotId;
        return this;
    }

    @Override
    public void executeLocally() throws Exception {
        FileStoreTable dataTable = (FileStoreTable) table;
        ConsumerManager consumerManager =
                new ConsumerManager(
                        dataTable.fileIO(),
                        dataTable.location(),
                        dataTable.snapshotManager().branch());
        if (Objects.isNull(nextSnapshotId)) {
            consumerManager.deleteConsumer(consumerId);
        } else {
            dataTable.snapshotManager().snapshot(nextSnapshotId);
            consumerManager.resetConsumer(consumerId, new Consumer(nextSnapshotId));
        }
    }
}
