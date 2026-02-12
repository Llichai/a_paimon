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

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;

import java.util.List;
import java.util.Map;

/**
 * 删除分区操作 - 用于从表中删除指定的分区。
 *
 * <p>DropPartitionAction 用于删除表中指定分区的所有数据。此操作仅适用于支持分区的表，并且表必须是
 * FileStoreTable 类型。删除分区是一个快速操作，直接标记分区数据为删除状态，而不需要重新扫描或处理数据。
 *
 * <p>分区删除的应用场景：
 * <ul>
 *   <li><b>数据清理</b>: 删除过期的分区数据（如按日期分区的表）
 *   <li><b>存储优化</b>: 清理不再需要的历史分区以节省存储空间
 *   <li><b>数据隔离</b>: 移除特定的数据子集而保留其他分区
 *   <li><b>合规性</b>: 删除敏感或需要清除的数据分区
 * </ul>
 *
 * <p>分区定义方式：
 * <pre>
 * Map&lt;String, String&gt; partition = new HashMap&lt;&gt;();
 * partition.put("year", "2023");
 * partition.put("month", "01");
 * partition.put("day", "15");
 * List&lt;Map&lt;String, String&gt;&gt; partitions = Arrays.asList(partition);
 * </pre>
 *
 * <p>操作流程：
 * <ol>
 *   <li>验证表是否为 FileStoreTable 类型（必须）
 *   <li>解析要删除的分区信息
 *   <li>通过 BatchTableCommit 执行分区截断操作
 *   <li>提交事务，标记分区数据为已删除
 * </ol>
 *
 * <p>限制条件：
 * <ul>
 *   <li>仅支持 FileStoreTable 类型
 *   <li>表必须配置有分区列
 *   <li>是一个本地操作，不需要启动 Flink 任务
 *   <li>无法恢复已删除的分区数据
 * </ul>
 *
 * @throws UnsupportedOperationException 如果表不是 FileStoreTable 类型
 * @see FastForwardAction
 * @see ExpirePartitionsAction
 */
public class DropPartitionAction extends TableActionBase implements LocalAction {

    /** 要删除的分区列表，每个 Map 包含分区列名与值的对应关系 */
    private final List<Map<String, String>> partitions;

    /**
     * 构造函数
     *
     * @param databaseName 数据库名称
     * @param tableName 表名称
     * @param partitions 要删除的分区列表，每个分区用 Map<分区列名, 分区值> 表示
     * @param catalogConfig Catalog 配置参数（包含 warehouse 路径等）
     * @throws UnsupportedOperationException 如果表不是 FileStoreTable 类型
     */
    public DropPartitionAction(
            String databaseName,
            String tableName,
            List<Map<String, String>> partitions,
            Map<String, String> catalogConfig) {
        super(databaseName, tableName, catalogConfig);
        if (!(table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Only FileStoreTable supports drop-partition action. The table type is '%s'.",
                            table.getClass().getName()));
        }

        this.partitions = partitions;
    }

    @Override
    public void executeLocally() throws Exception {
        // 获取表的批量写入构建器，用于创建提交事务处理器
        BatchTableCommit commit = table.newBatchWriteBuilder().newCommit();
        // 执行分区截断操作，标记指定分区的数据为已删除状态
        commit.truncatePartitions(partitions);
    }
}
