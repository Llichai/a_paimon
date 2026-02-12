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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.operation.PartitionExpire;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.TimeUtils;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.partition.PartitionExpireStrategy.createPartitionExpireStrategy;

/**
 * 分区过期删除操作 - 用于清理基于时间的分区。
 *
 * <p>ExpirePartitionsAction 用于删除表中达到指定年龄的分区。此操作仅适用于按时间分区的表，
 * 通过指定分区时间戳字段和时间阈值，自动识别和删除过期分区，从而释放存储空间。
 *
 * <p>分区过期策略：
 * <ul>
 *   <li><b>按时间删除</b>: 基于分区列的时间戳值判断是否过期
 *   <li><b>灵活的时间格式</b>: 支持多种日期格式（yyyy-MM-dd, yyyyMMdd 等）
 *   <li><b>自动识别分区列</b>: 根据指定的时间戳列自动定位分区
 *   <li><b>批量删除</b>: 高效地删除多个过期分区
 * </ul>
 *
 * <p>配置参数说明：
 * <pre>
 * partition_expiration_strategy: 分区过期策略（如按日期、小时等）
 * partition_timestamp_formatter: 分区时间戳格式化器
 * partition_timestamp_pattern: 分区列名的时间戳模式
 * table_config: 表级别的额外配置
 * </pre>
 *
 * <p>典型应用场景：
 * <ul>
 *   <li><b>日志数据管理</b>: 自动删除超过 30 天的日志分区
 *   <li><b>事件数据清理</b>: 清理历史事件数据
 *   <li><b>存储成本优化</b>: 定期清理过期分区以降低存储成本
 *   <li><b>数据合规性</b>: 满足数据保留期限要求
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * Map<String, String> tableConfig = new HashMap<>();
 * tableConfig.put("partition.expiration-strategy", "days");
 *
 * ExpirePartitionsAction action = new ExpirePartitionsAction(
 *     "db",
 *     "events_table",
 *     catalogConfig,
 *     tableConfig,
 *     "30 days",           // 删除 30 天前的分区
 *     "yyyy-MM-dd",        // 时间格式
 *     "dt",                // 分区列名
 *     "days"               // 过期策略
 * );
 * action.executeLocally();
 * }</pre>
 *
 * @throws UnsupportedOperationException 如果表不是 FileStoreTable 类型
 * @see ExpireSnapshotsAction
 * @see DropPartitionAction
 */
public class ExpirePartitionsAction extends TableActionBase implements LocalAction {
    /** 分区过期的时间阈值，如 "30 days" 或 "7 d" */
    private final String expirationTime;
    /** 分区过期和时间戳配置的映射 */
    private final Map<String, String> map;

    /**
     * 构造分区过期删除操作
     *
     * @param databaseName 数据库名称
     * @param tableName 表名称
     * @param catalogConfig Catalog 配置参数
     * @param tableConfig 表级别的额外配置
     * @param expirationTime 分区过期时间，如 "30 days"
     * @param timestampFormatter 分区时间戳的格式化器
     * @param timestampPattern 分区时间戳列的名称或模式
     * @param expireStrategy 分区过期策略，如 "days", "hours"
     * @throws UnsupportedOperationException 如果表不是 FileStoreTable 类型
     */
    public ExpirePartitionsAction(
            String databaseName,
            String tableName,
            Map<String, String> catalogConfig,
            Map<String, String> tableConfig,
            String expirationTime,
            String timestampFormatter,
            String timestampPattern,
            String expireStrategy) {
        super(databaseName, tableName, catalogConfig);
        if (!(table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Only FileStoreTable supports expire_partitions action. The table type is '%s'.",
                            table.getClass().getName()));
        }
        table = table.copy(tableConfig);
        this.expirationTime = expirationTime;
        map = new HashMap<>();
        map.put(CoreOptions.PARTITION_EXPIRATION_STRATEGY.key(), expireStrategy);
        map.put(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), timestampFormatter);
        map.put(CoreOptions.PARTITION_TIMESTAMP_PATTERN.key(), timestampPattern);
    }

    @Override
    public void executeLocally() throws Exception {
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        FileStore<?> fileStore = fileStoreTable.store();
        PartitionExpire partitionExpire =
                fileStore.newPartitionExpire(
                        "",
                        fileStoreTable,
                        TimeUtils.parseDuration(expirationTime),
                        Duration.ofMillis(0L),
                        createPartitionExpireStrategy(
                                CoreOptions.fromMap(map),
                                fileStore.partitionType(),
                                catalogLoader(),
                                new Identifier(
                                        identifier.getDatabaseName(), identifier.getTableName())));

        partitionExpire.expire(Long.MAX_VALUE);
    }
}
