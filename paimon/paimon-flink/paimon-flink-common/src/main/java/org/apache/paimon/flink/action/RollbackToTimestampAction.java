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

import org.apache.paimon.Snapshot;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 回滚到指定时间戳操作 - 用于将表恢复到指定时刻的状态。
 *
 * <p>RollbackToTimestampAction 用于将表回滚到距指定时间戳最近的历史快照状态。与直接指定快照 ID 不同，
 * 此操作允许用户根据时间点而不是快照 ID 来执行回滚，更加直观和易用。
 *
 * <p>工作原理：
 * <ol>
 *   <li>查询快照管理器，找到小于等于指定时间戳的最后一个快照
 *   <li>如果时间戳不对应任何快照，自动选择最接近的早期快照
 *   <li>将表回滚到该快照的状态
 * </ol>
 *
 * <p>时间戳格式：
 * <ul>
 *   <li>毫秒时间戳（Unix Epoch）: 如 1672531200000
 *   <li>建议使用毫秒精度以获得最佳匹配
 *   <li>如果指定的时间戳晚于任何快照，操作将失败
 * </ul>
 *
 * <p>与 RollbackToAction 的区别：
 * <pre>
 * RollbackToAction: 需要知道具体的快照 ID 或标签名称
 * RollbackToTimestampAction: 只需提供时间戳，系统自动查找对应快照
 * </pre>
 *
 * <p>应用场景：
 * <ul>
 *   <li><b>按时间恢复</b>: "我要恢复到昨天 14:00 的数据"
 *   <li><b>指定时刻对账</b>: 恢复到特定交接时间进行数据对账
 *   <li><b>事件追踪</b>: 追溯某个事件发生时的表状态
 *   <li><b>版本恢复</b>: 基于发布时间来恢复版本
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 获取当前时间戳（毫秒）
 * long targetTime = System.currentTimeMillis() - 86400000; // 24小时前
 *
 * RollbackToTimestampAction action = new RollbackToTimestampAction(
 *     "my_db",
 *     "my_table",
 *     targetTime,
 *     catalogConfig
 * );
 * action.executeLocally();
 * }</pre>
 *
 * <p>错误处理：
 * <ul>
 *   <li>如果指定的时间戳早于任何快照，将抛出异常
 *   <li>如果时间戳无效，系统会报错
 *   <li>快照必须存在且未被清理
 * </ul>
 *
 * @see RollbackToAction
 * @see CreateTagFromTimestampAction
 */
public class RollbackToTimestampAction extends TableActionBase implements LocalAction {

    private static final Logger LOG = LoggerFactory.getLogger(RollbackToTimestampAction.class);

    /** 目标回滚时间戳（毫秒） */
    private final Long timestamp;

    /**
     * 构造回滚到指定时间戳的操作
     *
     * @param databaseName 数据库名称
     * @param tableName 表名称
     * @param timestamp 目标时间戳（毫秒级 Unix Epoch）
     * @param catalogConfig Catalog 配置参数
     */
    public RollbackToTimestampAction(
            String databaseName,
            String tableName,
            Long timestamp,
            Map<String, String> catalogConfig) {
        super(databaseName, tableName, catalogConfig);
        this.timestamp = timestamp;
    }

    @Override
    public void executeLocally() throws Exception {
        LOG.debug("Run rollback-to-timestamp action with timestamp '{}'.", timestamp);

        // 验证表是否为 DataTable 类型
        if (!(table instanceof DataTable)) {
            throw new IllegalArgumentException("Unknown table: " + identifier);
        }

        // 强制转换为 FileStoreTable 以访问快照管理器
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        // 查找小于等于指定时间戳的最后一个快照
        Snapshot snapshot = fileStoreTable.snapshotManager().earlierOrEqualTimeMills(timestamp);
        // 验证找到的快照是否存在
        Preconditions.checkNotNull(
                snapshot, String.format("count not find snapshot earlier than %s", timestamp));
        // 执行回滚到该快照
        fileStoreTable.rollbackTo(snapshot.id());
    }
}
