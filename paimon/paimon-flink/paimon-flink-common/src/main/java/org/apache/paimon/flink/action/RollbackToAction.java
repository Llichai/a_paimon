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

import org.apache.paimon.table.DataTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 回滚到指定版本操作 - 用于将表恢复到历史快照的状态。
 *
 * <p>RollbackToAction 用于将表回滚到指定的快照版本。这是一个强大的恢复功能，允许用户从任何历史快照
 * 恢复数据。回滚操作修改主分支的指针，使表展现为指定快照时的状态。
 *
 * <p>版本指定方式：
 * <ul>
 *   <li><b>快照 ID</b>: 使用快照的数字 ID，如 "1", "2", "10" 等
 *   <li><b>标签名称</b>: 使用创建的标签名称，如 "v1.0", "release-2023"
 *   <li>内部会自动判断输入是否为纯数字，选择相应的回滚方式
 * </ul>
 *
 * <p>回滚的后果：
 * <ul>
 *   <li><b>数据恢复</b>: 所有数据回到指定快照的状态
 *   <li><b>元数据恢复</b>: 表的结构信息回到该快照的状态
 *   <li><b>快照保留</b>: 被回滚覆盖的快照仍然保留（除非手动删除）
 *   <li><b>分支独立</b>: 仅影响主分支，其他分支不受影响
 * </ul>
 *
 * <p>应用场景：
 * <ul>
 *   <li><b>数据错误恢复</b>: 发现数据错误后快速恢复
 *   <li><b>版本回滚</b>: 发布的版本有问题时回滚到上一个稳定版本
 *   <li><b>数据对账</b>: 恢复到某个时间点进行数据对账
 *   <li><b>异常排查</b>: 恢复表到正常状态
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 回滚到快照 ID 为 5 的状态
 * RollbackToAction action1 = new RollbackToAction(
 *     "my_db",
 *     "my_table",
 *     "5",
 *     catalogConfig
 * );
 * action1.executeLocally();
 *
 * // 回滚到标签 "v1.0" 对应的快照
 * RollbackToAction action2 = new RollbackToAction(
 *     "my_db",
 *     "my_table",
 *     "v1.0",
 *     catalogConfig
 * );
 * action2.executeLocally();
 * }</pre>
 *
 * <p>注意事项：
 * <ul>
 *   <li>回滚是不可逆的，请谨慎使用
 *   <li>确保要回滚的快照仍然存在且未被清理
 *   <li>回滚后的写入会创建新的快照，与回滚前的分支无法合并
 * </ul>
 *
 * @see CreateTagAction
 * @see RollbackToTimestampAction
 */
public class RollbackToAction extends TableActionBase implements LocalAction {

    private static final Logger LOG = LoggerFactory.getLogger(RollbackToAction.class);

    /** 要回滚到的版本标识（可以是快照 ID 或标签名称） */
    private final String version;

    /**
     * 构造回滚到指定版本的操作
     *
     * @param databaseName 数据库名称
     * @param tableName 表名称
     * @param version 目标版本（快照 ID 或标签名称）
     * @param catalogConfig Catalog 配置参数
     */
    public RollbackToAction(
            String databaseName,
            String tableName,
            String version,
            Map<String, String> catalogConfig) {
        super(databaseName, tableName, catalogConfig);
        this.version = version;
    }

    @Override
    public void executeLocally() throws Exception {
        LOG.debug("Run rollback-to action with snapshot id '{}'.", version);

        // 验证表是否为 DataTable 类型
        if (!(table instanceof DataTable)) {
            throw new IllegalArgumentException("Unknown table: " + identifier);
        }

        // 判断版本标识是否为纯数字快照 ID
        if (version.chars().allMatch(Character::isDigit)) {
            // 按快照 ID 回滚
            table.rollbackTo(Long.parseLong(version));
        } else {
            // 按标签名称回滚
            table.rollbackTo(version);
        }
    }
}
