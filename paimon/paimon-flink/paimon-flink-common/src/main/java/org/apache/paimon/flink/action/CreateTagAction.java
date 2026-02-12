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

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Map;

/**
 * 创建标签操作 - Flink 执行
 *
 * <p>CreateTagAction 用于在 Paimon 表中创建新的标签（Tag）。标签是 Paimon 版本管理的核心功能，
 * 用于标记表在某个时刻的状态快照，支持后续的版本回溯、分支创建等操作。
 *
 * <p>标签的主要特性:
 * <ul>
 *   <li><b>不可变</b>: 标签一旦创建就不能修改，保证数据的完整性
 *   <li><b>快照</b>: 标签是表在创建时刻的一个完整快照
 *   <li><b>时间保留</b>: 支持为标签设置保留期限
 *   <li><b>版本标记</b>: 便于识别重要版本（如 v1.0, v2.0 等）
 * </ul>
 *
 * <p>标签的应用场景:
 * <ul>
 *   <li><b>版本发布</b>: 发布新版本时创建标签
 *   <li><b>数据备份</b>: 定期创建标签作为数据快照
 *   <li><b>分支创建</b>: 基于特定标签创建开发分支
 *   <li><b>时间点恢复</b>: 快速回到某个历史版本
 *   <li><b>审计日志</b>: 追踪重要的数据变更点
 * </ul>
 *
 * <p>创建标签的方式:
 * <ul>
 *   <li><b>指定快照 ID</b>: 基于特定的快照 ID 创建标签
 *   <li><b>使用当前快照</b>: 基于表的最新快照创建标签
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 1. 基于最新快照创建标签
 * CreateTagAction action1 = new CreateTagAction(
 *     "my_db",
 *     "my_table",
 *     catalogConfig,
 *     "v1.0",
 *     null,                      // snapshotId = null，使用最新快照
 *     Duration.ofDays(30)         // 保留 30 天
 * );
 * action1.executeLocally();
 *
 * // 2. 基于指定快照创建标签
 * CreateTagAction action2 = new CreateTagAction(
 *     "my_db",
 *     "my_table",
 *     catalogConfig,
 *     "release-v2.0",
 *     123L,                       // snapshotId = 123L
 *     Duration.ofDays(365)        // 保留 1 年
 * );
 * action2.executeLocally();
 *
 * // 3. 创建永久保存的标签
 * CreateTagAction action3 = new CreateTagAction(
 *     "my_db",
 *     "my_table",
 *     catalogConfig,
 *     "archive-2024",
 *     null,
 *     null                        // timeRetained = null，永久保存
 * );
 * action3.executeLocally();
 * }</pre>
 *
 * <p>标签生命周期:
 * <pre>
 * 创建标签
 *     ↓
 * 设置保留期限（可选）
 *     ↓
 * 标签有效期内可读取数据
 *     ↓
 * 超期后根据过期策略删除（可选）
 *     ↓
 * 标签被清理
 * </pre>
 *
 * @see DeleteTagAction
 * @see ExpireTagsAction
 * @see org.apache.paimon.table.Table#createTag(String)
 * @see org.apache.paimon.table.Table#createTag(String, long)
 * @see org.apache.paimon.table.Table#createTag(String, java.time.Duration)
 */
public class CreateTagAction extends TableActionBase implements LocalAction {

    /** 要创建的标签名称 */
    private final String tagName;
    /**
     * 要基于的快照 ID（可选）。如果为 null，则基于表的最新快照创建；否则基于指定的快照 ID 创建
     */
    private final @Nullable Long snapshotId;
    /**
     * 标签的保留时间（可选）。表示标签有效期，超期后可能被过期策略清理。为 null 时标签永久保存
     */
    private final @Nullable Duration timeRetained;

    /**
     * 构造函数
     *
     * @param databaseName 数据库名称
     * @param tableName 表名称
     * @param catalogConfig Catalog 配置参数（如 warehouse 路径、连接信息等）
     * @param tagName 要创建的标签名称
     * @param snapshotId 要基于的快照 ID（可选，为 null 时使用最新快照）
     * @param timeRetained 标签的保留时间（可选，为 null 时永久保存）
     */
    public CreateTagAction(
            String databaseName,
            String tableName,
            Map<String, String> catalogConfig,
            String tagName,
            @Nullable Long snapshotId,
            @Nullable Duration timeRetained) {
        super(databaseName, tableName, catalogConfig);
        this.tagName = tagName;
        this.timeRetained = timeRetained;
        this.snapshotId = snapshotId;
    }

    /**
     * 本地执行创建标签操作
     *
     * <p>此方法在驱动程序中直接执行，无需启动 Flink 任务。
     * 根据是否指定了快照 ID 来选择不同的创建方式：
     * <ul>
     *   <li>如果指定了快照 ID：基于该快照创建标签
     *   <li>如果未指定快照 ID：基于最新快照创建标签
     * </ul>
     *
     * <p>执行步骤:
     * <ol>
     *   <li>获取快照信息
     *   <li>创建标签元数据
     *   <li>记录保留时间（如果指定）
     *   <li>提交标签信息
     * </ol>
     */
    @Override
    public void executeLocally() {
        // 根据是否指定了快照 ID 来选择创建方式
        if (snapshotId == null) {
            // 基于最新快照创建标签
            table.createTag(tagName, timeRetained);
        } else {
            // 基于指定快照 ID 创建标签
            table.createTag(tagName, snapshotId, timeRetained);
        }
    }
}
