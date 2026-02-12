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
 * 替换标签操作。
 *
 * <p>用于替换或更新 Paimon 表中的已存在标签。当需要将一个标签指向新的快照（版本）时，可以使用本操作。
 * 例如，可以将一个代表"v1.0"的标签重新指向最新的生产快照，而不需要删除和重新创建标签。
 *
 * <p>主要特性：
 * <ul>
 *   <li>支持将标签重新指向不同的快照</li>
 *   <li>支持更新标签的保留时间</li>
 *   <li>保留原标签的名称和引用关系</li>
 *   <li>本地执行操作，无需 Flink 集群</li>
 *   <li>操作原子性，确保数据一致性</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 *     // 替换标签，指向新快照，更新保留时间
 *     ReplaceTagAction action = new ReplaceTagAction(
 *         "mydb",
 *         "mytable",
 *         catalogConfig,
 *         "v1.0",                    // 标签名称
 *         12345L,                    // 新快照 ID（可选）
 *         Duration.ofDays(90)        // 保留 90 天（可选）
 *     );
 *     action.run();
 *
 *     // 仅更新标签的保留时间
 *     ReplaceTagAction action2 = new ReplaceTagAction(
 *         "mydb",
 *         "mytable",
 *         catalogConfig,
 *         "v1.0",
 *         null,                      // 保持原快照
 *         Duration.ofDays(180)       // 更新保留 180 天
 *     );
 *     action2.run();
 * }</pre>
 *
 * <p>参数说明：
 * <ul>
 *   <li>databaseName: 目标数据库名称</li>
 *   <li>tableName: 目标表名称</li>
 *   <li>tagName: 要替换的标签名称（标签必须已存在）</li>
 *   <li>snapshotId: 新快照的 ID（为 null 时保持原快照）</li>
 *   <li>timeRetained: 新的保留时间（为 null 时保持原保留时间）</li>
 * </ul>
 *
 * <p>典型应用场景：
 * <ul>
 *   <li>版本管理：保持固定的版本标签名称，更新其指向的快照</li>
 *   <li>标签生命周期：延长或缩短重要标签的保留时间</li>
 *   <li>生产环保：灰度发布时，将生产标签指向新版本</li>
 * </ul>
 *
 * <p>注意事项：
 * <ul>
 *   <li>标签必须已存在，否则操作失败</li>
 *   <li>快照 ID 必须有效且存在于表中</li>
 *   <li>保留时间的更新会影响自动过期清理策略</li>
 *   <li>不能删除有活跃分支引用的标签</li>
 * </ul>
 *
 * @see TableActionBase
 * @since 0.1
 */
public class ReplaceTagAction extends TableActionBase implements LocalAction {

    private final String tagName;
    private final @Nullable Long snapshotId;
    private final @Nullable Duration timeRetained;

    public ReplaceTagAction(
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

    @Override
    public void executeLocally() throws Exception {
        table.replaceTag(tagName, snapshotId, timeRetained);
    }
}
