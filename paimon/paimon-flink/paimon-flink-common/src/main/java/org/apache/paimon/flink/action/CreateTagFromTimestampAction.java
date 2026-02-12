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

import org.apache.paimon.flink.procedure.CreateTagFromTimestampProcedure;

import org.apache.flink.table.procedure.DefaultProcedureContext;

import java.util.Map;

/**
 * 从时间戳创建标签（Tag）操作。
 *
 * <p>用于根据指定的时间戳在 Paimon 表中创建标签。标签（Tag）是表在某一时刻的快照，记录了该时刻的表结构和数据状态。
 * 通过时间戳创建标签，可以快速回溯到过去的某个具体时间点。
 *
 * <p>主要特性：
 * <ul>
 *   <li>支持根据时间戳自动查找最接近的快照</li>
 *   <li>支持设置标签的保留时间（timeRetained），超期后自动清理</li>
 *   <li>本地执行操作，无需 Flink 集群</li>
 *   <li>自动化调用底层存储过程完成标签创建</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 *     CreateTagFromTimestampAction action = new CreateTagFromTimestampAction(
 *         "mydb",
 *         "mytable",
 *         "v1.0_2024-01-15",
 *         1705276800000L,  // 2024-01-15 00:00:00 UTC 的毫秒时间戳
 *         "7 d",           // 标签保留 7 天
 *         catalogConfig
 *     );
 *     action.run();
 * }</pre>
 *
 * <p>参数说明：
 * <ul>
 *   <li>database: 目标数据库名称</li>
 *   <li>table: 目标表名称</li>
 *   <li>tag: 新标签的名称，应具有唯一性</li>
 *   <li>timestamp: 时间戳（毫秒），用于定位快照</li>
 *   <li>timeRetained: 标签保留时间，如 "7 d"、"30 d"，超期后自动清理</li>
 * </ul>
 *
 * @see CreateTagFromTimestampProcedure
 * @see ActionBase
 * @since 0.1
 */
public class CreateTagFromTimestampAction extends ActionBase implements LocalAction {

    private final String database;
    private final String table;
    private final String tag;
    private final Long timestamp;
    private final String timeRetained;

    public CreateTagFromTimestampAction(
            String database,
            String table,
            String tag,
            Long timestamp,
            String timeRetained,
            Map<String, String> catalogConfig) {
        super(catalogConfig);
        this.database = database;
        this.table = table;
        this.tag = tag;
        this.timestamp = timestamp;
        this.timeRetained = timeRetained;
    }

    @Override
    public void executeLocally() throws Exception {
        CreateTagFromTimestampProcedure createTagFromTimestampProcedure =
                new CreateTagFromTimestampProcedure();
        createTagFromTimestampProcedure.withCatalog(catalog);
        createTagFromTimestampProcedure.call(
                new DefaultProcedureContext(env),
                database + "." + table,
                tag,
                timestamp,
                timeRetained);
    }
}
