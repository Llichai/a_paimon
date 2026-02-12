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

import org.apache.paimon.flink.procedure.CreateTagFromWatermarkProcedure;

import java.util.Map;

/**
 * 从水位线创建标签（Tag）操作。
 *
 * <p>用于根据 Flink 数据流的水位线（Watermark）在 Paimon 表中创建标签。水位线表示数据处理的进度，
 * 通过水位线创建标签可以基于事件时间而非处理时间来创建快照，更适合基于事件时间的场景。
 *
 * <p>主要特性：
 * <ul>
 *   <li>支持基于水位线时间戳创建标签</li>
 *   <li>基于事件时间而非处理时间，适合实时数据处理场景</li>
 *   <li>支持设置标签的保留时间（timeRetained），超期后自动清理</li>
 *   <li>本地执行操作，无需 Flink 集群</li>
 *   <li>自动调用底层存储过程完成标签创建</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 *     CreateTagFromWatermarkAction action = new CreateTagFromWatermarkAction(
 *         "mydb",
 *         "mytable",
 *         "checkpoint_2024_01_15",
 *         1705276800000L,  // 水位线时间戳（毫秒）
 *         "14 d",          // 标签保留 14 天
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
 *   <li>watermark: 水位线时间戳（毫秒），代表数据处理的进度</li>
 *   <li>timeRetained: 标签保留时间，如 "7 d"、"30 d"，超期后自动清理</li>
 * </ul>
 *
 * <p>与 CreateTagFromTimestampAction 的区别：
 * <ul>
 *   <li>本操作基于水位线（事件时间），更适合流式数据处理</li>
 *   <li>CreateTagFromTimestampAction 基于处理时间，更适合批处理场景</li>
 * </ul>
 *
 * @see CreateTagFromWatermarkProcedure
 * @see ActionBase
 * @since 0.1
 */
public class CreateTagFromWatermarkAction extends ActionBase implements LocalAction {

    private final String database;
    private final String table;
    private final String tag;
    private final Long watermark;
    private final String timeRetained;

    public CreateTagFromWatermarkAction(
            String database,
            String table,
            String tag,
            Long watermark,
            String timeRetained,
            Map<String, String> catalogConfig) {
        super(catalogConfig);
        this.database = database;
        this.table = table;
        this.tag = tag;
        this.watermark = watermark;
        this.timeRetained = timeRetained;
    }

    @Override
    public void executeLocally() throws Exception {
        CreateTagFromWatermarkProcedure createTagFromWatermarkProcedure =
                new CreateTagFromWatermarkProcedure();
        createTagFromWatermarkProcedure.withCatalog(catalog);
        createTagFromWatermarkProcedure.call(
                null, database + "." + table, tag, watermark, timeRetained);
    }
}
