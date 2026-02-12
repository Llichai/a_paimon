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

import org.apache.paimon.flink.procedure.MigrateTableProcedure;

import java.util.Map;

/**
 * 从外部表（如 Hive 表）迁移到 Paimon 表的操作。
 *
 * <p>用于将外部数据系统（主要是 Hive、Presto 等）中的表迁移到 Paimon 数据湖中。
 * 本操作支持多种连接器，可以从不同数据源平滑迁移数据和元数据到 Paimon。
 *
 * <p>支持的连接器类型：
 * <ul>
 *   <li>hive: 从 Hive 表迁移</li>
 *   <li>其他支持的外部数据源连接器</li>
 * </ul>
 *
 * <p>主要特性：
 * <ul>
 *   <li>支持从多种数据源迁移表数据</li>
 *   <li>保留原表的数据完整性和历史信息</li>
 *   <li>支持自定义 Paimon 表的属性和配置</li>
 *   <li>支持指定并行度加速迁移过程</li>
 *   <li>支持增量迁移和全量迁移</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 *     // 从 Hive 表迁移到 Paimon
 *     MigrateTableAction action = new MigrateTableAction(
 *         "hive",                          // 连接器类型
 *         "hive_db.hive_table",            // 源表完整名称
 *         catalogConfig,                   // Paimon Catalog 配置
 *         "primary-keys=id;partition=dt", // Paimon 表属性
 *         8                                // 并行度
 *     );
 *     action.run();
 *
 *     // 迁移后可以在 Paimon 中查询数据
 *     // SELECT * FROM paimon_db.hive_table;
 * }</pre>
 *
 * <p>参数说明：
 * <ul>
 *   <li>connector: 源数据系统的连接器类型（如 "hive"）</li>
 *   <li>hiveTableFullName: 源表的完整名称（格式：数据库.表名）</li>
 *   <li>catalogConfig: Paimon 目标 Catalog 的配置</li>
 *   <li>tableProperties: Paimon 表的属性配置（主键、分区列等）</li>
 *   <li>parallelism: 并行度，用于加速数据迁移过程</li>
 * </ul>
 *
 * <p>迁移过程：
 * <ul>
 *   <li>通过连接器连接源表并读取元数据</li>
 *   <li>在 Paimon 中创建对应的表结构</li>
 *   <li>并行读取源表数据文件</li>
 *   <li>转换数据格式并写入 Paimon 表</li>
 *   <li>完成元数据和数据的一致性提交</li>
 * </ul>
 *
 * <p>注意事项：
 * <ul>
 *   <li>源表应该在迁移期间保持稳定（无并发写入）</li>
 *   <li>目标 Paimon 表在迁移前必须不存在</li>
 *   <li>迁移时间取决于数据量和并行度设置</li>
 *   <li>建议在低负载时段执行迁移</li>
 *   <li>大表迁移建议分阶段或使用并行度优化</li>
 * </ul>
 *
 * @see MigrateTableProcedure
 * @see ActionBase
 * @since 0.1
 */
public class MigrateTableAction extends ActionBase implements LocalAction {

    private final String connector;
    private final String hiveTableFullName;
    private final String tableProperties;
    private final Integer parallelism;

    public MigrateTableAction(
            String connector,
            String hiveTableFullName,
            Map<String, String> catalogConfig,
            String tableProperties,
            Integer parallelism) {
        super(catalogConfig);
        this.connector = connector;
        this.hiveTableFullName = hiveTableFullName;
        this.tableProperties = tableProperties;
        this.parallelism = parallelism;
    }

    @Override
    public void executeLocally() throws Exception {
        MigrateTableProcedure migrateTableProcedure = new MigrateTableProcedure();
        migrateTableProcedure.withCatalog(catalog);
        migrateTableProcedure.call(
                null, connector, hiveTableFullName, null, tableProperties, parallelism, null);
    }
}
