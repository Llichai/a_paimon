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

import org.apache.paimon.flink.procedure.MigrateIcebergTableProcedure;

import java.util.Map;

/**
 * 从 Iceberg 表迁移到 Paimon 表的操作。
 *
 * <p>用于将 Apache Iceberg 中的表迁移到 Paimon 数据湖中。Iceberg 是另一个开源数据湖框架，
 * 本操作提供了平滑的迁移路径，允许用户将 Iceberg 表中的数据、元数据和配置完整迁移到 Paimon。
 *
 * <p>主要特性：
 * <ul>
 *   <li>支持 Iceberg 表的完整迁移（数据+元数据）</li>
 *   <li>保留原表的数据完整性和历史信息</li>
 *   <li>支持自定义 Paimon 表的属性和配置</li>
 *   <li>支持指定并行度加速迁移过程</li>
 *   <li>本地执行操作，通过存储过程完成迁移</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 *     // 迁移 Iceberg 表到 Paimon
 *     MigrateIcebergTableAction action = new MigrateIcebergTableAction(
 *         "iceberg_db.iceberg_table",
 *         catalogConfig,
 *         "warehouse=/path/to/iceberg/warehouse",  // Iceberg 配置
 *         "primary-keys=id",                         // Paimon 表属性
 *         8                                          // 并行度
 *     );
 *     action.run();
 *
 *     // 迁移后可以在 Paimon 中查询数据
 *     // SELECT * FROM paimon_db.iceberg_table;
 * }</pre>
 *
 * <p>参数说明：
 * <ul>
 *   <li>sourceTableFullName: Iceberg 源表的完整名称（格式：数据库.表名）</li>
 *   <li>catalogConfig: Paimon 目标 Catalog 的配置</li>
 *   <li>icebergProperties: Iceberg 连接配置，如仓库路径、元存储地址等</li>
 *   <li>tableProperties: Paimon 表的属性配置，如主键、分区列等</li>
 *   <li>parallelism: 并行度，用于加速数据迁移过程</li>
 * </ul>
 *
 * <p>迁移过程：
 * <ul>
 *   <li>连接 Iceberg 源表并读取元数据</li>
 *   <li>在 Paimon 中创建对应的表结构</li>
 *   <li>并行读取并转换 Iceberg 数据文件</li>
 *   <li>写入 Paimon 表中并完成提交</li>
 * </ul>
 *
 * <p>注意事项：
 * <ul>
 *   <li>源 Iceberg 表应该在迁移期间保持稳定（无并发写入）</li>
 *   <li>目标 Paimon 表在迁移前必须不存在</li>
 *   <li>迁移时间取决于数据量和并行度设置</li>
 *   <li>建议在低负载时段执行迁移</li>
 * </ul>
 *
 * @see MigrateIcebergTableProcedure
 * @see ActionBase
 * @since 0.1
 */
public class MigrateIcebergTableAction extends ActionBase implements LocalAction {

    private final String sourceTableFullName;
    private final String tableProperties;
    private final Integer parallelism;

    private final String icebergProperties;

    public MigrateIcebergTableAction(
            String sourceTableFullName,
            Map<String, String> catalogConfig,
            String icebergProperties,
            String tableProperties,
            Integer parallelism) {
        super(catalogConfig);
        this.sourceTableFullName = sourceTableFullName;
        this.tableProperties = tableProperties;
        this.parallelism = parallelism;
        this.icebergProperties = icebergProperties;
    }

    @Override
    public void executeLocally() throws Exception {
        MigrateIcebergTableProcedure migrateIcebergTableProcedure =
                new MigrateIcebergTableProcedure();
        migrateIcebergTableProcedure.withCatalog(catalog);
        migrateIcebergTableProcedure.call(
                null, sourceTableFullName, icebergProperties, tableProperties, parallelism);
    }
}
