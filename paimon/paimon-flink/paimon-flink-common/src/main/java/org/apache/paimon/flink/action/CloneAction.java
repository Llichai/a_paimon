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

import org.apache.paimon.catalog.CachingCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.clone.CloneFileFormatUtils;
import org.apache.paimon.flink.clone.CloneHiveTableUtils;
import org.apache.paimon.flink.clone.ClonePaimonTableUtils;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/**
 * 克隆表操作 - 用于在不同 Catalog 之间复制表数据和元数据。
 *
 * <p>CloneAction 用于将源表的数据和元数据复制到目标 Catalog 中，创建一个新表。支持 Hive 表到 Paimon
 * 表的克隆，以及 Paimon 表到 Paimon 表的克隆。克隆操作可以配置为仅复制元数据或同时复制数据。
 *
 * <p>克隆的主要特性：
 * <ul>
 *   <li><b>跨 Catalog 复制</b>: 支持在不同的 Catalog 之间传输表
 *   <li><b>灵活的克隆模式</b>: 支持元数据克隆或完整克隆
 *   <li><b>选择性复制</b>: 可以选择特定的子表或使用 SQL 条件过滤数据
 *   <li><b>格式转换</b>: 支持指定目标文件格式
 *   <li><b>冲突处理</b>: 支持目标表存在时的处理策略
 * </ul>
 *
 * <p>克隆来源：
 * <ul>
 *   <li><b>"hive"</b>: 从 Hive Metastore 克隆表，需要使用 HiveCatalog
 *   <li><b>"paimon"</b>: 从 Paimon Catalog 克隆表
 *   <li><b>"iceberg"</b>: 从 Iceberg Catalog 克隆表
 * </ul>
 *
 * <p>克隆模式：
 * <pre>
 * 元数据克隆（metaOnly=true）: 仅复制表结构和元数据，不复制数据
 * 完整克隆（metaOnly=false）: 复制表结构和所有数据
 * </pre>
 *
 * <p>应用场景：
 * <ul>
 *   <li><b>数据迁移</b>: 从 Hive 表迁移到 Paimon
 *   <li><b>表克隆</b>: 基于现有表创建副本或测试环境
 *   <li><b>跨地域复制</b>: 复制表到另一个 Catalog
 *   <li><b>数据子集提取</b>: 使用 SQL 条件复制部分数据
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * CloneAction action = new CloneAction(
 *     "source_db",                     // 源数据库
 *     "source_table",                  // 源表名
 *     sourceConfig,                    // 源 Catalog 配置
 *     "target_db",                     // 目标数据库
 *     "target_table",                  // 目标表名
 *     targetConfig,                    // 目标 Catalog 配置
 *     4,                               // 并行度
 *     "SELECT * WHERE year='2023'",    // 条件过滤（可选）
 *     null,                            // 包含的表
 *     null,                            // 排除的表
 *     "parquet",                       // 文件格式
 *     "hive",                          // 克隆来源
 *     false,                           // 是否仅复制元数据
 *     true                             // 目标存在时是否覆盖
 * );
 * action.run();
 * }</pre>
 *
 * <p>注意事项：
 * <ul>
 *   <li>如果从 Hive 克隆，源 Catalog 必须是 HiveCatalog
 *   <li>克隆操作是耗时的，特别是数据量大时
 *   <li>应该预先在目标 Catalog 中创建数据库
 *   <li>不支持克隆时修改表结构
 * </ul>
 *
 * @see MigrateDatabaseAction
 * @see MigrateTableAction
 */
public class CloneAction extends ActionBase {

    /** 源 Catalog 的配置信息 */
    private final Map<String, String> sourceCatalogConfig;
    /** 源数据库名称 */
    private final String sourceDatabase;
    /** 源表名称 */
    private final String sourceTableName;

    /** 目标 Catalog 的配置信息 */
    private final Map<String, String> targetCatalogConfig;
    /** 目标数据库名称 */
    private final String targetDatabase;
    /** 目标表名称 */
    private final String targetTableName;

    /** 克隆操作的并行度 */
    private final int parallelism;
    /** SQL 过滤条件，用于选择性复制数据（可选） */
    @Nullable private final String whereSql;
    /** 要包含的子表列表（在克隆数据库时使用，可选） */
    @Nullable private final List<String> includedTables;
    /** 要排除的子表列表（在克隆数据库时使用，可选） */
    @Nullable private final List<String> excludedTables;
    /** 目标文件格式（可选，如 parquet, orc） */
    @Nullable private final String preferFileFormat;
    /** 克隆的数据源类型（hive, paimon, iceberg 等） */
    private final String cloneFrom;
    /** 是否仅复制元数据（true 只复制元数据，false 复制完整数据） */
    private final boolean metaOnly;
    /** 目标表存在时是否覆盖 */
    private final boolean cloneIfExists;

    public CloneAction(
            String sourceDatabase,
            String sourceTableName,
            Map<String, String> sourceCatalogConfig,
            String targetDatabase,
            String targetTableName,
            Map<String, String> targetCatalogConfig,
            @Nullable Integer parallelism,
            @Nullable String whereSql,
            @Nullable List<String> includedTables,
            @Nullable List<String> excludedTables,
            @Nullable String preferFileFormat,
            String cloneFrom,
            boolean metaOnly,
            boolean cloneIfExists) {
        super(sourceCatalogConfig);

        if (cloneFrom.equalsIgnoreCase("hive")) {
            Catalog sourceCatalog = catalog;
            if (sourceCatalog instanceof CachingCatalog) {
                sourceCatalog = ((CachingCatalog) sourceCatalog).wrapped();
            }
            if (!(sourceCatalog instanceof HiveCatalog)) {
                throw new UnsupportedOperationException(
                        "Only support clone hive tables using HiveCatalog, but current source catalog is "
                                + sourceCatalog.getClass().getName());
            }
        }

        this.sourceDatabase = sourceDatabase;
        this.sourceTableName = sourceTableName;
        this.sourceCatalogConfig = sourceCatalogConfig;

        this.targetDatabase = targetDatabase;
        this.targetTableName = targetTableName;
        this.targetCatalogConfig = targetCatalogConfig;

        this.parallelism = parallelism == null ? env.getParallelism() : parallelism;
        this.whereSql = whereSql;
        this.includedTables = includedTables;
        this.excludedTables = excludedTables;
        CloneFileFormatUtils.validateFileFormat(preferFileFormat);
        this.preferFileFormat =
                StringUtils.isNullOrWhitespaceOnly(preferFileFormat)
                        ? preferFileFormat
                        : preferFileFormat.toLowerCase();
        this.cloneFrom = cloneFrom;
        this.metaOnly = metaOnly;
        this.cloneIfExists = cloneIfExists;
    }

    @Override
    public void build() throws Exception {
        switch (cloneFrom) {
            case "hive":
                CloneHiveTableUtils.build(
                        env,
                        catalog,
                        sourceDatabase,
                        sourceTableName,
                        sourceCatalogConfig,
                        targetDatabase,
                        targetTableName,
                        targetCatalogConfig,
                        parallelism,
                        whereSql,
                        includedTables,
                        excludedTables,
                        preferFileFormat,
                        metaOnly,
                        cloneIfExists);
                break;
            case "paimon":
                ClonePaimonTableUtils.build(
                        env,
                        catalog,
                        sourceDatabase,
                        sourceTableName,
                        sourceCatalogConfig,
                        targetDatabase,
                        targetTableName,
                        targetCatalogConfig,
                        parallelism,
                        whereSql,
                        includedTables,
                        excludedTables,
                        preferFileFormat,
                        metaOnly,
                        cloneIfExists);
                break;
        }
    }

    @Override
    public void run() throws Exception {
        build();
        execute("Clone job");
    }

    private void validateFileFormat(String preferFileFormat) {}
}
