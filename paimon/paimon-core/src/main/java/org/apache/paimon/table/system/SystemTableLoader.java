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

package org.apache.paimon.table.system;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.paimon.table.system.AggregationFieldsTable.AGGREGATION_FIELDS;
import static org.apache.paimon.table.system.AllPartitionsTable.ALL_PARTITIONS;
import static org.apache.paimon.table.system.AllTableOptionsTable.ALL_TABLE_OPTIONS;
import static org.apache.paimon.table.system.AllTablesTable.ALL_TABLES;
import static org.apache.paimon.table.system.AuditLogTable.AUDIT_LOG;
import static org.apache.paimon.table.system.BinlogTable.BINLOG;
import static org.apache.paimon.table.system.BranchesTable.BRANCHES;
import static org.apache.paimon.table.system.BucketsTable.BUCKETS;
import static org.apache.paimon.table.system.CatalogOptionsTable.CATALOG_OPTIONS;
import static org.apache.paimon.table.system.ConsumersTable.CONSUMERS;
import static org.apache.paimon.table.system.FilesTable.FILES;
import static org.apache.paimon.table.system.ManifestsTable.MANIFESTS;
import static org.apache.paimon.table.system.OptionsTable.OPTIONS;
import static org.apache.paimon.table.system.PartitionsTable.PARTITIONS;
import static org.apache.paimon.table.system.ReadOptimizedTable.READ_OPTIMIZED;
import static org.apache.paimon.table.system.RowTrackingTable.ROW_TRACKING;
import static org.apache.paimon.table.system.SchemasTable.SCHEMAS;
import static org.apache.paimon.table.system.SnapshotsTable.SNAPSHOTS;
import static org.apache.paimon.table.system.StatisticTable.STATISTICS;
import static org.apache.paimon.table.system.TableIndexesTable.TABLE_INDEXES;
import static org.apache.paimon.table.system.TagsTable.TAGS;

/**
 * 系统表加载器。
 *
 * <p>用于加载和管理 Paimon 的各种系统表。系统表提供了表的元数据信息,如快照、文件、分区等。
 *
 * <h2>支持的系统表类型</h2>
 * <ul>
 *   <li>{@code manifests} - Manifest 文件信息表
 *   <li>{@code snapshots} - 快照信息表
 *   <li>{@code options} - 表选项配置表
 *   <li>{@code schemas} - Schema 版本历史表
 *   <li>{@code partitions} - 分区元数据表
 *   <li>{@code buckets} - 分桶信息表
 *   <li>{@code audit_log} - 审计日志表
 *   <li>{@code files} - 数据文件详细信息表
 *   <li>{@code tags} - 标签管理表
 *   <li>{@code branches} - 分支管理表
 *   <li>{@code consumers} - 消费者进度表
 *   <li>{@code ro} - 读优化表
 *   <li>{@code aggregation_fields} - 聚合字段配置表
 *   <li>{@code statistics} - 表统计信息表
 *   <li>{@code binlog} - Binlog 格式变更日志表
 *   <li>{@code table_indexes} - 表索引信息表
 *   <li>{@code row_tracking} - 行跟踪表(包含 _ROW_ID 字段)
 * </ul>
 *
 * <h2>全局系统表</h2>
 * <p>以下系统表在 Catalog 级别提供,不依赖于特定的数据表:
 * <ul>
 *   <li>{@code tables} - 所有表的元数据信息
 *   <li>{@code partitions} - 所有表的分区信息
 *   <li>{@code all_table_options} - 所有表的选项配置
 *   <li>{@code catalog_options} - Catalog 级别的选项配置
 * </ul>
 *
 * <h2>使用方式</h2>
 * <pre>{@code
 * // 加载某个系统表
 * FileStoreTable dataTable = ...;
 * Table systemTable = SystemTableLoader.load("snapshots", dataTable);
 *
 * // 获取所有系统表名称
 * List<String> allSystemTables = SystemTableLoader.SYSTEM_TABLES;
 *
 * // 获取全局系统表名称
 * List<String> globalTables = SystemTableLoader.loadGlobalTableNames();
 * }</pre>
 *
 * @see Table
 * @see FileStoreTable
 */
public class SystemTableLoader {

    /**
     * 系统表加载器映射表。
     *
     * <p>将系统表名称映射到对应的构造函数,用于根据名称动态创建系统表实例。
     */
    public static final Map<String, Function<FileStoreTable, Table>> SYSTEM_TABLE_LOADERS =
            new ImmutableMap.Builder<String, Function<FileStoreTable, Table>>()
                    .put(MANIFESTS, ManifestsTable::new)
                    .put(SNAPSHOTS, SnapshotsTable::new)
                    .put(OPTIONS, OptionsTable::new)
                    .put(SCHEMAS, SchemasTable::new)
                    .put(PARTITIONS, PartitionsTable::new)
                    .put(BUCKETS, BucketsTable::new)
                    .put(AUDIT_LOG, AuditLogTable::new)
                    .put(FILES, FilesTable::new)
                    .put(TAGS, TagsTable::new)
                    .put(BRANCHES, BranchesTable::new)
                    .put(CONSUMERS, ConsumersTable::new)
                    .put(READ_OPTIMIZED, ReadOptimizedTable::new)
                    .put(AGGREGATION_FIELDS, AggregationFieldsTable::new)
                    .put(STATISTICS, StatisticTable::new)
                    .put(BINLOG, BinlogTable::new)
                    .put(TABLE_INDEXES, TableIndexesTable::new)
                    .put(ROW_TRACKING, RowTrackingTable::new)
                    .build();

    /** 所有支持的系统表名称列表。 */
    public static final List<String> SYSTEM_TABLES = new ArrayList<>(SYSTEM_TABLE_LOADERS.keySet());

    /** 全局级别系统表名称列表(Catalog 级别)。 */
    public static final List<String> GLOBAL_SYSTEM_TABLES =
            Arrays.asList(ALL_TABLES, ALL_PARTITIONS, ALL_TABLE_OPTIONS, CATALOG_OPTIONS);

    /**
     * 根据类型加载对应的系统表。
     *
     * @param type 系统表类型名称(不区分大小写),如 "snapshots", "files" 等
     * @param dataTable 数据表实例,系统表将基于此表提供元数据信息
     * @return 对应的系统表实例,如果类型不存在则返回 null
     */
    @Nullable
    public static Table load(String type, FileStoreTable dataTable) {
        return Optional.ofNullable(SYSTEM_TABLE_LOADERS.get(type.toLowerCase()))
                .map(f -> f.apply(dataTable))
                .orElse(null);
    }

    /**
     * 获取全局系统表的名称列表。
     *
     * @return 全局系统表名称列表
     */
    public static List<String> loadGlobalTableNames() {
        return GLOBAL_SYSTEM_TABLES;
    }
}
