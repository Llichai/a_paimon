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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.MultiTablesSinkMode;
import org.apache.paimon.flink.sink.cdc.UpdatedDataFieldsProcessFunction;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.MultiTablesSinkMode.COMBINED;
import static org.apache.paimon.flink.action.MultiTablesSinkMode.DIVIDED;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;
import static org.apache.paimon.utils.StringUtils.toLowerCaseIfNeed;

/**
 * CDC Action 通用工具类
 *
 * <p>CdcActionCommonUtils 提供了 CDC 数据同步的通用辅助方法，主要功能包括:
 * <ul>
 *   <li><b>Schema 校验</b>: 检查源表 Schema 与 Paimon 表 Schema 的兼容性
 *   <li><b>Schema 构建</b>: 根据源表结构和配置构建 Paimon 表的 Schema
 *   <li><b>表名映射</b>: 支持表名前缀/后缀转换和自定义映射
 *   <li><b>类型映射</b>: 源表数据类型到 Paimon 数据类型的转换
 *   <li><b>大小写转换</b>: 处理大小写敏感的字段名称
 * </ul>
 *
 * <p>配置参数说明:
 * <ul>
 *   <li><b>KAFKA_CONF</b>: Kafka 连接配置
 *   <li><b>MYSQL_CONF</b>: MySQL 源配置
 *   <li><b>POSTGRES_CONF</b>: PostgreSQL 源配置
 *   <li><b>MONGODB_CONF</b>: MongoDB 源配置
 *   <li><b>PULSAR_CONF</b>: Pulsar 连接配置
 *   <li><b>TABLE_PREFIX/SUFFIX</b>: 表名前缀/后缀
 *   <li><b>TABLE_MAPPING</b>: 自定义表名映射规则
 *   <li><b>PRIMARY_KEYS</b>: 指定主键列
 *   <li><b>PARTITION_KEYS</b>: 指定分区键列
 *   <li><b>COMPUTED_COLUMN</b>: 计算列定义
 *   <li><b>METADATA_COLUMN</b>: CDC 元数据列（database_name, table_name 等）
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 检查 Schema 兼容性
 * List<DataField> sourceFields = getSourceTableFields();
 * CdcActionCommonUtils.assertSchemaCompatible(paimonSchema, sourceFields);
 *
 * // 构建 Paimon Schema
 * Schema paimonSchema = CdcActionCommonUtils.buildPaimonSchema(
 *     "my_table",
 *     Arrays.asList("id"),        // primaryKeys
 *     Arrays.asList("year"),      // partitionKeys
 *     Arrays.asList(),             // computedColumns
 *     tableConfig,
 *     sourceSchema,
 *     new CdcMetadataConverter[]{}, // metadataConverters
 *     true                          // caseSensitive
 * );
 * }</pre>
 *
 * @see org.apache.paimon.flink.sink.cdc.UpdatedDataFieldsProcessFunction
 * @see ComputedColumn
 */
public class CdcActionCommonUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CdcActionCommonUtils.class);

    // ========== CDC 源配置常量 ==========
    /** Kafka 连接配置参数名 */
    public static final String KAFKA_CONF = "kafka_conf";
    /** MongoDB 源配置参数名 */
    public static final String MONGODB_CONF = "mongodb_conf";
    /** MySQL 源配置参数名 */
    public static final String MYSQL_CONF = "mysql_conf";
    /** PostgreSQL 源配置参数名 */
    public static final String POSTGRES_CONF = "postgres_conf";
    /** Pulsar 连接配置参数名 */
    public static final String PULSAR_CONF = "pulsar_conf";

    // ========== 表名转换配置常量 ==========
    /** 表名前缀配置参数名 */
    public static final String TABLE_PREFIX = "table_prefix";
    /** 表名后缀配置参数名 */
    public static final String TABLE_SUFFIX = "table_suffix";
    /** 数据库名前缀配置参数名 */
    public static final String TABLE_PREFIX_DB = "table_prefix_db";
    /** 数据库名后缀配置参数名 */
    public static final String TABLE_SUFFIX_DB = "table_suffix_db";
    /** 表名映射规则配置参数名（JSON 格式的映射关系） */
    public static final String TABLE_MAPPING = "table_mapping";

    // ========== 表过滤配置常量 ==========
    /** 包含的源表配置参数名（白名单） */
    public static final String INCLUDING_TABLES = "including_tables";
    /** 排除的源表配置参数名（黑名单） */
    public static final String EXCLUDING_TABLES = "excluding_tables";
    /** 包含的数据库配置参数名（白名单） */
    public static final String INCLUDING_DBS = "including_dbs";
    /** 排除的数据库配置参数名（黑名单） */
    public static final String EXCLUDING_DBS = "excluding_dbs";

    // ========== 类型和字段配置常量 ==========
    /** 数据类型映射配置参数名（JSON 格式的类型转换规则） */
    public static final String TYPE_MAPPING = "type_mapping";
    /** 分区键配置参数名 */
    public static final String PARTITION_KEYS = "partition_keys";
    /** 主键配置参数名 */
    public static final String PRIMARY_KEYS = "primary_keys";
    /** 计算列配置参数名（JSON 格式的列定义和表达式） */
    public static final String COMPUTED_COLUMN = "computed_column";
    /** CDC 元数据列配置参数名（如 database_name, table_name, op_ts） */
    public static final String METADATA_COLUMN = "metadata_column";

    // ========== 其他配置常量 ==========
    /** 多表分区键配置参数名（支持为不同表指定不同分区键） */
    public static final String MULTIPLE_TABLE_PARTITION_KEYS = "multiple_table_partition_keys";
    /** 急切初始化配置参数名（同步开始前初始化所有表） */
    public static final String EAGER_INIT = "eager_init";
    /** 从源 Schema 同步主键配置参数名 */
    public static final String SYNC_PKEYS_FROM_SOURCE_SCHEMA =
            "sync_primary_keys_from_source_schema";

    /**
     * 校验 Paimon Schema 与源表 Schema 的兼容性
     *
     * <p>此方法检查源表的所有字段是否都存在于 Paimon 表中，并且对应字段的类型是否可以转换。
     * 如果不兼容，抛出 IllegalArgumentException 异常。
     *
     * @param paimonSchema Paimon 目标表的 Schema
     * @param sourceTableFields 源表的字段列表
     * @throws IllegalArgumentException 当 Schema 不兼容时抛出，包含详细的错误信息
     */
    public static void assertSchemaCompatible(
            TableSchema paimonSchema, List<DataField> sourceTableFields) {
        if (!schemaCompatible(paimonSchema, sourceTableFields)) {
            throw new IllegalArgumentException(
                    "Paimon schema and source table schema are not compatible.\n"
                            + "Paimon fields are: "
                            + paimonSchema.fields()
                            + ".\nSource table fields are: "
                            + sourceTableFields);
        }
    }

    /**
     * 检查 Paimon Schema 与源表 Schema 的兼容性
     *
     * <p>遍历源表的所有字段，验证:
     * <ol>
     *   <li>字段名称是否存在于 Paimon Schema 中
     *   <li>字段类型是否可以从源类型转换为 Paimon 类型
     * </ol>
     *
     * <p>支持的类型转换规则由 {@link UpdatedDataFieldsProcessFunction#canConvert} 定义，
     * 使用默认的类型映射规则 {@link TypeMapping#defaultMapping()}。
     *
     * @param paimonSchema Paimon 目标表的 Schema
     * @param sourceTableFields 源表的字段列表
     * @return 如果兼容返回 true，否则返回 false
     */
    public static boolean schemaCompatible(
            TableSchema paimonSchema, List<DataField> sourceTableFields) {
        // 遍历源表的每个字段
        for (DataField field : sourceTableFields) {
            // 在 Paimon Schema 中查找同名字段
            int idx = paimonSchema.fieldNames().indexOf(field.name());
            if (idx < 0) {
                // 字段不存在，记录日志并返回不兼容
                LOG.info("Cannot find field '{}' in Paimon table.", field.name());
                return false;
            }
            // 获取 Paimon 中对应字段的类型
            DataType type = paimonSchema.fields().get(idx).type();
            // 检查源类型是否可以转换为 Paimon 类型
            if (UpdatedDataFieldsProcessFunction.canConvert(
                            field.type(), type, TypeMapping.defaultMapping())
                    != UpdatedDataFieldsProcessFunction.ConvertAction.CONVERT) {
                // 类型不兼容，记录日志并返回不兼容
                LOG.info(
                        "Cannot convert field '{}' from source table type '{}' to Paimon type '{}'.",
                        field.name(),
                        field.type(),
                        type);
                return false;
            }
        }
        return true;
    }

    public static List<String> listCaseConvert(List<String> origin, boolean caseSensitive) {
        return caseSensitive
                ? origin
                : origin.stream().map(String::toLowerCase).collect(Collectors.toList());
    }

    public static Schema buildPaimonSchema(
            String tableName,
            List<String> specifiedPartitionKeys,
            List<String> specifiedPrimaryKeys,
            List<ComputedColumn> computedColumns,
            Map<String, String> tableConfig,
            Schema sourceSchema,
            CdcMetadataConverter[] metadataConverters,
            boolean caseSensitive,
            boolean strictlyCheckSpecified,
            boolean requirePrimaryKeys,
            boolean syncPKeysFromSourceSchema) {
        Schema.Builder builder = Schema.newBuilder();

        // options
        builder.options(tableConfig);
        builder.options(sourceSchema.options());

        // fields
        List<String> allFieldNames = new ArrayList<>();

        for (DataField field : sourceSchema.fields()) {
            String fieldName = toLowerCaseIfNeed(field.name(), caseSensitive);
            allFieldNames.add(fieldName);
            builder.column(fieldName, field.type(), field.description());
        }

        for (ComputedColumn computedColumn : computedColumns) {
            String computedColumnName =
                    toLowerCaseIfNeed(computedColumn.columnName(), caseSensitive);
            allFieldNames.add(computedColumnName);
            builder.column(computedColumnName, computedColumn.columnType());
        }

        for (CdcMetadataConverter metadataConverter : metadataConverters) {
            String metadataColumnName =
                    toLowerCaseIfNeed(metadataConverter.columnName(), caseSensitive);
            allFieldNames.add(metadataColumnName);
            builder.column(metadataColumnName, metadataConverter.dataType());
        }

        checkDuplicateFields(tableName, allFieldNames);

        // primary keys
        specifiedPrimaryKeys = listCaseConvert(specifiedPrimaryKeys, caseSensitive);
        List<String> sourceSchemaPrimaryKeys =
                listCaseConvert(sourceSchema.primaryKeys(), caseSensitive);
        setPrimaryKeys(
                tableName,
                builder,
                specifiedPrimaryKeys,
                sourceSchemaPrimaryKeys,
                allFieldNames,
                strictlyCheckSpecified,
                requirePrimaryKeys,
                syncPKeysFromSourceSchema);

        // partition keys
        specifiedPartitionKeys = listCaseConvert(specifiedPartitionKeys, caseSensitive);
        setPartitionKeys(
                tableName, builder, specifiedPartitionKeys, allFieldNames, strictlyCheckSpecified);

        // comment
        builder.comment(sourceSchema.comment());

        return builder.build();
    }

    private static void setPrimaryKeys(
            String tableName,
            Schema.Builder builder,
            List<String> specifiedPrimaryKeys,
            List<String> sourceSchemaPrimaryKeys,
            List<String> allFieldNames,
            boolean strictlyCheckSpecified,
            boolean requirePrimaryKeys,
            boolean syncPKeysFromSourceSchema) {
        if (!specifiedPrimaryKeys.isEmpty()) {
            if (allFieldNames.containsAll(specifiedPrimaryKeys)) {
                builder.primaryKey(specifiedPrimaryKeys);
                return;
            }

            String message =
                    String.format(
                            "For sink table %s, not all specified primary keys '%s' exist in source tables or computed columns '%s'.",
                            tableName, specifiedPrimaryKeys, allFieldNames);
            if (strictlyCheckSpecified) {
                throw new IllegalArgumentException(message);
            } else {
                LOG.info(
                        "{} In this case at database-sync, we will set primary keys from source tables if exist, otherwise, primary keys are not set.",
                        message);
            }
        }

        if (syncPKeysFromSourceSchema && !sourceSchemaPrimaryKeys.isEmpty()) {
            builder.primaryKey(sourceSchemaPrimaryKeys);
            return;
        }

        if (requirePrimaryKeys && syncPKeysFromSourceSchema) {
            throw new IllegalArgumentException(
                    "Failed to set specified primary keys for sink table "
                            + tableName
                            + ". Also, can't infer primary keys from source table schemas because "
                            + "source tables have no primary keys or have different primary keys.");
        }
    }

    private static void setPartitionKeys(
            String tableName,
            Schema.Builder builder,
            List<String> specifiedPartitionKeys,
            List<String> allFieldNames,
            boolean strictlyCheckSpecified) {
        if (!specifiedPartitionKeys.isEmpty()) {
            if (allFieldNames.containsAll(specifiedPartitionKeys)) {
                builder.partitionKeys(specifiedPartitionKeys);
                return;
            }

            String message =
                    String.format(
                            "For sink table %s, not all specified partition keys '%s' exist in source tables or computed columns '%s'.",
                            tableName, specifiedPartitionKeys, allFieldNames);
            if (strictlyCheckSpecified) {
                throw new IllegalArgumentException(message);
            } else {
                LOG.info("{} In this case at database-sync, partition keys are not set.", message);
            }
        }
    }

    public static void checkDuplicateFields(String tableName, List<String> fieldNames) {
        Set<String> duplicates = Schema.duplicateFields(fieldNames);
        checkState(
                duplicates.isEmpty(),
                "Table %s contains duplicate columns: %s.\n"
                        + "Possible causes are: "
                        + "1. computed columns or metadata columns contain duplicate fields; "
                        + "2. the catalog is case-insensitive and the table columns duplicate after they are all converted to lower-case.",
                tableName,
                duplicates);
    }

    public static String tableList(
            MultiTablesSinkMode mode,
            String databasePattern,
            String includingTablePattern,
            List<Identifier> monitoredTables,
            List<Identifier> excludedTables) {
        if (mode == DIVIDED) {
            return dividedModeTableList(monitoredTables);
        } else if (mode == COMBINED) {
            return combinedModeTableList(databasePattern, includingTablePattern, excludedTables);
        }

        throw new UnsupportedOperationException("Unknown MultiTablesSinkMode: " + mode);
    }

    private static String dividedModeTableList(List<Identifier> monitoredTables) {
        // In DIVIDED mode, we only concern about existed tables
        return monitoredTables.stream()
                .map(t -> t.getDatabaseName() + "\\." + t.getObjectName())
                .collect(Collectors.joining("|"));
    }

    public static String combinedModeTableList(
            String databasePattern, String includingTablePattern, List<Identifier> excludedTables) {
        // In COMBINED mode, we should consider both existed tables
        // and possible newly created
        // tables, so we should use regular expression to monitor all valid tables and exclude
        // certain invalid tables

        // The table list is built by template:
        // (?!(^db\\.tbl$)|(^...$))((databasePattern)\\.(including_pattern1|...))

        // The excluding pattern ?!(^db\\.tbl$)|(^...$) can exclude tables whose qualified name
        // is exactly equal to 'db.tbl'
        // The including pattern (databasePattern)\\.(including_pattern1|...) can include tables
        // whose qualified name matches one of the patterns

        // a table can be monitored only when its name meets the including pattern and doesn't
        // be excluded by excluding pattern at the same time
        String includingPattern =
                String.format("(%s)\\.(%s)", databasePattern, includingTablePattern);

        if (excludedTables.isEmpty()) {
            return includingPattern;
        }

        String excludingPattern =
                excludedTables.stream()
                        .map(
                                t ->
                                        String.format(
                                                "(^%s$)",
                                                t.getDatabaseName() + "\\." + t.getObjectName()))
                        .collect(Collectors.joining("|"));
        excludingPattern = "?!" + excludingPattern;
        return String.format("(%s)(%s)", excludingPattern, includingPattern);
    }

    public static void checkRequiredOptions(
            Configuration config, String confName, ConfigOption<?>... configOptions) {
        for (ConfigOption<?> configOption : configOptions) {
            checkArgument(
                    config.contains(configOption),
                    "%s [%s] must be specified.",
                    confName,
                    configOption.key());
        }
    }

    public static void checkOneRequiredOption(
            Configuration config, String confName, ConfigOption<?>... configOptions) {
        checkArgument(
                Arrays.stream(configOptions).filter(config::contains).count() == 1,
                "%s must and can only set one of the following options: %s.",
                confName,
                Arrays.stream(configOptions)
                        .map(ConfigOption::key)
                        .collect(Collectors.joining(",")));
    }
}
