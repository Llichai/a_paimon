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

package org.apache.paimon.schema;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.Public;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.ReassignFieldId;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 表的 Schema 定义。
 *
 * <p>Schema 定义了表的结构和元数据，包括:
 * <ul>
 *   <li>字段列表及其类型
 *   <li>分区键
 *   <li>主键
 *   <li>表选项
 *   <li>表注释
 * </ul>
 *
 * <p>Schema 提供了灵活的构建方式，支持通过 {@link Builder} 构建器模式创建。
 *
 * <p>使用示例:
 * <pre>{@code
 * // 使用构建器创建 Schema
 * Schema schema = Schema.newBuilder()
 *     .column("id", DataTypes.INT())
 *     .column("name", DataTypes.STRING())
 *     .column("age", DataTypes.INT())
 *     .partitionKeys("dt")
 *     .primaryKey("id")
 *     .option("bucket", "4")
 *     .comment("用户表")
 *     .build();
 *
 * // 访问 Schema 信息
 * List<DataField> fields = schema.fields();
 * List<String> partitionKeys = schema.partitionKeys();
 * List<String> primaryKeys = schema.primaryKeys();
 * }</pre>
 *
 * @since 0.4.0
 */
@Public
@JsonIgnoreProperties(ignoreUnknown = true)
public class Schema {

    private static final String FIELD_FIELDS = "fields";
    private static final String FIELD_PARTITION_KEYS = "partitionKeys";
    private static final String FIELD_PRIMARY_KEYS = "primaryKeys";
    private static final String FIELD_OPTIONS = "options";
    private static final String FIELD_COMMENT = "comment";

    /** 字段列表。 */
    @JsonProperty(FIELD_FIELDS)
    private final List<DataField> fields;

    /** 分区键列表。 */
    @JsonProperty(FIELD_PARTITION_KEYS)
    private final List<String> partitionKeys;

    /** 主键列表。 */
    @JsonProperty(FIELD_PRIMARY_KEYS)
    private final List<String> primaryKeys;

    /** 表选项。 */
    @JsonProperty(FIELD_OPTIONS)
    private final Map<String, String> options;

    /** 表注释。 */
    @Nullable
    @JsonProperty(FIELD_COMMENT)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String comment;

    /**
     * 构造 Schema。
     *
     * @param fields 字段列表
     * @param partitionKeys 分区键列表
     * @param primaryKeys 主键列表
     * @param options 表选项
     * @param comment 表注释（可为 null）
     */
    @JsonCreator
    public Schema(
            @JsonProperty(FIELD_FIELDS) List<DataField> fields,
            @JsonProperty(FIELD_PARTITION_KEYS) List<String> partitionKeys,
            @JsonProperty(FIELD_PRIMARY_KEYS) List<String> primaryKeys,
            @JsonProperty(FIELD_OPTIONS) Map<String, String> options,
            @Nullable @JsonProperty(FIELD_COMMENT) String comment) {
        this.options = new HashMap<>(options);
        this.partitionKeys = normalizePartitionKeys(partitionKeys);
        this.primaryKeys = normalizePrimaryKeys(primaryKeys);
        this.fields = normalizeFields(fields, this.primaryKeys, this.partitionKeys);
        this.comment = comment;
    }

    /**
     * 获取 RowType 表示。
     *
     * @return 表的 RowType
     */
    public RowType rowType() {
        return new RowType(false, fields);
    }

    /**
     * 获取字段列表。
     *
     * @return 字段列表
     */
    @JsonGetter(FIELD_FIELDS)
    public List<DataField> fields() {
        return fields;
    }

    /**
     * 获取分区键列表。
     *
     * @return 分区键列表
     */
    @JsonGetter(FIELD_PARTITION_KEYS)
    public List<String> partitionKeys() {
        return partitionKeys;
    }

    /**
     * 获取主键列表。
     *
     * @return 主键列表
     */
    @JsonGetter(FIELD_PRIMARY_KEYS)
    public List<String> primaryKeys() {
        return primaryKeys;
    }

    /**
     * 获取表选项。
     *
     * @return 表选项映射
     */
    @JsonGetter(FIELD_OPTIONS)
    public Map<String, String> options() {
        return options;
    }

    /**
     * 获取表注释。
     *
     * @return 表注释（可能为 null）
     */
    @JsonGetter(FIELD_COMMENT)
    public String comment() {
        return comment;
    }

    /**
     * 使用新的 RowType 创建 Schema 副本。
     *
     * @param rowType 新的 RowType
     * @return 新的 Schema 实例
     */
    public Schema copy(RowType rowType) {
        return new Schema(rowType.getFields(), partitionKeys, primaryKeys, options, comment);
    }

    /**
     * 规范化字段列表。
     *
     * <p>验证并处理字段:
     * <ul>
     *   <li>检查字段名是否重复
     *   <li>验证分区键和主键是否在字段列表中
     *   <li>将主键字段设置为非空
     * </ul>
     *
     * @param fields 原始字段列表
     * @param primaryKeys 主键列表
     * @param partitionKeys 分区键列表
     * @return 规范化后的字段列表
     */
    private static List<DataField> normalizeFields(
            List<DataField> fields, List<String> primaryKeys, List<String> partitionKeys) {
        List<String> fieldNames = fields.stream().map(DataField::name).collect(Collectors.toList());

        Set<String> duplicateColumns = duplicateFields(fieldNames);
        Preconditions.checkState(
                duplicateColumns.isEmpty(),
                "Table column %s must not contain duplicate fields. Found: %s",
                fieldNames,
                duplicateColumns);

        Set<String> allFields = new HashSet<>(fieldNames);

        duplicateColumns = duplicateFields(partitionKeys);
        Preconditions.checkState(
                duplicateColumns.isEmpty(),
                "Partition key constraint %s must not contain duplicate columns. Found: %s",
                partitionKeys,
                duplicateColumns);
        Preconditions.checkState(
                allFields.containsAll(partitionKeys),
                "Table column %s should include all partition fields %s",
                fieldNames,
                partitionKeys);

        if (primaryKeys.isEmpty()) {
            return fields;
        }
        duplicateColumns = duplicateFields(primaryKeys);
        Preconditions.checkState(
                duplicateColumns.isEmpty(),
                "Primary key constraint %s must not contain duplicate columns. Found: %s",
                primaryKeys,
                duplicateColumns);
        Preconditions.checkState(
                allFields.containsAll(primaryKeys),
                "Table column %s should include all primary key constraint %s",
                fieldNames,
                primaryKeys);

        // primary key should not nullable
        Set<String> pkSet = new HashSet<>(primaryKeys);
        List<DataField> newFields = new ArrayList<>();
        for (DataField field : fields) {
            if (pkSet.contains(field.name()) && field.type().isNullable()) {
                newFields.add(
                        new DataField(
                                field.id(),
                                field.name(),
                                field.type().copy(false),
                                field.description(),
                                field.defaultValue()));
            } else {
                newFields.add(field);
            }
        }
        return newFields;
    }

    /**
     * 规范化主键列表。
     *
     * <p>如果选项中定义了主键，则从选项中提取并移除该配置。
     *
     * @param primaryKeys DDL 中定义的主键
     * @return 规范化后的主键列表
     */
    private List<String> normalizePrimaryKeys(List<String> primaryKeys) {
        if (options.containsKey(CoreOptions.PRIMARY_KEY.key())) {
            if (!primaryKeys.isEmpty()) {
                throw new RuntimeException(
                        "Cannot define primary key on DDL and table options at the same time.");
            }
            String pk = options.get(CoreOptions.PRIMARY_KEY.key());
            primaryKeys =
                    Arrays.stream(pk.split(","))
                            .map(String::trim)
                            .filter(s -> !s.isEmpty())
                            .collect(Collectors.toList());
            options.remove(CoreOptions.PRIMARY_KEY.key());
        }
        return primaryKeys;
    }

    /**
     * 规范化分区键列表。
     *
     * <p>如果选项中定义了分区，则从选项中提取并移除该配置。
     *
     * @param partitionKeys DDL 中定义的分区键
     * @return 规范化后的分区键列表
     */
    private List<String> normalizePartitionKeys(List<String> partitionKeys) {
        if (options.containsKey(CoreOptions.PARTITION.key())) {
            if (!partitionKeys.isEmpty()) {
                throw new RuntimeException(
                        "Cannot define partition on DDL and table options at the same time.");
            }
            String partitions = options.get(CoreOptions.PARTITION.key());
            partitionKeys =
                    Arrays.stream(partitions.split(","))
                            .map(String::trim)
                            .filter(s -> !s.isEmpty())
                            .collect(Collectors.toList());
            options.remove(CoreOptions.PARTITION.key());
        }
        return partitionKeys;
    }

    /**
     * 检测重复的字段名。
     *
     * @param names 字段名列表
     * @return 重复的字段名集合
     */
    public static Set<String> duplicateFields(List<String> names) {
        return names.stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .entrySet()
                .stream()
                .filter(e -> e.getValue() > 1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Schema that = (Schema) o;
        return Objects.equals(fields, that.fields)
                && Objects.equals(partitionKeys, that.partitionKeys)
                && Objects.equals(primaryKeys, that.primaryKeys)
                && Objects.equals(options, that.options)
                && Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields, partitionKeys, primaryKeys, options, comment);
    }

    @Override
    public String toString() {
        return "UpdateSchema{"
                + "fields="
                + fields
                + ", partitionKeys="
                + partitionKeys
                + ", primaryKeys="
                + primaryKeys
                + ", options="
                + options
                + ", comment="
                + comment
                + '}';
    }

    /**
     * 创建 Schema 构建器。
     *
     * @return Schema 构建器实例
     */
    public static Schema.Builder newBuilder() {
        return new Builder();
    }

    /**
     * Schema 构建器。
     *
     * <p>用于构建不可变的 {@link Schema} 实例，提供了流式 API 来配置 Schema 的各个部分。
     *
     * <p>使用示例:
     * <pre>{@code
     * Schema schema = Schema.newBuilder()
     *     .column("id", DataTypes.INT(), "用户ID")
     *     .column("name", DataTypes.STRING(), "用户名")
     *     .column("dt", DataTypes.STRING(), "分区日期")
     *     .partitionKeys("dt")
     *     .primaryKey("id")
     *     .option("bucket", "4")
     *     .comment("用户信息表")
     *     .build();
     * }</pre>
     */
    public static final class Builder {

        /** 列定义列表。 */
        private final List<DataField> columns = new ArrayList<>();

        /** 分区键列表。 */
        private List<String> partitionKeys = new ArrayList<>();

        /** 主键列表。 */
        private List<String> primaryKeys = new ArrayList<>();

        /** 表选项。 */
        private final Map<String, String> options = new HashMap<>();

        /** 表注释。 */
        @Nullable private String comment;

        /** 当前最大字段 ID。 */
        private final AtomicInteger highestFieldId = new AtomicInteger(-1);

        /**
         * 获取当前最大字段 ID。
         *
         * @return 最大字段 ID
         */
        public int getHighestFieldId() {
            return highestFieldId.get();
        }

        /**
         * 添加列到 Schema。
         *
         * @param columnName 列名
         * @param dataType 数据类型
         * @return Builder 实例（支持链式调用）
         */
        public Builder column(String columnName, DataType dataType) {
            return column(columnName, dataType, null);
        }

        /**
         * 添加带描述的列到 Schema。
         *
         * @param columnName 列名
         * @param dataType 数据类型
         * @param description 列描述
         * @return Builder 实例（支持链式调用）
         */
        public Builder column(String columnName, DataType dataType, @Nullable String description) {
            return column(columnName, dataType, description, null);
        }

        /**
         * 添加完整定义的列到 Schema。
         *
         * @param columnName 列名
         * @param dataType 数据类型
         * @param description 列描述
         * @param defaultValue 默认值
         * @return Builder 实例（支持链式调用）
         */
        public Builder column(
                String columnName,
                DataType dataType,
                @Nullable String description,
                @Nullable String defaultValue) {
            Preconditions.checkNotNull(columnName, "Column name must not be null.");
            Preconditions.checkNotNull(dataType, "Data type must not be null.");

            int id = highestFieldId.incrementAndGet();
            DataType reassignDataType = ReassignFieldId.reassign(dataType, highestFieldId);
            columns.add(new DataField(id, columnName, reassignDataType, description, defaultValue));
            return this;
        }

        /**
         * 声明分区键（可变参数）。
         *
         * @param columnNames 分区列名
         * @return Builder 实例（支持链式调用）
         */
        public Builder partitionKeys(String... columnNames) {
            return partitionKeys(Arrays.asList(columnNames));
        }

        /**
         * 声明分区键（列表）。
         *
         * @param columnNames 分区列名列表
         * @return Builder 实例（支持链式调用）
         */
        public Builder partitionKeys(List<String> columnNames) {
            this.partitionKeys = new ArrayList<>(columnNames);
            return this;
        }

        /**
         * 声明主键约束（可变参数）。
         *
         * <p>主键唯一标识表中的一行，主键列不能为 null。
         *
         * @param columnNames 主键列名
         * @return Builder 实例（支持链式调用）
         */
        public Builder primaryKey(String... columnNames) {
            return primaryKey(Arrays.asList(columnNames));
        }

        /**
         * 声明主键约束（列表）。
         *
         * <p>主键唯一标识表中的一行，主键列不能为 null。
         *
         * @param columnNames 主键列名列表
         * @return Builder 实例（支持链式调用）
         */
        public Builder primaryKey(List<String> columnNames) {
            this.primaryKeys = new ArrayList<>(columnNames);
            return this;
        }

        /**
         * 批量设置表选项。
         *
         * @param options 表选项映射
         * @return Builder 实例（支持链式调用）
         */
        public Builder options(Map<String, String> options) {
            this.options.putAll(options);
            return this;
        }

        /**
         * 设置单个表选项。
         *
         * @param key 选项键
         * @param value 选项值
         * @return Builder 实例（支持链式调用）
         */
        public Builder option(String key, String value) {
            this.options.put(key, value);
            return this;
        }

        /**
         * 设置表注释。
         *
         * @param comment 表注释
         * @return Builder 实例（支持链式调用）
         */
        public Builder comment(@Nullable String comment) {
            this.comment = comment;
            return this;
        }

        /**
         * 构建 Schema 实例。
         *
         * @return 不可变的 Schema 对象
         */
        public Schema build() {
            return new Schema(columns, partitionKeys, primaryKeys, options, comment);
        }
    }
}
