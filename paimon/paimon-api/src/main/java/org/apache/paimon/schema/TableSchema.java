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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.BUCKET_KEY;

/**
 * 表的 Schema。
 *
 * <p>与 {@link Schema} 相比，TableSchema 包含更多信息:
 * <ul>
 *   <li>schema ID: Schema 的版本标识
 *   <li>field ID: 每个字段的唯一标识
 *   <li>highestFieldId: 当前最大的字段 ID
 *   <li>bucket keys: 分桶键（从主键或配置中派生）
 *   <li>时间戳: Schema 创建时间
 * </ul>
 *
 * <p>TableSchema 是 Paimon 内部使用的 Schema 表示，包含了字段 ID 分配、
 * 分桶策略等运行时信息。Schema 是用户级别的 API，而 TableSchema 是存储级别的实现。
 *
 * <p>版本兼容性:
 * <ul>
 *   <li>Paimon 0.7: 版本号 1
 *   <li>Paimon 0.8: 版本号 2
 *   <li>当前版本: 版本号 3
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 从 Schema 创建 TableSchema
 * TableSchema tableSchema = TableSchema.create(1L, schema);
 *
 * // 获取字段信息
 * List<DataField> fields = tableSchema.fields();
 * List<String> primaryKeys = tableSchema.primaryKeys();
 * List<String> bucketKeys = tableSchema.bucketKeys();
 *
 * // 序列化/反序列化
 * String json = tableSchema.toString();
 * TableSchema deserialized = TableSchema.fromJson(json);
 * }</pre>
 */
public class TableSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Paimon 0.7 版本的 Schema 版本号。 */
    public static final int PAIMON_07_VERSION = 1;

    /** Paimon 0.8 版本的 Schema 版本号。 */
    public static final int PAIMON_08_VERSION = 2;

    /** 当前 Schema 版本号。 */
    public static final int CURRENT_VERSION = 3;

    /** Paimon Schema 版本号。 */
    private final int version;

    /** Schema ID（Schema 的唯一标识）。 */
    private final long id;

    /** 字段列表。 */
    private final List<DataField> fields;

    /**
     * 当前最大字段 ID。
     *
     * <p>注意：由于字段可能被删除，这个值可能大于当前字段列表的最大 ID。
     */
    private final int highestFieldId;

    /** 分区键列表。 */
    private final List<String> partitionKeys;

    /** 主键列表。 */
    private final List<String> primaryKeys;

    /** 分桶键列表（从配置或主键派生）。 */
    private final List<String> bucketKeys;

    /** 分桶数量。 */
    private final int numBucket;

    /** 表选项。 */
    private final Map<String, String> options;

    /** 表注释。 */
    private final @Nullable String comment;

    /** Schema 创建时间戳（毫秒）。 */
    private final long timeMillis;

    /**
     * 构造 TableSchema（使用当前版本号和当前时间戳）。
     *
     * @param id Schema ID
     * @param fields 字段列表
     * @param highestFieldId 最大字段 ID
     * @param partitionKeys 分区键列表
     * @param primaryKeys 主键列表
     * @param options 表选项
     * @param comment 表注释
     */
    public TableSchema(
            long id,
            List<DataField> fields,
            int highestFieldId,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> options,
            @Nullable String comment) {
        this(
                CURRENT_VERSION,
                id,
                fields,
                highestFieldId,
                partitionKeys,
                primaryKeys,
                options,
                comment,
                System.currentTimeMillis());
    }

    /**
     * 完整构造函数。
     *
     * <p>构造 TableSchema 并初始化分桶键:
     * <ul>
     *   <li>如果配置了 bucket-key，使用配置的值
     *   <li>否则使用裁剪后的主键作为分桶键
     * </ul>
     *
     * @param version Schema 版本号
     * @param id Schema ID
     * @param fields 字段列表
     * @param highestFieldId 最大字段 ID
     * @param partitionKeys 分区键列表
     * @param primaryKeys 主键列表
     * @param options 表选项
     * @param comment 表注释
     * @param timeMillis 创建时间戳
     */
    public TableSchema(
            int version,
            long id,
            List<DataField> fields,
            int highestFieldId,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> options,
            @Nullable String comment,
            long timeMillis) {
        this.version = version;
        this.id = id;
        this.fields = Collections.unmodifiableList(new ArrayList<>(fields));
        this.highestFieldId = highestFieldId;
        this.partitionKeys = partitionKeys;
        this.primaryKeys = primaryKeys;
        this.options = options;
        this.comment = comment;
        this.timeMillis = timeMillis;

        // try to trim to validate primary keys
        trimmedPrimaryKeys();

        // try to validate and initialize the bucket keys
        List<String> tmpBucketKeys = originalBucketKeys();
        if (tmpBucketKeys.isEmpty()) {
            tmpBucketKeys = trimmedPrimaryKeys();
        }
        bucketKeys = tmpBucketKeys;
        numBucket = CoreOptions.fromMap(options).bucket();
    }

    /**
     * 获取 Schema 版本号。
     *
     * @return 版本号
     */
    public int version() {
        return version;
    }

    /**
     * 获取 Schema ID。
     *
     * @return Schema ID
     */
    public long id() {
        return id;
    }

    /**
     * 获取字段列表。
     *
     * @return 不可变的字段列表
     */
    public List<DataField> fields() {
        return fields;
    }

    /**
     * 获取字段名列表。
     *
     * @return 字段名列表
     */
    public List<String> fieldNames() {
        return fields.stream().map(DataField::name).collect(Collectors.toList());
    }

    /**
     * 获取字段名到字段的映射。
     *
     * @return 字段名到 DataField 的映射
     */
    public Map<String, DataField> nameToFieldMap() {
        return fields.stream()
                .collect(Collectors.toMap(DataField::name, field -> field, (a, b) -> b));
    }

    /**
     * 获取字段 ID 到字段的映射。
     *
     * @return 字段 ID 到 DataField 的映射
     */
    public Map<Integer, DataField> idToFieldMap() {
        return fields.stream()
                .collect(Collectors.toMap(DataField::id, field -> field, (a, b) -> b));
    }

    /**
     * 获取最大字段 ID。
     *
     * @return 最大字段 ID
     */
    public int highestFieldId() {
        return highestFieldId;
    }

    /**
     * 获取分区键列表。
     *
     * @return 分区键列表
     */
    public List<String> partitionKeys() {
        return partitionKeys;
    }

    /**
     * 获取主键列表。
     *
     * @return 主键列表
     */
    public List<String> primaryKeys() {
        return primaryKeys;
    }

    /**
     * 获取裁剪后的主键列表（排除分区键）。
     *
     * <p>主键约束中如果包含所有分区键，会导致每个分区只有一条记录，
     * 因此需要从主键中排除分区键。
     *
     * @return 裁剪后的主键列表
     * @throws IllegalStateException 如果裁剪后的主键列表为空
     */
    public List<String> trimmedPrimaryKeys() {
        if (!primaryKeys.isEmpty()) {
            List<String> adjusted =
                    primaryKeys.stream()
                            .filter(pk -> !partitionKeys.contains(pk))
                            .collect(Collectors.toList());

            Preconditions.checkState(
                    !adjusted.isEmpty(),
                    String.format(
                            "Primary key constraint %s should not be same with partition fields %s,"
                                    + " this will result in only one record in a partition",
                            primaryKeys, partitionKeys));

            return adjusted;
        }

        return primaryKeys;
    }

    /**
     * 获取表选项。
     *
     * @return 表选项映射
     */
    public Map<String, String> options() {
        return options;
    }

    /**
     * 获取分桶数量。
     *
     * @return 分桶数
     */
    public int numBuckets() {
        return numBucket;
    }

    /**
     * 获取分桶键列表。
     *
     * @return 分桶键列表
     */
    public List<String> bucketKeys() {
        return bucketKeys;
    }

    /**
     * 判断是否支持跨分区更新。
     *
     * <p>当满足以下条件时返回 true:
     * <ul>
     *   <li>表有主键和分区键
     *   <li>主键不包含所有分区键
     * </ul>
     *
     * @return 如果支持跨分区更新返回 true
     */
    public boolean crossPartitionUpdate() {
        if (primaryKeys.isEmpty() || partitionKeys.isEmpty()) {
            return false;
        }

        return notContainsAll(primaryKeys, partitionKeys);
    }

    /**
     * 获取原始配置的分桶键。
     *
     * <p>从 bucket-key 选项中解析分桶键，并验证:
     * <ul>
     *   <li>分桶键必须在字段列表中
     *   <li>分桶键不能在分区键中
     *   <li>如果有主键，分桶键必须是主键的子集
     * </ul>
     *
     * @return 分桶键列表，如果未配置则返回空列表
     */
    private List<String> originalBucketKeys() {
        String key = options.get(BUCKET_KEY.key());
        if (StringUtils.isNullOrWhitespaceOnly(key)) {
            return Collections.emptyList();
        }
        List<String> bucketKeys = Arrays.asList(key.split(","));
        if (notContainsAll(fieldNames(), bucketKeys)) {
            throw new RuntimeException(
                    String.format(
                            "Field names %s should contains all bucket keys %s.",
                            fieldNames(), bucketKeys));
        }
        if (bucketKeys.stream().anyMatch(partitionKeys::contains)) {
            throw new RuntimeException(
                    String.format(
                            "Bucket keys %s should not in partition keys %s.",
                            bucketKeys, partitionKeys));
        }
        if (!primaryKeys.isEmpty()) {
            if (notContainsAll(primaryKeys, bucketKeys)) {
                throw new RuntimeException(
                        String.format(
                                "Primary keys %s should contains all bucket keys %s.",
                                primaryKeys, bucketKeys));
            }
        }
        return bucketKeys;
    }

    /**
     * 判断 all 是否不包含 contains 中的所有元素。
     *
     * @param all 全集列表
     * @param contains 待检查的子集列表
     * @return 如果 all 不包含 contains 的所有元素返回 true
     */
    private boolean notContainsAll(List<String> all, List<String> contains) {
        return !new HashSet<>(all).containsAll(new HashSet<>(contains));
    }

    /**
     * 获取表注释。
     *
     * @return 表注释（可能为 null）
     */
    public @Nullable String comment() {
        return comment;
    }

    /**
     * 获取 Schema 创建时间戳。
     *
     * @return 时间戳（毫秒）
     */
    public long timeMillis() {
        return timeMillis;
    }

    /**
     * 获取逻辑 RowType。
     *
     * @return 表的 RowType
     */
    public RowType logicalRowType() {
        return new RowType(fields);
    }

    /**
     * 获取分区字段的逻辑 RowType。
     *
     * @return 分区字段的 RowType
     */
    public RowType logicalPartitionType() {
        return projectedLogicalRowType(partitionKeys);
    }

    /**
     * 获取分桶键字段的逻辑 RowType。
     *
     * @return 分桶键字段的 RowType
     */
    public RowType logicalBucketKeyType() {
        return projectedLogicalRowType(bucketKeys());
    }

    /**
     * 获取裁剪后主键字段的逻辑 RowType。
     *
     * @return 裁剪后主键字段的 RowType
     */
    public RowType logicalTrimmedPrimaryKeysType() {
        return projectedLogicalRowType(trimmedPrimaryKeys());
    }

    /**
     * 获取主键字段的逻辑 RowType。
     *
     * @return 主键字段的 RowType
     */
    public RowType logicalPrimaryKeysType() {
        return projectedLogicalRowType(primaryKeys());
    }

    /**
     * 获取主键字段列表。
     *
     * @return 主键 DataField 列表
     */
    public List<DataField> primaryKeysFields() {
        return projectedDataFields(primaryKeys());
    }

    /**
     * 获取裁剪后的主键字段列表。
     *
     * @return 裁剪后的主键 DataField 列表
     */
    public List<DataField> trimmedPrimaryKeysFields() {
        return projectedDataFields(trimmedPrimaryKeys());
    }

    /**
     * 获取指定字段名的投影索引数组。
     *
     * @param projectedFieldNames 投影字段名列表
     * @return 字段索引数组
     */
    public int[] projection(List<String> projectedFieldNames) {
        List<String> fieldNames = fieldNames();
        return projectedFieldNames.stream().mapToInt(fieldNames::indexOf).toArray();
    }

    /**
     * 投影 TableSchema（仅包含指定的列）。
     *
     * @param writeCols 要保留的列名列表（null 表示保留所有列）
     * @return 投影后的 TableSchema
     */
    public TableSchema project(@Nullable List<String> writeCols) {
        if (writeCols == null) {
            return this;
        }

        return new TableSchema(
                version,
                id,
                new RowType(fields).project(writeCols).getFields(),
                highestFieldId,
                partitionKeys,
                primaryKeys,
                options,
                comment,
                timeMillis);
    }

    /**
     * 获取投影的字段列表。
     *
     * @param projectedFieldNames 投影字段名列表
     * @return 投影的 DataField 列表
     */
    private List<DataField> projectedDataFields(List<String> projectedFieldNames) {
        List<String> fieldNames = fieldNames();
        return projectedFieldNames.stream()
                .map(
                        k -> {
                            return !fieldNames.contains(k)
                                    ? null
                                    : fields.get(fieldNames.indexOf(k));
                        })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * 获取投影的逻辑 RowType。
     *
     * @param projectedFieldNames 投影字段名列表
     * @return 投影的 RowType
     */
    public RowType projectedLogicalRowType(List<String> projectedFieldNames) {
        return new RowType(projectedDataFields(projectedFieldNames));
    }

    /**
     * 使用新选项创建 TableSchema 副本。
     *
     * @param newOptions 新的表选项
     * @return 新的 TableSchema 实例
     */
    public TableSchema copy(Map<String, String> newOptions) {
        return new TableSchema(
                version,
                id,
                fields,
                highestFieldId,
                partitionKeys,
                primaryKeys,
                newOptions,
                comment,
                timeMillis);
    }

    @Override
    public String toString() {
        return JsonSerdeUtil.toJson(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableSchema tableSchema = (TableSchema) o;
        return version == tableSchema.version
                && Objects.equals(fields, tableSchema.fields)
                && Objects.equals(partitionKeys, tableSchema.partitionKeys)
                && Objects.equals(primaryKeys, tableSchema.primaryKeys)
                && Objects.equals(options, tableSchema.options)
                && Objects.equals(comment, tableSchema.comment)
                && timeMillis == tableSchema.timeMillis;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                version, fields, partitionKeys, primaryKeys, options, comment, timeMillis);
    }

    /**
     * 将 RowType 转换为字段列表。
     *
     * @param rowType RowType 对象
     * @return 字段列表
     */
    public static List<DataField> newFields(RowType rowType) {
        return rowType.getFields();
    }

    /**
     * 转换为用户级 Schema。
     *
     * @return Schema 对象
     */
    public Schema toSchema() {
        return new Schema(fields, partitionKeys, primaryKeys, options, comment);
    }

    // =================== 序列化/反序列化工具方法 =========================

    /**
     * 从 JSON 字符串反序列化 TableSchema。
     *
     * @param json JSON 字符串
     * @return TableSchema 对象
     */
    public static TableSchema fromJson(String json) {
        return JsonSerdeUtil.fromJson(json, TableSchema.class);
    }

    /**
     * 从 Schema 创建 TableSchema。
     *
     * <p>分配字段 ID 并计算最大字段 ID。
     *
     * @param schemaId Schema ID
     * @param schema Schema 对象
     * @return TableSchema 对象
     */
    public static TableSchema create(long schemaId, Schema schema) {
        List<DataField> fields = schema.fields();
        List<String> partitionKeys = schema.partitionKeys();
        List<String> primaryKeys = schema.primaryKeys();
        Map<String, String> options = schema.options();
        int highestFieldId = RowType.currentHighestFieldId(fields);

        return new TableSchema(
                schemaId,
                fields,
                highestFieldId,
                partitionKeys,
                primaryKeys,
                options,
                schema.comment());
    }
}
