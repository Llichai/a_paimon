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

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeCasts;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.ReassignFieldId;
import org.apache.paimon.types.RowType;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Schema 合并工具
 *
 * <p>SchemaMergingUtils 提供了自动合并两个 Schema 的功能，主要用于 Schema 自动演化（Automatic Schema Evolution）场景。
 * 当写入的数据包含新字段或类型变更时，Paimon 可以自动将这些变更合并到现有的 Schema 中。
 *
 * <p>核心功能：
 * <ul>
 *   <li>{@link #mergeSchemas(TableSchema, RowType, boolean)} - 合并表 Schema 和数据 Schema
 *   <li>{@link #merge(DataType, DataType, AtomicInteger, boolean)} - 递归合并数据类型
 * </ul>
 *
 * <p>Schema 合并策略：
 * <pre>
 * 1. 字段添加：
 *    - 表 Schema：[id INT, name STRING]
 *    - 数据 Schema：[id INT, name STRING, age INT]
 *    - 合并结果：[id INT, name STRING, age INT]  ← 新增 age 字段
 *
 * 2. 字段保留：
 *    - 表 Schema：[id INT, name STRING, age INT]
 *    - 数据 Schema：[id INT, name STRING]
 *    - 合并结果：[id INT, name STRING, age INT]  ← 保留 age 字段
 *
 * 3. 类型提升（隐式转换）：
 *    - 表 Schema：[id INT]
 *    - 数据 Schema：[id BIGINT]
 *    - 合并结果：[id BIGINT]  ← INT 提升为 BIGINT（安全转换）
 *
 * 4. 类型提升（显式转换，allowExplicitCast = true）：
 *    - 表 Schema：[id BIGINT]
 *    - 数据 Schema：[id INT]
 *    - 合并结果：[id INT]  ← 允许降级转换
 *
 * 5. 嵌套类型合并（ROW）：
 *    - 表 Schema：[user ROW<id INT, name STRING>]
 *    - 数据 Schema：[user ROW<id INT, name STRING, age INT>]
 *    - 合并结果：[user ROW<id INT, name STRING, age INT>]
 *
 * 6. 复杂类型合并（ARRAY）：
 *    - 表 Schema：[tags ARRAY<INT>]
 *    - 数据 Schema：[tags ARRAY<BIGINT>]
 *    - 合并结果：[tags ARRAY<BIGINT>]
 *
 * 7. 复杂类型合并（MAP）：
 *    - 表 Schema：[props MAP<STRING, INT>]
 *    - 数据 Schema：[props MAP<STRING, BIGINT>]
 *    - 合并结果：[props MAP<STRING, BIGINT>]
 *    - 注意：Map 的 key 类型必须完全一致，只能合并 value 类型
 * </pre>
 *
 * <p>类型兼容性规则：
 * <ul>
 *   <li>基本类型：通过 {@link org.apache.paimon.types.DataTypeCasts#supportsCast} 判断
 *   <li>数值类型：INT → BIGINT ✓, FLOAT → DOUBLE ✓, BIGINT → INT ✗（需 allowExplicitCast）
 *   <li>字符类型：VARCHAR(10) → VARCHAR(20) ✓, VARCHAR(20) → VARCHAR(10) ✗
 *   <li>时间类型：TIMESTAMP(3) → TIMESTAMP(6) ✓（精度提升）
 *   <li>复杂类型：递归合并元素类型
 * </ul>
 *
 * <p>Decimal 类型合并：
 * <pre>
 * 规则：scale 必须相同，precision 取较大值
 *
 * 示例：
 *   - DECIMAL(10, 2) + DECIMAL(15, 2) → DECIMAL(15, 2)  ✓
 *   - DECIMAL(10, 2) + DECIMAL(10, 3) → 失败（scale 不同）  ✗
 * </pre>
 *
 * <p>可空性（Nullability）处理：
 * <ul>
 *   <li>合并时总是保留表 Schema 的可空性设置
 *   <li>数据 Schema 的可空性被忽略
 *   <li>原因：已有数据可能包含 NULL 值，不能强制改为 NOT NULL
 * </ul>
 *
 * <p>字段 ID（Field ID）分配：
 * <ul>
 *   <li>保留表 Schema 中已存在字段的 Field ID
 *   <li>为新增字段分配新的 Field ID（highestFieldId + 1）
 *   <li>嵌套类型的子字段也会分配独立的 Field ID
 *   <li>Field ID 用于 Schema 演化时的字段映射
 * </ul>
 *
 * <p>字段顺序：
 * <ul>
 *   <li>保留表 Schema 中字段的原始顺序
 *   <li>新增字段追加到末尾
 *   <li>示例：表 [id, name] + 数据 [name, id, age] → [id, name, age]
 * </ul>
 *
 * <p>限制和约束：
 * <ul>
 *   <li>不支持字段删除：数据 Schema 缺少的字段会保留在表 Schema 中
 *   <li>不支持字段重命名：字段通过名称匹配，重命名被视为删除旧字段 + 添加新字段
 *   <li>Map 的 key 类型不能变更：key 类型必须完全一致
 *   <li>类型不兼容时会抛出异常：BIGINT → STRING ✗
 * </ul>
 *
 * <p>使用场景：
 * <pre>{@code
 * // 场景 1：CDC 自动 Schema 演化
 * // 上游数据库添加了新列
 * TableSchema currentSchema = schemaManager.latest().get();
 * RowType newDataType = ...; // CDC 数据包含新列
 * boolean updated = schemaManager.mergeSchema(newDataType, false);
 *
 * // 场景 2：手动合并 Schema
 * TableSchema currentSchema = ...;
 * RowType targetType = new RowType(
 *     Arrays.asList(
 *         new DataField(0, "id", DataTypes.BIGINT()),    // 类型提升：INT → BIGINT
 *         new DataField(1, "name", DataTypes.STRING()),
 *         new DataField(2, "age", DataTypes.INT())       // 新字段
 *     )
 * );
 * TableSchema mergedSchema = SchemaMergingUtils.mergeSchemas(
 *     currentSchema,
 *     targetType,
 *     false  // 不允许显式类型转换
 * );
 *
 * // 场景 3：嵌套类型合并
 * // 当前 Schema：user ROW<id INT, name STRING>
 * // 目标 Schema：user ROW<id BIGINT, name STRING, email STRING>
 * // 合并结果：user ROW<id BIGINT, name STRING, email STRING>
 * RowType currentRowType = new RowType(Arrays.asList(
 *     new DataField(0, "user", new RowType(Arrays.asList(
 *         new DataField(1, "id", DataTypes.INT()),
 *         new DataField(2, "name", DataTypes.STRING())
 *     )))
 * ));
 * RowType targetRowType = new RowType(Arrays.asList(
 *     new DataField(0, "user", new RowType(Arrays.asList(
 *         new DataField(1, "id", DataTypes.BIGINT()),
 *         new DataField(2, "name", DataTypes.STRING()),
 *         new DataField(3, "email", DataTypes.STRING())
 *     )))
 * ));
 * TableSchema merged = SchemaMergingUtils.mergeSchemas(
 *     currentSchema, targetRowType, false
 * );
 * }</pre>
 *
 * <p>与其他组件的关系：
 * <ul>
 *   <li>{@link SchemaManager#mergeSchema}：调用此工具进行 Schema 合并
 *   <li>{@link org.apache.paimon.types.DataTypeCasts}：判断类型是否可以转换
 *   <li>{@link org.apache.paimon.types.ReassignFieldId}：为新字段分配 Field ID
 *   <li>{@link SchemaEvolutionUtil}：合并后的 Schema 用于数据读取时的类型转换
 * </ul>
 *
 * @see TableSchema 表结构
 * @see SchemaManager Schema 管理器
 * @see SchemaEvolutionUtil Schema 演化工具
 * @see org.apache.paimon.types.DataTypeCasts 类型转换工具
 */
public class SchemaMergingUtils {

    public static TableSchema mergeSchemas(
            TableSchema currentTableSchema, RowType targetType, boolean allowExplicitCast) {
        RowType currentType = currentTableSchema.logicalRowType();
        if (currentType.equals(targetType)) {
            return currentTableSchema;
        }

        AtomicInteger highestFieldId = new AtomicInteger(currentTableSchema.highestFieldId());
        RowType newRowType =
                mergeSchemas(currentType, targetType, highestFieldId, allowExplicitCast);
        if (newRowType.equals(currentType)) {
            // It happens if the `targetType` only changes `nullability` but we always respect the
            // current's.
            return currentTableSchema;
        }

        return new TableSchema(
                currentTableSchema.id() + 1,
                newRowType.getFields(),
                highestFieldId.get(),
                currentTableSchema.partitionKeys(),
                currentTableSchema.primaryKeys(),
                currentTableSchema.options(),
                currentTableSchema.comment());
    }

    public static RowType mergeSchemas(
            RowType tableSchema,
            RowType dataSchema,
            AtomicInteger highestFieldId,
            boolean allowExplicitCast) {
        return (RowType) merge(tableSchema, dataSchema, highestFieldId, allowExplicitCast);
    }

    /**
     * Merge the base data type and the update data type if possible.
     *
     * <p>For RowType, find the fields which exists in both the base schema and the update schema,
     * and try to merge them by calling the method iteratively; remain those fields that are only in
     * the base schema and append those fields that are only in the update schema.
     *
     * <p>For other complex type, try to merge the element types.
     *
     * <p>For primitive data type, we treat that's compatible if the original type can be safely
     * cast to the new type.
     */
    public static DataType merge(
            DataType base0,
            DataType update0,
            AtomicInteger highestFieldId,
            boolean allowExplicitCast) {
        // Here we try to merge the base0 and update0 without regard to the nullability,
        // and set the base0's nullability to the return's.
        DataType base = base0.copy(true);
        DataType update = update0.copy(true);

        if (base.equals(update)) {
            return base0;
        } else if (base instanceof RowType && update instanceof RowType) {
            List<DataField> baseFields = ((RowType) base).getFields();
            List<DataField> updateFields = ((RowType) update).getFields();
            Map<String, DataField> updateFieldMap =
                    updateFields.stream()
                            .collect(Collectors.toMap(DataField::name, Function.identity()));
            List<DataField> updatedFields =
                    baseFields.stream()
                            .map(
                                    baseField -> {
                                        if (updateFieldMap.containsKey(baseField.name())) {
                                            DataField updateField =
                                                    updateFieldMap.get(baseField.name());
                                            DataType updatedDataType =
                                                    merge(
                                                            baseField.type(),
                                                            updateField.type(),
                                                            highestFieldId,
                                                            allowExplicitCast);
                                            return new DataField(
                                                    baseField.id(),
                                                    baseField.name(),
                                                    updatedDataType,
                                                    baseField.description(),
                                                    baseField.defaultValue());
                                        } else {
                                            return baseField;
                                        }
                                    })
                            .collect(Collectors.toList());

            Map<String, DataField> baseFieldMap =
                    baseFields.stream()
                            .collect(Collectors.toMap(DataField::name, Function.identity()));
            List<DataField> newFields =
                    updateFields.stream()
                            .filter(field -> !baseFieldMap.containsKey(field.name()))
                            .map(field -> assignIdForNewField(field, highestFieldId))
                            .map(field -> field.copy(true))
                            .collect(Collectors.toList());

            updatedFields.addAll(newFields);
            return new RowType(base0.isNullable(), updatedFields);
        } else if (base instanceof MapType && update instanceof MapType) {
            return new MapType(
                    base0.isNullable(),
                    merge(
                            ((MapType) base).getKeyType(),
                            ((MapType) update).getKeyType(),
                            highestFieldId,
                            allowExplicitCast),
                    merge(
                            ((MapType) base).getValueType(),
                            ((MapType) update).getValueType(),
                            highestFieldId,
                            allowExplicitCast));
        } else if (base instanceof ArrayType && update instanceof ArrayType) {
            return new ArrayType(
                    base0.isNullable(),
                    merge(
                            ((ArrayType) base).getElementType(),
                            ((ArrayType) update).getElementType(),
                            highestFieldId,
                            allowExplicitCast));
        } else if (base instanceof MultisetType && update instanceof MultisetType) {
            return new MultisetType(
                    base0.isNullable(),
                    merge(
                            ((MultisetType) base).getElementType(),
                            ((MultisetType) update).getElementType(),
                            highestFieldId,
                            allowExplicitCast));
        } else if (base instanceof DecimalType && update instanceof DecimalType) {
            if (((DecimalType) base).getScale() == ((DecimalType) update).getScale()) {
                return new DecimalType(
                        base0.isNullable(),
                        Math.max(
                                ((DecimalType) base).getPrecision(),
                                ((DecimalType) update).getPrecision()),
                        ((DecimalType) base).getScale());
            } else {
                throw new UnsupportedOperationException(
                        String.format(
                                "Failed to merge decimal types with different scale: %s and %s",
                                base, update));
            }
        } else if (supportsDataTypesCast(base, update, allowExplicitCast)) {
            if (DataTypes.getLength(base).isPresent() && DataTypes.getLength(update).isPresent()) {
                // this will check and merge types which has a `length` attribute, like BinaryType,
                // CharType, VarBinaryType, VarCharType.
                if (allowExplicitCast
                        || DataTypes.getLength(base).getAsInt()
                                <= DataTypes.getLength(update).getAsInt()) {
                    return update.copy(base0.isNullable());
                } else {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Failed to merge the target type that has a smaller length: %s and %s",
                                    base, update));
                }
            } else if (DataTypes.getPrecision(base).isPresent()
                    && DataTypes.getPrecision(update).isPresent()) {
                // this will check and merge types which has a `precision` attribute, like
                // LocalZonedTimestampType, TimeType, TimestampType.
                if (allowExplicitCast
                        || DataTypes.getPrecision(base).getAsInt()
                                <= DataTypes.getPrecision(update).getAsInt()) {
                    return update.copy(base0.isNullable());
                } else {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Failed to merge the target type that has a lower precision: %s and %s",
                                    base, update));
                }
            } else {
                return update.copy(base0.isNullable());
            }
        } else {
            throw new UnsupportedOperationException(
                    String.format("Failed to merge data types %s and %s", base, update));
        }
    }

    private static boolean supportsDataTypesCast(
            DataType sourceType, DataType targetType, boolean allowExplicitCast) {
        return DataTypeCasts.supportsCast(sourceType, targetType, allowExplicitCast);
    }

    private static DataField assignIdForNewField(DataField field, AtomicInteger highestFieldId) {
        DataType dataType = ReassignFieldId.reassign(field.type(), highestFieldId);
        return new DataField(
                highestFieldId.incrementAndGet(),
                field.name(),
                dataType,
                field.description(),
                field.defaultValue());
    }
}
