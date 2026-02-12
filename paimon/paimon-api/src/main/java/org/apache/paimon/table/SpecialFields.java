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

package org.apache.paimon.table;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link org.apache.paimon.types.RowType} 中的特殊字段，具有特定的字段 ID。
 *
 * <p><b>系统字段</b>（System fields）:
 * <p>这些字段用于 Paimon 内部管理，具有固定的字段 ID 范围（从 {@link #SYSTEM_FIELD_ID_START} 开始）:
 * <ul>
 *   <li>{@code _KEY_<key-field>}: 键值对的键字段。ID = 1073741823 + {@code (field-id)}
 *   <li>{@code _SEQUENCE_NUMBER}: 键值对的序列号。ID = 2147483646
 *   <li>{@code _VALUE_KIND}: 键值对的类型（见 {@link org.apache.paimon.types.RowKind}）。ID = 2147483645
 *   <li>{@code _LEVEL}: 键值对所在的 LSM 树层级。ID = 2147483644
 *   <li>{@code rowkind}: 审计日志系统表中的 rowkind 字段。ID = 2147483643
 *   <li>{@code _ROW_ID}: 行跟踪的行 ID 字段。ID = 2147483642
 * </ul>
 *
 * <p><b>结构化类型字段</b>（Structured type fields）:
 * <p>这些 ID 主要用作 Parquet 文件中的字段 ID，使计算引擎可以直接通过 ID 读取字段。
 * 这些 ID 不存储在 {@link org.apache.paimon.types.DataField} 中。
 * <ul>
 *   <li>数组元素字段: ID = 536870911 + 1024 * {@code (array-field-id)} + depth
 *   <li>Map 键字段: ID = 536870911 - 1024 * {@code (map-field-id)} - depth
 *   <li>Map 值字段: ID = 536870911 + 1024 * {@code (map-field-id)} + depth
 * </ul>
 *
 * <p><b>示例</b>:
 * <pre>{@code
 * // ARRAY<MAP<INT, ARRAY<INT>>> 类型，外层数组的字段 ID 为 10
 * // - Map（外层数组的元素）的字段 ID = 536870911 + 1024 * 10 + 1
 * // - Map 键（int）的字段 ID = 536870911 - 1024 * 10 - 2
 * // - Map 值（内层数组）的字段 ID = 536870911 + 1024 * 10 + 2
 * // - 内层数组元素（int）的字段 ID = 536870911 + 1024 * 10 + 3
 * }</pre>
 */
public class SpecialFields {

    // ----------------------------------------------------------------------------------------
    // 系统字段
    // ----------------------------------------------------------------------------------------

    /** 系统字段 ID 起始值（Integer.MAX_VALUE / 2 = 1073741823）。 */
    public static final int SYSTEM_FIELD_ID_START = Integer.MAX_VALUE / 2;

    /** 键字段名前缀。 */
    public static final String KEY_FIELD_PREFIX = "_KEY_";

    /** 键字段 ID 起始值。 */
    public static final int KEY_FIELD_ID_START = SYSTEM_FIELD_ID_START;

    /** 序列号字段：用于记录键值对的版本序列号。 */
    public static final DataField SEQUENCE_NUMBER =
            new DataField(Integer.MAX_VALUE - 1, "_SEQUENCE_NUMBER", DataTypes.BIGINT().notNull());

    /** 值类型字段：用于记录键值对的操作类型（INSERT/UPDATE/DELETE 等）。 */
    public static final DataField VALUE_KIND =
            new DataField(Integer.MAX_VALUE - 2, "_VALUE_KIND", DataTypes.TINYINT().notNull());

    /** 层级字段：用于记录键值对所在的 LSM 树层级。 */
    public static final DataField LEVEL =
            new DataField(Integer.MAX_VALUE - 3, "_LEVEL", DataTypes.INT().notNull());

    /** 行类型字段：仅用于审计日志表（AuditLogTable）。 */
    public static final DataField ROW_KIND =
            new DataField(
                    Integer.MAX_VALUE - 4, "rowkind", new VarCharType(VarCharType.MAX_LENGTH));

    /** 行 ID 字段：用于行跟踪功能。 */
    public static final DataField ROW_ID =
            new DataField(Integer.MAX_VALUE - 5, "_ROW_ID", DataTypes.BIGINT().notNull());

    /** 系统字段名称集合。 */
    public static final Set<String> SYSTEM_FIELD_NAMES =
            Stream.of(
                            SEQUENCE_NUMBER.name(),
                            VALUE_KIND.name(),
                            LEVEL.name(),
                            ROW_KIND.name(),
                            ROW_ID.name())
                    .collect(Collectors.toSet());

    /**
     * 判断字段 ID 是否为系统字段。
     *
     * @param fieldId 字段 ID
     * @return 如果是系统字段返回 true
     */
    public static boolean isSystemField(int fieldId) {
        return fieldId >= SYSTEM_FIELD_ID_START;
    }

    /**
     * 判断字段名是否为系统字段。
     *
     * @param field 字段名
     * @return 如果是系统字段返回 true
     */
    public static boolean isSystemField(String field) {
        return field.startsWith(KEY_FIELD_PREFIX) || SYSTEM_FIELD_NAMES.contains(field);
    }

    /**
     * 判断字段名是否为键字段。
     *
     * @param field 字段名
     * @return 如果是键字段返回 true
     */
    public static boolean isKeyField(String field) {
        return field.startsWith(KEY_FIELD_PREFIX);
    }

    // ----------------------------------------------------------------------------------------
    // 结构化类型字段
    // ----------------------------------------------------------------------------------------

    /** 结构化类型字段 ID 基准值（Integer.MAX_VALUE / 4 = 536870911）。 */
    public static final int STRUCTURED_TYPE_FIELD_ID_BASE = Integer.MAX_VALUE / 4;

    /** 结构化类型字段深度限制（1024）。 */
    public static final int STRUCTURED_TYPE_FIELD_DEPTH_LIMIT = 1 << 10;

    /**
     * 获取数组元素字段的 ID。
     *
     * @param arrayFieldId 数组字段的 ID
     * @param depth 嵌套深度
     * @return 数组元素字段的 ID
     */
    public static int getArrayElementFieldId(int arrayFieldId, int depth) {
        return STRUCTURED_TYPE_FIELD_ID_BASE
                + arrayFieldId * STRUCTURED_TYPE_FIELD_DEPTH_LIMIT
                + depth;
    }

    /**
     * 获取 Map 键字段的 ID。
     *
     * @param mapFieldId Map 字段的 ID
     * @param depth 嵌套深度
     * @return Map 键字段的 ID
     */
    public static int getMapKeyFieldId(int mapFieldId, int depth) {
        return STRUCTURED_TYPE_FIELD_ID_BASE
                - mapFieldId * STRUCTURED_TYPE_FIELD_DEPTH_LIMIT
                - depth;
    }

    /**
     * 获取 Map 值字段的 ID。
     *
     * @param mapFieldId Map 字段的 ID
     * @param depth 嵌套深度
     * @return Map 值字段的 ID
     */
    public static int getMapValueFieldId(int mapFieldId, int depth) {
        return STRUCTURED_TYPE_FIELD_ID_BASE
                + mapFieldId * STRUCTURED_TYPE_FIELD_DEPTH_LIMIT
                + depth;
    }

    /**
     * 为 RowType 添加行跟踪字段。
     *
     * <p>添加 {@link #ROW_ID} 和 {@link #SEQUENCE_NUMBER} 字段，使用默认的非空约束。
     *
     * @param rowType 原始 RowType
     * @return 带行跟踪字段的 RowType
     */
    public static RowType rowTypeWithRowTracking(RowType rowType) {
        return rowTypeWithRowTracking(rowType, false, false);
    }

    /**
     * 为 RowType 添加行跟踪字段（支持自定义可空性）。
     *
     * @param rowType 原始 RowType
     * @param rowIdNullable ROW_ID 是否可为 null（对用户不为 null，但在读取时可为 null）
     * @param sequenceNumberNullable SEQUENCE_NUMBER 是否可为 null（对用户不为 null，但在读写时可为 null）
     * @return 带行跟踪字段的 RowType
     */
    public static RowType rowTypeWithRowTracking(
            RowType rowType, boolean rowIdNullable, boolean sequenceNumberNullable) {
        List<DataField> fieldsWithRowTracking = new ArrayList<>(rowType.getFields());

        fieldsWithRowTracking.forEach(
                f -> {
                    if (ROW_ID.name().equals(f.name()) || SEQUENCE_NUMBER.name().equals(f.name())) {
                        throw new IllegalArgumentException(
                                "Row tracking field name '"
                                        + f.name()
                                        + "' conflicts with existing field names.");
                    }
                });
        fieldsWithRowTracking.add(rowIdNullable ? ROW_ID.copy(true) : ROW_ID);
        fieldsWithRowTracking.add(
                sequenceNumberNullable ? SEQUENCE_NUMBER.copy(true) : SEQUENCE_NUMBER);
        return new RowType(fieldsWithRowTracking);
    }

    /**
     * 为 RowType 添加行 ID 字段。
     *
     * @param rowType 原始 RowType
     * @return 带行 ID 字段的 RowType
     */
    public static RowType rowTypeWithRowId(RowType rowType) {
        List<DataField> fieldsWithRowTracking = new ArrayList<>(rowType.getFields());

        fieldsWithRowTracking.forEach(
                f -> {
                    if (ROW_ID.name().equals(f.name())) {
                        throw new IllegalArgumentException(
                                "Row tracking field name '"
                                        + f.name()
                                        + "' conflicts with existing field names.");
                    }
                });
        fieldsWithRowTracking.add(SpecialFields.ROW_ID);
        return new RowType(fieldsWithRowTracking);
    }
}
