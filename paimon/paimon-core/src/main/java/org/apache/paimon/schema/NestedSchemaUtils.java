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
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 嵌套 Schema 工具
 *
 * <p>NestedSchemaUtils 提供了处理嵌套列（Nested Column）Schema 变更的工具方法。
 * 支持 ROW、ARRAY、MAP、MULTISET 等复杂类型的 Schema 演化。
 *
 * <p>嵌套类型（Nested Type）：
 * <ul>
 *   <li>ROW：结构体类型，包含多个命名字段
 *   <li>ARRAY：数组类型，包含相同类型的多个元素
 *   <li>MAP：映射类型，包含键值对
 *   <li>MULTISET：多重集合类型，类似于 ARRAY 但无序
 * </ul>
 *
 * <p>核心功能：
 * <ul>
 *   <li>{@link #generateNestedColumnUpdates}：生成嵌套列的 Schema 变更操作
 *   <li>递归处理嵌套类型的字段变更
 *   <li>支持添加、删除、修改嵌套字段
 *   <li>支持类型变更和可空性变更
 * </ul>
 *
 * <p>支持的 Schema 演化操作：
 * <pre>
 * 1. ROW 类型演化：
 *    旧类型：ROW<id INT, name STRING>
 *    新类型：ROW<id BIGINT, name STRING, age INT>
 *    变更：
 *    - 修改类型：id INT → BIGINT
 *    - 添加字段：age INT
 *
 * 2. ARRAY 类型演化：
 *    旧类型：ARRAY<INT>
 *    新类型：ARRAY<BIGINT>
 *    变更：
 *    - 修改元素类型：INT → BIGINT
 *
 * 3. MAP 类型演化：
 *    旧类型：MAP<STRING, INT>
 *    新类型：MAP<STRING, BIGINT>
 *    变更：
 *    - 修改 value 类型：INT → BIGINT
 *    - 注意：key 类型不能变更
 *
 * 4. MULTISET 类型演化：
 *    旧类型：MULTISET<INT>
 *    新类型：MULTISET<BIGINT>
 *    变更：
 *    - 修改元素类型：INT → BIGINT
 * </pre>
 *
 * <p>字段路径（Field Path）表示：
 * <pre>
 * 嵌套字段通过路径表示：
 *
 * 示例 1：ROW 嵌套
 *   Schema：user ROW<id INT, address ROW<city STRING, zip INT>>
 *   字段路径：
 *   - user.id：["user", "id"]
 *   - user.address.city：["user", "address", "city"]
 *
 * 示例 2：ARRAY 嵌套
 *   Schema：tags ARRAY<STRING>
 *   字段路径：
 *   - tags.element：["tags", "element"]
 *   - "element" 是虚拟字段名，表示数组元素
 *
 * 示例 3：MAP 嵌套
 *   Schema：props MAP<STRING, INT>
 *   字段路径：
 *   - props.value：["props", "value"]
 *   - "value" 是虚拟字段名，表示 Map 的值
 *   - Map 的 key 不能演化，所以没有 "key" 路径
 * </pre>
 *
 * <p>ROW 类型演化的约束：
 * <ul>
 *   <li>字段顺序必须保持：已有字段的相对顺序不能改变
 *   <li>可以添加新字段：新字段追加到末尾或指定位置
 *   <li>可以删除字段：删除不再需要的字段
 *   <li>可以修改字段类型：类型必须兼容（能安全转换）
 *   <li>可以修改字段注释
 *   <li>可以修改可空性
 * </ul>
 *
 * <p>MAP 类型演化的约束：
 * <ul>
 *   <li>key 类型必须保持一致：MAP&lt;STRING, INT&gt; 的 key 不能改为 INT
 *   <li>只能演化 value 类型：MAP&lt;STRING, INT&gt; → MAP&lt;STRING, BIGINT&gt;
 *   <li>原因：key 用于查找，类型变更会导致数据不一致
 * </ul>
 *
 * <p>使用场景：
 * <pre>{@code
 * // 场景 1：CDC 自动 Schema 演化（Flink CDC）
 * // 上游数据库修改了嵌套字段
 * DataType oldType = DataTypes.ROW(
 *     DataTypes.FIELD("id", DataTypes.INT()),
 *     DataTypes.FIELD("name", DataTypes.STRING())
 * );
 * DataType newType = DataTypes.ROW(
 *     DataTypes.FIELD("id", DataTypes.BIGINT()),  // 类型变更
 *     DataTypes.FIELD("name", DataTypes.STRING()),
 *     DataTypes.FIELD("age", DataTypes.INT())     // 新字段
 * );
 * List<String> fieldNames = Arrays.asList("user");
 * List<SchemaChange> changes = new ArrayList<>();
 * NestedSchemaUtils.generateNestedColumnUpdates(
 *     fieldNames,
 *     oldType,
 *     newType,
 *     changes
 * );
 * // 生成的 SchemaChange：
 * // - UpdateColumnType: user.id INT → BIGINT
 * // - AddColumn: user.age INT
 *
 * // 场景 2：手动修改嵌套字段
 * // 修改 ARRAY 的元素类型
 * DataType oldArray = DataTypes.ARRAY(DataTypes.INT());
 * DataType newArray = DataTypes.ARRAY(DataTypes.BIGINT());
 * List<String> fieldNames = Arrays.asList("tags");
 * List<SchemaChange> changes = new ArrayList<>();
 * NestedSchemaUtils.generateNestedColumnUpdates(
 *     fieldNames,
 *     oldArray,
 *     newArray,
 *     changes
 * );
 * // 生成的 SchemaChange：
 * // - UpdateColumnType: tags.element INT → BIGINT
 *
 * // 场景 3：修改 MAP 的 value 类型
 * DataType oldMap = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());
 * DataType newMap = DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT());
 * List<String> fieldNames = Arrays.asList("props");
 * List<SchemaChange> changes = new ArrayList<>();
 * NestedSchemaUtils.generateNestedColumnUpdates(
 *     fieldNames,
 *     oldMap,
 *     newMap,
 *     changes
 * );
 * // 生成的 SchemaChange：
 * // - UpdateColumnType: props.value INT → BIGINT
 * }</pre>
 *
 * <p>与 FlinkCatalog 和 CDC 的关系：
 * <ul>
 *   <li>FlinkCatalog：使用此工具处理 Flink SQL 的嵌套列变更
 *   <li>CDC Source：使用此工具处理 CDC 数据的嵌套列变更
 *   <li>统一的处理逻辑，确保不同来源的 Schema 演化行为一致
 * </ul>
 *
 * <p>递归处理流程：
 * <pre>
 * 1. 判断数据类型：
 *    - ROW：调用 handleRowTypeUpdate
 *    - ARRAY：调用 handleArrayTypeUpdate
 *    - MAP：调用 handleMapTypeUpdate
 *    - MULTISET：调用 handleMultisetTypeUpdate
 *    - 基本类型：调用 handlePrimitiveTypeUpdate
 *
 * 2. 处理嵌套字段：
 *    - 递归调用 generateNestedColumnUpdates
 *    - 构建完整的字段路径
 *    - 生成对应的 SchemaChange
 *
 * 3. 处理可空性：
 *    - 调用 handleNullabilityChange
 *    - 生成 UpdateColumnNullability
 * </pre>
 *
 * @see SchemaChange Schema 变更操作
 * @see SchemaManager Schema 管理器
 * @see org.apache.paimon.types.RowType ROW 类型
 * @see org.apache.paimon.types.ArrayType ARRAY 类型
 * @see org.apache.paimon.types.MapType MAP 类型
 * @see org.apache.paimon.types.MultisetType MULTISET 类型
 */
public class NestedSchemaUtils {

    /**
     * Generates nested column updates for schema evolution. Handles all nested types: ROW, ARRAY,
     * MAP, MULTISET. Creates proper field paths with markers like "element" for arrays and "value"
     * for maps.
     *
     * @param fieldNames The current field path as a list of field names
     * @param oldType The old data type
     * @param newType The new data type
     * @param schemaChanges List to collect the generated schema changes
     */
    public static void generateNestedColumnUpdates(
            List<String> fieldNames,
            DataType oldType,
            DataType newType,
            List<SchemaChange> schemaChanges) {

        if (oldType.getTypeRoot() == DataTypeRoot.ROW) {
            handleRowTypeUpdate(fieldNames, oldType, newType, schemaChanges);
        } else if (oldType.getTypeRoot() == DataTypeRoot.ARRAY) {
            handleArrayTypeUpdate(fieldNames, oldType, newType, schemaChanges);
        } else if (oldType.getTypeRoot() == DataTypeRoot.MAP) {
            handleMapTypeUpdate(fieldNames, oldType, newType, schemaChanges);
        } else if (oldType.getTypeRoot() == DataTypeRoot.MULTISET) {
            handleMultisetTypeUpdate(fieldNames, oldType, newType, schemaChanges);
        } else {
            // For primitive types, update the column type directly
            handlePrimitiveTypeUpdate(fieldNames, oldType, newType, schemaChanges);
        }

        // Handle nullability changes for all types
        handleNullabilityChange(fieldNames, oldType, newType, schemaChanges);
    }

    private static void handleRowTypeUpdate(
            List<String> fieldNames,
            DataType oldType,
            DataType newType,
            List<SchemaChange> schemaChanges) {

        String joinedNames = String.join(".", fieldNames);

        Preconditions.checkArgument(
                newType.getTypeRoot() == DataTypeRoot.ROW,
                "Column %s can only be updated to row type, and cannot be updated to %s type",
                joinedNames,
                newType.getTypeRoot());

        RowType oldRowType = (RowType) oldType;
        RowType newRowType = (RowType) newType;

        // check that existing fields maintain their order
        Map<String, Integer> oldFieldOrders = new HashMap<>();
        for (int i = 0; i < oldRowType.getFieldCount(); i++) {
            oldFieldOrders.put(oldRowType.getFields().get(i).name(), i);
        }

        int lastIdx = -1;
        String lastFieldName = "";
        for (DataField newField : newRowType.getFields()) {
            String name = newField.name();
            if (oldFieldOrders.containsKey(name)) {
                int idx = oldFieldOrders.get(name);
                Preconditions.checkState(
                        lastIdx < idx,
                        "Order of existing fields in column %s must be kept the same. "
                                + "However, field %s and %s have changed their orders.",
                        joinedNames,
                        lastFieldName,
                        name);
                lastIdx = idx;
                lastFieldName = name;
            }
        }

        // drop fields
        Set<String> newFieldNames = new HashSet<>(newRowType.getFieldNames());
        for (String name : oldRowType.getFieldNames()) {
            if (!newFieldNames.contains(name)) {
                List<String> dropColumnNames = new ArrayList<>(fieldNames);
                dropColumnNames.add(name);
                schemaChanges.add(SchemaChange.dropColumn(dropColumnNames.toArray(new String[0])));
            }
        }

        for (int i = 0; i < newRowType.getFieldCount(); i++) {
            DataField field = newRowType.getFields().get(i);
            String name = field.name();
            List<String> fullFieldNames = new ArrayList<>(fieldNames);
            fullFieldNames.add(name);

            if (!oldFieldOrders.containsKey(name)) {
                // add fields
                SchemaChange.Move move;
                if (i == 0) {
                    move = SchemaChange.Move.first(name);
                } else {
                    String lastName = newRowType.getFields().get(i - 1).name();
                    move = SchemaChange.Move.after(name, lastName);
                }
                schemaChanges.add(
                        SchemaChange.addColumn(
                                fullFieldNames.toArray(new String[0]),
                                field.type(),
                                field.description(),
                                move));
            } else {
                // update existing fields
                DataField oldField = oldRowType.getFields().get(oldFieldOrders.get(name));
                if (!Objects.equals(oldField.description(), field.description())) {
                    schemaChanges.add(
                            SchemaChange.updateColumnComment(
                                    fullFieldNames.toArray(new String[0]), field.description()));
                }
                generateNestedColumnUpdates(
                        fullFieldNames, oldField.type(), field.type(), schemaChanges);
            }
        }
    }

    private static void handleArrayTypeUpdate(
            List<String> fieldNames,
            DataType oldType,
            DataType newType,
            List<SchemaChange> schemaChanges) {

        String joinedNames = String.join(".", fieldNames);
        Preconditions.checkArgument(
                newType.getTypeRoot() == DataTypeRoot.ARRAY,
                "Column %s can only be updated to array type, and cannot be updated to %s type",
                joinedNames,
                newType);

        List<String> fullFieldNames = new ArrayList<>(fieldNames);
        // add a dummy column name indicating the element of array
        fullFieldNames.add("element");

        generateNestedColumnUpdates(
                fullFieldNames,
                ((ArrayType) oldType).getElementType(),
                ((ArrayType) newType).getElementType(),
                schemaChanges);
    }

    private static void handleMapTypeUpdate(
            List<String> fieldNames,
            DataType oldType,
            DataType newType,
            List<SchemaChange> schemaChanges) {

        String joinedNames = String.join(".", fieldNames);
        Preconditions.checkArgument(
                newType.getTypeRoot() == DataTypeRoot.MAP,
                "Column %s can only be updated to map type, and cannot be updated to %s type",
                joinedNames,
                newType);

        MapType oldMapType = (MapType) oldType;
        MapType newMapType = (MapType) newType;

        Preconditions.checkArgument(
                oldMapType.getKeyType().equals(newMapType.getKeyType()),
                "Cannot update key type of column %s from %s type to %s type",
                joinedNames,
                oldMapType.getKeyType(),
                newMapType.getKeyType());

        List<String> fullFieldNames = new ArrayList<>(fieldNames);
        // add a dummy column name indicating the value of map
        fullFieldNames.add("value");

        generateNestedColumnUpdates(
                fullFieldNames,
                oldMapType.getValueType(),
                newMapType.getValueType(),
                schemaChanges);
    }

    private static void handleMultisetTypeUpdate(
            List<String> fieldNames,
            DataType oldType,
            DataType newType,
            List<SchemaChange> schemaChanges) {

        String joinedNames = String.join(".", fieldNames);

        Preconditions.checkArgument(
                newType.getTypeRoot() == DataTypeRoot.MULTISET,
                "Column %s can only be updated to multiset type, and cannot be updated to %s type",
                joinedNames,
                newType);

        List<String> fullFieldNames = new ArrayList<>(fieldNames);
        // Add the special "element" marker for multiset element access
        fullFieldNames.add("element");

        generateNestedColumnUpdates(
                fullFieldNames,
                ((MultisetType) oldType).getElementType(),
                ((MultisetType) newType).getElementType(),
                schemaChanges);
    }

    private static void handlePrimitiveTypeUpdate(
            List<String> fieldNames,
            DataType oldType,
            DataType newType,
            List<SchemaChange> schemaChanges) {

        if (!oldType.equalsIgnoreNullable(newType)) {
            schemaChanges.add(
                    SchemaChange.updateColumnType(
                            fieldNames.toArray(new String[0]), newType, false));
        }
    }

    private static void handleNullabilityChange(
            List<String> fieldNames,
            DataType oldType,
            DataType newType,
            List<SchemaChange> schemaChanges) {

        if (oldType.isNullable() != newType.isNullable()) {
            schemaChanges.add(
                    SchemaChange.updateColumnNullability(
                            fieldNames.toArray(new String[0]), newType.isNullable()));
        }
    }
}
