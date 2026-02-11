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

import org.apache.paimon.casting.CastElementGetter;
import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.casting.CastFieldGetter;
import org.apache.paimon.casting.CastedArray;
import org.apache.paimon.casting.CastedMap;
import org.apache.paimon.casting.CastedRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateReplaceVisitor;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;
import org.apache.paimon.utils.ProjectedRow;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * Schema 演化工具
 *
 * <p>SchemaEvolutionUtil 提供了处理 Schema 演化时的数据读取功能。当表的 Schema 发生变更后，
 * 历史数据文件可能使用旧的 Schema，此工具负责将旧 Schema 的数据适配到新 Schema。
 *
 * <p>核心功能：
 * <ul>
 *   <li>索引映射：{@link #createIndexMapping} - 创建字段索引映射（旧 Schema → 新 Schema）
 *   <li>类型转换映射：{@link #createIndexCastMapping} - 创建索引和类型转换映射
 *   <li>谓词下推：{@link #devolveFilters} - 将过滤条件适配到旧 Schema
 *   <li>类型转换：{@link #createCastExecutor} - 创建类型转换执行器
 * </ul>
 *
 * <p>Schema 演化场景示例：
 * <pre>
 * 场景 1：添加字段
 *   旧 Schema（Schema ID = 0）：[id INT, name STRING]
 *   新 Schema（Schema ID = 1）：[id INT, name STRING, age INT]
 *   数据文件：使用 Schema ID = 0
 *
 *   读取策略：
 *   - 创建索引映射：[0 → 0, 1 → 1, 2 → -1]
 *   - 字段 0 (id)：从数据文件的字段 0 读取
 *   - 字段 1 (name)：从数据文件的字段 1 读取
 *   - 字段 2 (age)：返回 NULL（数据文件不存在此字段）
 *
 * 场景 2：删除字段
 *   旧 Schema：[id INT, name STRING, age INT]
 *   新 Schema：[id INT, name STRING]
 *   数据文件：使用旧 Schema
 *
 *   读取策略：
 *   - 创建索引映射：[0 → 0, 1 → 1]
 *   - 只读取需要的字段，忽略 age 字段
 *
 * 场景 3：字段重命名
 *   旧 Schema：[id INT, userName STRING]
 *   新 Schema：[id INT, user_name STRING]
 *   数据文件：使用旧 Schema
 *
 *   读取策略：
 *   - 通过 Field ID 匹配（而非字段名）
 *   - userName 和 user_name 的 Field ID 相同
 *   - 创建索引映射：[0 → 0, 1 → 1]
 *
 * 场景 4：类型变更
 *   旧 Schema：[id INT, age INT]
 *   新 Schema：[id BIGINT, age BIGINT]
 *   数据文件：使用旧 Schema
 *
 *   读取策略：
 *   - 创建索引映射：[0 → 0, 1 → 1]
 *   - 创建类型转换映射：[INT→BIGINT, INT→BIGINT]
 *   - 读取数据后执行类型转换
 *
 * 场景 5：字段顺序变更
 *   旧 Schema：[id INT, name STRING, age INT]
 *   新 Schema：[age INT, id INT, name STRING]
 *   数据文件：使用旧 Schema
 *
 *   读取策略：
 *   - 创建索引映射：[2 → 0, 0 → 1, 1 → 2]
 *   - 按新 Schema 的顺序重新排列字段
 * </pre>
 *
 * <p>索引映射（Index Mapping）：
 * <ul>
 *   <li>将新 Schema 的字段索引映射到旧 Schema 的字段索引
 *   <li>通过 Field ID 匹配（而非字段名）
 *   <li>如果字段不存在，映射为 -1（NULL_FIELD_INDEX）
 *   <li>如果映射结果是 [0, 1, 2, ...]，则返回 null（无需映射）
 * </ul>
 *
 * <p>类型转换映射（Cast Mapping）：
 * <ul>
 *   <li>为需要类型转换的字段创建 {@link org.apache.paimon.casting.CastFieldGetter}
 *   <li>支持基本类型转换：INT → BIGINT, FLOAT → DOUBLE 等
 *   <li>支持复杂类型转换：ROW、ARRAY、MAP、MULTISET
 *   <li>如果所有字段类型一致，返回 null（无需转换）
 * </ul>
 *
 * <p>谓词下推（Filter Pushdown）：
 * <pre>
 * 问题：查询使用新 Schema 的字段名和类型，但数据文件使用旧 Schema
 *
 * 解决：将过滤条件从新 Schema 转换为旧 Schema
 *
 * 示例：
 *   新 Schema：[id BIGINT, name STRING, age INT]
 *   旧 Schema：[id INT, name STRING]
 *   过滤条件：id > 1000 AND age > 18
 *
 *   转换后：
 *   - id > 1000：转换为 INT 类型的比较（因为旧 Schema 是 INT）
 *   - age > 18：删除（旧 Schema 没有 age 字段，无法过滤）
 * </pre>
 *
 * <p>复杂类型的 Schema 演化：
 * <ul>
 *   <li>ROW 类型：递归处理嵌套字段的索引映射和类型转换
 *   <li>ARRAY 类型：转换元素类型（如 ARRAY&lt;INT&gt; → ARRAY&lt;BIGINT&gt;）
 *   <li>MAP 类型：只能转换 value 类型，key 类型必须一致
 *   <li>MULTISET 类型：转换元素类型
 * </ul>
 *
 * <p>与 Batch 6 的关系（DataEvolutionSplitRead）：
 * <ul>
 *   <li>{@link org.apache.paimon.operation.DataEvolutionSplitRead}：使用此工具进行数据演化读取
 *   <li>DataEvolutionSplitRead 负责读取 BLOB 类型字段（大对象）
 *   <li>SchemaEvolutionUtil 负责普通字段的 Schema 演化
 *   <li>两者配合实现完整的数据演化功能
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 1. 创建索引映射
 * List<DataField> tableFields = currentSchema.fields();  // 新 Schema 字段
 * List<DataField> dataFields = fileSchema.fields();      // 数据文件的 Schema 字段
 * int[] indexMapping = SchemaEvolutionUtil.createIndexMapping(tableFields, dataFields);
 * // 结果示例：[0, 1, -1] 表示：字段 0→0, 字段 1→1, 字段 2 不存在
 *
 * // 2. 创建索引和类型转换映射
 * IndexCastMapping mapping = SchemaEvolutionUtil.createIndexCastMapping(
 *     tableFields,
 *     dataFields
 * );
 * int[] indexes = mapping.getIndexMapping();
 * CastFieldGetter[] casts = mapping.getCastMapping();
 *
 * // 3. 读取数据时应用映射
 * InternalRow dataRow = ...; // 从数据文件读取的 Row
 * if (indexes != null) {
 *     dataRow = ProjectedRow.from(indexes).replaceRow(dataRow);  // 重排字段
 * }
 * if (casts != null) {
 *     dataRow = CastedRow.from(casts).replaceRow(dataRow);  // 类型转换
 * }
 *
 * // 4. 转换过滤条件
 * List<Predicate> filters = Arrays.asList(
 *     new LeafPredicate(
 *         LeafPredicate.GREATER_THAN,
 *         DataTypes.BIGINT(),
 *         0,
 *         "id",
 *         Collections.singletonList(1000L)
 *     )
 * );
 * List<Predicate> dataFilters = SchemaEvolutionUtil.devolveFilters(
 *     tableFields,
 *     dataFields,
 *     filters,
 *     false  // 不保留新字段的过滤条件
 * );
 * // 结果：过滤条件的字段名、索引、类型都已适配到旧 Schema
 * }</pre>
 *
 * <p>性能优化：
 * <ul>
 *   <li>如果 Schema 没有变化，索引映射返回 null（避免无意义的投影）
 *   <li>如果类型没有变化，类型转换映射返回 null（避免无意义的转换）
 *   <li>类型转换使用高效的 {@link org.apache.paimon.casting.CastExecutor}
 *   <li>投影使用 {@link org.apache.paimon.utils.ProjectedRow}（避免数据复制）
 * </ul>
 *
 * @see TableSchema 表结构
 * @see SchemaManager Schema 管理器
 * @see IndexCastMapping 索引和类型转换映射
 * @see org.apache.paimon.operation.DataEvolutionSplitRead 数据演化读取
 * @see org.apache.paimon.casting.CastExecutor 类型转换执行器
 * @see org.apache.paimon.utils.ProjectedRow 投影行
 */
public class SchemaEvolutionUtil {

    private static final int NULL_FIELD_INDEX = -1;

    /**
     * Create index mapping from table fields to underlying data fields. For example, the table and
     * data fields are as follows
     *
     * <ul>
     *   <li>table fields: 1->c, 6->b, 3->a
     *   <li>data fields: 1->a, 3->c
     * </ul>
     *
     * <p>We can get the index mapping [0, -1, 1], in which 0 is the index of table field 1->c in
     * data fields, -1 is the index of 6->b in data fields and 1 is the index of 3->a in data
     * fields.
     *
     * @param tableFields the fields of table
     * @param dataFields the fields of underlying data
     * @return the index mapping
     */
    @Nullable
    public static int[]     createIndexMapping(
            List<DataField> tableFields, List<DataField> dataFields) {
        int[] indexMapping = new int[tableFields.size()];
        Map<Integer, Integer> fieldIdToIndex = new HashMap<>();
        for (int i = 0; i < dataFields.size(); i++) {
            fieldIdToIndex.put(dataFields.get(i).id(), i);
        }

        for (int i = 0; i < tableFields.size(); i++) {
            int fieldId = tableFields.get(i).id();
            Integer dataFieldIndex = fieldIdToIndex.get(fieldId);
            if (dataFieldIndex != null) {
                indexMapping[i] = dataFieldIndex;
            } else {
                indexMapping[i] = NULL_FIELD_INDEX;
            }
        }

        for (int i = 0; i < indexMapping.length; i++) {
            if (indexMapping[i] != i) {
                return indexMapping;
            }
        }
        return null;
    }

    /** Create index mapping from table fields to underlying data fields. */
    public static IndexCastMapping createIndexCastMapping(
            List<DataField> tableFields, List<DataField> dataFields) {
        int[] indexMapping = createIndexMapping(tableFields, dataFields);
        CastFieldGetter[] castMapping =
                createCastFieldGetterMapping(tableFields, dataFields, indexMapping);
        return new IndexCastMapping() {
            @Nullable
            @Override
            public int[] getIndexMapping() {
                return indexMapping;
            }

            @Nullable
            @Override
            public CastFieldGetter[] getCastMapping() {
                return castMapping;
            }
        };
    }

    /**
     * When pushing down filters after schema evolution, we should devolve the literals from new
     * types (in dataFields) to original types (in tableFields). We will visit all predicate in
     * filters, reset its field index, name and type, and ignore predicate if the field is not
     * exist.
     *
     * @param tableFields the table fields
     * @param dataFields the underlying data fields
     * @param filters the filters
     * @param keepNewFieldFilter true if keep new field filter, the new field filter needs to be
     *     properly handled
     * @return the data filters
     */
    public static List<Predicate> devolveFilters(
            List<DataField> tableFields,
            List<DataField> dataFields,
            List<Predicate> filters,
            boolean keepNewFieldFilter) {
        if (filters == null) {
            return Collections.emptyList();
        }

        Map<String, DataField> nameToTableFields =
                tableFields.stream().collect(Collectors.toMap(DataField::name, f -> f));
        LinkedHashMap<Integer, DataField> idToDataFields = new LinkedHashMap<>();
        dataFields.forEach(f -> idToDataFields.put(f.id(), f));
        List<Predicate> dataFilters = new ArrayList<>(filters.size());

        PredicateReplaceVisitor visitor =
                predicate -> {
                    Optional<FieldRef> fieldRefOptional = predicate.fieldRefOptional();
                    if (!fieldRefOptional.isPresent()) {
                        return Optional.empty();
                    }
                    FieldRef fieldRef = fieldRefOptional.get();
                    DataField tableField =
                            checkNotNull(
                                    nameToTableFields.get(fieldRef.name()),
                                    String.format("Find no field %s", fieldRef.name()));
                    DataField dataField = idToDataFields.get(tableField.id());
                    if (dataField == null) {
                        return keepNewFieldFilter ? Optional.of(predicate) : Optional.empty();
                    }

                    return CastExecutors.castLiteralsWithEvolution(
                                    predicate.literals(), fieldRef.type(), dataField.type())
                            .map(
                                    literals ->
                                            new LeafPredicate(
                                                    predicate.function(),
                                                    dataField.type(),
                                                    indexOf(dataField, idToDataFields),
                                                    dataField.name(),
                                                    literals));
                };

        for (Predicate predicate : filters) {
            predicate.visit(visitor).ifPresent(dataFilters::add);
        }
        return dataFilters;
    }

    private static int indexOf(DataField dataField, LinkedHashMap<Integer, DataField> dataFields) {
        int index = 0;
        for (Map.Entry<Integer, DataField> entry : dataFields.entrySet()) {
            if (dataField.id() == entry.getKey()) {
                return index;
            }
            index++;
        }

        throw new IllegalArgumentException(
                String.format("Can't find data field %s", dataField.name()));
    }

    /**
     * Create getter and casting mapping from table fields to underlying data fields with given
     * index mapping. For example, the table and data fields are as follows
     *
     * <ul>
     *   <li>table fields: 1->c INT, 6->b STRING, 3->a BIGINT
     *   <li>data fields: 1->a BIGINT, 3->c DOUBLE
     * </ul>
     *
     * <p>We can get the column types (1->a BIGINT), (3->c DOUBLE) from data fields for (1->c INT)
     * and (3->a BIGINT) in table fields through index mapping [0, -1, 1], then compare the data
     * type and create getter and casting mapping.
     *
     * @param tableFields the fields of table
     * @param dataFields the fields of underlying data
     * @param indexMapping the index mapping from table fields to data fields
     * @return the getter and casting mapping
     */
    private static CastFieldGetter[] createCastFieldGetterMapping(
            List<DataField> tableFields, List<DataField> dataFields, int[] indexMapping) {
        CastFieldGetter[] converterMapping = new CastFieldGetter[tableFields.size()];
        boolean castExist = false;

        for (int i = 0; i < tableFields.size(); i++) {
            int dataIndex = indexMapping == null ? i : indexMapping[i];
            if (dataIndex < 0) {
                converterMapping[i] =
                        new CastFieldGetter(row -> null, CastExecutors.identityCastExecutor());
            } else {
                DataField tableField = tableFields.get(i);
                DataField dataField = dataFields.get(dataIndex);
                if (!dataField.type().equalsIgnoreNullable(tableField.type())) {
                    castExist = true;
                }

                // Create getter with index i and projected row data will convert to underlying data
                converterMapping[i] =
                        new CastFieldGetter(
                                InternalRowUtils.createNullCheckingFieldGetter(dataField.type(), i),
                                createCastExecutor(dataField.type(), tableField.type()));
            }
        }

        return castExist ? converterMapping : null;
    }

    private static CastExecutor<?, ?> createCastExecutor(DataType inputType, DataType targetType) {
        if (targetType.equalsIgnoreNullable(inputType)) {
            return CastExecutors.identityCastExecutor();
        } else if (inputType instanceof RowType && targetType instanceof RowType) {
            return createRowCastExecutor((RowType) inputType, (RowType) targetType);
        } else if (inputType instanceof ArrayType && targetType instanceof ArrayType) {
            return createArrayCastExecutor((ArrayType) inputType, (ArrayType) targetType);
        } else if (inputType instanceof MapType && targetType instanceof MapType) {
            return createMapCastExecutor((MapType) inputType, (MapType) targetType);
        } else {
            return checkNotNull(
                    CastExecutors.resolve(inputType, targetType),
                    "Cannot cast from type %s to type %s",
                    inputType,
                    targetType);
        }
    }

    private static CastExecutor<InternalRow, InternalRow> createRowCastExecutor(
            RowType inputType, RowType targetType) {
        int[] indexMapping = createIndexMapping(targetType.getFields(), inputType.getFields());
        CastFieldGetter[] castFieldGetters =
                createCastFieldGetterMapping(
                        targetType.getFields(), inputType.getFields(), indexMapping);

        ProjectedRow projectedRow = indexMapping == null ? null : ProjectedRow.from(indexMapping);
        CastedRow castedRow = castFieldGetters == null ? null : CastedRow.from(castFieldGetters);
        return value -> {
            if (projectedRow != null) {
                value = projectedRow.replaceRow(value);
            }
            if (castedRow != null) {
                value = castedRow.replaceRow(value);
            }
            return value;
        };
    }

    private static CastExecutor<InternalArray, InternalArray> createArrayCastExecutor(
            ArrayType inputType, ArrayType targetType) {
        CastElementGetter castElementGetter =
                new CastElementGetter(
                        InternalArray.createElementGetter(inputType.getElementType()),
                        createCastExecutor(
                                inputType.getElementType(), targetType.getElementType()));

        CastedArray castedArray = CastedArray.from(castElementGetter);
        return castedArray::replaceArray;
    }

    private static CastExecutor<InternalMap, InternalMap> createMapCastExecutor(
            MapType inputType, MapType targetType) {
        checkState(
                inputType.getKeyType().equals(targetType.getKeyType()),
                "Cannot cast map type %s to map type %s, because they have different key types.",
                inputType.getKeyType(),
                targetType.getKeyType());
        CastElementGetter castElementGetter =
                new CastElementGetter(
                        InternalArray.createElementGetter(inputType.getValueType()),
                        createCastExecutor(inputType.getValueType(), targetType.getValueType()));

        CastedMap castedMap = CastedMap.from(castElementGetter);
        return castedMap::replaceMap;
    }
}
