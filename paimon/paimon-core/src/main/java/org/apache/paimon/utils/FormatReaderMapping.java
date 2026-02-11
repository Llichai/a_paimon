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

package org.apache.paimon.utils;

import org.apache.paimon.casting.CastFieldGetter;
import org.apache.paimon.data.variant.VariantMetadataUtils;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.partition.PartitionUtils;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.SortValue;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.schema.IndexCastMapping;
import org.apache.paimon.schema.SchemaEvolutionUtil;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.apache.paimon.predicate.PredicateBuilder.excludePredicateWithFields;
import static org.apache.paimon.table.SpecialFields.KEY_FIELD_ID_START;

/**
 * 格式读取器映射
 *
 * <p>FormatReaderMapping 封装了索引映射和格式读取器，用于处理表模式演化（Schema Evolution）和字段读取优化。
 *
 * <p>核心功能：
 * <ul>
 *   <li>模式演化：{@link #indexMapping} - 数据模式字段到表模式字段的索引映射
 *   <li>类型转换：{@link #castMapping} - 协助 indexMapping 进行不同数据类型的转换
 *   <li>分区字段：{@link #partitionPair} - 分区字段映射，将分区字段添加到读取字段
 *   <li>键字段裁剪：避免同时读取 _KEY_a 和 a 字段
 *   <li>字段投影：仅读取需要的字段，减少 I/O
 * </ul>
 *
 * <p>三步构建过程（详见 {@link Builder#build}）：
 * <ol>
 *   <li><b>字段映射</b>：计算 readDataFields，生成 indexCastMapping
 *   <li><b>键字段裁剪</b>：避免同时读取 _KEY_a 和 a（示例：[_KEY_a, _KEY_b, a, b, c] -> [a, b, c]，映射为 [0,1,0,1,2]）
 *   <li><b>分区字段裁剪</b>：移除分区字段，减少实际读取字段数量
 * </ol>
 *
 * <p>使用场景：
 * <ul>
 *   <li>读取优化：仅读取需要的字段，减少 I/O
 *   <li>模式演化：兼容不同版本的数据模式
 *   <li>类型转换：自动处理数据类型变化
 *   <li>分区过滤：跳过分区字段的读取
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建 FormatReaderMapping.Builder
 * FormatReaderMapping.Builder builder = new FormatReaderMapping.Builder(
 *     formatDiscover,
 *     readFields,      // 需要读取的字段
 *     fieldsExtractor, // 字段提取器
 *     filters,         // 过滤条件
 *     topN,            // TopN 优化
 *     limit            // Limit 优化
 * );
 *
 * // 构建 FormatReaderMapping
 * FormatReaderMapping mapping = builder.build(
 *     "parquet",       // 文件格式
 *     tableSchema,     // 表模式
 *     dataSchema       // 数据模式
 * );
 *
 * // 使用 FormatReaderMapping
 * int[] indexMapping = mapping.getIndexMapping();      // 获取索引映射
 * CastFieldGetter[] castMapping = mapping.getCastMapping(); // 获取类型转换映射
 * FormatReaderFactory readerFactory = mapping.getReaderFactory(); // 获取读取器工厂
 * }</pre>
 *
 * @see Builder
 * @see org.apache.paimon.schema.SchemaEvolutionUtil
 */
public class FormatReaderMapping {

    /**
     * 索引映射（数据模式字段到表模式字段）
     *
     * <p>用于实现 Paimon 的模式演化。它结合了 trimmedKeyMapping，
     * 将键字段映射到值字段。
     */
    @Nullable private final int[] indexMapping;

    /** 类型转换映射（协助 indexMapping 进行不同数据类型的转换） */
    @Nullable private final CastFieldGetter[] castMapping;

    /** 分区字段映射（将分区字段添加到读取字段） */
    @Nullable private final Pair<int[], RowType> partitionPair;

    /** 格式读取器工厂 */
    private final FormatReaderFactory readerFactory;

    /** 数据模式 */
    private final TableSchema dataSchema;

    /** 数据过滤器 */
    private final List<Predicate> dataFilters;

    /** 系统字段映射（字段名 -> 字段索引） */
    private final Map<String, Integer> systemFields;

    /** TopN 优化 */
    @Nullable private final TopN topN;

    /** Limit 优化 */
    @Nullable private final Integer limit;

    /**
     * 构造 FormatReaderMapping
     *
     * @param indexMapping 索引映射
     * @param castMapping 类型转换映射
     * @param trimmedKeyMapping 裁剪后的键字段映射
     * @param partitionPair 分区字段映射
     * @param readerFactory 格式读取器工厂
     * @param dataSchema 数据模式
     * @param dataFilters 数据过滤器
     * @param systemFields 系统字段映射
     * @param topN TopN 优化
     * @param limit Limit 优化
     */
    public FormatReaderMapping(
            @Nullable int[] indexMapping,
            @Nullable CastFieldGetter[] castMapping,
            @Nullable int[] trimmedKeyMapping,
            @Nullable Pair<int[], RowType> partitionPair,
            FormatReaderFactory readerFactory,
            TableSchema dataSchema,
            List<Predicate> dataFilters,
            Map<String, Integer> systemFields,
            @Nullable TopN topN,
            @Nullable Integer limit) {
        this.indexMapping = combine(indexMapping, trimmedKeyMapping);
        this.castMapping = castMapping;
        this.readerFactory = readerFactory;
        this.partitionPair = partitionPair;
        this.dataSchema = dataSchema;
        this.dataFilters = dataFilters;
        this.systemFields = systemFields;
        this.topN = topN;
        this.limit = limit;
    }

    /**
     * 组合索引映射和裁剪后的键字段映射
     *
     * @param indexMapping 索引映射
     * @param trimmedKeyMapping 裁剪后的键字段映射
     * @return 组合后的映射
     */
    private int[] combine(@Nullable int[] indexMapping, @Nullable int[] trimmedKeyMapping) {
        if (indexMapping == null) {
            return trimmedKeyMapping;
        }
        if (trimmedKeyMapping == null) {
            return indexMapping;
        }

        int[] combined = new int[indexMapping.length];

        for (int i = 0; i < indexMapping.length; i++) {
            if (indexMapping[i] < 0) {
                combined[i] = indexMapping[i];
            } else {
                combined[i] = trimmedKeyMapping[indexMapping[i]];
            }
        }
        return combined;
    }

    @Nullable
    public int[] getIndexMapping() {
        return indexMapping;
    }

    @Nullable
    public CastFieldGetter[] getCastMapping() {
        return castMapping;
    }

    @Nullable
    public Pair<int[], RowType> getPartitionPair() {
        return partitionPair;
    }

    public Map<String, Integer> getSystemFields() {
        return systemFields;
    }

    public FormatReaderFactory getReaderFactory() {
        return readerFactory;
    }

    public TableSchema getDataSchema() {
        return dataSchema;
    }

    public List<Predicate> getDataFilters() {
        return dataFilters;
    }

    @Nullable
    public TopN getTopN() {
        return topN;
    }

    @Nullable
    public Integer getLimit() {
        return limit;
    }

    /** Builder for {@link FormatReaderMapping}. */
    public static class Builder {

        /** 文件格式发现器 */
        private final FileFormatDiscover formatDiscover;
        /** 读取字段列表 */
        private final List<DataField> readFields;
        /** 字段提取器（从表模式中提取字段） */
        private final Function<TableSchema, List<DataField>> fieldsExtractor;
        /** 过滤条件列表 */
        @Nullable private final List<Predicate> filters;
        /** TopN 优化 */
        @Nullable private final TopN topN;
        /** Limit 优化 */
        @Nullable private final Integer limit;

        /**
         * 构造 Builder
         *
         * @param formatDiscover 文件格式发现器
         * @param readFields 读取字段列表
         * @param fieldsExtractor 字段提取器
         * @param filters 过滤条件列表
         * @param topN TopN 优化
         * @param limit Limit 优化
         */
        public Builder(
                FileFormatDiscover formatDiscover,
                List<DataField> readFields,
                Function<TableSchema, List<DataField>> fieldsExtractor,
                @Nullable List<Predicate> filters,
                @Nullable TopN topN,
                @Nullable Integer limit) {
            this.formatDiscover = formatDiscover;
            this.readFields = readFields;
            this.fieldsExtractor = fieldsExtractor;
            this.filters = filters;
            this.topN = topN;
            this.limit = limit;
        }

        /**
         * 构建 FormatReaderMapping（三步构建过程）
         *
         * <p>步骤 1：计算 readDataFields，生成 indexCastMapping
         * <ul>
         *   <li>readDataFields：从数据模式中读取的字段
         *   <li>indexCastMapping：将 readDataFields 的索引映射到数据模式的索引
         * </ul>
         *
         * <p>步骤 2：计算裁剪 _KEY_ 字段的映射
         * <ul>
         *   <li>问题：我们需要 _KEY_a, _KEY_b, _FIELD_SEQUENCE, _ROW_KIND, a, b, c, d, e, f, g
         *   <li>优化：不需要同时读取 _KEY_a 和 a，_KEY_b 和 b
         *   <li>裁剪前：_KEY_a, _KEY_b, _FIELD_SEQUENCE, _ROW_KIND, a, b, c, d, e, f, g
         *   <li>裁剪后：a, b, _FIELD_SEQUENCE, _ROW_KIND, c, d, e, f, g
         *   <li>映射：[0,1,2,3,0,1,4,5,6,7,8]（将裁剪后的列转换为裁剪前的列）
         * </ul>
         *
         * <p>步骤 3：移除分区字段
         * <ul>
         *   <li>我们希望读取的字段数远少于 readDataFields
         *   <li>移除分区字段，生成 partitionMappingAndFieldsWithoutPartitionPair
         *   <li>减少实际读取字段数量，并告诉我们如何将其映射回来
         * </ul>
         *
         * @param formatIdentifier 格式标识符（如 "parquet", "orc"）
         * @param tableSchema 表模式
         * @param dataSchema 数据模式
         * @param expectedFields 预期字段
         * @param enabledFilterPushDown 是否启用过滤器下推
         * @return FormatReaderMapping 实例
         */
        public FormatReaderMapping build(
                String formatIdentifier,
                TableSchema tableSchema,
                TableSchema dataSchema,
                List<DataField> expectedFields,
                boolean enabledFilterPushDown) {

            // extract the whole data fields in logic.
            List<DataField> allDataFieldsInFile =
                    new ArrayList<>(fieldsExtractor.apply(dataSchema));
            Map<String, Integer> systemFields = findSystemFields(expectedFields);

            List<DataField> readDataFields = readDataFields(allDataFieldsInFile, expectedFields);
            IndexCastMapping indexCastMapping =
                    SchemaEvolutionUtil.createIndexCastMapping(expectedFields, readDataFields);

            // map from key fields reading to value fields reading
            Pair<int[], RowType> trimmedKeyPair =
                    trimKeyFields(readDataFields, allDataFieldsInFile);
            // build partition mapping and filter partition fields
            Pair<Pair<int[], RowType>, List<DataField>> trimmedResult =
                    PartitionUtils.trimPartitionFields(
                            dataSchema, trimmedKeyPair.getRight().getFields());
            Pair<int[], RowType> partitionMapping = trimmedResult.getLeft();

            RowType actualReadRowType = new RowType(trimmedResult.getRight());

            // build read filters
            List<Predicate> readFilters =
                    enabledFilterPushDown ? readFilters(filters, tableSchema, dataSchema) : null;

            return new FormatReaderMapping(
                    indexCastMapping.getIndexMapping(),
                    indexCastMapping.getCastMapping(),
                    trimmedKeyPair.getLeft(),
                    partitionMapping,
                    formatDiscover
                            .discover(formatIdentifier)
                            .createReaderFactory(
                                    new RowType(allDataFieldsInFile),
                                    actualReadRowType,
                                    readFilters),
                    dataSchema,
                    readFilters,
                    systemFields,
                    evolutionTopN(tableSchema, dataSchema),
                    limit);
        }

        /**
         * 演化 TopN（检查字段是否发生变化）
         *
         * <p>如果 TopN 中的字段在表模式和数据模式之间发生了变化，则不推送 TopN。
         *
         * @param tableSchema 表模式
         * @param dataSchema 数据模式
         * @return 演化后的 TopN，如果字段发生变化则返回 null
         */
        @Nullable
        private TopN evolutionTopN(TableSchema tableSchema, TableSchema dataSchema) {
            TopN pushTopN = topN;
            if (pushTopN != null) {
                Map<String, DataField> tableFields = tableSchema.nameToFieldMap();
                Map<Integer, DataField> dataFields = dataSchema.idToFieldMap();
                for (SortValue value : pushTopN.orders()) {
                    DataField tableField = tableFields.get(value.field().name());
                    DataField dataField = dataFields.get(tableField.id());
                    if (!Objects.equals(tableField, dataField)) {
                        pushTopN = null;
                        break;
                    }
                }
            }
            return pushTopN;
        }

        public FormatReaderMapping build(
                String formatIdentifier, TableSchema tableSchema, TableSchema dataSchema) {
            return build(formatIdentifier, tableSchema, dataSchema, readFields, true);
        }

        /**
         * 查找系统字段（如 _KEY_、_FIELD_SEQUENCE 等）
         *
         * @param readTableFields 读取的表字段
         * @return 系统字段映射（字段名 -> 字段索引）
         */
        private Map<String, Integer> findSystemFields(List<DataField> readTableFields) {
            Map<String, Integer> systemFields = new HashMap<>();
            for (int i = 0; i < readTableFields.size(); i++) {
                DataField field = readTableFields.get(i);
                if (SpecialFields.isSystemField(field.name())) {
                    systemFields.put(field.name(), i);
                }
            }
            return systemFields;
        }

        /**
         * 裁剪键字段（避免同时读取 _KEY_a 和 a）
         *
         * <p>示例：
         * <pre>
         * 输入字段：_KEY_a, _KEY_b, _FIELD_SEQUENCE, _ROW_KIND, a, b, c
         * 输出字段：a, b, _FIELD_SEQUENCE, _ROW_KIND, c
         * 映射：[0, 1, 2, 3, 0, 1, 4]
         * </pre>
         *
         * @param fieldsWithoutPartition 不包含分区的字段
         * @param fields 全部字段
         * @return 映射和裁剪后的字段类型
         */
        static Pair<int[], RowType> trimKeyFields(
                List<DataField> fieldsWithoutPartition, List<DataField> fields) {
            int[] map = new int[fieldsWithoutPartition.size()];
            List<DataField> trimmedFields = new ArrayList<>();
            Map<Integer, DataField> fieldMap = new HashMap<>();
            Map<Integer, Integer> positionMap = new HashMap<>();

            for (DataField field : fields) {
                fieldMap.put(field.id(), field);
            }

            for (int i = 0; i < fieldsWithoutPartition.size(); i++) {
                DataField field = fieldsWithoutPartition.get(i);
                boolean keyField = SpecialFields.isKeyField(field.name());
                int id = keyField ? field.id() - KEY_FIELD_ID_START : field.id();
                // field in data schema
                DataField f = fieldMap.get(id);

                if (f != null) {
                    if (positionMap.containsKey(id)) {
                        map[i] = positionMap.get(id);
                    } else {
                        map[i] = positionMap.computeIfAbsent(id, k -> trimmedFields.size());
                        // If the target field is not key field, we remain what it is, because it
                        // may be projected. Example: the target field is a row type, but only read
                        // the few fields in it. If we simply trimmedFields.add(f), we will read
                        // more fields than we need.
                        trimmedFields.add(keyField ? f : field);
                    }
                } else {
                    throw new RuntimeException("Can't find field with id: " + id + " in fields.");
                }
            }

            return Pair.of(map, new RowType(trimmedFields));
        }

        /**
         * 计算实际需要读取的数据字段
         *
         * @param allDataFields 全部数据字段
         * @param expectedFields 预期字段
         * @return 实际读取的数据字段
         */
        private List<DataField> readDataFields(
                List<DataField> allDataFields, List<DataField> expectedFields) {
            List<DataField> readDataFields = new ArrayList<>();
            for (DataField dataField : allDataFields) {
                expectedFields.stream()
                        .filter(f -> f.id() == dataField.id())
                        .findFirst()
                        .ifPresent(
                                field -> {
                                    DataType prunedType =
                                            pruneDataType(field.type(), dataField.type());
                                    if (prunedType != null) {
                                        readDataFields.add(dataField.newType(prunedType));
                                    }
                                });
            }
            return readDataFields;
        }

        /**
         * 裁剪数据类型（仅读取需要的嵌套字段）
         *
         * <p>对于复杂类型（ROW、MAP、ARRAY），仅读取需要的子字段。
         *
         * @param readType 读取类型
         * @param dataType 数据类型
         * @return 裁剪后的数据类型，如果全部字段被裁剪则返回 null
         */
        @Nullable
        private DataType pruneDataType(DataType readType, DataType dataType) {
            switch (readType.getTypeRoot()) {
                case ROW:
                    RowType r = (RowType) readType;
                    if (VariantMetadataUtils.isVariantRowType(r)) {
                        return readType;
                    }
                    RowType d = (RowType) dataType;
                    ArrayList<DataField> newFields = new ArrayList<>();
                    for (DataField rf : r.getFields()) {
                        if (d.containsField(rf.id())) {
                            DataField df = d.getField(rf.id());
                            DataType newType = pruneDataType(rf.type(), df.type());
                            if (newType == null) {
                                continue;
                            }
                            newFields.add(df.newType(newType));
                        }
                    }
                    if (newFields.isEmpty()) {
                        // When all fields are pruned, we should not return an empty row type
                        return null;
                    }
                    return d.copy(newFields);
                case MAP:
                    DataType keyType =
                            pruneDataType(
                                    ((MapType) readType).getKeyType(),
                                    ((MapType) dataType).getKeyType());
                    DataType valueType =
                            pruneDataType(
                                    ((MapType) readType).getValueType(),
                                    ((MapType) dataType).getValueType());
                    if (keyType == null || valueType == null) {
                        return null;
                    }
                    return ((MapType) dataType).newKeyValueType(keyType, valueType);
                case ARRAY:
                    DataType elementType =
                            pruneDataType(
                                    ((ArrayType) readType).getElementType(),
                                    ((ArrayType) dataType).getElementType());
                    if (elementType == null) {
                        return null;
                    }
                    return ((ArrayType) dataType).newElementType(elementType);
                default:
                    return dataType;
            }
        }

        /**
         * 计算读取过滤器（演化过滤器并排除分区字段）
         *
         * @param filters 原始过滤器
         * @param tableSchema 表模式
         * @param fileSchema 文件模式
         * @return 读取过滤器（不包含分区过滤器）
         */
        private List<Predicate> readFilters(
                List<Predicate> filters, TableSchema tableSchema, TableSchema fileSchema) {
            List<Predicate> dataFilters =
                    tableSchema.id() == fileSchema.id()
                            ? filters
                            : SchemaEvolutionUtil.devolveFilters(
                                    tableSchema.fields(), fileSchema.fields(), filters, false);

            // Skip pushing down partition filters to reader.
            return excludePredicateWithFields(
                    dataFilters, new HashSet<>(fileSchema.partitionKeys()));
        }
    }
}
