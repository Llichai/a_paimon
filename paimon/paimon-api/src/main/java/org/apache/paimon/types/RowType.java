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

package org.apache.paimon.types;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 表示字段序列的数据类型。
 *
 * <p>RowType(行类型)是 Paimon 最重要的复杂类型之一,它表示一个结构化的数据类型,
 * 由多个命名字段组成。行类型是表的 Schema 的核心,每个表的行对应一个 RowType,
 * 其中每列对应 RowType 的一个字段,字段的序号与列的位置相对应。
 *
 * <p>设计特点:
 * <ul>
 *     <li><b>字段组合</b>: 由一个或多个 {@link DataField} 组成</li>
 *     <li><b>字段唯一性</b>: 同一RowType中的字段名称必须唯一</li>
 *     <li><b>字段ID追踪</b>: 每个字段都有唯一的ID,用于 Schema 演化</li>
 *     <li><b>嵌套支持</b>: 字段可以是任意类型,包括嵌套的 RowType</li>
 *     <li><b>高效访问</b>: 提供多种索引方式(按名称、按ID、按位置)</li>
 * </ul>
 *
 * <p>核心功能:
 * <ul>
 *     <li><b>字段查询</b>: 支持通过名称、ID、索引查询字段</li>
 *     <li><b>类型投影</b>: 支持列裁剪,选择部分字段构造新的 RowType</li>
 *     <li><b>Schema 演化</b>: 通过字段 ID 跟踪字段变更</li>
 *     <li><b>类型比较</b>: 支持多种比较方式(完全相等、忽略ID、忽略可空性等)</li>
 * </ul>
 *
 * <p>与 SQL 标准的关系:
 * <p>在 SQL 标准中,RowType 是表行的最具体类型。相比 SQL 标准,Paimon 的 RowType
 * 增加了可选的字段描述(description),简化了复杂结构的处理。
 *
 * <p>使用示例:
 * <pre>{@code
 * // 方式1: 使用 DataTypes 工厂方法
 * RowType rowType = DataTypes.ROW(
 *     DataTypes.FIELD(0, "id", DataTypes.INT()),
 *     DataTypes.FIELD(1, "name", DataTypes.STRING()),
 *     DataTypes.FIELD(2, "age", DataTypes.INT())
 * );
 *
 * // 方式2: 使用 Builder
 * RowType rowType2 = RowType.builder()
 *     .field("id", DataTypes.INT())
 *     .field("name", DataTypes.STRING(), "用户名称")
 *     .field("age", DataTypes.INT())
 *     .build();
 *
 * // 字段访问
 * DataField nameField = rowType.getField("name");
 * int nameIndex = rowType.getFieldIndex("name");
 * DataType nameType = rowType.getTypeAt(1);
 *
 * // 列裁剪(投影)
 * RowType projected = rowType.project("id", "name");  // 只保留id和name字段
 * }</pre>
 *
 * <p>字段索引机制:
 * <p>为了提高字段查询性能,RowType 内部维护了多个索引:
 * <ul>
 *     <li>nameToField: 字段名称 -> 字段对象</li>
 *     <li>nameToIndex: 字段名称 -> 字段索引</li>
 *     <li>fieldIdToField: 字段ID -> 字段对象</li>
 *     <li>fieldIdToIndex: 字段ID -> 字段索引</li>
 * </ul>
 * 这些索引是延迟初始化的,只有在第一次使用时才会创建。
 *
 * @see DataField 数据字段
 * @see DataTypes 数据类型工厂
 * @since 0.4.0
 */
@Public
public final class RowType extends DataType {

    private static final long serialVersionUID = 1L;

    /** JSON 序列化时的字段名称常量 */
    private static final String FIELD_FIELDS = "fields";

    /** RowType 的 SQL 字符串格式模板 */
    public static final String FORMAT = "ROW<%s>";

    /** 该 RowType 包含的所有字段,不可变列表 */
    private final List<DataField> fields;

    // 以下字段是延迟初始化的索引,用于高效的字段查询
    /** 字段名称到字段对象的映射(延迟初始化) */
    private transient volatile Map<String, DataField> laziedNameToField;

    /** 字段名称到字段索引的映射(延迟初始化) */
    private transient volatile Map<String, Integer> laziedNameToIndex;

    /** 字段 ID 到字段对象的映射(延迟初始化) */
    private transient volatile Map<Integer, DataField> laziedFieldIdToField;

    /** 字段 ID 到字段索引的映射(延迟初始化) */
    private transient volatile Map<Integer, Integer> laziedFieldIdToIndex;

    /**
     * 创建一个行类型。
     *
     * @param isNullable 该行类型是否可为 null
     * @param fields 字段列表
     */
    public RowType(boolean isNullable, List<DataField> fields) {
        super(isNullable, DataTypeRoot.ROW);
        this.fields =
                Collections.unmodifiableList(
                        new ArrayList<>(
                                Preconditions.checkNotNull(fields, "Fields must not be null.")));

        validateFields(fields);
    }

    /**
     * 创建一个可空的行类型。
     *
     * <p>此构造函数用于 JSON 反序列化。
     *
     * @param fields 字段列表
     */
    @JsonCreator
    public RowType(@JsonProperty(FIELD_FIELDS) List<DataField> fields) {
        this(true, fields);
    }

    /**
     * 创建一个具有新字段列表的行类型副本。
     *
     * @param newFields 新的字段列表
     * @return 新的行类型实例
     */
    public RowType copy(List<DataField> newFields) {
        return new RowType(isNullable(), newFields);
    }

    /**
     * 获取该行类型的所有字段。
     *
     * @return 不可变的字段列表
     */
    public List<DataField> getFields() {
        return fields;
    }

    /**
     * 获取所有字段的名称列表。
     *
     * @return 字段名称列表,顺序与字段顺序一致
     */
    public List<String> getFieldNames() {
        return fields.stream().map(DataField::name).collect(Collectors.toList());
    }

    /**
     * 获取所有字段的类型列表。
     *
     * @return 字段类型列表,顺序与字段顺序一致
     */
    public List<DataType> getFieldTypes() {
        return fields.stream().map(DataField::type).collect(Collectors.toList());
    }

    /**
     * 获取指定位置的字段类型。
     *
     * @param i 字段索引,从 0 开始
     * @return 该位置的字段类型
     */
    public DataType getTypeAt(int i) {
        return fields.get(i).type();
    }

    /**
     * 获取该行类型的字段数量。
     *
     * @return 字段数量
     */
    public int getFieldCount() {
        return fields.size();
    }

    /**
     * 根据字段名称获取字段索引。
     *
     * @param fieldName 字段名称
     * @return 字段索引,如果字段不存在则返回 -1
     */
    public int getFieldIndex(String fieldName) {
        return nameToIndex().getOrDefault(fieldName, -1);
    }

    /**
     * 获取多个字段的索引数组。
     *
     * @param projectFields 字段名称列表
     * @return 字段索引数组,顺序与输入列表一致
     */
    public int[] getFieldIndices(List<String> projectFields) {
        int[] projection = new int[projectFields.size()];
        for (int i = 0; i < projection.length; i++) {
            projection[i] = getFieldIndex(projectFields.get(i));
        }
        return projection;
    }

    /**
     * 判断该行类型是否包含指定名称的字段。
     *
     * @param fieldName 字段名称
     * @return 如果包含该字段则返回 true,否则返回 false
     */
    public boolean containsField(String fieldName) {
        return nameToField().containsKey(fieldName);
    }

    /**
     * 判断该行类型是否包含指定 ID 的字段。
     *
     * @param fieldId 字段 ID
     * @return 如果包含该字段则返回 true,否则返回 false
     */
    public boolean containsField(int fieldId) {
        return fieldIdToField().containsKey(fieldId);
    }

    /**
     * 判断该行类型是否不包含指定名称的字段。
     *
     * @param fieldName 字段名称
     * @return 如果不包含该字段则返回 true,否则返回 false
     */
    public boolean notContainsField(String fieldName) {
        return !containsField(fieldName);
    }

    /**
     * 根据字段名称获取字段对象。
     *
     * @param fieldName 字段名称
     * @return 字段对象
     * @throws RuntimeException 如果字段不存在
     */
    public DataField getField(String fieldName) {
        DataField field = nameToField().get(fieldName);
        if (field == null) {
            throw new RuntimeException("Cannot find field: " + fieldName);
        }
        return field;
    }

    /**
     * 根据字段 ID 获取字段对象。
     *
     * @param fieldId 字段 ID
     * @return 字段对象
     * @throws RuntimeException 如果字段不存在
     */
    public DataField getField(int fieldId) {
        DataField field = fieldIdToField().get(fieldId);
        if (field == null) {
            throw new RuntimeException("Cannot find field by field id: " + fieldId);
        }
        return field;
    }

    /**
     * 根据字段 ID 获取字段索引。
     *
     * @param fieldId 字段 ID
     * @return 字段索引(从 0 开始)
     * @throws RuntimeException 如果字段不存在
     */
    public int getFieldIndexByFieldId(int fieldId) {
        Integer index = fieldIdToIndex().get(fieldId);
        if (index == null) {
            throw new RuntimeException("Cannot find field index by FieldId " + fieldId);
        }
        return index;
    }

    @Override
    public int defaultSize() {
        return fields.stream().mapToInt(f -> f.type().defaultSize()).sum();
    }

    @Override
    public RowType copy(boolean isNullable) {
        return new RowType(
                isNullable, fields.stream().map(DataField::copy).collect(Collectors.toList()));
    }

    @Override
    public RowType notNull() {
        return copy(false);
    }

    @Override
    public String asSQLString() {
        return withNullability(
                FORMAT,
                fields.stream().map(DataField::asSQLString).collect(Collectors.joining(", ")));
    }

    @Override
    public void serializeJson(JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeStringField("type", isNullable() ? "ROW" : "ROW NOT NULL");
        generator.writeArrayFieldStart("fields");
        for (DataField field : getFields()) {
            field.serializeJson(generator);
        }
        generator.writeEndArray();
        generator.writeEndObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        RowType rowType = (RowType) o;
        return fields.equals(rowType.fields);
    }

    @Override
    public boolean equalsIgnoreFieldId(DataType o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        RowType other = (RowType) o;
        if (fields.size() != other.fields.size()) {
            return false;
        }
        for (int i = 0; i < fields.size(); i++) {
            if (!fields.get(i).equalsIgnoreFieldId(other.fields.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isPrunedFrom(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        RowType rowType = (RowType) o;
        for (DataField field : fields) {
            if (!field.isPrunedFrom(rowType.getField(field.id()))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields);
    }

    private static void validateFields(List<DataField> fields) {
        final List<String> fieldNames =
                fields.stream().map(DataField::name).collect(Collectors.toList());
        if (fieldNames.stream().anyMatch(StringUtils::isNullOrWhitespaceOnly)) {
            throw new IllegalArgumentException(
                    "Field names must contain at least one non-whitespace character.");
        }
        final Set<String> duplicates = Schema.duplicateFields(fieldNames);

        if (!duplicates.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("Field names must be unique. Found duplicates: %s", duplicates));
        }
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public void collectFieldIds(Set<Integer> fieldIds) {
        for (DataField field : fields) {
            if (fieldIds.contains(field.id())) {
                throw new RuntimeException(
                        String.format("Broken schema, field id %s is duplicated.", field.id()));
            }
            fieldIds.add(field.id());
            field.type().collectFieldIds(fieldIds);
        }
    }

    public RowType project(int[] mapping) {
        List<DataField> fields = getFields();
        return new RowType(
                        Arrays.stream(mapping).mapToObj(fields::get).collect(Collectors.toList()))
                .copy(isNullable());
    }

    public RowType project(List<String> names) {
        List<DataField> fields = getFields();
        List<String> fieldNames = fields.stream().map(DataField::name).collect(Collectors.toList());
        return new RowType(
                        names.stream()
                                .map(k -> fields.get(fieldNames.indexOf(k)))
                                .collect(Collectors.toList()))
                .copy(isNullable());
    }

    public int[] projectIndexes(List<String> names) {
        List<String> fieldNames = fields.stream().map(DataField::name).collect(Collectors.toList());
        return names.stream().mapToInt(fieldNames::indexOf).toArray();
    }

    public RowType project(String... names) {
        return project(Arrays.asList(names));
    }

    private Map<String, DataField> nameToField() {
        Map<String, DataField> nameToField = this.laziedNameToField;
        if (nameToField == null) {
            nameToField = new HashMap<>();
            for (DataField field : fields) {
                nameToField.put(field.name(), field);
            }
            this.laziedNameToField = nameToField;
        }
        return nameToField;
    }

    private Map<String, Integer> nameToIndex() {
        Map<String, Integer> nameToIndex = this.laziedNameToIndex;
        if (nameToIndex == null) {
            nameToIndex = new HashMap<>();
            for (int i = 0; i < fields.size(); i++) {
                nameToIndex.put(fields.get(i).name(), i);
            }
            this.laziedNameToIndex = nameToIndex;
        }
        return nameToIndex;
    }

    private Map<Integer, DataField> fieldIdToField() {
        Map<Integer, DataField> fieldIdToField = this.laziedFieldIdToField;
        if (fieldIdToField == null) {
            fieldIdToField = new HashMap<>();
            for (DataField field : fields) {
                fieldIdToField.put(field.id(), field);
            }
            this.laziedFieldIdToField = fieldIdToField;
        }
        return fieldIdToField;
    }

    private Map<Integer, Integer> fieldIdToIndex() {
        Map<Integer, Integer> fieldIdToIndex = this.laziedFieldIdToIndex;
        if (fieldIdToIndex == null) {
            fieldIdToIndex = new HashMap<>();
            for (int i = 0; i < fields.size(); i++) {
                fieldIdToIndex.put(fields.get(i).id(), i);
            }
            this.laziedFieldIdToIndex = fieldIdToIndex;
        }
        return fieldIdToIndex;
    }

    public static RowType of() {
        return new RowType(true, Collections.emptyList());
    }

    public static RowType of(DataField... fields) {
        final List<DataField> fs = new ArrayList<>(Arrays.asList(fields));
        return new RowType(true, fs);
    }

    public static RowType of(DataType... types) {
        final List<DataField> fields = new ArrayList<>();
        for (int i = 0; i < types.length; i++) {
            fields.add(new DataField(i, "f" + i, types[i]));
        }
        return new RowType(true, fields);
    }

    public static RowType of(DataType[] types, String[] names) {
        List<DataField> fields = new ArrayList<>();
        for (int i = 0; i < types.length; i++) {
            fields.add(new DataField(i, names[i], types[i]));
        }
        return new RowType(true, fields);
    }

    public static int currentHighestFieldId(List<DataField> fields) {
        Set<Integer> fieldIds = new HashSet<>();
        new RowType(fields).collectFieldIds(fieldIds);
        return fieldIds.stream()
                .filter(i -> !SpecialFields.isSystemField(i))
                .max(Integer::compareTo)
                .orElse(-1);
    }

    public static Builder builder() {
        return builder(new AtomicInteger(-1));
    }

    public static Builder builder(AtomicInteger fieldId) {
        return builder(true, fieldId);
    }

    public static Builder builder(boolean isNullable, AtomicInteger fieldId) {
        return new Builder(isNullable, fieldId);
    }

    /** Builder of {@link RowType}. */
    public static class Builder {

        private final List<DataField> fields = new ArrayList<>();

        private final boolean isNullable;
        private final AtomicInteger fieldId;

        private Builder(boolean isNullable, AtomicInteger fieldId) {
            this.isNullable = isNullable;
            this.fieldId = fieldId;
        }

        public Builder field(String name, DataType type) {
            fields.add(new DataField(fieldId.incrementAndGet(), name, type));
            return this;
        }

        public Builder field(String name, DataType type, @Nullable String description) {
            fields.add(new DataField(fieldId.incrementAndGet(), name, type, description));
            return this;
        }

        public Builder field(
                String name,
                DataType type,
                @Nullable String description,
                @Nullable String defaultValue) {
            fields.add(
                    new DataField(
                            fieldId.incrementAndGet(), name, type, description, defaultValue));
            return this;
        }

        public Builder fields(List<DataType> types) {
            for (int i = 0; i < types.size(); i++) {
                field("f" + i, types.get(i));
            }
            return this;
        }

        public Builder fields(DataType... types) {
            for (int i = 0; i < types.length; i++) {
                field("f" + i, types[i]);
            }
            return this;
        }

        public Builder fields(DataType[] types, String[] names) {
            for (int i = 0; i < types.length; i++) {
                field(names[i], types[i]);
            }
            return this;
        }

        public RowType build() {
            return new RowType(isNullable, fields);
        }
    }
}
