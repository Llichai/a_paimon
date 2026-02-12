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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.types.DataType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * Schema 变更操作。
 *
 * <p>定义了对表 Schema 进行修改的各种操作类型:
 * <ul>
 *   <li><b>选项操作</b>: {@link SetOption}, {@link RemoveOption}
 *   <li><b>注释操作</b>: {@link UpdateComment}
 *   <li><b>列操作</b>: {@link AddColumn}, {@link RenameColumn}, {@link DropColumn}
 *   <li><b>列属性操作</b>: {@link UpdateColumnType}, {@link UpdateColumnNullability},
 *       {@link UpdateColumnComment}, {@link UpdateColumnDefaultValue}
 *   <li><b>列位置操作</b>: {@link UpdateColumnPosition}, {@link Move}
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 添加新列
 * SchemaChange addCol = SchemaChange.addColumn("new_col", DataTypes.STRING());
 *
 * // 修改列类型
 * SchemaChange updateType = SchemaChange.updateColumnType("col1", DataTypes.BIGINT());
 *
 * // 重命名列
 * SchemaChange rename = SchemaChange.renameColumn("old_name", "new_name");
 *
 * // 删除列
 * SchemaChange drop = SchemaChange.dropColumn("col_to_drop");
 *
 * // 设置表选项
 * SchemaChange setOpt = SchemaChange.setOption("bucket", "8");
 * }</pre>
 *
 * @since 0.4.0
 */
@Public
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = SchemaChange.Actions.FIELD_ACTION)
@JsonSubTypes({
    @JsonSubTypes.Type(
            value = SchemaChange.SetOption.class,
            name = SchemaChange.Actions.SET_OPTION_ACTION),
    @JsonSubTypes.Type(
            value = SchemaChange.RemoveOption.class,
            name = SchemaChange.Actions.REMOVE_OPTION_ACTION),
    @JsonSubTypes.Type(
            value = SchemaChange.UpdateComment.class,
            name = SchemaChange.Actions.UPDATE_COMMENT_ACTION),
    @JsonSubTypes.Type(
            value = SchemaChange.AddColumn.class,
            name = SchemaChange.Actions.ADD_COLUMN_ACTION),
    @JsonSubTypes.Type(
            value = SchemaChange.RenameColumn.class,
            name = SchemaChange.Actions.RENAME_COLUMN_ACTION),
    @JsonSubTypes.Type(
            value = SchemaChange.DropColumn.class,
            name = SchemaChange.Actions.DROP_COLUMN_ACTION),
    @JsonSubTypes.Type(
            value = SchemaChange.UpdateColumnType.class,
            name = SchemaChange.Actions.UPDATE_COLUMN_TYPE_ACTION),
    @JsonSubTypes.Type(
            value = SchemaChange.UpdateColumnNullability.class,
            name = SchemaChange.Actions.UPDATE_COLUMN_NULLABILITY_ACTION),
    @JsonSubTypes.Type(
            value = SchemaChange.UpdateColumnComment.class,
            name = SchemaChange.Actions.UPDATE_COLUMN_COMMENT_ACTION),
    @JsonSubTypes.Type(
            value = SchemaChange.UpdateColumnDefaultValue.class,
            name = SchemaChange.Actions.UPDATE_COLUMN_DEFAULT_VALUE_ACTION),
    @JsonSubTypes.Type(
            value = SchemaChange.UpdateColumnPosition.class,
            name = SchemaChange.Actions.UPDATE_COLUMN_POSITION_ACTION),
})
public interface SchemaChange extends Serializable {

    /**
     * 创建设置表选项的变更。
     *
     * @param key 选项键
     * @param value 选项值
     * @return SetOption 变更实例
     */
    static SchemaChange setOption(String key, String value) {
        return new SetOption(key, value);
    }

    /**
     * 创建移除表选项的变更。
     *
     * @param key 选项键
     * @return RemoveOption 变更实例
     */
    static SchemaChange removeOption(String key) {
        return new RemoveOption(key);
    }

    /**
     * 创建更新表注释的变更。
     *
     * @param comment 新注释（null 表示移除注释）
     * @return UpdateComment 变更实例
     */
    static SchemaChange updateComment(@Nullable String comment) {
        return new UpdateComment(comment);
    }

    /**
     * 创建添加列的变更（不带注释和位置）。
     *
     * @param fieldName 字段名
     * @param dataType 数据类型
     * @return AddColumn 变更实例
     */
    static SchemaChange addColumn(String fieldName, DataType dataType) {
        return addColumn(fieldName, dataType, null, null);
    }

    /**
     * 创建添加列的变更（带注释）。
     *
     * @param fieldName 字段名
     * @param dataType 数据类型
     * @param comment 列注释
     * @return AddColumn 变更实例
     */
    static SchemaChange addColumn(String fieldName, DataType dataType, String comment) {
        return new AddColumn(new String[] {fieldName}, dataType, comment, null);
    }

    /**
     * 创建添加列的变更（带注释和位置）。
     *
     * @param fieldName 字段名
     * @param dataType 数据类型
     * @param comment 列注释
     * @param move 列位置
     * @return AddColumn 变更实例
     */
    static SchemaChange addColumn(String fieldName, DataType dataType, String comment, Move move) {
        return new AddColumn(new String[] {fieldName}, dataType, comment, move);
    }

    /**
     * 创建添加嵌套列的变更。
     *
     * @param fieldNames 嵌套字段路径
     * @param dataType 数据类型
     * @param comment 列注释
     * @param move 列位置
     * @return AddColumn 变更实例
     */
    static SchemaChange addColumn(
            String[] fieldNames, DataType dataType, String comment, Move move) {
        return new AddColumn(fieldNames, dataType, comment, move);
    }

    /**
     * 创建重命名列的变更。
     *
     * @param fieldName 原字段名
     * @param newName 新字段名
     * @return RenameColumn 变更实例
     */
    static SchemaChange renameColumn(String fieldName, String newName) {
        return new RenameColumn(new String[] {fieldName}, newName);
    }

    /**
     * 创建重命名嵌套列的变更。
     *
     * @param fieldNames 嵌套字段路径
     * @param newName 新字段名
     * @return RenameColumn 变更实例
     */
    static SchemaChange renameColumn(String[] fieldNames, String newName) {
        return new RenameColumn(fieldNames, newName);
    }

    /**
     * 创建删除列的变更。
     *
     * @param fieldName 字段名
     * @return DropColumn 变更实例
     */
    static SchemaChange dropColumn(String fieldName) {
        return new DropColumn(new String[] {fieldName});
    }

    /**
     * 创建删除嵌套列的变更。
     *
     * @param fieldNames 嵌套字段路径
     * @return DropColumn 变更实例
     */
    static SchemaChange dropColumn(String[] fieldNames) {
        return new DropColumn(fieldNames);
    }

    /**
     * 创建更新列类型的变更（不保留可空性）。
     *
     * @param fieldName 字段名
     * @param newDataType 新数据类型
     * @return UpdateColumnType 变更实例
     */
    static SchemaChange updateColumnType(String fieldName, DataType newDataType) {
        return new UpdateColumnType(new String[] {fieldName}, newDataType, false);
    }

    /**
     * 创建更新列类型的变更。
     *
     * @param fieldName 字段名
     * @param newDataType 新数据类型
     * @param keepNullability 是否保留原可空性
     * @return UpdateColumnType 变更实例
     */
    static SchemaChange updateColumnType(
            String fieldName, DataType newDataType, boolean keepNullability) {
        return new UpdateColumnType(new String[] {fieldName}, newDataType, keepNullability);
    }

    /**
     * 创建更新嵌套列类型的变更。
     *
     * @param fieldNames 嵌套字段路径
     * @param newDataType 新数据类型
     * @param keepNullability 是否保留原可空性
     * @return UpdateColumnType 变更实例
     */
    static SchemaChange updateColumnType(
            String[] fieldNames, DataType newDataType, boolean keepNullability) {
        return new UpdateColumnType(fieldNames, newDataType, keepNullability);
    }

    /**
     * 创建更新列可空性的变更。
     *
     * @param fieldName 字段名
     * @param newNullability 新的可空性
     * @return UpdateColumnNullability 变更实例
     */
    static SchemaChange updateColumnNullability(String fieldName, boolean newNullability) {
        return new UpdateColumnNullability(new String[] {fieldName}, newNullability);
    }

    /**
     * 创建更新嵌套列可空性的变更。
     *
     * @param fieldNames 嵌套字段路径
     * @param newNullability 新的可空性
     * @return UpdateColumnNullability 变更实例
     */
    static SchemaChange updateColumnNullability(String[] fieldNames, boolean newNullability) {
        return new UpdateColumnNullability(fieldNames, newNullability);
    }

    /**
     * 创建更新列注释的变更。
     *
     * @param fieldName 字段名
     * @param comment 新注释
     * @return UpdateColumnComment 变更实例
     */
    static SchemaChange updateColumnComment(String fieldName, String comment) {
        return new UpdateColumnComment(new String[] {fieldName}, comment);
    }

    /**
     * 创建更新嵌套列注释的变更。
     *
     * @param fieldNames 嵌套字段路径
     * @param comment 新注释
     * @return UpdateColumnComment 变更实例
     */
    static SchemaChange updateColumnComment(String[] fieldNames, String comment) {
        return new UpdateColumnComment(fieldNames, comment);
    }

    /**
     * 创建更新列默认值的变更。
     *
     * @param fieldNames 嵌套字段路径
     * @param defaultValue 新默认值
     * @return UpdateColumnDefaultValue 变更实例
     */
    static SchemaChange updateColumnDefaultValue(String[] fieldNames, String defaultValue) {
        return new UpdateColumnDefaultValue(fieldNames, defaultValue);
    }

    /**
     * 创建更新列位置的变更。
     *
     * @param move 移动操作
     * @return UpdateColumnPosition 变更实例
     */
    static SchemaChange updateColumnPosition(Move move) {
        return new UpdateColumnPosition(move);
    }

    /**
     * 设置表选项的 Schema 变更。
     *
     * <p>用于添加或更新表的配置选项。
     */
    final class SetOption implements SchemaChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_KEY = "key";
        private static final String FIELD_VALUE = "value";

        /** 选项键。 */
        @JsonProperty(FIELD_KEY)
        private final String key;

        /** 选项值。 */
        @JsonProperty(FIELD_VALUE)
        private final String value;

        @JsonCreator
        private SetOption(
                @JsonProperty(FIELD_KEY) String key, @JsonProperty(FIELD_VALUE) String value) {
            this.key = key;
            this.value = value;
        }

        @JsonGetter(FIELD_KEY)
        public String key() {
            return key;
        }

        @JsonGetter(FIELD_VALUE)
        public String value() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SetOption that = (SetOption) o;
            return key.equals(that.key) && value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }
    }

    /**
     * 移除表选项的 Schema 变更。
     *
     * <p>用于删除表的配置选项。
     */
    final class RemoveOption implements SchemaChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_KEY = "key";

        @JsonProperty(FIELD_KEY)
        private final String key;

        private RemoveOption(@JsonProperty(FIELD_KEY) String key) {
            this.key = key;
        }

        @JsonGetter(FIELD_KEY)
        public String key() {
            return key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RemoveOption that = (RemoveOption) o;
            return key.equals(that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key);
        }
    }

    /**
     * 更新表注释的 Schema 变更。
     *
     * <p>用于修改或移除表的注释（comment 为 null 时表示移除注释）。
     */
    final class UpdateComment implements SchemaChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_COMMENT = "comment";

        // If comment is null, means to remove comment
        @JsonProperty(FIELD_COMMENT)
        private final @Nullable String comment;

        private UpdateComment(@JsonProperty(FIELD_COMMENT) @Nullable String comment) {
            this.comment = comment;
        }

        @JsonGetter(FIELD_COMMENT)
        public @Nullable String comment() {
            return comment;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            UpdateComment that = (UpdateComment) object;
            return Objects.equals(comment, that.comment);
        }

        @Override
        public int hashCode() {
            return Objects.hash(comment);
        }
    }

    /**
     * 添加列的 Schema 变更。
     *
     * <p>用于在表中添加新列，支持:
     * <ul>
     *   <li>指定列的数据类型和注释
     *   <li>指定列在表中的位置（通过 Move 对象）
     *   <li>支持嵌套字段（通过 fieldNames 数组）
     * </ul>
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    final class AddColumn implements SchemaChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_FILED_NAMES = "fieldNames";
        private static final String FIELD_DATA_TYPE = "dataType";
        private static final String FIELD_COMMENT = "comment";
        private static final String FIELD_MOVE = "move";

        @JsonProperty(FIELD_FILED_NAMES)
        private final String[] fieldNames;

        @JsonProperty(FIELD_DATA_TYPE)
        private final DataType dataType;

        @JsonProperty(FIELD_COMMENT)
        private final String description;

        @JsonProperty(FIELD_MOVE)
        private final Move move;

        @JsonCreator
        private AddColumn(
                @JsonProperty(FIELD_FILED_NAMES) String[] fieldNames,
                @JsonProperty(FIELD_DATA_TYPE) DataType dataType,
                @JsonProperty(FIELD_COMMENT) String description,
                @JsonProperty(FIELD_MOVE) Move move) {
            this.fieldNames = fieldNames;
            this.dataType = dataType;
            this.description = description;
            this.move = move;
        }

        @JsonGetter(FIELD_FILED_NAMES)
        public String[] fieldNames() {
            return fieldNames;
        }

        @JsonGetter(FIELD_DATA_TYPE)
        public DataType dataType() {
            return dataType;
        }

        @Nullable
        @JsonGetter(FIELD_COMMENT)
        public String description() {
            return description;
        }

        @Nullable
        @JsonGetter(FIELD_MOVE)
        public Move move() {
            return move;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AddColumn addColumn = (AddColumn) o;
            return Arrays.equals(fieldNames, addColumn.fieldNames)
                    && dataType.equals(addColumn.dataType)
                    && Objects.equals(description, addColumn.description)
                    && Objects.equals(move, addColumn.move);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(dataType, description);
            result = 31 * result + Objects.hashCode(fieldNames);
            result = 31 * result + Objects.hashCode(move);
            return result;
        }
    }

    /**
     * 重命名列的 Schema 变更。
     *
     * <p>用于修改列的名称，支持嵌套字段的重命名。
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    final class RenameColumn implements SchemaChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_FILED_NAMES = "fieldNames";
        private static final String FIELD_NEW_NAME = "newName";

        @JsonProperty(FIELD_FILED_NAMES)
        private final String[] fieldNames;

        @JsonProperty(FIELD_NEW_NAME)
        private final String newName;

        @JsonCreator
        private RenameColumn(
                @JsonProperty(FIELD_FILED_NAMES) String[] fieldNames,
                @JsonProperty(FIELD_NEW_NAME) String newName) {
            this.fieldNames = fieldNames;
            this.newName = newName;
        }

        @JsonGetter(FIELD_FILED_NAMES)
        public String[] fieldNames() {
            return fieldNames;
        }

        @JsonGetter(FIELD_NEW_NAME)
        public String newName() {
            return newName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RenameColumn that = (RenameColumn) o;
            return Arrays.equals(fieldNames, that.fieldNames)
                    && Objects.equals(newName, that.newName);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(newName);
            result = 31 * result + Objects.hashCode(fieldNames);
            return result;
        }
    }

    /**
     * 删除列的 Schema 变更。
     *
     * <p>用于从表中删除列，支持嵌套字段的删除。
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    final class DropColumn implements SchemaChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_FILED_NAMES = "fieldNames";

        @JsonProperty(FIELD_FILED_NAMES)
        private final String[] fieldNames;

        @JsonCreator
        private DropColumn(@JsonProperty(FIELD_FILED_NAMES) String[] fieldNames) {
            this.fieldNames = fieldNames;
        }

        @JsonGetter(FIELD_FILED_NAMES)
        public String[] fieldNames() {
            return fieldNames;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DropColumn that = (DropColumn) o;
            return Arrays.equals(fieldNames, that.fieldNames);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(fieldNames);
        }
    }

    /**
     * 更新列类型的 Schema 变更。
     *
     * <p>用于修改列的数据类型。可选择是否保留原有的可空性设置。
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    final class UpdateColumnType implements SchemaChange {

        private static final long serialVersionUID = 1L;
        private static final String FIELD_FILED_NAMES = "fieldNames";
        private static final String FIELD_NEW_DATA_TYPE = "newDataType";
        private static final String FIELD_KEEP_NULLABILITY = "keepNullability";

        /** 字段名路径（支持嵌套字段）。 */
        @JsonProperty(FIELD_FILED_NAMES)
        private final String[] fieldNames;

        /** 新的数据类型。 */
        @JsonProperty(FIELD_NEW_DATA_TYPE)
        private final DataType newDataType;

        /** 是否保留原有的可空性设置（true 表示不改变可空性）。 */
        @JsonProperty(FIELD_KEEP_NULLABILITY)
        private final boolean keepNullability;

        @JsonCreator
        private UpdateColumnType(
                @JsonProperty(FIELD_FILED_NAMES) String[] fieldNames,
                @JsonProperty(FIELD_NEW_DATA_TYPE) DataType newDataType,
                @JsonProperty(FIELD_KEEP_NULLABILITY) boolean keepNullability) {
            this.fieldNames = fieldNames;
            this.newDataType = newDataType;
            this.keepNullability = keepNullability;
        }

        @JsonGetter(FIELD_FILED_NAMES)
        public String[] fieldNames() {
            return fieldNames;
        }

        @JsonGetter(FIELD_NEW_DATA_TYPE)
        public DataType newDataType() {
            return newDataType;
        }

        @JsonGetter(FIELD_KEEP_NULLABILITY)
        public boolean keepNullability() {
            return keepNullability;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            UpdateColumnType that = (UpdateColumnType) o;
            return Arrays.equals(fieldNames, that.fieldNames)
                    && newDataType.equals(that.newDataType);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(newDataType);
            result = 31 * result + Objects.hashCode(fieldNames);
            return result;
        }
    }

    /**
     * 更新列位置的 Schema 变更。
     *
     * <p>用于调整列在表中的位置。
     */
    final class UpdateColumnPosition implements SchemaChange {

        private static final long serialVersionUID = 1L;
        private static final String FIELD_MOVE = "move";

        @JsonProperty(FIELD_MOVE)
        private final Move move;

        @JsonCreator
        private UpdateColumnPosition(@JsonProperty(FIELD_MOVE) Move move) {
            this.move = move;
        }

        @JsonGetter(FIELD_MOVE)
        public Move move() {
            return move;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            UpdateColumnPosition updateColumnPosition = (UpdateColumnPosition) o;
            return Objects.equals(move, updateColumnPosition.move);
        }

        @Override
        public int hashCode() {
            return Objects.hash(move);
        }
    }

    /**
     * 表示在结构体中请求的列移动操作。
     *
     * <p>支持四种移动类型:
     * <ul>
     *   <li>FIRST: 移动到首位
     *   <li>AFTER: 移动到指定列之后
     *   <li>BEFORE: 移动到指定列之前
     *   <li>LAST: 移动到末尾
     * </ul>
     *
     * <p>使用示例:
     * <pre>{@code
     * // 移动到首位
     * Move move1 = Move.first("col1");
     *
     * // 移动到 col2 之后
     * Move move2 = Move.after("col1", "col2");
     *
     * // 移动到 col3 之前
     * Move move3 = Move.before("col1", "col3");
     *
     * // 移动到末尾
     * Move move4 = Move.last("col1");
     * }</pre>
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    class Move implements Serializable {

        /** 移动类型枚举。 */
        public enum MoveType {
            /** 移动到首位。 */
            FIRST,
            /** 移动到指定列之后。 */
            AFTER,
            /** 移动到指定列之前。 */
            BEFORE,
            /** 移动到末尾。 */
            LAST
        }

        /**
         * 创建移动到首位的操作。
         *
         * @param fieldName 要移动的字段名
         * @return Move 实例
         */
        public static Move first(String fieldName) {
            return new Move(fieldName, null, MoveType.FIRST);
        }

        /**
         * 创建移动到指定列之后的操作。
         *
         * @param fieldName 要移动的字段名
         * @param referenceFieldName 参考字段名
         * @return Move 实例
         */
        public static Move after(String fieldName, String referenceFieldName) {
            return new Move(fieldName, referenceFieldName, MoveType.AFTER);
        }

        /**
         * 创建移动到指定列之前的操作。
         *
         * @param fieldName 要移动的字段名
         * @param referenceFieldName 参考字段名
         * @return Move 实例
         */
        public static Move before(String fieldName, String referenceFieldName) {
            return new Move(fieldName, referenceFieldName, MoveType.BEFORE);
        }

        /**
         * 创建移动到末尾的操作。
         *
         * @param fieldName 要移动的字段名
         * @return Move 实例
         */
        public static Move last(String fieldName) {
            return new Move(fieldName, null, MoveType.LAST);
        }

        private static final long serialVersionUID = 1L;

        private static final String FIELD_FILED_NAME = "fieldName";
        private static final String FIELD_REFERENCE_FIELD_NAME = "referenceFieldName";
        private static final String FIELD_TYPE = "type";

        @JsonProperty(FIELD_FILED_NAME)
        private final String fieldName;

        @JsonProperty(FIELD_REFERENCE_FIELD_NAME)
        private final String referenceFieldName;

        @JsonProperty(FIELD_TYPE)
        private final MoveType type;

        @JsonCreator
        public Move(
                @JsonProperty(FIELD_FILED_NAME) String fieldName,
                @JsonProperty(FIELD_REFERENCE_FIELD_NAME) String referenceFieldName,
                @JsonProperty(FIELD_TYPE) MoveType type) {
            this.fieldName = fieldName;
            this.referenceFieldName = referenceFieldName;
            this.type = type;
        }

        @JsonGetter(FIELD_FILED_NAME)
        public String fieldName() {
            return fieldName;
        }

        @JsonGetter(FIELD_REFERENCE_FIELD_NAME)
        public String referenceFieldName() {
            return referenceFieldName;
        }

        @JsonGetter(FIELD_TYPE)
        public MoveType type() {
            return type;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Move move = (Move) o;
            return Objects.equals(fieldName, move.fieldName)
                    && Objects.equals(referenceFieldName, move.referenceFieldName)
                    && Objects.equals(type, move.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, referenceFieldName, type);
        }
    }

    /**
     * 更新列可空性的 Schema 变更。
     *
     * <p>用于修改列是否允许 null 值，支持嵌套字段。
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    final class UpdateColumnNullability implements SchemaChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_FILED_NAMES = "fieldNames";
        private static final String FIELD_NEW_NULLABILITY = "newNullability";

        @JsonProperty(FIELD_FILED_NAMES)
        private final String[] fieldNames;

        @JsonProperty(FIELD_NEW_NULLABILITY)
        private final boolean newNullability;

        @JsonCreator
        public UpdateColumnNullability(
                @JsonProperty(FIELD_FILED_NAMES) String[] fieldNames,
                @JsonProperty(FIELD_NEW_NULLABILITY) boolean newNullability) {
            this.fieldNames = fieldNames;
            this.newNullability = newNullability;
        }

        @JsonGetter(FIELD_FILED_NAMES)
        public String[] fieldNames() {
            return fieldNames;
        }

        @JsonGetter(FIELD_NEW_NULLABILITY)
        public boolean newNullability() {
            return newNullability;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof UpdateColumnNullability)) {
                return false;
            }
            UpdateColumnNullability that = (UpdateColumnNullability) o;
            return newNullability == that.newNullability
                    && Arrays.equals(fieldNames, that.fieldNames);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(newNullability);
            result = 31 * result + Arrays.hashCode(fieldNames);
            return result;
        }
    }

    /**
     * 更新列注释的 Schema 变更。
     *
     * <p>用于修改列的注释信息，支持嵌套字段。
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    final class UpdateColumnComment implements SchemaChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_FILED_NAMES = "fieldNames";
        private static final String FIELD_NEW_COMMENT = "newComment";

        @JsonProperty(FIELD_FILED_NAMES)
        private final String[] fieldNames;

        @JsonProperty(FIELD_NEW_COMMENT)
        private final String newDescription;

        @JsonCreator
        public UpdateColumnComment(
                @JsonProperty(FIELD_FILED_NAMES) String[] fieldNames,
                @JsonProperty(FIELD_NEW_COMMENT) String newDescription) {
            this.fieldNames = fieldNames;
            this.newDescription = newDescription;
        }

        @JsonGetter(FIELD_FILED_NAMES)
        public String[] fieldNames() {
            return fieldNames;
        }

        @JsonGetter(FIELD_NEW_COMMENT)
        public String newDescription() {
            return newDescription;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof UpdateColumnComment)) {
                return false;
            }
            UpdateColumnComment that = (UpdateColumnComment) o;
            return Arrays.equals(fieldNames, that.fieldNames)
                    && newDescription.equals(that.newDescription);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(newDescription);
            result = 31 * result + Arrays.hashCode(fieldNames);
            return result;
        }
    }

    /**
     * 更新列默认值的 Schema 变更。
     *
     * <p>用于修改列的默认值。
     */
    final class UpdateColumnDefaultValue implements SchemaChange {

        private static final long serialVersionUID = 1L;
        private static final String FIELD_FILED_NAMES = "fieldNames";
        private static final String FIELD_NEW_DEFAULT_VALUE = "newDefaultValue";

        @JsonProperty(FIELD_FILED_NAMES)
        private final String[] fieldNames;

        @JsonProperty(FIELD_NEW_DEFAULT_VALUE)
        private final String newDefaultValue;

        @JsonCreator
        private UpdateColumnDefaultValue(
                @JsonProperty(FIELD_FILED_NAMES) String[] fieldNames,
                @JsonProperty(FIELD_NEW_DEFAULT_VALUE) String newDefaultValue) {
            this.fieldNames = fieldNames;
            this.newDefaultValue = newDefaultValue;
        }

        @JsonGetter(FIELD_FILED_NAMES)
        public String[] fieldNames() {
            return fieldNames;
        }

        @JsonGetter(FIELD_NEW_DEFAULT_VALUE)
        public String newDefaultValue() {
            return newDefaultValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            UpdateColumnDefaultValue that = (UpdateColumnDefaultValue) o;
            return Arrays.equals(fieldNames, that.fieldNames)
                    && newDefaultValue.equals(that.newDefaultValue);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(newDefaultValue);
            result = 31 * result + Objects.hashCode(fieldNames);
            return result;
        }
    }

    /**
     * Schema 变更操作类型标识。
     *
     * <p>用于 JSON 序列化/反序列化时区分不同的 Schema 变更类型。
     */
    class Actions {
        /** 操作类型字段名。 */
        public static final String FIELD_ACTION = "action";

        /** 设置选项操作。 */
        public static final String SET_OPTION_ACTION = "setOption";

        /** 移除选项操作。 */
        public static final String REMOVE_OPTION_ACTION = "removeOption";

        /** 更新注释操作。 */
        public static final String UPDATE_COMMENT_ACTION = "updateComment";

        /** 添加列操作。 */
        public static final String ADD_COLUMN_ACTION = "addColumn";

        /** 重命名列操作。 */
        public static final String RENAME_COLUMN_ACTION = "renameColumn";

        /** 删除列操作。 */
        public static final String DROP_COLUMN_ACTION = "dropColumn";

        /** 更新列类型操作。 */
        public static final String UPDATE_COLUMN_TYPE_ACTION = "updateColumnType";

        /** 更新列可空性操作。 */
        public static final String UPDATE_COLUMN_NULLABILITY_ACTION = "updateColumnNullability";

        /** 更新列注释操作。 */
        public static final String UPDATE_COLUMN_COMMENT_ACTION = "updateColumnComment";

        /** 更新列默认值操作。 */
        public static final String UPDATE_COLUMN_DEFAULT_VALUE_ACTION = "updateColumnDefaultValue";

        /** 更新列位置操作。 */
        public static final String UPDATE_COLUMN_POSITION_ACTION = "updateColumnPosition";

        private Actions() {}
    }
}
