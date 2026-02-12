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

package org.apache.paimon.view;

import org.apache.paimon.annotation.Public;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * 视图变更操作接口。
 *
 * <p>该接口定义了对视图进行修改的各种操作,包括设置选项、更新注释、管理方言查询等。
 * 所有变更操作都是不可变的,每个操作创建一个新的变更对象。
 *
 * <h2>支持的变更类型</h2>
 * <ul>
 *   <li><b>SetViewOption</b>: 设置视图配置选项
 *   <li><b>RemoveViewOption</b>: 移除视图配置选项
 *   <li><b>UpdateViewComment</b>: 更新视图注释
 *   <li><b>AddDialect</b>: 添加新的方言查询
 *   <li><b>UpdateDialect</b>: 更新现有方言查询
 *   <li><b>DropDialect</b>: 删除方言查询
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 设置视图选项
 * ViewChange setOption = ViewChange.setOption("key", "value");
 *
 * // 移除视图选项
 * ViewChange removeOption = ViewChange.removeOption("key");
 *
 * // 更新视图注释
 * ViewChange updateComment = ViewChange.updateComment("新的视图描述");
 *
 * // 添加 Spark 方言查询
 * ViewChange addDialect = ViewChange.addDialect(
 *     "spark",
 *     "SELECT * FROM table WHERE condition"
 * );
 *
 * // 更新 Flink 方言查询
 * ViewChange updateDialect = ViewChange.updateDialect(
 *     "flink",
 *     "SELECT * FROM table WHERE new_condition"
 * );
 *
 * // 删除方言查询
 * ViewChange dropDialect = ViewChange.dropDialect("hive");
 *
 * // 批量应用变更
 * List<ViewChange> changes = Arrays.asList(
 *     ViewChange.setOption("option1", "value1"),
 *     ViewChange.updateComment("Updated view"),
 *     ViewChange.addDialect("presto", "SELECT ...")
 * );
 * catalog.alterView(identifier, changes.toArray(new ViewChange[0]));
 * }</pre>
 *
 * <h2>设计理念</h2>
 * <p>视图变更采用命令模式设计:
 * <ul>
 *   <li>每个变更类型是一个独立的命令对象
 *   <li>支持序列化,可以持久化变更历史
 *   <li>类型安全,通过 Jackson 注解支持 JSON 序列化
 *   <li>不可变性,保证线程安全和可预测性
 * </ul>
 *
 * @see View
 * @since 1.0
 */
@Public
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = ViewChange.Actions.FIELD_TYPE)
@JsonSubTypes({
    @JsonSubTypes.Type(
            value = ViewChange.SetViewOption.class,
            name = ViewChange.Actions.SET_OPTION_ACTION),
    @JsonSubTypes.Type(
            value = ViewChange.RemoveViewOption.class,
            name = ViewChange.Actions.REMOVE_OPTION_ACTION),
    @JsonSubTypes.Type(
            value = ViewChange.UpdateViewComment.class,
            name = ViewChange.Actions.UPDATE_COMMENT_ACTION),
    @JsonSubTypes.Type(
            value = ViewChange.AddDialect.class,
            name = ViewChange.Actions.ADD_DIALECT_ACTION),
    @JsonSubTypes.Type(
            value = ViewChange.UpdateDialect.class,
            name = ViewChange.Actions.UPDATE_DIALECT_ACTION),
    @JsonSubTypes.Type(
            value = ViewChange.DropDialect.class,
            name = ViewChange.Actions.DROP_DIALECT_ACTION)
})
public interface ViewChange extends Serializable {

    static ViewChange setOption(String key, String value) {
        return new ViewChange.SetViewOption(key, value);
    }

    static ViewChange removeOption(String key) {
        return new ViewChange.RemoveViewOption(key);
    }

    static ViewChange updateComment(String comment) {
        return new ViewChange.UpdateViewComment(comment);
    }

    static ViewChange addDialect(String dialect, String query) {
        return new AddDialect(dialect, query);
    }

    static ViewChange updateDialect(String dialect, String query) {
        return new UpdateDialect(dialect, query);
    }

    static ViewChange dropDialect(String dialect) {
        return new DropDialect(dialect);
    }

    /**
     * 设置视图选项变更。
     *
     * <p>用于为视图添加或更新配置选项。如果选项已存在,则覆盖原有值。
     *
     * <p>使用场景:
     * <ul>
     *   <li>设置视图的刷新策略
     *   <li>配置视图的缓存参数
     *   <li>添加自定义元数据
     * </ul>
     */
    final class SetViewOption implements ViewChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_KEY = "key";
        private static final String FIELD_VALUE = "value";

        @JsonProperty(FIELD_KEY)
        private final String key;

        @JsonProperty(FIELD_VALUE)
        private final String value;

        @JsonCreator
        private SetViewOption(
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
            SetViewOption that = (SetViewOption) o;
            return key.equals(that.key) && value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }
    }

    /**
     * 移除视图选项变更。
     *
     * <p>用于从视图配置中删除指定的选项。如果选项不存在,操作将被忽略。
     *
     * <p>使用场景:
     * <ul>
     *   <li>清理不再需要的配置
     *   <li>重置为默认值
     * </ul>
     */
    final class RemoveViewOption implements ViewChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_KEY = "key";

        @JsonProperty(FIELD_KEY)
        private final String key;

        private RemoveViewOption(@JsonProperty(FIELD_KEY) String key) {
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
            RemoveViewOption that = (RemoveViewOption) o;
            return key.equals(that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key);
        }
    }

    /**
     * 更新视图注释变更。
     *
     * <p>用于更新或删除视图的注释信息。如果 comment 为 null,表示删除注释。
     *
     * <p>使用场景:
     * <ul>
     *   <li>更新视图的描述信息
     *   <li>添加视图的使用说明
     *   <li>删除过时的注释
     * </ul>
     */
    final class UpdateViewComment implements ViewChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_COMMENT = "comment";

        // If comment is null, means to remove comment
        @JsonProperty(FIELD_COMMENT)
        private final @Nullable String comment;

        private UpdateViewComment(@JsonProperty(FIELD_COMMENT) @Nullable String comment) {
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
            UpdateViewComment that = (UpdateViewComment) object;
            return Objects.equals(comment, that.comment);
        }

        @Override
        public int hashCode() {
            return Objects.hash(comment);
        }
    }

    /**
     * 添加方言查询变更。
     *
     * <p>为视图添加特定 SQL 方言的查询定义。方言查询允许针对不同的计算引擎
     * 提供优化的查询语句。
     *
     * <p>支持的方言包括:
     * <ul>
     *   <li>spark: Apache Spark SQL
     *   <li>flink: Apache Flink SQL
     *   <li>hive: Apache Hive SQL
     *   <li>presto: Presto SQL
     * </ul>
     *
     * <p>示例:
     * <pre>{@code
     * ViewChange.addDialect(
     *     "spark",
     *     "SELECT *, current_timestamp() as ts FROM source_table"
     * );
     * }</pre>
     */
    final class AddDialect implements ViewChange {
        private static final long serialVersionUID = 1L;
        private static final String FIELD_DIALECT = "dialect";
        private static final String FIELD_QUERY = "query";

        @JsonProperty(FIELD_DIALECT)
        private final String dialect;

        @JsonProperty(FIELD_QUERY)
        private final String query;

        @JsonCreator
        public AddDialect(
                @JsonProperty(FIELD_DIALECT) String dialect,
                @JsonProperty(FIELD_QUERY) String query) {
            this.dialect = dialect;
            this.query = query;
        }

        @JsonGetter(FIELD_DIALECT)
        public String dialect() {
            return dialect;
        }

        @JsonGetter(FIELD_QUERY)
        public String query() {
            return query;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            AddDialect that = (AddDialect) object;
            return Objects.equals(dialect, that.dialect) && Objects.equals(query, that.query);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dialect, query);
        }
    }

    /**
     * 更新方言查询变更。
     *
     * <p>更新视图中已存在的方言查询定义。如果指定的方言不存在,行为取决于具体实现。
     *
     * <p>使用场景:
     * <ul>
     *   <li>修正方言查询中的错误
     *   <li>优化特定引擎的查询性能
     *   <li>适配新版本引擎的语法
     * </ul>
     */
    final class UpdateDialect implements ViewChange {
        private static final long serialVersionUID = 1L;
        private static final String FIELD_DIALECT = "dialect";
        private static final String FIELD_QUERY = "query";

        @JsonProperty(FIELD_DIALECT)
        private final String dialect;

        @JsonProperty(FIELD_QUERY)
        private final String query;

        @JsonCreator
        public UpdateDialect(
                @JsonProperty(FIELD_DIALECT) String dialect,
                @JsonProperty(FIELD_QUERY) String query) {
            this.dialect = dialect;
            this.query = query;
        }

        @JsonGetter(FIELD_DIALECT)
        public String dialect() {
            return dialect;
        }

        @JsonGetter(FIELD_QUERY)
        public String query() {
            return query;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            UpdateDialect that = (UpdateDialect) object;
            return Objects.equals(dialect, that.dialect) && Objects.equals(query, that.query);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dialect, query);
        }
    }

    /**
     * 删除方言查询变更。
     *
     * <p>从视图中删除指定的方言查询。删除后,该方言将回退使用默认查询。
     *
     * <p>使用场景:
     * <ul>
     *   <li>移除不再支持的方言
     *   <li>简化视图配置
     *   <li>统一使用默认查询
     * </ul>
     */
    final class DropDialect implements ViewChange {
        private static final long serialVersionUID = 1L;
        private static final String FIELD_DIALECT = "dialect";

        @JsonProperty(FIELD_DIALECT)
        private final String dialect;

        @JsonCreator
        public DropDialect(@JsonProperty(FIELD_DIALECT) String dialect) {
            this.dialect = dialect;
        }

        @JsonGetter(FIELD_DIALECT)
        public String dialect() {
            return dialect;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            DropDialect that = (DropDialect) object;
            return Objects.equals(dialect, that.dialect);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dialect);
        }
    }

    /**
     * 视图变更操作类型常量。
     *
     * <p>定义了所有支持的视图变更操作类型的字符串常量,用于 JSON 序列化和反序列化。
     */
    class Actions {
        public static final String FIELD_TYPE = "action";
        public static final String ADD_DIALECT_ACTION = "addDialect";
        public static final String UPDATE_DIALECT_ACTION = "updateDialect";
        public static final String DROP_DIALECT_ACTION = "dropDialect";
        public static final String SET_OPTION_ACTION = "setOption";
        public static final String REMOVE_OPTION_ACTION = "removeOption";
        public static final String UPDATE_COMMENT_ACTION = "updateComment";

        private Actions() {}
    }
}
