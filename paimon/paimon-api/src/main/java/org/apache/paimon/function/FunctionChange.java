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

package org.apache.paimon.function;

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
 * 函数变更接口,表示对函数的修改操作。
 *
 * <p>该接口定义了可以对函数进行的各种变更操作,包括:
 * <ul>
 *   <li>设置/删除函数选项
 *   <li>更新函数注释
 *   <li>添加/更新/删除函数定义
 * </ul>
 *
 * <h2>变更类型</h2>
 * <ol>
 *   <li>{@link SetFunctionOption}: 设置函数配置选项
 *   <li>{@link RemoveFunctionOption}: 删除函数配置选项
 *   <li>{@link UpdateFunctionComment}: 更新函数注释
 *   <li>{@link AddDefinition}: 添加函数定义
 *   <li>{@link UpdateDefinition}: 更新函数定义
 *   <li>{@link DropDefinition}: 删除函数定义
 * </ol>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 设置函数选项
 * FunctionChange change1 = FunctionChange.setOption("timeout", "30s");
 *
 * // 更新函数注释
 * FunctionChange change2 = FunctionChange.updateComment("Updated function description");
 *
 * // 添加 SQL 定义
 * FunctionDefinition sqlDef = FunctionDefinition.sql("SELECT x + y");
 * FunctionChange change3 = FunctionChange.addDefinition("sql", sqlDef);
 *
 * // 删除定义
 * FunctionChange change4 = FunctionChange.dropDefinition("old_impl");
 *
 * // 应用变更
 * catalog.alterFunction(identifier, change1, change2, change3);
 * }</pre>
 *
 * @see Function
 * @see FunctionDefinition
 */
@Public
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = FunctionChange.Actions.FIELD_TYPE)
@JsonSubTypes({
    @JsonSubTypes.Type(
            value = FunctionChange.SetFunctionOption.class,
            name = FunctionChange.Actions.SET_OPTION_ACTION),
    @JsonSubTypes.Type(
            value = FunctionChange.RemoveFunctionOption.class,
            name = FunctionChange.Actions.REMOVE_OPTION_ACTION),
    @JsonSubTypes.Type(
            value = FunctionChange.UpdateFunctionComment.class,
            name = FunctionChange.Actions.UPDATE_COMMENT_ACTION),
    @JsonSubTypes.Type(
            value = FunctionChange.AddDefinition.class,
            name = FunctionChange.Actions.ADD_DEFINITION_ACTION),
    @JsonSubTypes.Type(
            value = FunctionChange.UpdateDefinition.class,
            name = FunctionChange.Actions.UPDATE_DEFINITION_ACTION),
    @JsonSubTypes.Type(
            value = FunctionChange.DropDefinition.class,
            name = FunctionChange.Actions.DROP_DEFINITION_ACTION)
})
public interface FunctionChange extends Serializable {

    /**
     * 创建设置函数选项的变更。
     *
     * @param key 选项键
     * @param value 选项值
     * @return SetFunctionOption 变更对象
     */
    static FunctionChange setOption(String key, String value) {
        return new FunctionChange.SetFunctionOption(key, value);
    }

    /**
     * 创建删除函数选项的变更。
     *
     * @param key 要删除的选项键
     * @return RemoveFunctionOption 变更对象
     */
    static FunctionChange removeOption(String key) {
        return new FunctionChange.RemoveFunctionOption(key);
    }

    /**
     * 创建更新函数注释的变更。
     *
     * @param comment 新的注释内容
     * @return UpdateFunctionComment 变更对象
     */
    static FunctionChange updateComment(String comment) {
        return new FunctionChange.UpdateFunctionComment(comment);
    }

    /**
     * 创建添加函数定义的变更。
     *
     * @param name 定义名称
     * @param definition 函数定义
     * @return AddDefinition 变更对象
     */
    static FunctionChange addDefinition(String name, FunctionDefinition definition) {
        return new FunctionChange.AddDefinition(name, definition);
    }

    /**
     * 创建更新函数定义的变更。
     *
     * @param name 定义名称
     * @param definition 新的函数定义
     * @return UpdateDefinition 变更对象
     */
    static FunctionChange updateDefinition(String name, FunctionDefinition definition) {
        return new FunctionChange.UpdateDefinition(name, definition);
    }

    /**
     * 创建删除函数定义的变更。
     *
     * @param name 要删除的定义名称
     * @return DropDefinition 变更对象
     */
    static FunctionChange dropDefinition(String name) {
        return new FunctionChange.DropDefinition(name);
    }

    /** 设置函数选项的变更操作。 */
    final class SetFunctionOption implements FunctionChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_KEY = "key";
        private static final String FIELD_VALUE = "value";

        @JsonProperty(FIELD_KEY)
        private final String key;

        @JsonProperty(FIELD_VALUE)
        private final String value;

        @JsonCreator
        private SetFunctionOption(
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
            SetFunctionOption that = (SetFunctionOption) o;
            return key.equals(that.key) && value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }
    }

    /** 删除函数选项的变更操作。 */
    final class RemoveFunctionOption implements FunctionChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_KEY = "key";

        @JsonProperty(FIELD_KEY)
        private final String key;

        private RemoveFunctionOption(@JsonProperty(FIELD_KEY) String key) {
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
            RemoveFunctionOption that = (RemoveFunctionOption) o;
            return key.equals(that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key);
        }
    }

    /** 更新函数注释的变更操作。 */
    final class UpdateFunctionComment implements FunctionChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_COMMENT = "comment";

        // 如果 comment 为 null,表示删除注释
        @JsonProperty(FIELD_COMMENT)
        private final @Nullable String comment;

        private UpdateFunctionComment(@JsonProperty(FIELD_COMMENT) @Nullable String comment) {
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
            UpdateFunctionComment that = (UpdateFunctionComment) object;
            return Objects.equals(comment, that.comment);
        }

        @Override
        public int hashCode() {
            return Objects.hash(comment);
        }
    }

    /** 添加函数定义的变更操作。 */
    final class AddDefinition implements FunctionChange {
        private static final long serialVersionUID = 1L;
        private static final String FIELD_DEFINITION_NAME = "name";
        private static final String FIELD_DEFINITION = "definition";

        @JsonProperty(FIELD_DEFINITION_NAME)
        private final String name;

        @JsonProperty(FIELD_DEFINITION)
        private final FunctionDefinition definition;

        @JsonCreator
        public AddDefinition(
                @JsonProperty(FIELD_DEFINITION_NAME) String name,
                @JsonProperty(FIELD_DEFINITION) FunctionDefinition definition) {
            this.name = name;
            this.definition = definition;
        }

        @JsonGetter(FIELD_DEFINITION_NAME)
        public String name() {
            return name;
        }

        @JsonGetter(FIELD_DEFINITION)
        public FunctionDefinition definition() {
            return definition;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            AddDefinition that = (AddDefinition) object;
            return Objects.equals(name, that.name) && Objects.equals(definition, that.definition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, definition);
        }
    }

    /** 更新函数定义的变更操作。 */
    final class UpdateDefinition implements FunctionChange {
        private static final long serialVersionUID = 1L;
        private static final String FIELD_DEFINITION_NAME = "name";
        private static final String FIELD_DEFINITION = "definition";

        @JsonProperty(FIELD_DEFINITION_NAME)
        private final String name;

        @JsonProperty(FIELD_DEFINITION)
        private final FunctionDefinition definition;

        @JsonCreator
        public UpdateDefinition(
                @JsonProperty(FIELD_DEFINITION_NAME) String name,
                @JsonProperty(FIELD_DEFINITION) FunctionDefinition definition) {
            this.name = name;
            this.definition = definition;
        }

        @JsonGetter(FIELD_DEFINITION_NAME)
        public String name() {
            return name;
        }

        @JsonGetter(FIELD_DEFINITION)
        public FunctionDefinition definition() {
            return definition;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            UpdateDefinition that = (UpdateDefinition) object;
            return Objects.equals(name, that.name) && Objects.equals(definition, that.definition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(definition, definition);
        }
    }

    /** 删除函数定义的变更操作。 */
    final class DropDefinition implements FunctionChange {
        private static final long serialVersionUID = 1L;
        private static final String FIELD_DEFINITION_NAME = "name";

        @JsonProperty(FIELD_DEFINITION_NAME)
        private final String name;

        @JsonCreator
        public DropDefinition(@JsonProperty(FIELD_DEFINITION_NAME) String name) {
            this.name = name;
        }

        @JsonGetter(FIELD_DEFINITION_NAME)
        public String name() {
            return name;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            DropDefinition that = (DropDefinition) object;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }
    }

    /** 函数变更操作的类型常量。 */
    class Actions {
        public static final String FIELD_TYPE = "action";
        public static final String ADD_DEFINITION_ACTION = "addDefinition";
        public static final String UPDATE_DEFINITION_ACTION = "updateDefinition";
        public static final String DROP_DEFINITION_ACTION = "dropDefinition";
        public static final String SET_OPTION_ACTION = "setOption";
        public static final String REMOVE_OPTION_ACTION = "removeOption";
        public static final String UPDATE_COMMENT_ACTION = "updateComment";

        private Actions() {}
    }
}
