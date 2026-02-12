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

import org.apache.paimon.types.DataField;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 视图的 Schema 定义。
 *
 * <p>该类封装了视图的所有 schema 相关信息,包括字段定义、查询语句、
 * 方言查询、注释和配置选项。所有字段都是不可变的,支持 JSON 序列化。
 *
 * <h2>包含的信息</h2>
 * <ul>
 *   <li><b>fields</b>: 视图输出的字段列表
 *   <li><b>query</b>: 默认的查询语句
 *   <li><b>dialects</b>: 方言到查询语句的映射
 *   <li><b>comment</b>: 视图的注释(可选)
 *   <li><b>options</b>: 视图的配置选项
 * </ul>
 *
 * <h2>序列化支持</h2>
 * <p>该类使用 Jackson 注解支持 JSON 序列化:
 * <ul>
 *   <li>@JsonProperty: 定义 JSON 字段名称
 *   <li>@JsonCreator: 定义反序列化构造器
 *   <li>@JsonGetter: 定义序列化访问器
 *   <li>@JsonInclude: comment 字段仅在非 null 时序列化
 *   <li>@JsonIgnoreProperties: 忽略未知字段
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建 ViewSchema
 * List<DataField> fields = Arrays.asList(
 *     new DataField(0, "id", DataTypes.BIGINT()),
 *     new DataField(1, "name", DataTypes.STRING())
 * );
 * String query = "SELECT id, name FROM users";
 * Map<String, String> dialects = Map.of(
 *     "spark", "SELECT id, name FROM users",
 *     "flink", "SELECT id, name FROM users"
 * );
 * String comment = "用户信息视图";
 * Map<String, String> options = Map.of(
 *     "connector", "kafka",
 *     "topic", "users"
 * );
 *
 * ViewSchema schema = new ViewSchema(
 *     fields, query, dialects, comment, options
 * );
 *
 * // 访问字段
 * List<DataField> schemaFields = schema.fields();
 * String defaultQuery = schema.query();
 * Map<String, String> schemaDialects = schema.dialects();
 * }</pre>
 *
 * <h2>设计理念</h2>
 * <p>ViewSchema 采用值对象模式:
 * <ul>
 *   <li>不可变性:所有字段都是 final 的
 *   <li>完整性:包含视图的所有 schema 信息
 *   <li>序列化:支持持久化和网络传输
 *   <li>类型安全:使用强类型字段定义
 * </ul>
 *
 * @see View
 * @see ViewImpl
 * @since 1.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ViewSchema {

    private static final String FIELD_FIELDS = "fields";
    private static final String FIELD_QUERY = "query";
    private static final String FIELD_DIALECTS = "dialects";
    private static final String FIELD_COMMENT = "comment";
    private static final String FIELD_OPTIONS = "options";

    @JsonProperty(FIELD_FIELDS)
    private final List<DataField> fields;

    @JsonProperty(FIELD_QUERY)
    private final String query;

    @JsonProperty(FIELD_DIALECTS)
    private final Map<String, String> dialects;

    @Nullable
    @JsonProperty(FIELD_COMMENT)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String comment;

    @JsonProperty(FIELD_OPTIONS)
    private final Map<String, String> options;

    @JsonCreator
    public ViewSchema(
            @JsonProperty(FIELD_FIELDS) List<DataField> fields,
            @JsonProperty(FIELD_QUERY) String query,
            @JsonProperty(FIELD_DIALECTS) Map<String, String> dialects,
            @Nullable @JsonProperty(FIELD_COMMENT) String comment,
            @JsonProperty(FIELD_OPTIONS) Map<String, String> options) {
        this.fields = fields;
        this.query = query;
        this.dialects = dialects;
        this.options = options;
        this.comment = comment;
    }

    @JsonGetter(FIELD_FIELDS)
    public List<DataField> fields() {
        return fields;
    }

    @JsonGetter(FIELD_QUERY)
    public String query() {
        return query;
    }

    @JsonGetter(FIELD_DIALECTS)
    public Map<String, String> dialects() {
        return dialects;
    }

    @Nullable
    @JsonGetter(FIELD_COMMENT)
    public String comment() {
        return comment;
    }

    @JsonGetter(FIELD_OPTIONS)
    public Map<String, String> options() {
        return options;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ViewSchema that = (ViewSchema) o;
        return Objects.equals(fields, that.fields)
                && Objects.equals(query, that.query)
                && Objects.equals(dialects, that.dialects)
                && Objects.equals(comment, that.comment)
                && Objects.equals(options, that.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields, query, dialects, comment, options);
    }
}
