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

package org.apache.paimon.rest.requests;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.rest.RESTRequest;
import org.apache.paimon.view.ViewSchema;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 创建视图请求。
 *
 * <p>用于向 REST 服务器发送创建视图的请求,包含视图标识符和视图 Schema。
 *
 * <p>视图是基于查询的虚拟表,不存储实际数据,每次查询时动态计算结果。
 *
 * <p>JSON 序列化格式:
 *
 * <pre>{@code
 * {
 *   "identifier": {
 *     "database": "my_db",
 *     "table": "my_view"
 *   },
 *   "schema": {
 *     "query": "SELECT id, name FROM users WHERE active = true",
 *     "comment": "Active users view"
 *   }
 * }
 * }</pre>
 *
 * <p>示例: 创建视图
 *
 * <pre>{@code
 * Identifier identifier = Identifier.create("my_db", "active_users");
 * ViewSchema schema = new ViewSchema(
 *     "SELECT id, name FROM users WHERE active = true",
 *     "Active users view"
 * );
 * CreateViewRequest request = new CreateViewRequest(identifier, schema);
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateViewRequest implements RESTRequest {

    private static final String FIELD_IDENTIFIER = "identifier";
    private static final String FIELD_SCHEMA = "schema";

    /** 视图标识符(包含数据库名和视图名)。 */
    @JsonProperty(FIELD_IDENTIFIER)
    private final Identifier identifier;

    /** 视图 Schema 定义(包含查询语句等)。 */
    @JsonProperty(FIELD_SCHEMA)
    private final ViewSchema schema;

    /**
     * 构造函数。
     *
     * @param identifier 视图标识符
     * @param schema 视图 Schema 定义
     */
    @JsonCreator
    public CreateViewRequest(
            @JsonProperty(FIELD_IDENTIFIER) Identifier identifier,
            @JsonProperty(FIELD_SCHEMA) ViewSchema schema) {
        this.schema = schema;
        this.identifier = identifier;
    }

    /**
     * 获取视图标识符。
     *
     * @return 视图标识符
     */
    @JsonGetter(FIELD_IDENTIFIER)
    public Identifier getIdentifier() {
        return identifier;
    }

    /**
     * 获取视图 Schema 定义。
     *
     * @return 视图 Schema
     */
    @JsonGetter(FIELD_SCHEMA)
    public ViewSchema getSchema() {
        return schema;
    }
}
