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
import org.apache.paimon.schema.Schema;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 创建表请求。
 *
 * <p>用于向 REST 服务器发送创建表的请求,包含表标识符和完整的表 Schema。
 *
 * <p>JSON 序列化格式:
 *
 * <pre>{@code
 * {
 *   "identifier": {
 *     "database": "my_db",
 *     "table": "my_table"
 *   },
 *   "schema": {
 *     "fields": [
 *       {"id": 0, "name": "id", "type": "BIGINT"},
 *       {"id": 1, "name": "name", "type": "STRING"}
 *     ],
 *     "partitionKeys": ["dt"],
 *     "primaryKeys": ["id"],
 *     "options": {
 *       "bucket": "4"
 *     }
 *   }
 * }
 * }</pre>
 *
 * <p>示例: 创建分区表
 *
 * <pre>{@code
 * Identifier identifier = Identifier.create("my_db", "users");
 * Schema schema = Schema.newBuilder()
 *     .column("id", DataTypes.BIGINT())
 *     .column("name", DataTypes.STRING())
 *     .column("dt", DataTypes.STRING())
 *     .partitionKeys("dt")
 *     .primaryKey("id")
 *     .option("bucket", "4")
 *     .build();
 * CreateTableRequest request = new CreateTableRequest(identifier, schema);
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateTableRequest implements RESTRequest {

    private static final String FIELD_IDENTIFIER = "identifier";
    private static final String FIELD_SCHEMA = "schema";

    /** 表标识符(包含数据库名和表名)。 */
    @JsonProperty(FIELD_IDENTIFIER)
    private final Identifier identifier;

    /** 表 Schema 定义。 */
    @JsonProperty(FIELD_SCHEMA)
    private final Schema schema;

    /**
     * 构造函数。
     *
     * @param identifier 表标识符
     * @param schema 表 Schema 定义
     */
    @JsonCreator
    public CreateTableRequest(
            @JsonProperty(FIELD_IDENTIFIER) Identifier identifier,
            @JsonProperty(FIELD_SCHEMA) Schema schema) {
        this.schema = schema;
        this.identifier = identifier;
    }

    /**
     * 获取表标识符。
     *
     * @return 表标识符
     */
    @JsonGetter(FIELD_IDENTIFIER)
    public Identifier getIdentifier() {
        return identifier;
    }

    /**
     * 获取表 Schema 定义。
     *
     * @return Schema 对象
     */
    @JsonGetter(FIELD_SCHEMA)
    public Schema getSchema() {
        return schema;
    }
}
