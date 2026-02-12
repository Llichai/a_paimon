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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 注册表请求。
 *
 * <p>用于向 REST 服务器发送注册现有表的请求。此操作将外部存储路径中的表注册到 catalog 中。
 *
 * <p>注册表操作不会创建新表,而是将已存在的表数据关联到指定的表标识符。
 *
 * <p>JSON 序列化格式:
 *
 * <pre>{@code
 * {
 *   "identifier": {
 *     "database": "my_db",
 *     "table": "my_table"
 *   },
 *   "path": "hdfs://namenode:8020/warehouse/my_table"
 * }
 * }</pre>
 *
 * <p>示例: 注册现有表
 *
 * <pre>{@code
 * Identifier identifier = Identifier.create("my_db", "imported_table");
 * String path = "hdfs://namenode:8020/data/existing_table";
 * RegisterTableRequest request = new RegisterTableRequest(identifier, path);
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RegisterTableRequest implements RESTRequest {

    private static final String FIELD_IDENTIFIER = "identifier";

    private static final String FIELD_PATH = "path";

    /** 表标识符(包含数据库名和表名)。 */
    @JsonProperty(FIELD_IDENTIFIER)
    private final Identifier identifier;

    /** 表数据的存储路径。 */
    @JsonProperty(FIELD_PATH)
    private final String path;

    /**
     * 构造函数。
     *
     * @param identifier 表标识符
     * @param path 表数据的存储路径
     */
    @JsonCreator
    public RegisterTableRequest(
            @JsonProperty(FIELD_IDENTIFIER) Identifier identifier,
            @JsonProperty(FIELD_PATH) String path) {
        this.identifier = identifier;
        this.path = path;
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
     * 获取表数据的存储路径。
     *
     * @return 存储路径
     */
    @JsonGetter(FIELD_PATH)
    public String getPath() {
        return path;
    }
}
