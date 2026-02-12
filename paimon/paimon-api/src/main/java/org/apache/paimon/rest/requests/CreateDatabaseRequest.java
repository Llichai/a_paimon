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

import org.apache.paimon.rest.RESTRequest;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * 创建数据库请求。
 *
 * <p>用于向 REST 服务器发送创建数据库的请求,包含数据库名称和配置选项。
 *
 * <p>JSON 序列化格式:
 *
 * <pre>{@code
 * {
 *   "name": "my_database",
 *   "options": {
 *     "warehouse": "/path/to/warehouse",
 *     "metastore": "hive"
 *   }
 * }
 * }</pre>
 *
 * <p>示例: 创建新数据库
 *
 * <pre>{@code
 * Map<String, String> options = new HashMap<>();
 * options.put("warehouse", "/user/hive/warehouse");
 * options.put("comment", "Production database");
 * CreateDatabaseRequest request = new CreateDatabaseRequest("prod_db", options);
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateDatabaseRequest implements RESTRequest {

    private static final String FIELD_NAME = "name";
    private static final String FIELD_OPTIONS = "options";

    /** 数据库名称。 */
    @JsonProperty(FIELD_NAME)
    private final String name;

    /** 数据库配置选项。 */
    @JsonProperty(FIELD_OPTIONS)
    private final Map<String, String> options;

    /**
     * 构造函数。
     *
     * @param name 数据库名称
     * @param options 数据库配置选项
     */
    @JsonCreator
    public CreateDatabaseRequest(
            @JsonProperty(FIELD_NAME) String name,
            @JsonProperty(FIELD_OPTIONS) Map<String, String> options) {
        this.name = name;
        this.options = options;
    }

    /**
     * 获取数据库名称。
     *
     * @return 数据库名称
     */
    @JsonGetter(FIELD_NAME)
    public String getName() {
        return name;
    }

    /**
     * 获取数据库配置选项。
     *
     * @return 配置选项映射
     */
    @JsonGetter(FIELD_OPTIONS)
    public Map<String, String> getOptions() {
        return options;
    }
}
