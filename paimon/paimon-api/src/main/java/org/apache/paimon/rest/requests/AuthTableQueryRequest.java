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

import javax.annotation.Nullable;

import java.util.List;

/**
 * 表查询授权请求。
 *
 * <p>用于向 REST 服务器发送表查询授权请求,指定需要查询的列。
 *
 * <p>JSON 序列化格式:
 *
 * <pre>{@code
 * {
 *   "select": ["col1", "col2", "col3"]
 * }
 * }</pre>
 *
 * <p>或者查询所有列:
 *
 * <pre>{@code
 * {
 *   "select": null
 * }
 * }</pre>
 *
 * <p>示例: 授权查询特定列
 *
 * <pre>{@code
 * List<String> columns = Arrays.asList("id", "name", "age");
 * AuthTableQueryRequest request = new AuthTableQueryRequest(columns);
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AuthTableQueryRequest implements RESTRequest {

    private static final String FIELD_SELECT = "select";

    /** 要查询的列名列表,为 null 表示查询所有列。 */
    @JsonProperty(FIELD_SELECT)
    @Nullable
    private final List<String> select;

    /**
     * 构造函数。
     *
     * @param select 要查询的列名列表,为 null 表示查询所有列
     */
    @JsonCreator
    public AuthTableQueryRequest(@JsonProperty(FIELD_SELECT) @Nullable List<String> select) {
        this.select = select;
    }

    /**
     * 获取要查询的列名列表。
     *
     * @return 列名列表,为 null 表示查询所有列
     */
    @JsonGetter(FIELD_SELECT)
    @Nullable
    public List<String> select() {
        return select;
    }
}
