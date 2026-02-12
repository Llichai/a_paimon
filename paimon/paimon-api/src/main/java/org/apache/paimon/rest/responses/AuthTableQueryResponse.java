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

package org.apache.paimon.rest.responses;

import org.apache.paimon.rest.RESTResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * 表查询授权响应对象。
 *
 * <p>该类表示表查询授权的响应,包含行级过滤和列级脱敏规则。
 *
 * <p>响应字段说明:
 * <ul>
 *   <li>filter - 行级过滤表达式列表,用于限制可查询的数据行
 *   <li>columnMasking - 列级脱敏映射,键为列名,值为脱敏表达式
 * </ul>
 *
 * <p>JSON 格式示例:
 * <pre>
 * {
 *   "filter": ["age > 18", "status = 'active'"],
 *   "columnMasking": {
 *     "phone": "mask(phone)",
 *     "email": "substr(email, 0, 3) || '***'"
 *   }
 * }
 * </pre>
 *
 * <p>该响应用于实现细粒度的数据访问控制和隐私保护。
 *
 * @since 1.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AuthTableQueryResponse implements RESTResponse {

    private static final String FIELD_FILTER = "filter";
    private static final String FIELD_COLUMN_MASKING = "columnMasking";

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_FILTER)
    private final List<String> filter;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_COLUMN_MASKING)
    private final Map<String, String> columnMasking;

    /**
     * 构造表查询授权响应对象。
     *
     * @param filter 行级过滤表达式列表
     * @param columnMasking 列级脱敏映射
     */
    @JsonCreator
    public AuthTableQueryResponse(
            @JsonProperty(FIELD_FILTER) List<String> filter,
            @JsonProperty(FIELD_COLUMN_MASKING) Map<String, String> columnMasking) {
        this.filter = filter;
        this.columnMasking = columnMasking;
    }

    /**
     * 获取行级过滤表达式列表。
     *
     * @return 过滤表达式列表,可能为 null
     */
    @JsonGetter(FIELD_FILTER)
    public List<String> filter() {
        return filter;
    }

    /**
     * 获取列级脱敏映射。
     *
     * @return 列脱敏映射,键为列名,值为脱敏表达式,可能为 null
     */
    @JsonGetter(FIELD_COLUMN_MASKING)
    public Map<String, String> columnMasking() {
        return columnMasking;
    }
}
