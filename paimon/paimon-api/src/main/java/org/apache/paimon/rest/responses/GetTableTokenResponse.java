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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * 获取表令牌响应对象。
 *
 * <p>该类表示获取表访问令牌的响应,用于临时访问授权。
 *
 * <p>响应字段:
 * <ul>
 *   <li>token - 令牌键值对映射
 *   <li>expiresAtMillis - 令牌过期时间戳(毫秒)
 * </ul>
 *
 * <p>JSON 格式示例:
 * <pre>
 * {
 *   "token": {
 *     "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
 *     "token_type": "Bearer"
 *   },
 *   "expiresAtMillis": 1609545600000
 * }
 * </pre>
 *
 * <p>客户端应在令牌过期前重新获取新令牌。
 *
 * @since 1.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GetTableTokenResponse implements RESTResponse {

    private static final String FIELD_TOKEN = "token";
    private static final String FIELD_EXPIRES_AT_MILLIS = "expiresAtMillis";

    @JsonProperty(FIELD_TOKEN)
    private final Map<String, String> token;

    @JsonProperty(FIELD_EXPIRES_AT_MILLIS)
    private final long expiresAtMillis;

    @JsonCreator
    public GetTableTokenResponse(
            @JsonProperty(FIELD_TOKEN) Map<String, String> token,
            @JsonProperty(FIELD_EXPIRES_AT_MILLIS) long expiresAtMillis) {
        this.token = token;
        this.expiresAtMillis = expiresAtMillis;
    }

    @JsonGetter(FIELD_TOKEN)
    public Map<String, String> getToken() {
        return token;
    }

    @JsonGetter(FIELD_EXPIRES_AT_MILLIS)
    public long getExpiresAtMillis() {
        return expiresAtMillis;
    }
}
