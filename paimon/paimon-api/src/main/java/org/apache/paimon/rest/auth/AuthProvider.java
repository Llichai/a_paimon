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

package org.apache.paimon.rest.auth;

import java.util.Map;

/**
 * REST API 认证提供者接口。
 *
 * <p>这是所有认证提供者的基础接口,负责生成 HTTP 请求所需的认证头部信息。
 * 不同的认证机制(如 Bearer Token、DLF 等)需要实现此接口。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li>根据请求参数生成认证 HTTP 头
 *   <li>支持多种认证机制
 *   <li>与基础头部合并生成最终请求头
 * </ul>
 *
 * <h2>支持的认证类型</h2>
 * <ul>
 *   <li><b>Bearer Token</b>: 使用 Authorization: Bearer token 头
 *   <li><b>DLF (Data Lake Formation)</b>: 阿里云数据湖认证,支持签名和 STS Token
 *   <li><b>自定义</b>: 可以实现此接口添加自定义认证机制
 * </ul>
 *
 * <h2>工作流程</h2>
 * <pre>
 * 1. 接收基础头部(如 Content-Type 等)
 * 2. 接收请求参数(路径、方法、数据等)
 * 3. 根据认证机制生成认证头(如 Authorization, Signature 等)
 * 4. 合并基础头部和认证头
 * 5. 返回完整的 HTTP 请求头
 * </pre>
 *
 * <h2>实现示例</h2>
 * <pre>{@code
 * // Bearer Token 认证实现
 * public class BearerTokenAuthProvider implements AuthProvider {
 *     private final String token;
 *
 *     public BearerTokenAuthProvider(String token) {
 *         this.token = token;
 *     }
 *
 *     @Override
 *     public Map<String, String> mergeAuthHeader(
 *             Map<String, String> baseHeader,
 *             RESTAuthParameter parameter) {
 *         Map<String, String> headers = new HashMap<>(baseHeader);
 *         headers.put("Authorization", "Bearer " + token);
 *         return headers;
 *     }
 * }
 *
 * // 使用认证提供者
 * AuthProvider authProvider = new BearerTokenAuthProvider("my-token-123");
 * RESTAuthFunction authFunction = new RESTAuthFunction(
 *     Collections.singletonMap("Content-Type", "application/json"),
 *     authProvider
 * );
 *
 * // 在 REST 客户端中使用
 * GetDatabaseResponse response = client.get(
 *     "/v1/databases/mydb",
 *     GetDatabaseResponse.class,
 *     authFunction
 * );
 * }</pre>
 *
 * <h2>线程安全性</h2>
 * <p>实现类应该是线程安全的,因为同一个 AuthProvider 实例可能被多个线程共享使用。
 *
 * @see RESTAuthFunction
 * @see RESTAuthParameter
 * @see org.apache.paimon.rest.auth.BearTokenAuthProvider
 * @see org.apache.paimon.rest.auth.DLFAuthProvider
 */
public interface AuthProvider {

    /**
     * 合并基础头部和认证头部。
     *
     * <p>此方法根据请求参数生成认证头,并与基础头部合并。
     * 如果基础头部和认证头有相同的键,认证头的值会覆盖基础头部的值。
     *
     * @param baseHeader 基础 HTTP 头部,通常包含 Content-Type、Accept 等标准头部
     * @param restAuthParameter 请求参数,包含资源路径、HTTP 方法、查询参数和请求体等信息
     * @return 合并后的完整 HTTP 请求头,包含基础头和认证头
     */
    Map<String, String> mergeAuthHeader(
            Map<String, String> baseHeader, RESTAuthParameter restAuthParameter);
}
