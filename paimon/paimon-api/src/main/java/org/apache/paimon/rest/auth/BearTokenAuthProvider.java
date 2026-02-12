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

import java.util.HashMap;
import java.util.Map;

/**
 * Bearer Token 认证提供者。
 *
 * <p>实现基于 Bearer Token 的 HTTP 认证机制。这是一种简单但广泛使用的认证方式,
 * 通过在 HTTP 请求头中添加 "Authorization: Bearer {token}" 来进行身份验证。
 *
 * <h2>认证机制</h2>
 * <p>Bearer Token 认证将 token 添加到 HTTP Authorization 头部:
 * <pre>
 * Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
 * </pre>
 *
 * <h2>主要特性</h2>
 * <ul>
 *   <li><b>简单高效</b>: 只需在请求头中添加固定的 token
 *   <li><b>无状态</b>: 每个请求独立,不需要维护会话状态
 *   <li><b>标准化</b>: 遵循 RFC 6750 标准
 *   <li><b>广泛支持</b>: 被大多数 REST API 框架支持
 * </ul>
 *
 * <h2>工作流程</h2>
 * <pre>
 * 1. 创建 BearTokenAuthProvider 实例,传入 token
 * 2. REST 客户端调用 mergeAuthHeader()
 * 3. 生成包含 "Authorization: Bearer {token}" 的请求头
 * 4. 发送 HTTP 请求到服务器
 * 5. 服务器验证 token 并处理请求
 * </pre>
 *
 * <h2>使用示例</h2>
 *
 * <p><b>基础使用</b>:
 * <pre>{@code
 * // 1. 创建认证提供者
 * String token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...";
 * AuthProvider authProvider = new BearTokenAuthProvider(token);
 *
 * // 2. 创建认证函数
 * Map<String, String> baseHeaders = new HashMap<>();
 * baseHeaders.put("Content-Type", "application/json");
 * RESTAuthFunction authFunction = new RESTAuthFunction(baseHeaders, authProvider);
 *
 * // 3. 使用认证函数
 * HttpClient client = new HttpClient("http://localhost:8080");
 * GetDatabaseResponse response = client.get(
 *     "/v1/databases/mydb",
 *     GetDatabaseResponse.class,
 *     authFunction
 * );
 * }</pre>
 *
 * <p><b>与 RESTApi 集成</b>:
 * <pre>{@code
 * Options options = new Options();
 * options.set(RESTCatalogOptions.URI, "http://localhost:8080");
 * options.set(RESTCatalogOptions.WAREHOUSE, "my_warehouse");
 * options.set(RESTCatalogOptions.TOKEN_PROVIDER, "bear");
 * options.set(RESTCatalogOptions.TOKEN, "your-access-token");
 *
 * RESTApi api = new RESTApi(options);
 * // BearTokenAuthProvider 会被自动创建并使用
 * }</pre>
 *
 * <p><b>生成的 HTTP 头</b>:
 * <pre>
 * Content-Type: application/json
 * Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
 * </pre>
 *
 * <h2>安全建议</h2>
 * <ul>
 *   <li><b>使用 HTTPS</b>: Bearer Token 在传输过程中是明文的,必须使用 HTTPS 加密
 *   <li><b>Token 存储</b>: 不要在代码中硬编码 token,使用环境变量或配置文件
 *   <li><b>Token 轮换</b>: 定期更新 token 以提高安全性
 *   <li><b>最小权限</b>: Token 应该只授予必要的权限
 *   <li><b>Token 过期</b>: 设置合理的过期时间,使用 JWT 时可以在 token 中包含过期信息
 * </ul>
 *
 * <h2>线程安全性</h2>
 * <p>此类是线程安全的,可以在多线程环境中共享使用。token 字段是 protected 的,可以被子类扩展。
 *
 * @see AuthProvider
 * @see RESTAuthFunction
 * @see DLFAuthProvider
 */
public class BearTokenAuthProvider implements AuthProvider {

    /** Authorization HTTP 头部的键名。 */
    public static final String AUTHORIZATION_HEADER_KEY = "Authorization";

    /** Bearer token 的前缀,用于构造 Authorization 头部值。 */
    private static final String BEARER_PREFIX = "Bearer ";

    /** 访问令牌,用于身份验证。设置为 protected 允许子类访问和扩展。 */
    protected String token;

    /**
     * 创建 Bearer Token 认证提供者。
     *
     * @param token 访问令牌,不能为 null
     */
    public BearTokenAuthProvider(String token) {
        this.token = token;
    }

    /**
     * 合并基础头部和认证头部。
     *
     * <p>将 "Authorization: Bearer {token}" 添加到基础头部中。
     *
     * @param baseHeader 基础 HTTP 头部,通常包含 Content-Type 等
     * @param restAuthParameter 请求参数(此认证方式不使用请求参数)
     * @return 包含 Authorization 头部的完整 HTTP 请求头
     */

    @Override
    public Map<String, String> mergeAuthHeader(
            Map<String, String> baseHeader, RESTAuthParameter restAuthParameter) {
        Map<String, String> headersWithAuth = new HashMap<>(baseHeader);
        headersWithAuth.put(AUTHORIZATION_HEADER_KEY, BEARER_PREFIX + token);
        return headersWithAuth;
    }
}
