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
import java.util.function.Function;

/**
 * REST 认证函数。
 *
 * <p>这是一个函数式接口的实现类,用于在 REST API 请求中生成认证头部。
 * 它封装了初始头部和认证提供者,并在每次请求时生成完整的 HTTP 头部。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li>封装认证提供者和基础头部
 *   <li>实现 {@link Function} 接口,用于函数式调用
 *   <li>根据请求参数动态生成认证头
 * </ul>
 *
 * <h2>设计模式</h2>
 * <p>这个类使用了函数式编程模式,实现了 {@code Function<RESTAuthParameter, Map<String, String>>}:
 * <ul>
 *   <li>输入: RESTAuthParameter (请求参数)
 *   <li>输出: Map<String, String> (HTTP 头部)
 *   <li>优势: 可以作为 lambda 表达式传递,代码更简洁
 * </ul>
 *
 * <h2>使用流程</h2>
 * <pre>
 * 1. 创建时提供初始头部和认证提供者
 * 2. 在 REST 请求时,自动调用 apply() 方法
 * 3. apply() 方法委托给认证提供者生成认证头
 * 4. 返回完整的 HTTP 请求头
 * </pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 准备基础头部
 * Map<String, String> baseHeaders = new HashMap<>();
 * baseHeaders.put("Content-Type", "application/json");
 * baseHeaders.put("Accept", "application/json");
 * baseHeaders.put("User-Agent", "Paimon-REST-Client/1.0");
 *
 * // 2. 创建认证提供者
 * AuthProvider authProvider = new BearTokenAuthProvider("my-secret-token");
 *
 * // 3. 创建认证函数
 * RESTAuthFunction authFunction = new RESTAuthFunction(baseHeaders, authProvider);
 *
 * // 4. 在 REST 客户端中使用
 * RESTClient client = new HttpClient("http://localhost:8080");
 *
 * // GET 请求
 * GetDatabaseResponse db = client.get(
 *     "/v1/databases/mydb",
 *     GetDatabaseResponse.class,
 *     authFunction  // 自动生成认证头
 * );
 *
 * // POST 请求
 * CreateTableRequest request = new CreateTableRequest(identifier, schema);
 * client.post(
 *     "/v1/databases/mydb/tables",
 *     request,
 *     authFunction  // 自动生成认证头
 * );
 * }</pre>
 *
 * <h2>生成的 HTTP 头示例</h2>
 * <pre>
 * Content-Type: application/json
 * Accept: application/json
 * User-Agent: Paimon-REST-Client/1.0
 * Authorization: Bearer my-secret-token
 * </pre>
 *
 * <h2>与其他组件的关系</h2>
 * <pre>
 * RESTClient
 *    └── 调用 RESTAuthFunction.apply()
 *           └── 委托给 AuthProvider.mergeAuthHeader()
 *                  └── 根据认证类型生成头部
 *                       ├── BearTokenAuthProvider → Authorization: Bearer token
 *                       └── DLFAuthProvider → x-dlf-signature, x-dlf-date 等
 * </pre>
 *
 * <h2>线程安全性</h2>
 * <p>这个类是不可变的,因此是线程安全的。同一个实例可以在多个线程中共享使用。
 *
 * @see AuthProvider
 * @see RESTAuthParameter
 * @see RESTClient
 * @see Function
 */
public class RESTAuthFunction implements Function<RESTAuthParameter, Map<String, String>> {

    /** 初始 HTTP 头部,包含 Content-Type 等基础头部。 */
    private final Map<String, String> initHeader;

    /** 认证提供者,负责生成认证相关的 HTTP 头部。 */
    private final AuthProvider authProvider;

    /**
     * 创建 REST 认证函数。
     *
     * @param initHeader 初始 HTTP 头部,通常包含 Content-Type、Accept 等
     * @param authProvider 认证提供者,用于生成认证头部
     */
    public RESTAuthFunction(Map<String, String> initHeader, AuthProvider authProvider) {
        this.initHeader = initHeader;
        this.authProvider = authProvider;
    }

    /**
     * 根据请求参数生成完整的 HTTP 头部。
     *
     * <p>此方法是 {@link Function} 接口的实现,会在每次 REST 请求时被调用。
     *
     * @param restAuthParameter 请求参数,包含资源路径、HTTP 方法、查询参数和请求体
     * @return 完整的 HTTP 请求头,包含基础头部和认证头部
     */
    @Override
    public Map<String, String> apply(RESTAuthParameter restAuthParameter) {
        return authProvider.mergeAuthHeader(initHeader, restAuthParameter);
    }
}
