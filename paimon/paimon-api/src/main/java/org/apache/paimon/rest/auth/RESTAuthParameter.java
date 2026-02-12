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

import static org.apache.paimon.rest.RESTUtil.encodeString;

/**
 * REST 认证参数。
 *
 * <p>这个类封装了生成认证头部所需的所有请求信息,包括资源路径、HTTP 方法、
 * 查询参数和请求体数据。认证提供者可以根据这些参数生成适当的认证头部。
 *
 * <h2>主要用途</h2>
 * <ul>
 *   <li>传递请求信息给认证提供者
 *   <li>支持基于请求内容的签名认证(如 DLF)
 *   <li>自动对查询参数进行 URL 编码
 * </ul>
 *
 * <h2>包含的信息</h2>
 * <ul>
 *   <li><b>resourcePath</b>: 资源路径,如 /v1/databases/mydb
 *   <li><b>parameters</b>: 查询参数,如 maxResults=100&pageToken=abc
 *   <li><b>method</b>: HTTP 方法,如 GET、POST、DELETE
 *   <li><b>data</b>: 请求体数据,已序列化为 JSON 字符串
 * </ul>
 *
 * <h2>使用场景</h2>
 * <p><b>无签名认证</b>(如 Bearer Token):
 * <ul>
 *   <li>只需要固定的 token,不需要使用请求参数
 *   <li>认证头是静态的,与请求内容无关
 * </ul>
 *
 * <p><b>签名认证</b>(如 DLF、AWS Signature V4):
 * <ul>
 *   <li>需要根据请求路径、方法、参数、数据计算签名
 *   <li>认证头是动态的,每个请求都不同
 *   <li>可以防止请求被篡改
 * </ul>
 *
 * <h2>URL 编码处理</h2>
 * <p>构造函数会自动对查询参数的值进行 URL 编码:
 * <pre>{@code
 * // 输入参数
 * Map<String, String> params = new HashMap<>();
 * params.put("databaseName", "my database");  // 包含空格
 * params.put("tableName", "table/with/slash");  // 包含斜杠
 *
 * // 创建认证参数
 * RESTAuthParameter authParam = new RESTAuthParameter(
 *     "/v1/databases/my database/tables",
 *     params,
 *     "GET",
 *     null
 * );
 *
 * // 参数已被编码
 * authParam.parameters().get("databaseName");  // "my%20database"
 * authParam.parameters().get("tableName");     // "table%2Fwith%2Fslash"
 * }</pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 示例 1: GET 请求(无请求体)
 * Map<String, String> queryParams = new HashMap<>();
 * queryParams.put("maxResults", "100");
 * queryParams.put("pageToken", "token123");
 *
 * RESTAuthParameter authParam = new RESTAuthParameter(
 *     "/v1/databases/mydb/tables",
 *     queryParams,
 *     "GET",
 *     null  // GET 请求无请求体
 * );
 *
 * // 示例 2: POST 请求(有请求体)
 * CreateDatabaseRequest request = new CreateDatabaseRequest("mydb", properties);
 * String requestBody = RESTUtil.encodedBody(request);
 *
 * RESTAuthParameter authParam = new RESTAuthParameter(
 *     "/v1/databases",
 *     Collections.emptyMap(),  // POST 通常无查询参数
 *     "POST",
 *     requestBody  // 序列化后的 JSON
 * );
 *
 * // 示例 3: 用于 DLF 签名认证
 * DLFAuthProvider dlfAuth = new DLFAuthProvider(options);
 * Map<String, String> baseHeaders = Collections.singletonMap("Content-Type", "application/json");
 * Map<String, String> fullHeaders = dlfAuth.mergeAuthHeader(baseHeaders, authParam);
 *
 * // DLF 会使用这些参数计算签名
 * // fullHeaders 包含:
 * // - x-dlf-signature: 根据路径、方法、参数、数据计算的签名
 * // - x-dlf-date: 请求时间戳
 * // - Authorization: STS Token (如果使用临时凭证)
 * }</pre>
 *
 * <h2>不可变性</h2>
 * <p>这个类的所有字段都是 final 的,创建后不可修改。参数 Map 在构造时被复制,
 * 外部修改不会影响内部状态。
 *
 * @see AuthProvider
 * @see RESTAuthFunction
 * @see RESTUtil#encodeString(String)
 */
public class RESTAuthParameter {

    /** 资源路径,如 /v1/databases/mydb/tables。 */
    private final String resourcePath;

    /** URL 编码后的查询参数,键值对形式。 */
    private final Map<String, String> parameters;

    /** HTTP 方法,如 GET、POST、DELETE。 */
    private final String method;

    /** 请求体数据,已序列化为 JSON 字符串,GET 请求时为 null。 */
    private final String data;

    /**
     * 创建 REST 认证参数。
     *
     * <p>查询参数的值会被自动 URL 编码以确保正确传输。
     *
     * @param resourcePath 资源路径,如 /v1/databases/mydb
     * @param parameters 查询参数,值会被自动 URL 编码
     * @param method HTTP 方法,如 GET、POST、DELETE
     * @param data 请求体数据(已序列化为 JSON),GET 请求传 null
     */
    public RESTAuthParameter(
            String resourcePath, Map<String, String> parameters, String method, String data) {
        this.resourcePath = resourcePath;
        this.parameters = new HashMap<>();
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            this.parameters.put(entry.getKey(), encodeString(entry.getValue()));
        }
        this.method = method;
        this.data = data;
    }

    /**
     * 获取资源路径。
     *
     * @return 资源路径,如 /v1/databases/mydb/tables
     */
    public String resourcePath() {
        return resourcePath;
    }

    /**
     * 获取 URL 编码后的查询参数。
     *
     * @return 查询参数 Map,值已进行 URL 编码
     */
    public Map<String, String> parameters() {
        return parameters;
    }

    /**
     * 获取 HTTP 方法。
     *
     * @return HTTP 方法,如 GET、POST、DELETE
     */
    public String method() {
        return method;
    }

    /**
     * 获取请求体数据。
     *
     * @return 请求体的 JSON 字符串,GET 请求返回 null
     */
    public String data() {
        return data;
    }
}
