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

package org.apache.paimon.rest;

import org.apache.paimon.rest.auth.RESTAuthFunction;

import java.util.Map;

/**
 * REST Catalog HTTP 客户端接口。
 *
 * <p>这是与 REST Catalog 服务器交互的基本 HTTP 客户端接口,提供了标准的 REST API 操作方法。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li>支持 GET、POST、DELETE 等 HTTP 方法
 *   <li>自动处理请求和响应的序列化/反序列化
 *   <li>集成认证机制
 *   <li>支持查询参数
 * </ul>
 *
 * <h2>认证机制</h2>
 * <p>所有请求都需要传入 {@link RESTAuthFunction} 来处理认证:
 * <ul>
 *   <li>Bearer Token 认证
 *   <li>DLF (Data Lake Formation) 认证
 *   <li>自定义认证提供者
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建 HTTP 客户端
 * RESTClient client = new HttpClient("http://localhost:8080");
 *
 * // 执行 GET 请求
 * GetDatabaseResponse response = client.get(
 *     "/v1/databases/mydb",
 *     GetDatabaseResponse.class,
 *     authFunction
 * );
 *
 * // 执行带查询参数的 GET 请求
 * Map<String, String> params = new HashMap<>();
 * params.put("maxResults", "100");
 * ListTablesResponse tables = client.get(
 *     "/v1/databases/mydb/tables",
 *     params,
 *     ListTablesResponse.class,
 *     authFunction
 * );
 *
 * // 执行 POST 请求
 * CreateDatabaseRequest request = new CreateDatabaseRequest("mydb", properties);
 * client.post("/v1/databases", request, authFunction);
 *
 * // 执行 DELETE 请求
 * client.delete("/v1/databases/mydb", authFunction);
 * }</pre>
 *
 * <h2>错误处理</h2>
 * <p>客户端会根据 HTTP 状态码抛出相应的异常:
 * <ul>
 *   <li>404 - {@link org.apache.paimon.rest.exceptions.NoSuchResourceException}
 *   <li>409 - {@link org.apache.paimon.rest.exceptions.AlreadyExistsException}
 *   <li>403 - {@link org.apache.paimon.rest.exceptions.ForbiddenException}
 *   <li>其他 - {@link org.apache.paimon.rest.exceptions.RESTException}
 * </ul>
 *
 * @see HttpClient
 * @see RESTAuthFunction
 * @see RESTRequest
 * @see RESTResponse
 */
public interface RESTClient {

    /**
     * 执行 GET 请求。
     *
     * @param path REST API 路径
     * @param responseType 响应类型
     * @param restAuthFunction 认证函数,用于生成认证头
     * @param <T> 响应类型,必须继承 {@link RESTResponse}
     * @return 反序列化后的响应对象
     * @throws org.apache.paimon.rest.exceptions.RESTException 如果请求失败
     */
    <T extends RESTResponse> T get(
            String path, Class<T> responseType, RESTAuthFunction restAuthFunction);

    /**
     * 执行带查询参数的 GET 请求。
     *
     * @param path REST API 路径
     * @param queryParams 查询参数,会被自动编码并附加到 URL
     * @param responseType 响应类型
     * @param restAuthFunction 认证函数,用于生成认证头
     * @param <T> 响应类型,必须继承 {@link RESTResponse}
     * @return 反序列化后的响应对象
     * @throws org.apache.paimon.rest.exceptions.RESTException 如果请求失败
     */
    <T extends RESTResponse> T get(
            String path,
            Map<String, String> queryParams,
            Class<T> responseType,
            RESTAuthFunction restAuthFunction);

    /**
     * 执行 POST 请求,不期望响应体。
     *
     * @param path REST API 路径
     * @param body 请求体,会被序列化为 JSON
     * @param restAuthFunction 认证函数,用于生成认证头
     * @param <T> 响应类型,必须继承 {@link RESTResponse}
     * @return 响应对象,如果服务器未返回响应体则为 null
     * @throws org.apache.paimon.rest.exceptions.RESTException 如果请求失败
     */
    <T extends RESTResponse> T post(
            String path, RESTRequest body, RESTAuthFunction restAuthFunction);

    /**
     * 执行 POST 请求,期望特定类型的响应。
     *
     * @param path REST API 路径
     * @param body 请求体,会被序列化为 JSON
     * @param responseType 响应类型
     * @param restAuthFunction 认证函数,用于生成认证头
     * @param <T> 响应类型,必须继承 {@link RESTResponse}
     * @return 反序列化后的响应对象
     * @throws org.apache.paimon.rest.exceptions.RESTException 如果请求失败
     */
    <T extends RESTResponse> T post(
            String path,
            RESTRequest body,
            Class<T> responseType,
            RESTAuthFunction restAuthFunction);

    /**
     * 执行不带请求体的 DELETE 请求。
     *
     * @param path REST API 路径
     * @param restAuthFunction 认证函数,用于生成认证头
     * @param <T> 响应类型,必须继承 {@link RESTResponse}
     * @return 响应对象,通常为 null
     * @throws org.apache.paimon.rest.exceptions.RESTException 如果请求失败
     */
    <T extends RESTResponse> T delete(String path, RESTAuthFunction restAuthFunction);

    /**
     * 执行带请求体的 DELETE 请求。
     *
     * <p>某些 REST API 的 DELETE 操作可能需要在请求体中传递额外信息。
     *
     * @param path REST API 路径
     * @param body 请求体,会被序列化为 JSON
     * @param restAuthFunction 认证函数,用于生成认证头
     * @param <T> 响应类型,必须继承 {@link RESTResponse}
     * @return 响应对象,通常为 null
     * @throws org.apache.paimon.rest.exceptions.RESTException 如果请求失败
     */
    <T extends RESTResponse> T delete(
            String path, RESTRequest body, RESTAuthFunction restAuthFunction);
}
