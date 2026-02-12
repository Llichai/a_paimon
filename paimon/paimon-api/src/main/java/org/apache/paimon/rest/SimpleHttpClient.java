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

import org.apache.paimon.utils.StringUtils;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicHeader;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import static org.apache.paimon.rest.HttpClientUtils.createBuilder;

/**
 * 简单的 HTTP 客户端包装器。
 *
 * <p>这是一个轻量级的 HTTP 客户端,封装了 Apache HttpClient,提供简化的 GET 和 POST 操作。
 * 实现为单例模式,应用程序中共享一个实例以提高性能。
 *
 * <h2>主要特性</h2>
 * <ul>
 *   <li>单例模式:通过 {@link #INSTANCE} 访问
 *   <li>支持 GET 和 POST 请求
 *   <li>自动处理请求体序列化
 *   <li>支持自定义 HTTP 头
 *   <li>自动处理查询参数
 *   <li>连接池管理和重用
 * </ul>
 *
 * <h2>与 HttpClient 的区别</h2>
 * <ul>
 *   <li>SimpleHttpClient: 轻量级,直接返回字符串响应,适合简单场景
 *   <li>HttpClient: 功能完整,集成认证和错误处理,适合 REST Catalog 使用
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>简单的 HTTP 调用
 *   <li>不需要复杂认证的场景
 *   <li>工具类和测试代码
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // GET 请求
 * String response = SimpleHttpClient.INSTANCE.get("http://example.com/api/config");
 * System.out.println(response);
 *
 * // 带查询参数的 GET 请求
 * Map<String, String> params = new HashMap<>();
 * params.put("maxResults", "100");
 * params.put("pageToken", "abc123");
 * String response = SimpleHttpClient.INSTANCE.get(
 *     "http://example.com/api/tables",
 *     params,
 *     null
 * );
 *
 * // 带自定义头的 GET 请求
 * Map<String, String> headers = new HashMap<>();
 * headers.put("Authorization", "Bearer token123");
 * headers.put("Accept", "application/json");
 * String response = SimpleHttpClient.INSTANCE.get(
 *     "http://example.com/api/data",
 *     params,
 *     headers
 * );
 *
 * // POST 请求
 * Map<String, String> requestBody = new HashMap<>();
 * requestBody.put("name", "my_database");
 * requestBody.put("owner", "user1");
 *
 * Map<String, String> headers = new HashMap<>();
 * headers.put("Content-Type", "application/json");
 *
 * String response = SimpleHttpClient.INSTANCE.post(
 *     "http://example.com/api/databases",
 *     requestBody,
 *     headers
 * );
 *
 * // 使用完毕后关闭(通常在应用关闭时)
 * SimpleHttpClient.INSTANCE.close();
 * }</pre>
 *
 * <h2>错误处理</h2>
 * <p>如果请求失败或响应不成功,会抛出 {@link RuntimeException}:
 * <ul>
 *   <li>响应体为空 → "ResponseBody is null or empty."
 *   <li>HTTP 状态码非 2xx → "Response is not successful, response is ..."
 *   <li>网络错误 → "Failed to convert HTTP response body to string"
 * </ul>
 *
 * <h2>线程安全性</h2>
 * <p>内部的 {@link CloseableHttpClient} 使用连接池,是线程安全的,
 * 可以在多线程环境中共享使用。
 *
 * <h2>资源管理</h2>
 * <p>客户端实现了 {@link Closeable} 接口,应该在应用关闭时调用 {@link #close()} 方法释放连接资源。
 *
 * @see HttpClient
 * @see RESTUtil
 * @see HttpClientUtils
 */
public class SimpleHttpClient implements Closeable {

    /** 全局单例实例。 */
    public static final SimpleHttpClient INSTANCE = new SimpleHttpClient();

    /** 底层的 Apache HTTP 客户端。 */
    private final CloseableHttpClient client;

    /**
     * 私有构造函数,创建 HTTP 客户端实例。
     *
     * <p>使用 {@link HttpClientUtils#createBuilder()} 创建默认配置的客户端。
     */
    private SimpleHttpClient() {
        this.client = createBuilder().build();
    }

    /**
     * 发送 POST 请求。
     *
     * @param url 目标 URL
     * @param body 请求体,会被序列化为 JSON 或表单数据
     * @param headers HTTP 头部,可以为 null
     * @return 响应体字符串
     * @throws IOException 如果请求失败
     * @throws RuntimeException 如果响应不成功或响应体为空
     */
    public String post(String url, Object body, Map<String, String> headers) throws IOException {
        HttpPost httpPost = new HttpPost(url);
        if (headers != null) {
            httpPost.setHeaders(
                    headers.entrySet().stream()
                            .map(entry -> new BasicHeader(entry.getKey(), entry.getValue()))
                            .toArray(Header[]::new));
        }
        String encodedBody = RESTUtil.encodedBody(body);
        if (encodedBody != null) {
            httpPost.setEntity(new StringEntity(encodedBody));
        }

        return exec(httpPost);
    }

    /**
     * 发送带查询参数和自定义头的 GET 请求。
     *
     * @param url 目标 URL
     * @param queryParams 查询参数,会被自动编码并附加到 URL,可以为 null
     * @param headers HTTP 头部,可以为 null
     * @return 响应体字符串
     * @throws IOException 如果请求失败
     * @throws RuntimeException 如果响应不成功或响应体为空
     */
    public String get(String url, Map<String, String> queryParams, Map<String, String> headers)
            throws IOException {
        HttpGet httpGet = new HttpGet(RESTUtil.buildRequestUrl(url, queryParams));
        if (headers != null) {
            httpGet.setHeaders(
                    headers.entrySet().stream()
                            .map(entry -> new BasicHeader(entry.getKey(), entry.getValue()))
                            .toArray(Header[]::new));
        }
        return exec(httpGet);
    }

    /**
     * 发送简单的 GET 请求(无参数,无自定义头)。
     *
     * @param url 目标 URL
     * @return 响应体字符串
     * @throws IOException 如果请求失败
     * @throws RuntimeException 如果响应不成功或响应体为空
     */
    public String get(String url) throws IOException {
        return get(url, null, null);
    }

    /**
     * 执行 HTTP 请求并返回响应体字符串。
     *
     * <p>这是一个私有方法,用于实际执行请求并处理响应。
     *
     * @param request HTTP 请求对象 (GET 或 POST)
     * @return 响应体字符串
     * @throws RuntimeException 如果响应不成功、响应体为空或网络错误
     */
    private String exec(HttpUriRequestBase request) {
        try {
            return client.execute(
                    request,
                    response -> {
                        String responseBodyStr = RESTUtil.extractResponseBodyAsString(response);

                        if (StringUtils.isNullOrWhitespaceOnly(responseBodyStr)
                                || !RESTUtil.isSuccessful(response)) {
                            throw new RuntimeException(
                                    RESTUtil.isSuccessful(response)
                                            ? "ResponseBody is null or empty."
                                            : String.format(
                                                    "Response is not successful, response is %s",
                                                    response));
                        }
                        return responseBodyStr;
                    });
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to convert HTTP response body to string, error : " + e.getMessage());
        }
    }

    /**
     * 关闭 HTTP 客户端并释放连接资源。
     *
     * <p>应该在应用程序关闭时调用此方法,以确保所有连接被正确关闭。
     *
     * @throws IOException 如果关闭失败
     * @throws RuntimeException 包装任何关闭时发生的异常
     */
    @Override
    public void close() throws IOException {
        try {
            client.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
