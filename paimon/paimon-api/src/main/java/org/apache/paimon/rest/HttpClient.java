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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.rest.auth.RESTAuthFunction;
import org.apache.paimon.rest.auth.RESTAuthParameter;
import org.apache.paimon.rest.exceptions.RESTException;
import org.apache.paimon.rest.interceptor.LoggingInterceptor;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicHeader;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.apache.paimon.rest.HttpClientUtils.DEFAULT_HTTP_CLIENT;

/**
 * REST Catalog 的 Apache HTTP 客户端实现。
 *
 * <p>这是 {@link RESTClient} 接口的具体实现,使用 Apache HttpClient 5.x 作为底层 HTTP 库。
 * 提供完整的 REST API 请求能力,包括 GET、POST、DELETE 等 HTTP 方法。
 *
 * <h2>主要特性</h2>
 * <ul>
 *   <li><b>连接池管理</b>: 使用 Apache HttpClient 的连接池,提高性能
 *   <li><b>认证集成</b>: 集成 {@link RESTAuthFunction},支持多种认证机制
 *   <li><b>错误处理</b>: 自动解析错误响应并抛出相应异常
 *   <li><b>请求追踪</b>: 支持请求 ID 追踪,方便日志关联和问题排查
 *   <li><b>自动重试</b>: 底层 HTTP 客户端支持指数退避重试策略
 * </ul>
 *
 * <h2>认证流程</h2>
 * <pre>
 * 1. 构造 HTTP 请求(GET/POST/DELETE)
 * 2. 调用 getHeaders() 生成认证头部
 * 3. RESTAuthFunction.apply() → AuthProvider.mergeAuthHeader()
 * 4. 将认证头部添加到 HTTP 请求
 * 5. 发送请求到服务器
 * 6. 处理响应或错误
 * </pre>
 *
 * <h2>错误处理机制</h2>
 * <p>根据 HTTP 状态码自动映射到不同的异常类型:
 * <ul>
 *   <li>2xx → 成功,返回响应对象
 *   <li>404 → {@link NoSuchResourceException}
 *   <li>409 → {@link AlreadyExistsException}
 *   <li>403 → {@link ForbiddenException}
 *   <li>其他 → {@link RESTException}
 * </ul>
 *
 * <h2>请求 ID 追踪</h2>
 * <p>支持从响应头中提取请求 ID,用于日志关联:
 * <ul>
 *   <li>优先使用 {@code X-Request-Id} 头部
 *   <li>如果不存在,查找任何包含 "request-id" 的头部
 *   <li>如果都不存在,使用默认值 "unknown"
 * </ul>
 *
 * <h2>URI 规范化</h2>
 * <p>构造函数会自动规范化 URI:
 * <pre>{@code
 * // 输入              → 规范化后
 * "localhost:8080"   → "http://localhost:8080"
 * "http://host/"     → "http://host"
 * "https://host:443" → "https://host:443"
 * }</pre>
 *
 * <h2>使用示例</h2>
 *
 * <p><b>基础使用</b>:
 * <pre>{@code
 * // 1. 创建客户端
 * HttpClient client = new HttpClient("http://localhost:8080");
 *
 * // 2. 配置认证
 * Map<String, String> baseHeaders = Collections.singletonMap("Content-Type", "application/json");
 * AuthProvider authProvider = new BearTokenAuthProvider("my-token");
 * RESTAuthFunction authFunction = new RESTAuthFunction(baseHeaders, authProvider);
 *
 * // 3. 发送请求
 * GetDatabaseResponse response = client.get(
 *     "/v1/databases/mydb",
 *     GetDatabaseResponse.class,
 *     authFunction
 * );
 * }</pre>
 *
 * <p><b>GET 请求(带查询参数)</b>:
 * <pre>{@code
 * Map<String, String> queryParams = new HashMap<>();
 * queryParams.put("maxResults", "100");
 * queryParams.put("pageToken", "abc123");
 *
 * ListTablesResponse response = client.get(
 *     "/v1/databases/mydb/tables",
 *     queryParams,
 *     ListTablesResponse.class,
 *     authFunction
 * );
 * }</pre>
 *
 * <p><b>POST 请求</b>:
 * <pre>{@code
 * CreateDatabaseRequest request = new CreateDatabaseRequest("mydb", properties);
 *
 * // POST 无响应体
 * client.post("/v1/databases", request, authFunction);
 *
 * // POST 有响应体
 * CreateDatabaseResponse response = client.post(
 *     "/v1/databases",
 *     request,
 *     CreateDatabaseResponse.class,
 *     authFunction
 * );
 * }</pre>
 *
 * <p><b>DELETE 请求</b>:
 * <pre>{@code
 * // 简单 DELETE
 * client.delete("/v1/databases/mydb", authFunction);
 *
 * // 带请求体的 DELETE
 * DeleteRequest deleteRequest = new DeleteRequest(...);
 * client.delete("/v1/databases/mydb", deleteRequest, authFunction);
 * }</pre>
 *
 * <h2>自定义错误处理</h2>
 * <pre>{@code
 * HttpClient client = new HttpClient("http://localhost:8080");
 *
 * // 设置自定义错误处理器
 * client.setErrorHandler(new ErrorHandler() {
 *     @Override
 *     public void accept(ErrorResponse error, String requestId) {
 *         // 自定义错误处理逻辑
 *         LOG.error("Request {} failed: {}", requestId, error.message());
 *         throw new CustomException(error.message());
 *     }
 * });
 * }</pre>
 *
 * <h2>线程安全性</h2>
 * <p>HttpClient 是线程安全的,可以在多个线程中共享使用。
 * 内部的 Apache HttpClient 使用连接池,能够高效处理并发请求。
 *
 * <h2>底层 HTTP 客户端配置</h2>
 * <p>使用 {@link HttpClientUtils#DEFAULT_HTTP_CLIENT},包含以下特性:
 * <ul>
 *   <li>连接池: 最大连接数和每个路由的最大连接数
 *   <li>超时配置: 连接超时、socket 超时、请求超时
 *   <li>重试策略: 指数退避重试
 *   <li>拦截器: 日志记录、请求追踪
 * </ul>
 *
 * @see RESTClient
 * @see RESTAuthFunction
 * @see ErrorHandler
 * @see HttpClientUtils
 */
public class HttpClient implements RESTClient {

    /** REST 服务器的规范化 URI,末尾不包含斜杠。 */
    private final String uri;

    /** 错误处理器,用于处理 HTTP 错误响应。 */
    private ErrorHandler errorHandler;

    /**
     * 创建 HTTP 客户端。
     *
     * <p>URI 会被自动规范化:
     * <ul>
     *   <li>如果不包含协议,自动添加 "http://"
     *   <li>移除末尾的斜杠
     * </ul>
     *
     * @param uri REST 服务器的 URI
     */
    public HttpClient(String uri) {
        this.uri = normalizeUri(uri);
        this.errorHandler = DefaultErrorHandler.getInstance();
    }

    @Override
    public <T extends RESTResponse> T get(
            String path, Class<T> responseType, RESTAuthFunction restAuthFunction) {
        Header[] authHeaders = getHeaders(path, "GET", "", restAuthFunction);
        HttpGet httpGet = new HttpGet(getRequestUrl(path, null));
        httpGet.setHeaders(authHeaders);
        return exec(httpGet, responseType);
    }

    @Override
    public <T extends RESTResponse> T get(
            String path,
            Map<String, String> queryParams,
            Class<T> responseType,
            RESTAuthFunction restAuthFunction) {
        Header[] authHeaders = getHeaders(path, queryParams, "GET", "", restAuthFunction);
        HttpGet httpGet = new HttpGet(getRequestUrl(path, queryParams));
        httpGet.setHeaders(authHeaders);
        return exec(httpGet, responseType);
    }

    @Override
    public <T extends RESTResponse> T post(
            String path, RESTRequest body, RESTAuthFunction restAuthFunction) {
        return post(path, body, null, restAuthFunction);
    }

    @Override
    public <T extends RESTResponse> T post(
            String path,
            RESTRequest body,
            Class<T> responseType,
            RESTAuthFunction restAuthFunction) {
        HttpPost httpPost = new HttpPost(getRequestUrl(path, null));
        String encodedBody = RESTUtil.encodedBody(body);
        if (encodedBody != null) {
            httpPost.setEntity(new StringEntity(encodedBody));
        }
        Header[] authHeaders = getHeaders(path, "POST", encodedBody, restAuthFunction);
        httpPost.setHeaders(authHeaders);
        return exec(httpPost, responseType);
    }

    @Override
    public <T extends RESTResponse> T delete(String path, RESTAuthFunction restAuthFunction) {
        return delete(path, null, restAuthFunction);
    }

    @Override
    public <T extends RESTResponse> T delete(
            String path, RESTRequest body, RESTAuthFunction restAuthFunction) {
        HttpDelete httpDelete = new HttpDelete(getRequestUrl(path, null));
        String encodedBody = RESTUtil.encodedBody(body);
        if (encodedBody != null) {
            httpDelete.setEntity(new StringEntity(encodedBody));
        }
        Header[] authHeaders = getHeaders(path, "DELETE", encodedBody, restAuthFunction);
        httpDelete.setHeaders(authHeaders);
        return exec(httpDelete, null);
    }

    @VisibleForTesting
    void setErrorHandler(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    private <T extends RESTResponse> T exec(HttpUriRequestBase request, Class<T> responseType) {
        try {
            return DEFAULT_HTTP_CLIENT.execute(
                    request,
                    response -> {
                        String responseBodyStr = RESTUtil.extractResponseBodyAsString(response);
                        if (!RESTUtil.isSuccessful(response)) {
                            ErrorResponse error;
                            try {
                                error = RESTApi.fromJson(responseBodyStr, ErrorResponse.class);
                            } catch (JsonProcessingException e) {
                                error =
                                        new ErrorResponse(
                                                null,
                                                null,
                                                responseBodyStr != null
                                                        ? responseBodyStr
                                                        : "response body is null",
                                                response.getCode());
                            }
                            errorHandler.accept(error, extractRequestId(response));
                        }
                        if (responseType != null && responseBodyStr != null) {
                            return RESTApi.fromJson(responseBodyStr, responseType);
                        } else if (responseType == null) {
                            return null;
                        } else {
                            throw new RESTException("response body is null.");
                        }
                    });
        } catch (IOException e) {
            throw new RESTException(
                    e, "Error occurred while processing %s request", request.getMethod());
        }
    }

    private String normalizeUri(String rawUri) {
        if (StringUtils.isEmpty(rawUri)) {
            throw new IllegalArgumentException("uri is empty which must be defined.");
        }

        String normalized =
                rawUri.endsWith("/") ? rawUri.substring(0, rawUri.length() - 1) : rawUri;

        if (!normalized.startsWith("http://") && !normalized.startsWith("https://")) {
            normalized = String.format("http://%s", normalized);
        }

        return normalized;
    }

    @VisibleForTesting
    protected String getRequestUrl(String path, Map<String, String> queryParams) {
        String fullPath = StringUtils.isNullOrWhitespaceOnly(path) ? uri : uri + path;
        return RESTUtil.buildRequestUrl(fullPath, queryParams);
    }

    @VisibleForTesting
    public String uri() {
        return uri;
    }

    private static String extractRequestId(ClassicHttpResponse response) {
        Header header = response.getFirstHeader(LoggingInterceptor.REQUEST_ID_KEY);
        if (header != null && header.getValue() != null) {
            return header.getValue();
        }

        // look for any header containing "request-id"
        return Arrays.stream(response.getHeaders())
                .filter(
                        h ->
                                h.getName() != null
                                        && h.getName().toLowerCase().contains("request-id"))
                .map(Header::getValue)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(LoggingInterceptor.DEFAULT_REQUEST_ID);
    }

    private static Header[] getHeaders(
            String path,
            String method,
            String data,
            Function<RESTAuthParameter, Map<String, String>> headerFunction) {
        return getHeaders(path, Collections.emptyMap(), method, data, headerFunction);
    }

    private static Header[] getHeaders(
            String path,
            Map<String, String> queryParams,
            String method,
            String data,
            Function<RESTAuthParameter, Map<String, String>> headerFunction) {
        if (headerFunction == null) {
            return new Header[0];
        }
        RESTAuthParameter restAuthParameter =
                new RESTAuthParameter(path, queryParams, method, data);
        Map<String, String> headers = headerFunction.apply(restAuthParameter);
        return headers.entrySet().stream()
                .map(entry -> new BasicHeader(entry.getKey(), entry.getValue()))
                .toArray(Header[]::new);
    }
}
