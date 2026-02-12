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

package org.apache.paimon.rest.interceptor;

import org.apache.hc.core5.http.EntityDetails;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.HttpResponseInterceptor;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.http.protocol.HttpCoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;

/**
 * HTTP 请求日志拦截器。
 *
 * <p>这是一个 {@link HttpResponseInterceptor} 实现,用于记录 REST API 请求的详细信息。
 * 该拦截器在 HTTP 响应处理阶段执行,计算请求的执行时间并记录请求的元数据。
 *
 * <h3>记录的信息:</h3>
 * <ul>
 *   <li>请求 ID (x-request-id):用于跟踪和关联日志
 *   <li>HTTP 方法:GET、POST、PUT、DELETE 等
 *   <li>请求 URL:完整的请求路径
 *   <li>执行时长:从请求开始到响应返回的毫秒数
 * </ul>
 *
 * <h3>工作流程:</h3>
 * <ol>
 *   <li>{@link TimingInterceptor} 在请求开始时记录时间戳到上下文
 *   <li>HTTP 请求被处理
 *   <li>LoggingInterceptor 在响应时读取开始时间,计算执行时长
 *   <li>从响应头中提取 x-request-id
 *   <li>记录完整的请求信息到日志
 * </ol>
 *
 * <h3>日志格式:</h3>
 * <pre>
 * [rest] requestId:{id} method:{method} url:{url} duration:{ms}ms
 * </pre>
 *
 * <h3>使用示例:</h3>
 * <pre>{@code
 * // 创建 HTTP 客户端并注册拦截器
 * HttpClientBuilder builder = HttpClientBuilder.create();
 *
 * // 添加请求拦截器(记录开始时间)
 * builder.addRequestInterceptorFirst(new TimingInterceptor());
 *
 * // 添加响应拦截器(记录日志)
 * builder.addResponseInterceptorLast(new LoggingInterceptor());
 *
 * CloseableHttpClient httpClient = builder.build();
 *
 * // 执行请求时将自动记录日志
 * HttpGet request = new HttpGet("http://example.com/api/table/list");
 * httpClient.execute(request);
 *
 * // 日志输出示例:
 * // [rest] requestId:abc123 method:GET url:http://example.com/api/table/list duration:156ms
 * }</pre>
 *
 * <h3>请求 ID 处理:</h3>
 * <ul>
 *   <li>优先使用响应头中的 x-request-id 字段
 *   <li>如果响应头不包含 x-request-id,使用默认值 "unknown"
 *   <li>请求 ID 由服务器生成,用于分布式链路追踪
 * </ul>
 *
 * <h3>配置常量:</h3>
 * <ul>
 *   <li>{@link #REQUEST_ID_KEY} = "x-request-id":请求 ID 的响应头字段名
 *   <li>{@link #DEFAULT_REQUEST_ID} = "unknown":默认请求 ID
 *   <li>{@link #REQUEST_START_TIME_KEY} = "request-start-time":上下文中存储开始时间的键
 * </ul>
 *
 * <h3>异常处理:</h3>
 * <p>如果解析请求 URI 失败(URISyntaxException),将记录警告日志但不会中断请求处理。
 *
 * <h3>性能考虑:</h3>
 * <ul>
 *   <li>日志记录在响应阶段执行,不会增加请求处理延迟
 *   <li>使用 SLF4J 作为日志框架,支持异步日志
 *   <li>日志级别为 INFO,可以根据需要调整
 * </ul>
 *
 * @see TimingInterceptor 配合使用的请求时间记录拦截器
 * @see HttpResponseInterceptor Apache HttpClient 响应拦截器接口
 * @since 1.0
 */
public class LoggingInterceptor implements HttpResponseInterceptor {

    /** SLF4J 日志记录器。 */
    private static final Logger LOG = LoggerFactory.getLogger(LoggingInterceptor.class);

    /** 请求 ID 的响应头字段名。用于从响应头中提取请求 ID。 */
    public static final String REQUEST_ID_KEY = "x-request-id";

    /** 默认请求 ID。当响应头中不包含 x-request-id 时使用此值。 */
    public static final String DEFAULT_REQUEST_ID = "unknown";

    /** 请求开始时间的上下文键。用于从 HTTP 上下文中获取请求开始时间戳。 */
    public static final String REQUEST_START_TIME_KEY = "request-start-time";

    /**
     * 处理 HTTP 响应,记录请求日志。
     *
     * <p>此方法在 HTTP 响应返回时被调用,从上下文中读取请求开始时间,
     * 计算请求执行时长,并记录完整的请求信息。
     *
     * @param httpResponse HTTP 响应对象,包含响应头等信息
     * @param entityDetails 响应实体详情(可能为 null)
     * @param httpContext HTTP 上下文,包含请求和其他上下文信息
     */
    @Override
    public void process(
            HttpResponse httpResponse, EntityDetails entityDetails, HttpContext httpContext) {
        // 将通用上下文转换为 HTTP 核心上下文
        HttpCoreContext coreContext = HttpCoreContext.cast(httpContext);

        // 获取原始请求对象
        HttpRequest request = coreContext.getRequest();

        // 从上下文中获取请求开始时间戳
        Long startTime = (Long) coreContext.getAttribute(REQUEST_START_TIME_KEY);

        // 计算请求执行时长(毫秒)
        long durationMs = System.currentTimeMillis() - startTime;

        // 从响应头中提取请求 ID,如果不存在则使用默认值
        String requestId =
                httpResponse.getHeaders(REQUEST_ID_KEY).length > 0
                        ? httpResponse.getFirstHeader(REQUEST_ID_KEY).getValue()
                        : DEFAULT_REQUEST_ID;

        try {
            // 记录请求信息:请求ID、方法、URL、执行时长
            LOG.info(
                    "[rest] requestId:{} method:{} url:{} duration:{}ms",
                    requestId,
                    request.getMethod(),
                    request.getUri(),
                    durationMs);
        } catch (URISyntaxException e) {
            // URI 解析失败时记录警告,但不影响请求处理
            LOG.warn("Failed to log rest request: {}", e.getMessage());
        }
    }
}
