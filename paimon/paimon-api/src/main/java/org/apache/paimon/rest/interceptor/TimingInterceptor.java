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
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpRequestInterceptor;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.http.protocol.HttpCoreContext;

import java.io.IOException;

import static org.apache.paimon.rest.interceptor.LoggingInterceptor.REQUEST_START_TIME_KEY;

/**
 * HTTP 请求计时拦截器。
 *
 * <p>这是一个 {@link HttpRequestInterceptor} 实现,用于记录 HTTP 请求的开始时间。
 * 该拦截器在请求发送前执行,将当前时间戳存储到 HTTP 上下文中,
 * 供后续的 {@link LoggingInterceptor} 使用来计算请求执行时长。
 *
 * <h3>主要功能:</h3>
 * <ul>
 *   <li>在请求发送前记录时间戳
 *   <li>将时间戳存储到 HTTP 上下文的属性中
 *   <li>配合 LoggingInterceptor 实现请求时长统计
 * </ul>
 *
 * <h3>工作流程:</h3>
 * <ol>
 *   <li>HTTP 客户端准备发送请求
 *   <li>TimingInterceptor.process() 被调用
 *   <li>记录当前时间戳到上下文 (键为 "request-start-time")
 *   <li>请求被发送到服务器
 *   <li>服务器处理请求
 *   <li>响应返回时,LoggingInterceptor 读取时间戳并计算时长
 * </ol>
 *
 * <h3>与 LoggingInterceptor 的配合:</h3>
 * <pre>
 * TimingInterceptor (请求前)    →  HTTP 请求处理  →  LoggingInterceptor (响应后)
 *     ↓                                                     ↓
 * 记录开始时间到上下文                              读取开始时间,计算时长
 * context.put("request-start-time", now)          duration = now - startTime
 * </pre>
 *
 * <h3>使用示例:</h3>
 * <pre>{@code
 * // 创建 HTTP 客户端
 * HttpClientBuilder builder = HttpClientBuilder.create();
 *
 * // 注册计时拦截器 (必须在请求发送前执行)
 * builder.addRequestInterceptorFirst(new TimingInterceptor());
 *
 * // 注册日志拦截器 (在响应返回后执行)
 * builder.addResponseInterceptorLast(new LoggingInterceptor());
 *
 * CloseableHttpClient httpClient = builder.build();
 *
 * // 执行请求
 * HttpGet request = new HttpGet("http://example.com/api/tables");
 * HttpResponse response = httpClient.execute(request);
 *
 * // 日志输出:
 * // [rest] requestId:abc123 method:GET url:http://example.com/api/tables duration:156ms
 * }</pre>
 *
 * <h3>上下文键:</h3>
 * <p>使用常量 {@link LoggingInterceptor#REQUEST_START_TIME_KEY} ("request-start-time")
 * 作为上下文属性的键名,确保 TimingInterceptor 和 LoggingInterceptor 使用相同的键。
 *
 * <h3>时间精度:</h3>
 * <p>使用 {@link System#currentTimeMillis()} 获取时间戳,精度为毫秒级。
 * 足够用于 REST API 请求的性能监控。
 *
 * <h3>应用场景:</h3>
 * <ul>
 *   <li>API 性能监控:统计每个请求的响应时间
 *   <li>慢查询分析:识别执行时间过长的请求
 *   <li>SLA 监控:检查是否满足服务级别协议
 *   <li>性能调优:找出性能瓶颈
 *   <li>分布式追踪:配合请求 ID 进行链路追踪
 * </ul>
 *
 * <h3>最佳实践:</h3>
 * <ul>
 *   <li>将 TimingInterceptor 添加为第一个请求拦截器,确保时间准确
 *   <li>将 LoggingInterceptor 添加为最后一个响应拦截器,包含完整处理时间
 *   <li>配合请求 ID 进行分布式链路追踪
 *   <li>在生产环境中启用,用于性能监控和故障排查
 * </ul>
 *
 * @see LoggingInterceptor 配合使用的日志记录拦截器
 * @see HttpRequestInterceptor Apache HttpClient 请求拦截器接口
 * @since 1.0
 */
public class TimingInterceptor implements HttpRequestInterceptor {

    /**
     * 处理 HTTP 请求,记录请求开始时间。
     *
     * <p>此方法在 HTTP 请求发送前被调用,将当前时间戳存储到 HTTP 上下文中,
     * 供后续的响应拦截器使用。
     *
     * @param httpRequest HTTP 请求对象
     * @param entityDetails 请求实体详情(可能为 null)
     * @param httpContext HTTP 上下文,用于存储请求开始时间
     * @throws HttpException 如果发生 HTTP 协议错误
     * @throws IOException 如果发生 I/O 错误
     */
    @Override
    public void process(
            HttpRequest httpRequest, EntityDetails entityDetails, HttpContext httpContext)
            throws HttpException, IOException {
        // 将通用上下文转换为 HTTP 核心上下文
        HttpCoreContext coreContext = HttpCoreContext.cast(httpContext);

        // 记录请求开始时间戳(毫秒)到上下文
        coreContext.setAttribute(REQUEST_START_TIME_KEY, System.currentTimeMillis());
    }
}
