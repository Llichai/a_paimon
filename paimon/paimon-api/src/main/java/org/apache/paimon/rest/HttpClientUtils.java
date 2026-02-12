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

import org.apache.paimon.rest.interceptor.LoggingInterceptor;
import org.apache.paimon.rest.interceptor.TimingInterceptor;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.client5.http.ssl.HttpsSupport;
import org.apache.hc.core5.reactor.ssl.SSLBufferMode;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.util.Timeout;

import java.io.IOException;
import java.io.InputStream;

/**
 * Apache HttpClient 构建器的工具类。
 *
 * <p>提供预配置的 HTTP 客户端实例和构建器,用于 REST Catalog 与远程服务器的通信。
 * 统一管理 HTTP 客户端的配置,包括连接池、超时、重试策略、TLS 支持等。
 *
 * <h2>核心功能</h2>
 * <ul>
 *   <li><b>默认客户端</b>: 提供预配置的全局 HTTP 客户端实例
 *   <li><b>日志拦截器</b>: 自动添加请求追踪和日志记录功能
 *   <li><b>连接池管理</b>: 配置最大连接数和每路由的最大连接数
 *   <li><b>超时控制</b>: 设置连接和响应超时
 *   <li><b>重试策略</b>: 支持指数退避的自动重试
 *   <li><b>TLS 支持</b>: 启用 TLS 1.2 和 1.3 协议
 * </ul>
 *
 * <h2>默认配置</h2>
 * <pre>
 * 连接池:
 * - 最大连接数: 100
 * - 每个路由的最大连接数: 100
 *
 * 超时设置:
 * - 连接请求超时: 3 分钟
 * - 响应超时: 3 分钟
 *
 * 重试策略:
 * - 最大重试次数: 5 次
 * - 重试算法: 指数退避 + 随机抖动
 *
 * TLS 支持:
 * - 协议: TLSv1.2, TLSv1.3
 * - 主机名验证: 默认验证器
 * </pre>
 *
 * <h2>拦截器链</h2>
 * <p>默认的日志客户端包含以下拦截器:
 * <pre>
 * 请求拦截器:
 * └── TimingInterceptor (first) - 记录请求开始时间
 *
 * 响应拦截器:
 * └── LoggingInterceptor (last) - 记录请求详情和耗时
 * </pre>
 *
 * <h2>使用示例</h2>
 *
 * <p><b>使用默认客户端</b>:
 * <pre>{@code
 * // 直接使用预配置的客户端
 * CloseableHttpClient client = HttpClientUtils.DEFAULT_HTTP_CLIENT;
 *
 * HttpGet request = new HttpGet("http://localhost:8080/api/tables");
 * CloseableHttpResponse response = client.execute(request);
 * }</pre>
 *
 * <p><b>创建带日志的自定义客户端</b>:
 * <pre>{@code
 * // 基于日志构建器创建客户端
 * HttpClientBuilder builder = HttpClientUtils.createLoggingBuilder();
 *
 * // 添加自定义配置
 * builder.setDefaultHeaders(Arrays.asList(
 *     new BasicHeader("User-Agent", "Paimon/1.0")
 * ));
 *
 * CloseableHttpClient client = builder.build();
 * }</pre>
 *
 * <p><b>创建基础客户端</b>:
 * <pre>{@code
 * // 创建不带拦截器的基础客户端
 * HttpClientBuilder builder = HttpClientUtils.createBuilder();
 *
 * // 添加自定义拦截器
 * builder.addRequestInterceptorFirst(new MyAuthInterceptor());
 * builder.addResponseInterceptorLast(new MyMetricsInterceptor());
 *
 * CloseableHttpClient client = builder.build();
 * }</pre>
 *
 * <p><b>下载文件流</b>:
 * <pre>{@code
 * // 从 URI 获取输入流
 * try (InputStream in = HttpClientUtils.getAsInputStream(
 *         "http://localhost:8080/api/files/data.parquet")) {
 *     // 处理流数据
 *     byte[] buffer = new byte[8192];
 *     int bytesRead;
 *     while ((bytesRead = in.read(buffer)) != -1) {
 *         // 处理数据
 *     }
 * }
 * }</pre>
 *
 * <h2>连接池管理</h2>
 * <p>使用 PoolingHttpClientConnectionManager 管理连接:
 * <ul>
 *   <li>支持 HTTP/1.1 持久连接(Keep-Alive)
 *   <li>自动复用连接,减少建立连接的开销
 *   <li>当连接池满时,请求会阻塞等待可用连接
 *   <li>支持每个路由(目标服务器)独立的连接限制
 * </ul>
 *
 * <h2>TLS/SSL 配置</h2>
 * <p>默认启用安全的 TLS 配置:
 * <ul>
 *   <li><b>支持的协议</b>: TLS 1.2, TLS 1.3(不支持旧的 SSLv3, TLS 1.0, TLS 1.1)
 *   <li><b>SSL 上下文</b>: 使用系统默认的信任存储
 *   <li><b>主机名验证</b>: 启用,防止中间人攻击
 *   <li><b>缓冲模式</b>: 静态缓冲,提高性能
 * </ul>
 *
 * <h2>重试策略</h2>
 * <p>使用 {@link ExponentialHttpRequestRetryStrategy} 实现智能重试:
 * <ul>
 *   <li>最大重试 5 次
 *   <li>仅对幂等请求(GET、PUT、DELETE)重试
 *   <li>对临时性错误(429、503)自动重试
 *   <li>重试间隔: 1秒、2秒、4秒、8秒、16秒
 *   <li>添加随机抖动,避免惊群效应
 *   <li>支持服务器的 Retry-After 响应头
 * </ul>
 *
 * <h2>线程安全性</h2>
 * <p>DEFAULT_HTTP_CLIENT 是线程安全的单例,可以在多个线程中共享使用。
 * 内部的连接池会自动处理并发请求。
 *
 * <h2>资源管理</h2>
 * <p>CloseableHttpClient 实现了 Closeable 接口,使用完毕后应该关闭:
 * <pre>{@code
 * CloseableHttpClient client = HttpClientUtils.createBuilder().build();
 * try {
 *     // 使用客户端
 * } finally {
 *     client.close(); // 释放连接池资源
 * }
 * }</pre>
 *
 * <p>注意: DEFAULT_HTTP_CLIENT 是全局单例,不应该手动关闭。
 *
 * <h2>性能优化建议</h2>
 * <ul>
 *   <li><b>复用客户端</b>: 创建客户端开销较大,应该复用同一个实例
 *   <li><b>使用连接池</b>: 避免为每个请求创建新的客户端
 *   <li><b>合理设置超时</b>: 根据实际网络情况调整超时时间
 *   <li><b>释放响应</b>: 确保响应被完全消费或关闭
 * </ul>
 *
 * @see HttpClientBuilder
 * @see ExponentialHttpRequestRetryStrategy
 * @see TimingInterceptor
 * @see LoggingInterceptor
 */
public class HttpClientUtils {

    /**
     * 默认的 HTTP 客户端实例。
     *
     * <p>这是一个预配置的单例客户端,包含:
     * <ul>
     *   <li>请求计时拦截器({@link TimingInterceptor})
     *   <li>日志记录拦截器({@link LoggingInterceptor})
     *   <li>连接池管理
     *   <li>超时配置
     *   <li>指数退避重试策略
     *   <li>TLS 支持
     * </ul>
     *
     * <p>线程安全,可以在多个线程中共享使用。
     *
     * @see #createLoggingBuilder()
     */
    public static final CloseableHttpClient DEFAULT_HTTP_CLIENT = createLoggingBuilder().build();

    /**
     * 创建带日志功能的 HTTP 客户端构建器。
     *
     * <p>在基础构建器的基础上,添加请求追踪和日志记录拦截器:
     * <ul>
     *   <li>{@link TimingInterceptor}: 在请求开始时记录时间戳
     *   <li>{@link LoggingInterceptor}: 在响应完成时记录请求详情和耗时
     * </ul>
     *
     * <p>拦截器的执行顺序:
     * <pre>
     * 请求阶段:
     * TimingInterceptor (first) → 其他拦截器 → 发送请求
     *
     * 响应阶段:
     * 接收响应 → 其他拦截器 → LoggingInterceptor (last)
     * </pre>
     *
     * <p>使用示例:
     * <pre>{@code
     * // 创建带日志的客户端
     * CloseableHttpClient client = HttpClientUtils.createLoggingBuilder().build();
     *
     * // 执行请求,自动记录日志
     * HttpGet request = new HttpGet("http://localhost:8080/api/tables");
     * CloseableHttpResponse response = client.execute(request);
     *
     * // 日志输出示例:
     * // [INFO] HTTP GET http://localhost:8080/api/tables completed in 125ms with status 200
     * }</pre>
     *
     * @return 配置好日志拦截器的 HTTP 客户端构建器
     * @see TimingInterceptor
     * @see LoggingInterceptor
     * @see #createBuilder()
     */
    public static HttpClientBuilder createLoggingBuilder() {
        HttpClientBuilder clientBuilder = createBuilder();
        clientBuilder
                .addRequestInterceptorFirst(new TimingInterceptor())
                .addResponseInterceptorLast(new LoggingInterceptor());
        return clientBuilder;
    }

    /**
     * 创建基础的 HTTP 客户端构建器。
     *
     * <p>提供预配置的 HTTP 客户端构建器,包含以下默认配置:
     *
     * <p><b>超时配置</b>:
     * <ul>
     *   <li>连接请求超时: 3 分钟 - 从连接池获取连接的超时时间
     *   <li>响应超时: 3 分钟 - 等待服务器响应的超时时间
     * </ul>
     *
     * <p><b>连接池配置</b>:
     * <ul>
     *   <li>最大连接数: 100 - 连接池中的最大连接数
     *   <li>每个路由的最大连接数: 100 - 对同一目标服务器的最大并发连接数
     * </ul>
     *
     * <p><b>重试策略</b>:
     * <ul>
     *   <li>使用 {@link ExponentialHttpRequestRetryStrategy}
     *   <li>最大重试次数: 5 次
     *   <li>重试间隔: 指数退避(1秒、2秒、4秒、8秒、16秒)
     * </ul>
     *
     * <p><b>TLS 支持</b>:
     * <ul>
     *   <li>支持的协议: TLS 1.2, TLS 1.3
     *   <li>启用主机名验证
     * </ul>
     *
     * <p>使用示例:
     * <pre>{@code
     * // 创建基础客户端
     * HttpClientBuilder builder = HttpClientUtils.createBuilder();
     *
     * // 自定义配置
     * builder.setDefaultHeaders(Arrays.asList(
     *     new BasicHeader("User-Agent", "MyApp/1.0"),
     *     new BasicHeader("Accept", "application/json")
     * ));
     *
     * // 添加自定义拦截器
     * builder.addRequestInterceptorFirst(new MyAuthInterceptor());
     *
     * // 构建客户端
     * CloseableHttpClient client = builder.build();
     * }</pre>
     *
     * <p><b>超时时间说明</b>:
     * <ul>
     *   <li><b>连接请求超时</b>: 控制从连接池获取连接的等待时间。如果连接池已满,
     *       请求会阻塞等待可用连接,超过此时间会抛出 ConnectionPoolTimeoutException
     *   <li><b>响应超时</b>: 控制等待服务器返回数据的时间。包括建立连接、发送请求、
     *       接收响应的总时间,超过此时间会抛出 SocketTimeoutException
     * </ul>
     *
     * @return 预配置的 HTTP 客户端构建器
     * @see #createLoggingBuilder()
     * @see #configureConnectionManager()
     * @see ExponentialHttpRequestRetryStrategy
     */
    public static HttpClientBuilder createBuilder() {
        HttpClientBuilder clientBuilder = HttpClients.custom();
        RequestConfig requestConfig =
                RequestConfig.custom()
                        .setConnectionRequestTimeout(Timeout.ofMinutes(3))
                        .setResponseTimeout(Timeout.ofMinutes(3))
                        .build();
        clientBuilder.setDefaultRequestConfig(requestConfig);

        clientBuilder.setConnectionManager(configureConnectionManager());
        clientBuilder.setRetryStrategy(new ExponentialHttpRequestRetryStrategy(5));
        return clientBuilder;
    }

    /**
     * 配置 HTTP 客户端的连接管理器。
     *
     * <p>使用 PoolingHttpClientConnectionManager 提供连接池功能:
     *
     * <p><b>连接池配置</b>:
     * <ul>
     *   <li><b>最大连接数</b>: 100 - 连接池中可以保持的最大连接数
     *   <li><b>每个路由的最大连接数</b>: 100 - 对同一目标(协议+主机+端口)的最大并发连接数
     *   <li><b>使用系统属性</b>: 允许通过系统属性覆盖默认配置
     * </ul>
     *
     * <p><b>TLS 配置</b>:
     * <ul>
     *   <li><b>支持的协议</b>: TLS 1.2, TLS 1.3
     *   <li><b>SSL 上下文</b>: 使用系统默认的信任存储(cacerts)
     *   <li><b>缓冲模式</b>: 静态缓冲(SSLBufferMode.STATIC),提高性能
     *   <li><b>主机名验证</b>: 使用默认的主机名验证器,防止中间人攻击
     * </ul>
     *
     * <p><b>工作原理</b>:
     * <pre>
     * 1. 客户端发起请求
     * 2. 连接管理器检查连接池
     *    ├─ 存在可用连接 → 复用连接
     *    └─ 不存在可用连接
     *       ├─ 连接数 < 最大连接数 → 创建新连接
     *       └─ 连接数 >= 最大连接数 → 等待连接释放(最多等待连接请求超时时间)
     * 3. 请求完成后,连接返回连接池(而不是关闭)
     * </pre>
     *
     * <p><b>性能优化</b>:
     * <ul>
     *   <li>连接复用显著减少 TCP 握手开销
     *   <li>对于 HTTPS,避免重复的 TLS 握手
     *   <li>连接池自动管理连接的生命周期
     *   <li>支持 HTTP/1.1 持久连接(Keep-Alive)
     * </ul>
     *
     * <p><b>路由的概念</b>:
     * <pre>
     * 路由 = 协议 + 主机 + 端口
     *
     * 例如,以下是不同的路由:
     * - http://host1:8080  (路由1)
     * - http://host2:8080  (路由2)
     * - https://host1:443  (路由3)
     * - http://host1:9090  (路由4)
     * </pre>
     *
     * <p><b>TLS 安全性</b>:
     * <ul>
     *   <li>不支持已废弃的 SSLv3, TLS 1.0, TLS 1.1
     *   <li>使用系统信任的 CA 证书验证服务器证书
     *   <li>验证证书中的主机名与请求的主机名匹配
     * </ul>
     *
     * @return 配置好的连接管理器
     */
    private static HttpClientConnectionManager configureConnectionManager() {
        PoolingHttpClientConnectionManagerBuilder connectionManagerBuilder =
                PoolingHttpClientConnectionManagerBuilder.create();
        connectionManagerBuilder.useSystemProperties().setMaxConnTotal(100).setMaxConnPerRoute(100);

        // support TLS
        String[] tlsProtocols = {"TLSv1.2", "TLSv1.3"};
        connectionManagerBuilder.setTlsSocketStrategy(
                new DefaultClientTlsStrategy(
                        SSLContexts.createDefault(),
                        tlsProtocols,
                        null,
                        SSLBufferMode.STATIC,
                        HttpsSupport.getDefaultHostnameVerifier()));

        return connectionManagerBuilder.build();
    }

    /**
     * 从指定 URI 获取输入流。
     *
     * <p>执行 HTTP GET 请求并返回响应体的输入流。使用默认的 HTTP 客户端({@link #DEFAULT_HTTP_CLIENT})。
     *
     * <p><b>工作流程</b>:
     * <pre>
     * 1. 创建 HTTP GET 请求
     * 2. 使用默认客户端执行请求
     * 3. 检查响应状态码
     *    ├─ 200 OK → 返回响应体输入流
     *    └─ 其他状态码 → 抛出 RuntimeException
     * </pre>
     *
     * <p>使用示例:
     * <pre>{@code
     * // 下载文件
     * String uri = "http://localhost:8080/api/files/data.parquet";
     * try (InputStream in = HttpClientUtils.getAsInputStream(uri);
     *      FileOutputStream out = new FileOutputStream("local.parquet")) {
     *
     *     byte[] buffer = new byte[8192];
     *     int bytesRead;
     *     while ((bytesRead = in.read(buffer)) != -1) {
     *         out.write(buffer, 0, bytesRead);
     *     }
     * }
     * }</pre>
     *
     * <p><b>注意事项</b>:
     * <ul>
     *   <li><b>资源管理</b>: 必须关闭返回的 InputStream,否则会导致连接泄漏
     *   <li><b>错误处理</b>: 仅对 200 状态码返回流,其他状态码会抛出异常
     *   <li><b>超时</b>: 使用默认客户端的超时配置(3分钟)
     *   <li><b>重试</b>: 自动应用指数退避重试策略
     * </ul>
     *
     * <p><b>连接泄漏预防</b>:
     * <pre>{@code
     * // 正确: 使用 try-with-resources
     * try (InputStream in = HttpClientUtils.getAsInputStream(uri)) {
     *     // 处理流
     * } // 自动关闭流
     *
     * // 错误: 未关闭流
     * InputStream in = HttpClientUtils.getAsInputStream(uri);
     * // 处理流
     * // 忘记关闭 → 连接泄漏!
     * }</pre>
     *
     * @param uri 要请求的 URI,必须是完整的 HTTP/HTTPS URL
     * @return 响应体的输入流
     * @throws IOException 如果网络 I/O 发生错误
     * @throws RuntimeException 如果 HTTP 响应状态码不是 200
     */
    public static InputStream getAsInputStream(String uri) throws IOException {
        HttpGet httpGet = new HttpGet(uri);
        CloseableHttpResponse response = DEFAULT_HTTP_CLIENT.execute(httpGet);
        if (response.getCode() != 200) {
            throw new RuntimeException("HTTP error code: " + response.getCode());
        }
        return response.getEntity().getContent();
    }
}
