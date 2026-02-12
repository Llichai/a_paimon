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

import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableSet;

import org.apache.hc.client5.http.HttpRequestRetryStrategy;
import org.apache.hc.client5.http.utils.DateUtils;
import org.apache.hc.core5.concurrent.CancellableDependency;
import org.apache.hc.core5.http.ConnectionClosedException;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.util.TimeValue;

import javax.net.ssl.SSLException;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 指数退避的 HTTP 请求重试策略。
 *
 * <p>实现 Apache HttpClient 的 {@link HttpRequestRetryStrategy} 接口,
 * 提供智能的自动重试机制,用于处理临时性的网络错误和服务器过载。
 *
 * <h2>核心特性</h2>
 * <ul>
 *   <li><b>指数退避</b>: 重试间隔呈指数增长,避免对服务器造成压力
 *   <li><b>随机抖动</b>: 在重试间隔上添加随机抖动,避免惊群效应
 *   <li><b>幂等性检查</b>: 仅对幂等请求(GET、PUT、DELETE)进行重试
 *   <li><b>异常过滤</b>: 区分可重试和不可重试的异常
 *   <li><b>状态码检查</b>: 对特定的 HTTP 状态码(429、503)进行重试
 *   <li><b>Retry-After 支持</b>: 遵守服务器返回的 Retry-After 响应头
 * </ul>
 *
 * <h2>重试算法</h2>
 * <p><b>指数退避计算公式</b>:
 * <pre>
 * delayMillis = 1000 * min(2^(execCount - 1), 64)
 * jitter = random(0, delayMillis * 0.1)
 * actualDelay = delayMillis + jitter
 *
 * 重试次数 | 基础延迟 | 抖动范围    | 实际延迟范围
 * --------|---------|------------|----------------
 * 1       | 1秒     | 0-100ms    | 1.0-1.1秒
 * 2       | 2秒     | 0-200ms    | 2.0-2.2秒
 * 3       | 4秒     | 0-400ms    | 4.0-4.4秒
 * 4       | 8秒     | 0-800ms    | 8.0-8.8秒
 * 5       | 16秒    | 0-1600ms   | 16.0-17.6秒
 * 6       | 32秒    | 0-3200ms   | 32.0-35.2秒
 * 7+      | 64秒    | 0-6400ms   | 64.0-70.4秒(上限)
 * </pre>
 *
 * <h2>可重试的场景</h2>
 * <p><b>HTTP 状态码</b>:
 * <ul>
 *   <li><b>429 Too Many Requests</b>: 请求速率限制,服务器过载
 *   <li><b>503 Service Unavailable</b>: 服务暂时不可用
 * </ul>
 *
 * <p><b>HTTP 方法</b>:
 * <ul>
 *   <li>✅ GET - 幂等,可以安全重试
 *   <li>✅ PUT - 幂等,可以安全重试
 *   <li>✅ DELETE - 幂等,可以安全重试
 *   <li>✅ HEAD - 幂等,可以安全重试
 *   <li>✅ OPTIONS - 幂等,可以安全重试
 *   <li>❌ POST - 非幂等,不重试(可能导致重复创建资源)
 *   <li>❌ PATCH - 非幂等,不重试(可能导致重复修改)
 * </ul>
 *
 * <h2>不可重试的异常</h2>
 * <p>以下异常表示永久性错误,不应重试:
 * <ul>
 *   <li><b>InterruptedIOException</b>: 线程被中断,应该立即停止
 *   <li><b>UnknownHostException</b>: 主机名无法解析,重试也无法成功
 *   <li><b>ConnectException</b>: 连接被拒绝,服务未启动
 *   <li><b>ConnectionClosedException</b>: 连接已关闭
 *   <li><b>NoRouteToHostException</b>: 无法路由到主机,网络配置问题
 *   <li><b>SSLException</b>: SSL/TLS 握手失败,证书或配置问题
 * </ul>
 *
 * <h2>Retry-After 响应头</h2>
 * <p>服务器可以通过 Retry-After 响应头指定重试时间:
 * <pre>
 * HTTP/1.1 429 Too Many Requests
 * Retry-After: 120
 *
 * 或
 *
 * HTTP/1.1 503 Service Unavailable
 * Retry-After: Fri, 31 Dec 2025 23:59:59 GMT
 * </pre>
 *
 * <p>策略会优先使用 Retry-After 指定的时间,忽略指数退避算法。
 *
 * <h2>使用示例</h2>
 *
 * <p><b>创建 HTTP 客户端并配置重试策略</b>:
 * <pre>{@code
 * // 创建重试策略,最多重试 5 次
 * ExponentialHttpRequestRetryStrategy retryStrategy =
 *     new ExponentialHttpRequestRetryStrategy(5);
 *
 * // 配置 HTTP 客户端
 * CloseableHttpClient client = HttpClients.custom()
 *     .setRetryStrategy(retryStrategy)
 *     .build();
 *
 * // 执行请求,自动应用重试策略
 * HttpGet request = new HttpGet("http://api.example.com/data");
 * CloseableHttpResponse response = client.execute(request);
 * }</pre>
 *
 * <p><b>重试行为示例</b>:
 * <pre>{@code
 * // 场景1: 服务器返回 503,自动重试
 * GET /api/tables HTTP/1.1
 * → 503 Service Unavailable (第1次尝试)
 * → 等待 1 秒
 * → 503 Service Unavailable (第2次尝试)
 * → 等待 2 秒
 * → 200 OK (第3次尝试,成功!)
 *
 * // 场景2: 连接被拒绝,不重试
 * GET /api/tables HTTP/1.1
 * → ConnectException: Connection refused
 * → 立即失败,不重试
 *
 * // 场景3: POST 请求,不重试
 * POST /api/tables HTTP/1.1
 * → 503 Service Unavailable
 * → 不重试(POST 非幂等)
 *
 * // 场景4: 服务器指定 Retry-After
 * GET /api/tables HTTP/1.1
 * → 429 Too Many Requests
 * → Retry-After: 60
 * → 等待 60 秒(忽略指数退避)
 * → 200 OK (成功!)
 * }</pre>
 *
 * <h2>为什么使用指数退避</h2>
 * <ul>
 *   <li><b>避免雪崩</b>: 如果所有客户端同时重试,会加剧服务器负载
 *   <li><b>给服务器恢复时间</b>: 逐渐增加的延迟让服务器有时间恢复
 *   <li><b>公平性</b>: 随机抖动避免多个客户端同时重试
 *   <li><b>效率</b>: 临时性错误通常会在短时间内恢复
 * </ul>
 *
 * <h2>最佳实践</h2>
 * <ul>
 *   <li><b>合理设置最大重试次数</b>: 通常 3-5 次足够,太多会导致请求超时
 *   <li><b>幂等性设计</b>: 确保 PUT、DELETE 操作是幂等的,可以安全重试
 *   <li><b>超时配合</b>: 重试总时间不应超过客户端超时时间
 *   <li><b>监控重试</b>: 记录重试次数和原因,发现系统问题
 *   <li><b>优雅降级</b>: 重试失败后应该有降级策略
 * </ul>
 *
 * <h2>线程安全性</h2>
 * <p>此类是线程安全的,可以在多个线程中共享使用。
 * 内部使用 ThreadLocalRandom 生成随机抖动,避免线程竞争。
 *
 * @see HttpRequestRetryStrategy
 * @see HttpClientUtils#createBuilder()
 */
class ExponentialHttpRequestRetryStrategy implements HttpRequestRetryStrategy {

    /** 最大重试次数。 */
    private final int maxRetries;

    /** 不可重试的异常类型集合。 */
    private final Set<Class<? extends IOException>> nonRetriableExceptions;

    /** 可重试的 HTTP 状态码集合。 */
    private final Set<Integer> retriableCodes;

    /**
     * 创建指数退避重试策略。
     *
     * @param maximumRetries 最大重试次数,必须大于 0
     * @throws IllegalArgumentException 如果 maximumRetries <= 0
     */
    ExponentialHttpRequestRetryStrategy(int maximumRetries) {
        Preconditions.checkArgument(
                maximumRetries > 0,
                "Cannot set retries to %s, the value must be positive",
                maximumRetries);
        this.maxRetries = maximumRetries;
        this.retriableCodes =
                ImmutableSet.of(HttpStatus.SC_TOO_MANY_REQUESTS, HttpStatus.SC_SERVICE_UNAVAILABLE);
        this.nonRetriableExceptions =
                ImmutableSet.of(
                        InterruptedIOException.class,
                        UnknownHostException.class,
                        ConnectException.class,
                        ConnectionClosedException.class,
                        NoRouteToHostException.class,
                        SSLException.class);
    }

    /**
     * 判断是否应该重试因异常而失败的请求。
     *
     * <p><b>重试条件</b>:
     * <ol>
     *   <li>执行次数不超过最大重试次数
     *   <li>异常不在不可重试的异常列表中
     *   <li>请求未被取消
     *   <li>请求方法是幂等的(GET、PUT、DELETE 等)
     * </ol>
     *
     * <p>只有当所有条件都满足时才会重试。
     *
     * @param request HTTP 请求对象
     * @param exception 导致请求失败的异常
     * @param execCount 已执行的次数(包括首次尝试),从 1 开始
     * @param context HTTP 上下文
     * @return 如果应该重试返回 true,否则返回 false
     */
    @Override
    public boolean retryRequest(
            HttpRequest request, IOException exception, int execCount, HttpContext context) {
        if (execCount > maxRetries) {
            // Do not retry if over max retries
            return false;
        }

        if (nonRetriableExceptions.contains(exception.getClass())) {
            return false;
        } else {
            for (Class<? extends IOException> rejectException : nonRetriableExceptions) {
                if (rejectException.isInstance(exception)) {
                    return false;
                }
            }
        }

        if (request instanceof CancellableDependency
                && ((CancellableDependency) request).isCancelled()) {
            return false;
        }

        // Retry if the request is considered idempotent
        return Method.isIdempotent(request.getMethod());
    }

    /**
     * 判断是否应该重试收到响应的请求。
     *
     * <p>根据 HTTP 状态码决定是否重试:
     * <ul>
     *   <li><b>429 Too Many Requests</b>: 速率限制,应该重试
     *   <li><b>503 Service Unavailable</b>: 服务暂时不可用,应该重试
     * </ul>
     *
     * <p><b>重试条件</b>:
     * <ol>
     *   <li>执行次数不超过最大重试次数
     *   <li>响应状态码在可重试列表中
     * </ol>
     *
     * @param response HTTP 响应对象
     * @param execCount 已执行的次数(包括首次尝试),从 1 开始
     * @param context HTTP 上下文
     * @return 如果应该重试返回 true,否则返回 false
     */
    @Override
    public boolean retryRequest(HttpResponse response, int execCount, HttpContext context) {
        return execCount <= maxRetries && retriableCodes.contains(response.getCode());
    }

    /**
     * 获取重试间隔时间。
     *
     * <p>计算下一次重试前应该等待的时间,使用指数退避算法并添加随机抖动。
     *
     * <p><b>优先级</b>:
     * <ol>
     *   <li>如果响应包含 {@code Retry-After} 头部,优先使用服务器指定的时间
     *   <li>否则,使用指数退避算法计算重试间隔
     * </ol>
     *
     * <p><b>Retry-After 头部格式</b>:
     * <pre>
     * // 格式1: 秒数
     * Retry-After: 120
     *
     * // 格式2: HTTP 日期
     * Retry-After: Wed, 21 Oct 2025 07:28:00 GMT
     * </pre>
     *
     * <p><b>指数退避算法</b>:
     * <pre>{@code
     * // 基础延迟(毫秒) = 1000 * min(2^(execCount-1), 64)
     * delayMillis = 1000 * (int) Math.min(Math.pow(2.0, execCount - 1.0), 64.0);
     *
     * // 随机抖动(延迟的 0-10%)
     * jitter = ThreadLocalRandom.current().nextInt(Math.max(1, (int) (delayMillis * 0.1)));
     *
     * // 实际延迟
     * actualDelay = delayMillis + jitter;
     * }</pre>
     *
     * <p><b>重试间隔示例</b>:
     * <pre>
     * execCount | 基础延迟 | 抖动范围    | 实际延迟范围
     * ----------|---------|------------|----------------
     * 1         | 1秒     | 0-100ms    | 1.0-1.1秒
     * 2         | 2秒     | 0-200ms    | 2.0-2.2秒
     * 3         | 4秒     | 0-400ms    | 4.0-4.4秒
     * 4         | 8秒     | 0-800ms    | 8.0-8.8秒
     * 5         | 16秒    | 0-1600ms   | 16.0-17.6秒
     * 6         | 32秒    | 0-3200ms   | 32.0-35.2秒
     * 7+        | 64秒    | 0-6400ms   | 64.0-70.4秒(上限)
     * </pre>
     *
     * <p><b>为什么添加随机抖动</b>:
     * <ul>
     *   <li>避免惊群效应: 多个客户端不会同时重试
     *   <li>分散负载: 重试请求随机分布在时间段内
     *   <li>提高成功率: 减少服务器在同一时刻的突发负载
     * </ul>
     *
     * <p>使用示例:
     * <pre>{@code
     * // 示例1: 服务器指定 Retry-After
     * HTTP/1.1 429 Too Many Requests
     * Retry-After: 60
     * → 等待 60 秒(忽略指数退避)
     *
     * // 示例2: 第一次重试
     * execCount = 1
     * → 基础延迟 = 1秒
     * → 抖动 = 0-100ms
     * → 实际等待 1.0-1.1 秒
     *
     * // 示例3: 第五次重试
     * execCount = 5
     * → 基础延迟 = 16秒
     * → 抖动 = 0-1600ms
     * → 实际等待 16.0-17.6 秒
     * }</pre>
     *
     * @param response HTTP 响应对象
     * @param execCount 已执行的次数(包括首次尝试),从 1 开始
     * @param context HTTP 上下文
     * @return 重试前应该等待的时间
     */
    @Override
    public TimeValue getRetryInterval(HttpResponse response, int execCount, HttpContext context) {
        // a server may send a 429 / 503 with a Retry-After header
        // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After
        Header header = response.getFirstHeader(HttpHeaders.RETRY_AFTER);
        TimeValue retryAfter = null;
        if (header != null) {
            String value = header.getValue();
            try {
                retryAfter = TimeValue.ofSeconds(Long.parseLong(value));
            } catch (NumberFormatException ignore) {
                Instant retryAfterDate = DateUtils.parseStandardDate(value);
                if (retryAfterDate != null) {
                    retryAfter =
                            TimeValue.ofMilliseconds(
                                    retryAfterDate.toEpochMilli() - System.currentTimeMillis());
                }
            }

            if (TimeValue.isPositive(retryAfter)) {
                return retryAfter;
            }
        }

        int delayMillis = 1000 * (int) Math.min(Math.pow(2.0, (long) execCount - 1.0), 64.0);
        int jitter = ThreadLocalRandom.current().nextInt(Math.max(1, (int) (delayMillis * 0.1)));

        return TimeValue.ofMilliseconds(delayMillis + jitter);
    }
}
