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

package org.apache.paimon.rest.exceptions;

/**
 * HTTP 503 - Service Unavailable 异常。
 *
 * <p>当服务暂时无法处理请求时抛出此异常。表示服务器当前无法处理请求,但这是临时状态,
 * 过一段时间后可能会恢复。
 *
 * <h3>触发条件:</h3>
 * <ul>
 *   <li>服务器正在维护或升级中
 *   <li>服务器过载,无法处理更多请求
 *   <li>后端服务(如数据库、存储)不可用
 *   <li>连接池耗尽,无可用连接
 *   <li>服务正在启动或关闭过程中
 *   <li>触发了限流或熔断机制
 *   <li>依赖的外部服务不可达
 * </ul>
 *
 * <h3>HTTP 状态码: 503 Service Unavailable</h3>
 * <p>这是临时性的服务器错误,表示服务器暂时无法处理请求。通常服务器会在响应头中包含
 * Retry-After 字段,告诉客户端多久后可以重试。
 *
 * <h3>与维护模式:</h3>
 * <p>503 常用于表示服务进入维护模式。此时服务器能够响应请求,但选择不处理以避免数据不一致。
 *
 * <h3>使用示例:</h3>
 * <pre>{@code
 * // 服务器过载
 * if (activeConnections > maxConnections) {
 *     throw new ServiceUnavailableException(
 *         "Server is overloaded. Active connections: %d, max: %d",
 *         activeConnections, maxConnections);
 * }
 *
 * // 维护模式
 * if (isMaintenanceMode()) {
 *     throw new ServiceUnavailableException(
 *         "Service is under maintenance. Please try again later.");
 * }
 *
 * // 后端服务不可用
 * if (!isDatabaseAvailable()) {
 *     throw new ServiceUnavailableException(
 *         "Database service is temporarily unavailable");
 * }
 *
 * // 连接池耗尽
 * Connection conn = connectionPool.tryGetConnection(timeout);
 * if (conn == null) {
 *     throw new ServiceUnavailableException(
 *         "No available connections. Please retry after %d seconds", retryDelay);
 * }
 *
 * // 熔断器打开
 * if (circuitBreaker.isOpen()) {
 *     throw new ServiceUnavailableException(
 *         "Service is temporarily unavailable due to high error rate");
 * }
 * }</pre>
 *
 * <h3>与其他异常的区别:</h3>
 * <ul>
 *   <li>与 {@link ServiceFailureException} 不同:503 是临时不可用,500 是内部错误
 *   <li>与 {@link NotImplementedException} 不同:503 是临时状态,501 是功能不支持
 *   <li>503 表示应该重试,500 重试可能没有意义
 * </ul>
 *
 * <h3>重试策略:</h3>
 * <ul>
 *   <li>客户端应该在一段时间后重试(建议使用指数退避)
 *   <li>检查响应头中的 Retry-After 字段
 *   <li>限制重试次数,避免雪崩效应
 *   <li>考虑使用断路器模式
 * </ul>
 *
 * <h3>处理建议:</h3>
 * <ul>
 *   <li>服务端应实现优雅降级和限流机制
 *   <li>提供健康检查端点,便于负载均衡器判断
 *   <li>在响应中包含预计恢复时间
 *   <li>监控 503 错误率,及时发现系统问题
 * </ul>
 *
 * @since 1.0
 */
public class ServiceUnavailableException extends RESTException {

    /**
     * 构造一个带格式化消息的 ServiceUnavailable 异常。
     *
     * @param message 错误消息模板,使用 {@link String#format(String, Object...)} 语法
     * @param args 格式化参数,用于填充消息模板中的占位符
     */
    public ServiceUnavailableException(String message, Object... args) {
        super(String.format(message, args));
    }
}
