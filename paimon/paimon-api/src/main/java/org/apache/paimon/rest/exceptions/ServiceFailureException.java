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
 * HTTP 500 - Internal Server Error 异常。
 *
 * <p>当服务器内部发生错误,无法完成请求时抛出此异常。这是服务端错误,表示服务器遇到了意外情况,
 * 导致无法完成请求。
 *
 * <h3>触发条件:</h3>
 * <ul>
 *   <li>服务器内部代码错误(如空指针异常、数组越界等)
 *   <li>数据库连接失败或查询错误
 *   <li>文件系统访问失败
 *   <li>第三方服务调用失败
 *   <li>资源耗尽(如内存不足、线程池满)
 *   <li>配置错误导致的运行时异常
 *   <li>未预期的业务逻辑错误
 * </ul>
 *
 * <h3>HTTP 状态码: 500 Internal Server Error</h3>
 * <p>这是最通用的服务器错误状态码,表示服务器内部发生了错误。与 4xx 客户端错误不同,
 * 500 错误通常意味着问题出在服务器端,客户端无法通过修改请求来解决。
 *
 * <h3>使用示例:</h3>
 * <pre>{@code
 * // 数据库操作失败
 * try {
 *     database.executeQuery(sql);
 * } catch (SQLException e) {
 *     throw new ServiceFailureException(
 *         "Database query failed: %s", e.getMessage());
 * }
 *
 * // 文件系统错误
 * try {
 *     fileSystem.readFile(path);
 * } catch (IOException e) {
 *     throw new ServiceFailureException(
 *         "Failed to read file %s: %s", path, e.getMessage());
 * }
 *
 * // 未预期的状态
 * if (unexpectedState) {
 *     throw new ServiceFailureException(
 *         "Internal error: unexpected state %s", currentState);
 * }
 *
 * // 资源不足
 * if (availableMemory < requiredMemory) {
 *     throw new ServiceFailureException(
 *         "Insufficient memory: required %d MB, available %d MB",
 *         requiredMemory, availableMemory);
 * }
 * }</pre>
 *
 * <h3>与其他异常的区别:</h3>
 * <ul>
 *   <li>与 {@link BadRequestException} 不同:500 是服务器错误,400 是客户端请求错误
 *   <li>与 {@link ServiceUnavailableException} 不同:500 是内部错误,503 是服务暂时不可用
 *   <li>与 {@link NotImplementedException} 不同:500 是运行时错误,501 是功能未实现
 * </ul>
 *
 * <h3>处理建议:</h3>
 * <ul>
 *   <li>记录详细的错误日志和堆栈信息
 *   <li>检查服务器日志查找根本原因
 *   <li>客户端可以尝试重试,但需要限制重试次数
 *   <li>考虑是否需要降级或熔断机制
 * </ul>
 *
 * <h3>注意事项:</h3>
 * <ul>
 *   <li>不应该暴露敏感的内部错误信息给客户端
 *   <li>应该记录完整的异常堆栈用于调试
 *   <li>考虑使用监控和告警系统
 * </ul>
 *
 * @since 1.0
 */
public class ServiceFailureException extends RESTException {

    /**
     * 构造一个带格式化消息的 ServiceFailure 异常。
     *
     * @param message 错误消息模板,使用 {@link String#format(String, Object...)} 语法
     * @param args 格式化参数,用于填充消息模板中的占位符
     */
    public ServiceFailureException(String message, Object... args) {
        super(String.format(message, args));
    }
}
