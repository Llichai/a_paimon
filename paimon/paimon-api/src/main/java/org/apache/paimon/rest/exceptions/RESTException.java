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
 * REST 客户端异常的基类。
 *
 * <p>这是所有 REST API 相关异常的基础类,用于封装在 REST 请求处理过程中发生的各种错误。
 * 所有具体的 HTTP 状态码异常(如 400、401、403、404 等)都继承自此类。
 *
 * <h3>设计特点:</h3>
 * <ul>
 *   <li>继承自 {@link RuntimeException},无需强制捕获
 *   <li>支持格式化的错误消息,使用 {@link String#format(String, Object...)} 语法
 *   <li>支持异常链,可以包装底层异常
 *   <li>提供统一的异常处理基础
 * </ul>
 *
 * <h3>HTTP 状态码映射:</h3>
 * <ul>
 *   <li>400 Bad Request → {@link BadRequestException}
 *   <li>401 Unauthorized → {@link NotAuthorizedException}
 *   <li>403 Forbidden → {@link ForbiddenException}
 *   <li>404 Not Found → {@link NoSuchResourceException}
 *   <li>409 Conflict → {@link AlreadyExistsException}
 *   <li>500 Internal Server Error → {@link ServiceFailureException}
 *   <li>501 Not Implemented → {@link NotImplementedException}
 *   <li>503 Service Unavailable → {@link ServiceUnavailableException}
 * </ul>
 *
 * <h3>使用示例:</h3>
 * <pre>{@code
 * // 抛出格式化的异常消息
 * throw new RESTException("Failed to connect to %s:%d", host, port);
 *
 * // 包装底层异常
 * try {
 *     httpClient.execute(request);
 * } catch (IOException e) {
 *     throw new RESTException(e, "Request failed: %s", e.getMessage());
 * }
 *
 * // 使用子类处理特定 HTTP 错误
 * if (response.getStatusCode() == 404) {
 *     throw new NoSuchResourceException("table", tableName, "Table not found");
 * }
 * }</pre>
 *
 * @since 1.0
 */
public class RESTException extends RuntimeException {

    /**
     * 构造一个带格式化消息的 REST 异常。
     *
     * @param message 错误消息模板,使用 {@link String#format(String, Object...)} 语法
     * @param args 格式化参数,用于填充消息模板中的占位符
     */
    public RESTException(String message, Object... args) {
        super(String.format(message, args));
    }

    /**
     * 构造一个包含原因和格式化消息的 REST 异常。
     *
     * @param cause 导致此异常的底层异常
     * @param message 错误消息模板,使用 {@link String#format(String, Object...)} 语法
     * @param args 格式化参数,用于填充消息模板中的占位符
     */
    public RESTException(Throwable cause, String message, Object... args) {
        super(String.format(message, args), cause);
    }
}
