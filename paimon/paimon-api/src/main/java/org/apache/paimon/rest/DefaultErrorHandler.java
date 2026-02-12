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

import org.apache.paimon.rest.exceptions.AlreadyExistsException;
import org.apache.paimon.rest.exceptions.BadRequestException;
import org.apache.paimon.rest.exceptions.ForbiddenException;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.rest.exceptions.NotAuthorizedException;
import org.apache.paimon.rest.exceptions.NotImplementedException;
import org.apache.paimon.rest.exceptions.RESTException;
import org.apache.paimon.rest.exceptions.ServiceFailureException;
import org.apache.paimon.rest.exceptions.ServiceUnavailableException;
import org.apache.paimon.rest.responses.ErrorResponse;

import static org.apache.paimon.rest.interceptor.LoggingInterceptor.DEFAULT_REQUEST_ID;

/**
 * 默认的 REST 错误处理器实现。
 *
 * <p>这是 {@link ErrorHandler} 的默认实现,提供标准的 HTTP 错误码到异常类型的映射。
 * 实现为单例模式,整个应用共享一个实例。
 *
 * <h2>主要特性</h2>
 * <ul>
 *   <li>单例模式:通过 {@link #getInstance()} 获取实例
 *   <li>完整的 HTTP 错误码支持
 *   <li>自动附加请求 ID 到错误消息
 *   <li>区分资源类异常和普通异常
 * </ul>
 *
 * <h2>错误处理逻辑</h2>
 * <pre>
 * 1. 提取 HTTP 状态码
 * 2. 构建错误消息(包含请求 ID)
 * 3. 根据状态码抛出相应异常:
 *    - 400: BadRequestException
 *    - 401: NotAuthorizedException
 *    - 403: ForbiddenException
 *    - 404: NoSuchResourceException (包含资源类型和名称)
 *    - 409: AlreadyExistsException (包含资源类型和名称)
 *    - 500: ServiceFailureException
 *    - 501: NotImplementedException
 *    - 503: ServiceUnavailableException
 *    - 其他: RESTException
 * 4. 405, 406 状态码被忽略(继续抛出 RESTException)
 * </pre>
 *
 * <h2>请求 ID 处理</h2>
 * <p>如果请求 ID 不是默认值,会自动附加到错误消息中:
 * <pre>{@code
 * // 原始消息
 * "Table not found: my_table"
 *
 * // 附加请求 ID 后
 * "Table not found: my_table requestId:abc-123-def"
 * }</pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 获取默认错误处理器
 * ErrorHandler handler = DefaultErrorHandler.getInstance();
 *
 * // 在 HTTP 客户端中使用
 * HttpClient client = new HttpClient("http://localhost:8080");
 * client.setErrorHandler(handler);
 *
 * // 错误响应会被自动转换为异常
 * try {
 *     client.get("/v1/databases/not_exists", GetDatabaseResponse.class, authFunction);
 * } catch (NoSuchResourceException e) {
 *     // 处理资源不存在异常
 *     System.err.println("Database not found: " + e.getResourceName());
 *     System.err.println("Request ID: " + e.getMessage());
 * }
 * }</pre>
 *
 * <h2>异常层次结构</h2>
 * <pre>
 * RESTException (基类)
 *   ├── BadRequestException           (400)
 *   ├── NotAuthorizedException        (401)
 *   ├── ForbiddenException            (403)
 *   ├── NoSuchResourceException       (404)
 *   ├── AlreadyExistsException        (409)
 *   ├── ServiceFailureException       (500)
 *   ├── NotImplementedException       (501)
 *   └── ServiceUnavailableException   (503)
 * </pre>
 *
 * <h2>线程安全性</h2>
 * <p>这个类是无状态的,因此是线程安全的,可以在多线程环境中共享使用。
 *
 * @see ErrorHandler
 * @see ErrorResponse
 * @see org.apache.paimon.rest.exceptions.RESTException
 */
public class DefaultErrorHandler extends ErrorHandler {

    /** 单例实例。 */
    private static final ErrorHandler INSTANCE = new DefaultErrorHandler();

    /**
     * 获取默认错误处理器的单例实例。
     *
     * @return 错误处理器实例
     */
    public static ErrorHandler getInstance() {
        return INSTANCE;
    }

    /**
     * 处理错误响应并抛出相应的异常。
     *
     * @param error 错误响应对象,包含状态码、消息和资源信息
     * @param requestId 请求 ID,用于问题追踪
     * @throws BadRequestException 当状态码为 400 时
     * @throws NotAuthorizedException 当状态码为 401 时
     * @throws ForbiddenException 当状态码为 403 时
     * @throws NoSuchResourceException 当状态码为 404 时
     * @throws AlreadyExistsException 当状态码为 409 时
     * @throws ServiceFailureException 当状态码为 500 时
     * @throws NotImplementedException 当状态码为 501 时
     * @throws ServiceUnavailableException 当状态码为 503 时
     * @throws RESTException 对于其他未处理的状态码
     */
    @Override
    public void accept(ErrorResponse error, String requestId) {
        int code = error.getCode();
        String message;
        if (DEFAULT_REQUEST_ID.equals(requestId)) {
            message = error.getMessage();
        } else {
            // if we have a requestId, append it to the message
            message = String.format("%s requestId:%s", error.getMessage(), requestId);
        }
        switch (code) {
            case 400:
                throw new BadRequestException(String.format("%s", message));
            case 401:
                throw new NotAuthorizedException("Not authorized: %s", message);
            case 403:
                throw new ForbiddenException("Forbidden: %s", message);
            case 404:
                throw new NoSuchResourceException(
                        error.getResourceType(), error.getResourceName(), "%s", message);
            case 405:
            case 406:
                break;
            case 409:
                throw new AlreadyExistsException(
                        error.getResourceType(), error.getResourceName(), "%s", message);
            case 500:
                throw new ServiceFailureException("Server error: %s", message);
            case 501:
                throw new NotImplementedException(message);
            case 503:
                throw new ServiceUnavailableException("Service unavailable: %s", message);
            default:
                break;
        }

        throw new RESTException("Unable to process: %s", message);
    }
}
