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

import org.apache.paimon.rest.responses.ErrorResponse;

import java.util.function.BiConsumer;

/**
 * REST 客户端错误处理器。
 *
 * <p>这是一个抽象类,用于处理 REST API 调用时返回的错误响应。通过实现 {@link BiConsumer} 接口,
 * 接收错误响应和请求 ID,并将其转换为适当的异常抛出。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li>处理 HTTP 错误响应
 *   <li>将错误码映射到相应的异常类型
 *   <li>附加请求 ID 用于问题追踪
 * </ul>
 *
 * <h2>错误处理流程</h2>
 * <pre>
 * HTTP 响应 → ErrorResponse → ErrorHandler.accept() → 抛出特定异常
 * </pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建自定义错误处理器
 * ErrorHandler handler = new ErrorHandler() {
 *     @Override
 *     public void accept(ErrorResponse error, String requestId) {
 *         int code = error.getCode();
 *         String message = error.getMessage();
 *
 *         switch (code) {
 *             case 404:
 *                 throw new NoSuchResourceException(
 *                     error.getResourceType(),
 *                     error.getResourceName(),
 *                     message
 *                 );
 *             case 409:
 *                 throw new AlreadyExistsException(message);
 *             default:
 *                 throw new RESTException(message);
 *         }
 *     }
 * };
 *
 * // 在 HTTP 客户端中使用
 * httpClient.setErrorHandler(handler);
 * }</pre>
 *
 * <h2>标准 HTTP 错误码映射</h2>
 * <ul>
 *   <li>400 Bad Request → {@link org.apache.paimon.rest.exceptions.BadRequestException}
 *   <li>401 Unauthorized → {@link org.apache.paimon.rest.exceptions.NotAuthorizedException}
 *   <li>403 Forbidden → {@link org.apache.paimon.rest.exceptions.ForbiddenException}
 *   <li>404 Not Found → {@link org.apache.paimon.rest.exceptions.NoSuchResourceException}
 *   <li>409 Conflict → {@link org.apache.paimon.rest.exceptions.AlreadyExistsException}
 *   <li>500 Internal Server Error → {@link org.apache.paimon.rest.exceptions.ServiceFailureException}
 *   <li>501 Not Implemented → {@link org.apache.paimon.rest.exceptions.NotImplementedException}
 *   <li>503 Service Unavailable → {@link org.apache.paimon.rest.exceptions.ServiceUnavailableException}
 * </ul>
 *
 * @see DefaultErrorHandler
 * @see ErrorResponse
 * @see BiConsumer
 */
public abstract class ErrorHandler implements BiConsumer<ErrorResponse, String> {}
