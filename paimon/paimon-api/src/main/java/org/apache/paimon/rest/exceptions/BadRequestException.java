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
 * HTTP 400 - Bad Request 异常。
 *
 * <p>当客户端发送的请求存在语法错误或包含无效参数时抛出此异常。表示服务器无法理解或处理该请求。
 *
 * <h3>触发条件:</h3>
 * <ul>
 *   <li>请求参数格式不正确(如 JSON 格式错误)
 *   <li>缺少必需的请求参数
 *   <li>参数类型不匹配(如期望数字但传入字符串)
 *   <li>参数值超出允许范围
 *   <li>请求体包含无效的数据结构
 *   <li>API 版本不支持的请求格式
 * </ul>
 *
 * <h3>HTTP 状态码: 400 Bad Request</h3>
 * <p>这是客户端错误的一种,表明问题出在请求本身,而非服务器。客户端应该修正请求后重试。
 *
 * <h3>使用示例:</h3>
 * <pre>{@code
 * // 参数验证失败
 * if (tableName == null || tableName.isEmpty()) {
 *     throw new BadRequestException("Table name cannot be empty");
 * }
 *
 * // 格式化错误消息
 * if (pageSize < 1 || pageSize > 1000) {
 *     throw new BadRequestException(
 *         "Invalid page size: %d. Must be between 1 and 1000", pageSize);
 * }
 *
 * // JSON 解析错误
 * try {
 *     parseRequest(jsonString);
 * } catch (JsonParseException e) {
 *     throw new BadRequestException("Invalid JSON format: %s", e.getMessage());
 * }
 * }</pre>
 *
 * <h3>与其他异常的区别:</h3>
 * <ul>
 *   <li>与 {@link NotAuthorizedException} 不同:400 表示请求格式错误,401 表示缺少认证
 *   <li>与 {@link ForbiddenException} 不同:400 表示请求无效,403 表示权限不足
 *   <li>与 {@link NoSuchResourceException} 不同:400 表示请求格式错误,404 表示资源不存在
 * </ul>
 *
 * @since 1.0
 */
public class BadRequestException extends RESTException {

    /**
     * 构造一个带格式化消息的 BadRequest 异常。
     *
     * @param message 错误消息模板,使用 {@link String#format(String, Object...)} 语法
     * @param args 格式化参数,用于填充消息模板中的占位符
     */
    public BadRequestException(String message, Object... args) {
        super(message, args);
    }
}
