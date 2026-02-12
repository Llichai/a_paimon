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
 * HTTP 401 - Unauthorized 异常。
 *
 * <p>当请求缺少有效的认证凭据或认证凭据无效时抛出此异常。表示客户端需要提供身份认证才能访问资源。
 *
 * <h3>触发条件:</h3>
 * <ul>
 *   <li>请求未包含认证令牌(如 Bearer Token)
 *   <li>认证令牌已过期
 *   <li>认证令牌格式不正确或无效
 *   <li>用户名或密码错误
 *   <li>API Key 无效或已被撤销
 *   <li>OAuth2 授权失败
 * </ul>
 *
 * <h3>HTTP 状态码: 401 Unauthorized</h3>
 * <p>这是客户端认证错误,表示请求需要身份验证。服务器会在响应头中包含 WWW-Authenticate 字段,
 * 指示客户端应该使用何种认证方式。
 *
 * <h3>使用示例:</h3>
 * <pre>{@code
 * // 缺少认证令牌
 * if (authToken == null) {
 *     throw new NotAuthorizedException("Authentication token is required");
 * }
 *
 * // 令牌过期
 * if (tokenExpired(authToken)) {
 *     throw new NotAuthorizedException("Authentication token has expired");
 * }
 *
 * // 令牌验证失败
 * if (!validateToken(authToken)) {
 *     throw new NotAuthorizedException(
 *         "Invalid authentication token: %s", authToken.substring(0, 8) + "...");
 * }
 *
 * // 用户名密码错误
 * if (!authenticateUser(username, password)) {
 *     throw new NotAuthorizedException(
 *         "Authentication failed for user: %s", username);
 * }
 * }</pre>
 *
 * <h3>与其他异常的区别:</h3>
 * <ul>
 *   <li>与 {@link ForbiddenException} 不同:401 表示缺少认证,403 表示已认证但权限不足
 *   <li>与 {@link BadRequestException} 不同:401 表示认证问题,400 表示请求格式错误
 * </ul>
 *
 * <h3>处理建议:</h3>
 * <ul>
 *   <li>客户端应重新进行身份认证
 *   <li>刷新过期的认证令牌
 *   <li>提示用户重新登录
 * </ul>
 *
 * @since 1.0
 */
public class NotAuthorizedException extends RESTException {

    /**
     * 构造一个带格式化消息的 NotAuthorized 异常。
     *
     * @param message 错误消息模板,使用 {@link String#format(String, Object...)} 语法
     * @param args 格式化参数,用于填充消息模板中的占位符
     */
    public NotAuthorizedException(String message, Object... args) {
        super(String.format(message, args));
    }
}
