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
 * HTTP 403 - Forbidden 异常。
 *
 * <p>当客户端已通过身份认证,但没有足够的权限访问请求的资源时抛出此异常。
 * 表示服务器理解请求,但拒绝执行。
 *
 * <h3>触发条件:</h3>
 * <ul>
 *   <li>用户已认证但权限不足,无法执行操作
 *   <li>尝试访问未授权的资源(如其他用户的数据)
 *   <li>尝试执行被禁止的操作(如删除受保护的表)
 *   <li>资源访问受 ACL(访问控制列表)限制
 *   <li>IP 地址被限制访问
 *   <li>超出配额或速率限制
 * </ul>
 *
 * <h3>HTTP 状态码: 403 Forbidden</h3>
 * <p>这是权限不足错误,与 401 不同的是,客户端的身份是已知的,但没有访问权限。
 * 重新认证也无法解决问题,需要管理员授予相应权限。
 *
 * <h3>使用示例:</h3>
 * <pre>{@code
 * // 权限检查失败
 * if (!hasPermission(user, resource, "DELETE")) {
 *     throw new ForbiddenException(
 *         "User %s does not have DELETE permission on %s", user, resource);
 * }
 *
 * // 访问其他用户的资源
 * if (!resource.getOwner().equals(currentUser)) {
 *     throw new ForbiddenException(
 *         "Cannot access resource owned by %s", resource.getOwner());
 * }
 *
 * // 受保护的资源
 * if (table.isProtected()) {
 *     throw new ForbiddenException(
 *         "Table %s is protected and cannot be modified", table.getName());
 * }
 *
 * // 超出速率限制
 * if (rateLimiter.isExceeded(userId)) {
 *     throw new ForbiddenException(
 *         "Rate limit exceeded for user %s. Please retry later.", userId);
 * }
 * }</pre>
 *
 * <h3>与其他异常的区别:</h3>
 * <ul>
 *   <li>与 {@link NotAuthorizedException} 不同:403 表示已认证但权限不足,401 表示缺少认证
 *   <li>与 {@link NoSuchResourceException} 不同:403 表示无权访问,404 表示资源不存在
 * </ul>
 *
 * <h3>处理建议:</h3>
 * <ul>
 *   <li>联系管理员请求相应权限
 *   <li>检查当前用户的角色和权限配置
 *   <li>确认资源的访问控制策略
 * </ul>
 *
 * @since 1.0
 */
public class ForbiddenException extends RESTException {

    /**
     * 构造一个带格式化消息的 Forbidden 异常。
     *
     * @param message 错误消息模板,使用 {@link String#format(String, Object...)} 语法
     * @param args 格式化参数,用于填充消息模板中的占位符
     */
    public ForbiddenException(String message, Object... args) {
        super(message, args);
    }
}
