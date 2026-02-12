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
 * HTTP 501 - Not Implemented 异常。
 *
 * <p>当服务器不支持请求的功能时抛出此异常。表示服务器不具备完成请求所需的功能,
 * 或者该功能尚未实现。
 *
 * <h3>触发条件:</h3>
 * <ul>
 *   <li>调用尚未实现的 API 端点
 *   <li>使用服务器不支持的 HTTP 方法(如 PATCH、TRACE)
 *   <li>请求需要的功能在当前版本中不可用
 *   <li>功能被标记为废弃或计划中
 *   <li>可选功能未启用或未安装
 *   <li>API 版本不支持该操作
 * </ul>
 *
 * <h3>HTTP 状态码: 501 Not Implemented</h3>
 * <p>这是服务器端错误,表示服务器不支持请求所需的功能。与 400 Bad Request 不同,
 * 501 表示的是服务器能力不足,而不是请求格式错误。
 *
 * <h3>使用示例:</h3>
 * <pre>{@code
 * // 功能尚未实现
 * public void deletePartition(String partitionName) {
 *     throw new NotImplementedException(
 *         "Partition deletion is not yet implemented");
 * }
 *
 * // 不支持的 HTTP 方法
 * if (httpMethod.equals("PATCH")) {
 *     throw new NotImplementedException(
 *         "PATCH method is not supported for this resource");
 * }
 *
 * // 可选功能未启用
 * if (!isFeatureEnabled("advanced-query")) {
 *     throw new NotImplementedException(
 *         "Advanced query feature is not enabled on this server");
 * }
 *
 * // API 版本不支持
 * if (apiVersion < 2) {
 *     throw new NotImplementedException(
 *         "This operation requires API version 2 or higher. Current version: %d",
 *         apiVersion);
 * }
 *
 * // 存根实现
 * @Override
 * public List<Statistics> getTableStatistics(String tableName) {
 *     throw new NotImplementedException(
 *         "Table statistics collection is planned for future release");
 * }
 * }</pre>
 *
 * <h3>与其他异常的区别:</h3>
 * <ul>
 *   <li>与 {@link BadRequestException} 不同:501 表示功能不支持,400 表示请求格式错误
 *   <li>与 {@link ServiceFailureException} 不同:501 表示功能未实现,500 表示运行时错误
 *   <li>与 {@link ServiceUnavailableException} 不同:501 是永久性不支持,503 是临时不可用
 * </ul>
 *
 * <h3>使用场景:</h3>
 * <ul>
 *   <li>API 设计阶段,为未来功能预留接口
 *   <li>版本迭代过程中,标识计划中的功能
 *   <li>可选功能的存根实现
 *   <li>抽象方法的默认实现
 * </ul>
 *
 * <h3>处理建议:</h3>
 * <ul>
 *   <li>客户端应该检查服务器的能力声明(如通过配置或版本信息)
 *   <li>提供清晰的错误消息,说明何时或在哪个版本中可用
 *   <li>考虑提供替代方案或降级策略
 * </ul>
 *
 * @since 1.0
 */
public class NotImplementedException extends RESTException {

    /**
     * 构造一个带格式化消息的 NotImplemented 异常。
     *
     * @param message 错误消息模板,使用 {@link String#format(String, Object...)} 语法
     * @param args 格式化参数,用于填充消息模板中的占位符
     */
    public NotImplementedException(String message, Object... args) {
        super(String.format(message, args));
    }
}
