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
 * HTTP 409 - Conflict 异常。
 *
 * <p>当尝试创建已存在的资源时抛出此异常。此异常除了包含错误消息外,还携带资源类型和资源名称信息,
 * 便于调试和错误处理。
 *
 * <h3>触发条件:</h3>
 * <ul>
 *   <li>尝试创建已存在的表(table)
 *   <li>尝试创建已存在的数据库(database)
 *   <li>尝试创建已存在的分区(partition)
 *   <li>尝试添加已存在的标签(tag)
 *   <li>尝试创建已存在的分支(branch)
 *   <li>资源名称冲突
 *   <li>并发创建导致的冲突
 * </ul>
 *
 * <h3>HTTP 状态码: 409 Conflict</h3>
 * <p>表示请求与服务器的当前状态冲突。通常发生在创建操作中,客户端应该检查资源是否已存在,
 * 或使用 "IF NOT EXISTS" 语义避免此错误。
 *
 * <h3>资源类型示例:</h3>
 * <ul>
 *   <li>table - 表
 *   <li>database - 数据库
 *   <li>partition - 分区
 *   <li>tag - 标签
 *   <li>branch - 分支
 *   <li>view - 视图
 * </ul>
 *
 * <h3>使用示例:</h3>
 * <pre>{@code
 * // 表已存在
 * if (tableExists(databaseName, tableName)) {
 *     throw new AlreadyExistsException(
 *         "table",
 *         tableName,
 *         "Table %s.%s already exists",
 *         databaseName, tableName);
 * }
 *
 * // 数据库已存在
 * if (databaseExists(databaseName)) {
 *     throw new AlreadyExistsException(
 *         "database",
 *         databaseName,
 *         "Database %s already exists",
 *         databaseName);
 * }
 *
 * // 标签已存在
 * if (tagExists(tableName, tagName)) {
 *     throw new AlreadyExistsException(
 *         "tag",
 *         tagName,
 *         "Tag %s already exists on table %s",
 *         tagName, tableName);
 * }
 *
 * // 异常处理 - 忽略已存在错误
 * try {
 *     createTable(databaseName, tableName, schema);
 * } catch (AlreadyExistsException e) {
 *     if (ifNotExists) {
 *         System.out.printf("Table %s already exists, skipped.%n", e.resourceName());
 *     } else {
 *         throw e;
 *     }
 * }
 * }</pre>
 *
 * <h3>与其他异常的区别:</h3>
 * <ul>
 *   <li>与 {@link BadRequestException} 不同:409 表示资源冲突,400 表示请求格式错误
 *   <li>与 {@link NoSuchResourceException} 相反:409 表示资源已存在,404 表示资源不存在
 * </ul>
 *
 * <h3>处理建议:</h3>
 * <ul>
 *   <li>使用 "CREATE TABLE IF NOT EXISTS" 语义避免此错误
 *   <li>在创建前先检查资源是否存在
 *   <li>捕获此异常并根据业务需求决定是忽略还是报错
 * </ul>
 *
 * @since 1.0
 */
public class AlreadyExistsException extends RESTException {

    /** 资源类型,如 "table"、"database"、"partition" 等。 */
    private final String resourceType;

    /** 资源名称,如表名、数据库名等。 */
    private final String resourceName;

    /**
     * 构造一个带资源信息的 AlreadyExists 异常。
     *
     * @param resourceType 资源类型,如 "table"、"database"、"partition" 等
     * @param resourceName 资源名称,如表名、数据库名等
     * @param message 错误消息模板,使用 {@link String#format(String, Object...)} 语法
     * @param args 格式化参数,用于填充消息模板中的占位符
     */
    public AlreadyExistsException(
            String resourceType, String resourceName, String message, Object... args) {
        super(message, args);
        this.resourceType = resourceType;
        this.resourceName = resourceName;
    }

    /**
     * 获取资源类型。
     *
     * @return 资源类型,如 "table"、"database" 等
     */
    public String resourceType() {
        return resourceType;
    }

    /**
     * 获取资源名称。
     *
     * @return 资源名称,如表名、数据库名等
     */
    public String resourceName() {
        return resourceName;
    }
}
