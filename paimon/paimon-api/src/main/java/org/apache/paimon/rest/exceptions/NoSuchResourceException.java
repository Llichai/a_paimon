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
 * HTTP 404 - Not Found 异常。
 *
 * <p>当请求的资源不存在时抛出此异常。此异常除了包含错误消息外,还携带资源类型和资源名称信息,
 * 便于调试和错误处理。
 *
 * <h3>触发条件:</h3>
 * <ul>
 *   <li>请求的表(table)不存在
 *   <li>请求的数据库(database)不存在
 *   <li>请求的分区(partition)不存在
 *   <li>请求的快照(snapshot)不存在
 *   <li>请求的文件路径无效
 *   <li>资源已被删除
 *   <li>URL 路径错误
 * </ul>
 *
 * <h3>HTTP 状态码: 404 Not Found</h3>
 * <p>这是最常见的 HTTP 错误之一,表示服务器找不到请求的资源。可能是 URL 错误,
 * 或者资源确实不存在。
 *
 * <h3>资源类型示例:</h3>
 * <ul>
 *   <li>table - 表
 *   <li>database - 数据库
 *   <li>partition - 分区
 *   <li>snapshot - 快照
 *   <li>branch - 分支
 *   <li>tag - 标签
 * </ul>
 *
 * <h3>使用示例:</h3>
 * <pre>{@code
 * // 表不存在
 * if (!tableExists(databaseName, tableName)) {
 *     throw new NoSuchResourceException(
 *         "table",
 *         tableName,
 *         "Table %s.%s does not exist",
 *         databaseName, tableName);
 * }
 *
 * // 数据库不存在
 * if (!databaseExists(databaseName)) {
 *     throw new NoSuchResourceException(
 *         "database",
 *         databaseName,
 *         "Database %s not found",
 *         databaseName);
 * }
 *
 * // 快照不存在
 * if (getSnapshot(snapshotId) == null) {
 *     throw new NoSuchResourceException(
 *         "snapshot",
 *         String.valueOf(snapshotId),
 *         "Snapshot %d does not exist in table %s",
 *         snapshotId, tableName);
 * }
 *
 * // 异常处理
 * try {
 *     table.getPartition(partitionSpec);
 * } catch (NoSuchResourceException e) {
 *     System.err.printf("Resource not found: type=%s, name=%s%n",
 *         e.resourceType(), e.resourceName());
 * }
 * }</pre>
 *
 * <h3>与其他异常的区别:</h3>
 * <ul>
 *   <li>与 {@link BadRequestException} 不同:404 表示资源不存在,400 表示请求格式错误
 *   <li>与 {@link ForbiddenException} 不同:404 表示资源不存在,403 表示无权访问
 *   <li>有时为了安全考虑,权限不足时也可能返回 404 而非 403
 * </ul>
 *
 * @since 1.0
 */
public class NoSuchResourceException extends RESTException {

    /** 资源类型,如 "table"、"database"、"partition" 等。 */
    private final String resourceType;

    /** 资源名称,如表名、数据库名等。 */
    private final String resourceName;

    /**
     * 构造一个带资源信息的 NoSuchResource 异常。
     *
     * @param resourceType 资源类型,如 "table"、"database"、"partition" 等
     * @param resourceName 资源名称,如表名、数据库名等
     * @param message 错误消息模板,使用 {@link String#format(String, Object...)} 语法
     * @param args 格式化参数,用于填充消息模板中的占位符
     */
    public NoSuchResourceException(
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
