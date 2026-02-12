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

/**
 * REST 响应标记接口。
 *
 * <p>这是一个标记接口,用于标识所有从 REST Catalog 服务器返回的响应对象。
 *
 * <h2>主要用途</h2>
 * <ul>
 *   <li>类型安全:确保只有实现此接口的对象可以作为 REST 响应
 *   <li>反序列化支持:所有响应对象会从 JSON 自动反序列化
 *   <li>API 设计:清晰标识哪些类是响应对象
 * </ul>
 *
 * <h2>实现要求</h2>
 * <p>所有实现此接口的类应该:
 * <ul>
 *   <li>提供无参构造函数(用于反序列化)
 *   <li>使用 JavaBean 模式或不可变对象
 *   <li>字段使用合适的 Jackson 注解
 *   <li>重写 toString() 方法用于日志记录
 * </ul>
 *
 * <h2>响应类型示例</h2>
 * <pre>
 * Database 相关:
 *   - GetDatabaseResponse      - 获取数据库信息
 *   - ListDatabasesResponse    - 列出数据库
 *   - AlterDatabaseResponse    - 修改数据库响应
 *
 * Table 相关:
 *   - GetTableResponse         - 获取表信息
 *   - ListTablesResponse       - 列出表
 *   - CommitTableResponse      - 提交快照响应
 *
 * Snapshot 相关:
 *   - GetTableSnapshotResponse - 获取表快照
 *   - ListSnapshotsResponse    - 列出快照
 *
 * Partition 相关:
 *   - ListPartitionsResponse   - 列出分区
 *
 * Branch/Tag 相关:
 *   - ListBranchesResponse     - 列出分支
 *   - ListTagsResponse         - 列出标签
 *   - GetTagResponse           - 获取标签信息
 *
 * Config 相关:
 *   - ConfigResponse           - 配置信息
 *
 * Error 相关:
 *   - ErrorResponse            - 错误信息
 * </pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 发送请求并接收响应
 * GetDatabaseResponse response = restClient.get(
 *     "/v1/databases/my_database",
 *     GetDatabaseResponse.class,
 *     authFunction
 * );
 *
 * // 使用响应数据
 * String dbName = response.getName();
 * Map<String, String> properties = response.getProperties();
 * }</pre>
 *
 * @see RESTMessage
 * @see RESTRequest
 * @see RESTClient#get(String, Class, org.apache.paimon.rest.auth.RESTAuthFunction)
 */
public interface RESTResponse extends RESTMessage {}
