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
 * REST 请求标记接口。
 *
 * <p>这是一个标记接口,用于标识所有发送到 REST Catalog 服务器的请求对象。
 *
 * <h2>主要用途</h2>
 * <ul>
 *   <li>类型安全:确保只有实现此接口的对象可以作为 REST 请求
 *   <li>序列化支持:所有请求对象会被自动序列化为 JSON
 *   <li>API 设计:清晰标识哪些类是请求对象
 * </ul>
 *
 * <h2>实现要求</h2>
 * <p>所有实现此接口的类应该:
 * <ul>
 *   <li>是不可变的或使用 JavaBean 模式
 *   <li>提供无参构造函数(用于反序列化)
 *   <li>字段使用合适的 Jackson 注解
 *   <li>重写 toString() 方法用于日志记录
 * </ul>
 *
 * <h2>请求类型示例</h2>
 * <pre>
 * Database 相关:
 *   - CreateDatabaseRequest  - 创建数据库
 *   - AlterDatabaseRequest   - 修改数据库
 *
 * Table 相关:
 *   - CreateTableRequest     - 创建表
 *   - AlterTableRequest      - 修改表
 *   - RenameTableRequest     - 重命名表
 *   - CommitTableRequest     - 提交快照
 *
 * Partition 相关:
 *   - MarkDonePartitionsRequest - 标记分区完成
 *
 * Branch/Tag 相关:
 *   - CreateBranchRequest    - 创建分支
 *   - CreateTagRequest       - 创建标签
 *   - ForwardBranchRequest   - 快进分支
 * </pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建请求对象
 * CreateDatabaseRequest request = new CreateDatabaseRequest(
 *     "my_database",
 *     Collections.singletonMap("owner", "user1")
 * );
 *
 * // 通过 REST 客户端发送
 * restClient.post("/v1/databases", request, authFunction);
 * }</pre>
 *
 * @see RESTMessage
 * @see RESTResponse
 * @see RESTClient#post(String, RESTRequest, org.apache.paimon.rest.auth.RESTAuthFunction)
 */
public interface RESTRequest extends RESTMessage {}
