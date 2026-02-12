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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * REST 消息标记接口。
 *
 * <p>这是一个标记接口,用于标识所有 REST API 的请求和响应对象。
 *
 * <h2>主要特性</h2>
 * <ul>
 *   <li>所有 REST 请求和响应对象都必须实现此接口
 *   <li>自动忽略未知的 JSON 字段,提高向后兼容性
 *   <li>支持 Jackson JSON 序列化和反序列化
 * </ul>
 *
 * <h2>JSON 处理</h2>
 * <p>使用 {@code @JsonIgnoreProperties(ignoreUnknown = true)} 注解:
 * <ul>
 *   <li>反序列化时忽略响应中的未知字段
 *   <li>避免因 API 版本升级添加新字段而导致的反序列化失败
 *   <li>提高客户端与服务器之间的兼容性
 * </ul>
 *
 * <h2>继承层次</h2>
 * <pre>
 * RESTMessage
 *   ├── RESTRequest  - 所有请求对象的标记接口
 *   └── RESTResponse - 所有响应对象的标记接口
 * </pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 请求对象实现
 * public class CreateDatabaseRequest implements RESTRequest {
 *     private String name;
 *     private Map<String, String> properties;
 *     // getters and setters
 * }
 *
 * // 响应对象实现
 * public class GetDatabaseResponse implements RESTResponse {
 *     private String name;
 *     private Map<String, String> properties;
 *     // getters and setters
 * }
 * }</pre>
 *
 * @see RESTRequest
 * @see RESTResponse
 * @see org.apache.paimon.rest.RESTApi#toJson(Object)
 * @see org.apache.paimon.rest.RESTApi#fromJson(String, Class)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public interface RESTMessage {}
