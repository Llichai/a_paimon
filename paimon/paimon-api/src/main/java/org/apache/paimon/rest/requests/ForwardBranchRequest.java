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

package org.apache.paimon.rest.requests;

import org.apache.paimon.rest.RESTRequest;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 快进分支请求。
 *
 * <p>用于向 REST 服务器发送分支快进操作请求。快进操作将分支指针移动到更新的快照,前提是不存在分叉历史。
 *
 * <p>此请求通常用于将分支更新到最新状态,类似于 Git 的 fast-forward 操作。
 *
 * <p>JSON 序列化格式:
 *
 * <pre>{@code
 * {}
 * }</pre>
 *
 * <p>注意: 此请求不包含任何字段,所有必要信息通过 URL 路径参数传递。
 *
 * <p>示例:
 *
 * <pre>{@code
 * ForwardBranchRequest request = new ForwardBranchRequest();
 * // 请求路径通常为: /v1/tables/{database}/{table}/branches/{branch}/forward
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ForwardBranchRequest implements RESTRequest {}
