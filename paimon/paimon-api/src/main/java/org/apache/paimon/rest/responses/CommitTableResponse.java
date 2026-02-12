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

package org.apache.paimon.rest.responses;

import org.apache.paimon.rest.RESTResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 提交表快照响应对象。
 *
 * <p>该类表示表快照提交操作的响应结果。
 *
 * <p>响应字段说明:
 * <ul>
 *   <li>success - 提交是否成功的布尔值
 * </ul>
 *
 * <p>JSON 格式示例:
 * <pre>
 * {
 *   "success": true
 * }
 * </pre>
 *
 * <p>当 success 为 false 时,通常会伴随错误响应提供详细失败原因。
 *
 * @since 1.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CommitTableResponse implements RESTResponse {

    private static final String FIELD_SUCCESS = "success";

    @JsonProperty(FIELD_SUCCESS)
    private final boolean success;

    /**
     * 构造提交表响应对象。
     *
     * @param success 提交是否成功
     */
    @JsonCreator
    public CommitTableResponse(@JsonProperty(FIELD_SUCCESS) boolean success) {
        this.success = success;
    }

    /**
     * 检查提交是否成功。
     *
     * @return 如果提交成功返回 true,否则返回 false
     */
    @JsonGetter(FIELD_SUCCESS)
    public boolean isSuccess() {
        return success;
    }
}
