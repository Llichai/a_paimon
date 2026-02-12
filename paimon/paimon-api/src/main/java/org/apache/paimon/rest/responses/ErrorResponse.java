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

import javax.annotation.Nullable;

/**
 * 错误响应对象。
 *
 * <p>该类表示 REST API 调用失败时返回的错误信息。
 *
 * <p>响应字段说明:
 * <ul>
 *   <li>resourceType - 资源类型(如 DATABASE、TABLE、COLUMN 等)
 *   <li>resourceName - 资源名称
 *   <li>message - 错误消息描述
 *   <li>code - 错误代码(HTTP 状态码或自定义错误码)
 * </ul>
 *
 * <p>支持的资源类型常量:
 * <ul>
 *   <li>{@link #RESOURCE_TYPE_DATABASE} - 数据库
 *   <li>{@link #RESOURCE_TYPE_TABLE} - 表
 *   <li>{@link #RESOURCE_TYPE_COLUMN} - 列
 *   <li>{@link #RESOURCE_TYPE_SNAPSHOT} - 快照
 *   <li>{@link #RESOURCE_TYPE_BRANCH} - 分支
 *   <li>{@link #RESOURCE_TYPE_TAG} - 标签
 *   <li>{@link #RESOURCE_TYPE_VIEW} - 视图
 *   <li>{@link #RESOURCE_TYPE_DIALECT} - 方言
 *   <li>{@link #RESOURCE_TYPE_FUNCTION} - 函数
 *   <li>{@link #RESOURCE_TYPE_DEFINITION} - 定义
 * </ul>
 *
 * <p>JSON 格式示例:
 * <pre>
 * {
 *   "resourceType": "TABLE",
 *   "resourceName": "my_table",
 *   "message": "Table not found",
 *   "code": 404
 * }
 * </pre>
 *
 * @since 1.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ErrorResponse implements RESTResponse {

    public static final String RESOURCE_TYPE_DATABASE = "DATABASE";

    public static final String RESOURCE_TYPE_TABLE = "TABLE";

    public static final String RESOURCE_TYPE_COLUMN = "COLUMN";

    public static final String RESOURCE_TYPE_SNAPSHOT = "SNAPSHOT";

    public static final String RESOURCE_TYPE_BRANCH = "BRANCH";

    public static final String RESOURCE_TYPE_TAG = "TAG";

    public static final String RESOURCE_TYPE_VIEW = "VIEW";

    public static final String RESOURCE_TYPE_DIALECT = "DIALECT";

    public static final String RESOURCE_TYPE_FUNCTION = "FUNCTION";

    public static final String RESOURCE_TYPE_DEFINITION = "DEFINITION";

    private static final String FIELD_MESSAGE = "message";
    private static final String FIELD_RESOURCE_TYPE = "resourceType";
    private static final String FIELD_RESOURCE_NAME = "resourceName";
    private static final String FIELD_CODE = "code";

    @Nullable
    @JsonProperty(FIELD_RESOURCE_TYPE)
    private final String resourceType;

    @Nullable
    @JsonProperty(FIELD_RESOURCE_NAME)
    private final String resourceName;

    @JsonProperty(FIELD_MESSAGE)
    private final String message;

    @JsonProperty(FIELD_CODE)
    private final Integer code;

    /**
     * 构造错误响应对象。
     *
     * @param resourceType 资源类型,可为 null
     * @param resourceName 资源名称,可为 null
     * @param message 错误消息
     * @param code 错误代码
     */
    @JsonCreator
    public ErrorResponse(
            @Nullable @JsonProperty(FIELD_RESOURCE_TYPE) String resourceType,
            @Nullable @JsonProperty(FIELD_RESOURCE_NAME) String resourceName,
            @JsonProperty(FIELD_MESSAGE) String message,
            @JsonProperty(FIELD_CODE) int code) {
        this.resourceType = resourceType;
        this.resourceName = resourceName;
        this.message = message;
        this.code = code;
    }

    /**
     * 获取错误消息。
     *
     * @return 错误消息描述
     */
    @JsonGetter(FIELD_MESSAGE)
    public String getMessage() {
        return message;
    }

    /**
     * 获取资源类型。
     *
     * @return 资源类型,可能为 null
     */
    @JsonGetter(FIELD_RESOURCE_TYPE)
    public String getResourceType() {
        return resourceType;
    }

    /**
     * 获取资源名称。
     *
     * @return 资源名称,可能为 null
     */
    @JsonGetter(FIELD_RESOURCE_NAME)
    public String getResourceName() {
        return resourceName;
    }

    /**
     * 获取错误代码。
     *
     * @return 错误代码
     */
    @JsonGetter(FIELD_CODE)
    public Integer getCode() {
        return code;
    }
}
