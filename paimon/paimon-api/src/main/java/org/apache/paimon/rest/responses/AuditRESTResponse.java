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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * 审计 REST 响应基类。
 *
 * <p>该抽象类为数据库、表、视图等响应提供审计信息的基础实现。
 *
 * <p>审计字段说明:
 * <ul>
 *   <li>owner - 所有者名称
 *   <li>createdAt - 创建时间戳(毫秒)
 *   <li>createdBy - 创建者名称
 *   <li>updatedAt - 最后更新时间戳(毫秒)
 *   <li>updatedBy - 最后更新者名称
 * </ul>
 *
 * <p>JSON 格式示例:
 * <pre>
 * {
 *   "owner": "admin",
 *   "createdAt": 1609459200000,
 *   "createdBy": "user1",
 *   "updatedAt": 1609545600000,
 *   "updatedBy": "user2"
 * }
 * </pre>
 *
 * <p>子类应继承该类以获得统一的审计信息管理能力。
 *
 * @since 1.0
 */
public abstract class AuditRESTResponse implements RESTResponse {

    public static final String FIELD_OWNER = "owner";
    public static final String FIELD_CREATED_AT = "createdAt";
    public static final String FIELD_CREATED_BY = "createdBy";
    public static final String FIELD_UPDATED_AT = "updatedAt";
    public static final String FIELD_UPDATED_BY = "updatedBy";

    @JsonProperty(FIELD_OWNER)
    private final String owner;

    @JsonProperty(FIELD_CREATED_AT)
    private final long createdAt;

    @JsonProperty(FIELD_CREATED_BY)
    private final String createdBy;

    @JsonProperty(FIELD_UPDATED_AT)
    private final long updatedAt;

    @JsonProperty(FIELD_UPDATED_BY)
    private final String updatedBy;

    /**
     * 构造审计响应对象。
     *
     * @param owner 所有者名称
     * @param createdAt 创建时间戳(毫秒)
     * @param createdBy 创建者名称
     * @param updatedAt 最后更新时间戳(毫秒)
     * @param updatedBy 最后更新者名称
     */
    @JsonCreator
    public AuditRESTResponse(
            @JsonProperty(FIELD_OWNER) String owner,
            @JsonProperty(FIELD_CREATED_AT) long createdAt,
            @JsonProperty(FIELD_CREATED_BY) String createdBy,
            @JsonProperty(FIELD_UPDATED_AT) long updatedAt,
            @JsonProperty(FIELD_UPDATED_BY) String updatedBy) {
        this.owner = owner;
        this.createdAt = createdAt;
        this.createdBy = createdBy;
        this.updatedAt = updatedAt;
        this.updatedBy = updatedBy;
    }

    /**
     * 获取所有者名称。
     *
     * @return 所有者名称
     */
    @JsonGetter(FIELD_OWNER)
    public String getOwner() {
        return owner;
    }

    /**
     * 获取创建时间戳。
     *
     * @return 创建时间戳(毫秒)
     */
    @JsonGetter(FIELD_CREATED_AT)
    public long getCreatedAt() {
        return createdAt;
    }

    /**
     * 获取创建者名称。
     *
     * @return 创建者名称
     */
    @JsonGetter(FIELD_CREATED_BY)
    public String getCreatedBy() {
        return createdBy;
    }

    /**
     * 获取最后更新时间戳。
     *
     * @return 最后更新时间戳(毫秒)
     */
    @JsonGetter(FIELD_UPDATED_AT)
    public long getUpdatedAt() {
        return updatedAt;
    }

    /**
     * 获取最后更新者名称。
     *
     * @return 最后更新者名称
     */
    @JsonGetter(FIELD_UPDATED_BY)
    public String getUpdatedBy() {
        return updatedBy;
    }

    /**
     * 将审计选项放入配置映射中。
     *
     * <p>该方法将所有审计字段以字符串形式添加到提供的选项映射中。
     *
     * @param options 目标选项映射
     */
    public void putAuditOptionsTo(Map<String, String> options) {
        options.put(FIELD_OWNER, getOwner());
        options.put(FIELD_CREATED_BY, String.valueOf(getCreatedBy()));
        options.put(FIELD_CREATED_AT, String.valueOf(getCreatedAt()));
        options.put(FIELD_UPDATED_BY, String.valueOf(getUpdatedBy()));
        options.put(FIELD_UPDATED_AT, String.valueOf(getUpdatedAt()));
    }
}
