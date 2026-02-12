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

import java.util.List;

/**
 * 修改数据库响应对象。
 *
 * <p>该类表示修改数据库操作的响应,包含属性修改的详细信息。
 *
 * <p>响应字段说明:
 * <ul>
 *   <li>removed - 已删除的属性名列表
 *   <li>updated - 已更新的属性名列表
 *   <li>missing - 请求删除但实际不存在的属性名列表
 * </ul>
 *
 * <p>JSON 格式示例:
 * <pre>
 * {
 *   "removed": ["prop1", "prop2"],
 *   "updated": ["prop3", "prop4"],
 *   "missing": ["prop5"]
 * }
 * </pre>
 *
 * <p>该响应帮助客户端了解哪些属性修改成功,哪些属性不存在。
 *
 * @since 1.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AlterDatabaseResponse implements RESTResponse {

    private static final String FIELD_REMOVED = "removed";
    private static final String FIELD_UPDATED = "updated";
    private static final String FIELD_MISSING = "missing";

    @JsonProperty(FIELD_REMOVED)
    private final List<String> removed;

    @JsonProperty(FIELD_UPDATED)
    private final List<String> updated;

    @JsonProperty(FIELD_MISSING)
    private final List<String> missing;

    /**
     * 构造修改数据库响应对象。
     *
     * @param removed 已删除的属性名列表
     * @param updated 已更新的属性名列表
     * @param missing 请求删除但实际不存在的属性名列表
     */
    @JsonCreator
    public AlterDatabaseResponse(
            @JsonProperty(FIELD_REMOVED) List<String> removed,
            @JsonProperty(FIELD_UPDATED) List<String> updated,
            @JsonProperty(FIELD_MISSING) List<String> missing) {
        this.removed = removed;
        this.updated = updated;
        this.missing = missing;
    }

    /**
     * 获取已删除的属性名列表。
     *
     * @return 已删除的属性名列表
     */
    @JsonGetter(FIELD_REMOVED)
    public List<String> getRemoved() {
        return removed;
    }

    /**
     * 获取已更新的属性名列表。
     *
     * @return 已更新的属性名列表
     */
    @JsonGetter(FIELD_UPDATED)
    public List<String> getUpdated() {
        return updated;
    }

    /**
     * 获取请求删除但实际不存在的属性名列表。
     *
     * @return 不存在的属性名列表
     */
    @JsonGetter(FIELD_MISSING)
    public List<String> getMissing() {
        return missing;
    }
}
