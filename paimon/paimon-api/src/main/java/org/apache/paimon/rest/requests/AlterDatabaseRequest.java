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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * 修改数据库请求。
 *
 * <p>用于向 REST 服务器发送数据库属性修改请求,支持删除和更新数据库配置选项。
 *
 * <p>JSON 序列化格式:
 *
 * <pre>{@code
 * {
 *   "removals": ["option1", "option2"],
 *   "updates": {
 *     "option3": "value3",
 *     "option4": "value4"
 *   }
 * }
 * }</pre>
 *
 * <p>示例: 修改数据库配置
 *
 * <pre>{@code
 * List<String> removals = Arrays.asList("old.property");
 * Map<String, String> updates = new HashMap<>();
 * updates.put("new.property", "new_value");
 * AlterDatabaseRequest request = new AlterDatabaseRequest(removals, updates);
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AlterDatabaseRequest implements RESTRequest {

    private static final String FIELD_REMOVALS = "removals";
    private static final String FIELD_UPDATES = "updates";

    /** 需要删除的属性键列表。 */
    @JsonProperty(FIELD_REMOVALS)
    private final List<String> removals;

    /** 需要更新的属性键值对。 */
    @JsonProperty(FIELD_UPDATES)
    private final Map<String, String> updates;

    /**
     * 构造函数。
     *
     * @param removals 需要删除的属性键列表
     * @param updates 需要更新的属性键值对
     */
    @JsonCreator
    public AlterDatabaseRequest(
            @JsonProperty(FIELD_REMOVALS) List<String> removals,
            @JsonProperty(FIELD_UPDATES) Map<String, String> updates) {
        this.removals = removals;
        this.updates = updates;
    }

    /**
     * 获取需要删除的属性键列表。
     *
     * @return 属性键列表
     */
    @JsonGetter(FIELD_REMOVALS)
    public List<String> getRemovals() {
        return removals;
    }

    /**
     * 获取需要更新的属性键值对。
     *
     * @return 属性键值对映射
     */
    @JsonGetter(FIELD_UPDATES)
    public Map<String, String> getUpdates() {
        return updates;
    }
}
