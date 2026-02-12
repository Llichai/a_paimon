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
import org.apache.paimon.view.ViewChange;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * 修改视图请求。
 *
 * <p>用于向 REST 服务器发送视图变更请求,支持修改视图查询语句、注释等。
 *
 * <p>JSON 序列化格式:
 *
 * <pre>{@code
 * {
 *   "changes": [
 *     {
 *       "type": "UPDATE_COMMENT",
 *       "comment": "new comment"
 *     },
 *     {
 *       "type": "UPDATE_QUERY",
 *       "query": "SELECT * FROM new_table"
 *     }
 *   ]
 * }
 * }</pre>
 *
 * <p>示例: 更新视图查询和注释
 *
 * <pre>{@code
 * List<ViewChange> changes = Arrays.asList(
 *     ViewChange.updateComment("Updated view"),
 *     ViewChange.updateQuery("SELECT id, name FROM users WHERE active = true")
 * );
 * AlterViewRequest request = new AlterViewRequest(changes);
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AlterViewRequest implements RESTRequest {

    private static final String FIELD_CHANGES = "changes";

    /** 视图变更列表。 */
    @JsonProperty(FIELD_CHANGES)
    private final List<ViewChange> viewChanges;

    /**
     * 构造函数。
     *
     * @param viewChanges 视图变更列表
     */
    @JsonCreator
    public AlterViewRequest(@JsonProperty(FIELD_CHANGES) List<ViewChange> viewChanges) {
        this.viewChanges = viewChanges;
    }

    /**
     * 获取视图变更列表。
     *
     * @return 视图变更列表
     */
    @JsonGetter(FIELD_CHANGES)
    public List<ViewChange> viewChanges() {
        return viewChanges;
    }
}
