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
import org.apache.paimon.schema.SchemaChange;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * 修改表请求。
 *
 * <p>用于向 REST 服务器发送表 Schema 变更请求,支持添加列、删除列、重命名列、修改表配置等操作。
 *
 * <p>JSON 序列化格式:
 *
 * <pre>{@code
 * {
 *   "changes": [
 *     {
 *       "type": "ADD_COLUMN",
 *       "fieldName": "new_col",
 *       "dataType": "INT"
 *     },
 *     {
 *       "type": "SET_OPTION",
 *       "key": "bucket",
 *       "value": "10"
 *     }
 *   ]
 * }
 * }</pre>
 *
 * <p>示例: 添加列并修改配置
 *
 * <pre>{@code
 * List<SchemaChange> changes = Arrays.asList(
 *     SchemaChange.addColumn("age", DataTypes.INT()),
 *     SchemaChange.setOption("bucket", "10")
 * );
 * AlterTableRequest request = new AlterTableRequest(changes);
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AlterTableRequest implements RESTRequest {

    private static final String FIELD_NEW_UPDATE = "changes";

    /** Schema 变更列表。 */
    @JsonProperty(FIELD_NEW_UPDATE)
    private final List<SchemaChange> changes;

    /**
     * 构造函数。
     *
     * @param changes Schema 变更列表
     */
    @JsonCreator
    public AlterTableRequest(@JsonProperty(FIELD_NEW_UPDATE) List<SchemaChange> changes) {
        this.changes = changes;
    }

    /**
     * 获取 Schema 变更列表。
     *
     * @return Schema 变更列表
     */
    @JsonGetter(FIELD_NEW_UPDATE)
    public List<SchemaChange> getChanges() {
        return changes;
    }
}
