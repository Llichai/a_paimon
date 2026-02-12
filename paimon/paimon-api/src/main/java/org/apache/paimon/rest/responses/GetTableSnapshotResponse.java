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
import org.apache.paimon.table.TableSnapshot;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 获取表快照响应对象。
 *
 * <p>该类表示获取指定表的快照信息的响应。
 *
 * <p>响应字段:
 * <ul>
 *   <li>snapshot - 表快照对象,包含快照的完整信息
 * </ul>
 *
 * <p>JSON 格式示例:
 * <pre>
 * {
 *   "snapshot": {
 *     "id": 1,
 *     "schemaId": 0,
 *     "commitUser": "user1",
 *     "commitIdentifier": 123456789,
 *     "commitKind": "APPEND",
 *     "timeMillis": 1609459200000,
 *     ...
 *   }
 * }
 * </pre>
 *
 * @since 1.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GetTableSnapshotResponse implements RESTResponse {

    private static final String FIELD_SNAPSHOT = "snapshot";

    @JsonProperty(FIELD_SNAPSHOT)
    private final TableSnapshot snapshot;

    @JsonCreator
    public GetTableSnapshotResponse(@JsonProperty(FIELD_SNAPSHOT) TableSnapshot snapshot) {
        this.snapshot = snapshot;
    }

    @JsonGetter(FIELD_SNAPSHOT)
    public TableSnapshot getSnapshot() {
        return snapshot;
    }
}
