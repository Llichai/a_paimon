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

import org.apache.paimon.Snapshot;
import org.apache.paimon.rest.RESTResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 获取版本快照响应对象。
 *
 * <p>该类表示通过版本号获取表快照信息的响应。
 *
 * <p>响应字段:
 * <ul>
 *   <li>snapshot - 指定版本的快照对象
 * </ul>
 *
 * <p>与 {@link GetTableSnapshotResponse} 的区别在于获取方式:
 * <ul>
 *   <li>GetTableSnapshotResponse - 获取当前快照
 *   <li>GetVersionSnapshotResponse - 获取特定版本的历史快照
 * </ul>
 *
 * @since 1.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GetVersionSnapshotResponse implements RESTResponse {

    private static final String FIELD_SNAPSHOT = "snapshot";

    @JsonProperty(FIELD_SNAPSHOT)
    private final Snapshot snapshot;

    @JsonCreator
    public GetVersionSnapshotResponse(@JsonProperty(FIELD_SNAPSHOT) Snapshot snapshot) {
        this.snapshot = snapshot;
    }

    @JsonGetter(FIELD_SNAPSHOT)
    public Snapshot getSnapshot() {
        return snapshot;
    }
}
