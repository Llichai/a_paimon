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

import org.apache.paimon.Snapshot;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.rest.RESTRequest;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * 提交表快照请求。
 *
 * <p>用于向 REST 服务器发送表快照提交请求,包含快照数据和分区统计信息。
 *
 * <p>此请求用于将新的快照提交到表中,同时可选地包含分区统计信息用于优化查询计划。
 *
 * <p>JSON 序列化格式:
 *
 * <pre>{@code
 * {
 *   "tableId": "table-uuid-123",
 *   "snapshot": {
 *     "id": 1,
 *     "schemaId": 0,
 *     "baseManifestList": "manifest-list-abc",
 *     "deltaManifestList": "manifest-list-def",
 *     "commitUser": "user",
 *     "commitIdentifier": 100,
 *     "commitKind": "APPEND",
 *     "timeMillis": 1640000000000
 *   },
 *   "statistics": [
 *     {
 *       "partition": {"year": "2024"},
 *       "recordCount": 1000
 *     }
 *   ]
 * }
 * }</pre>
 *
 * <p>示例: 提交新快照
 *
 * <pre>{@code
 * Snapshot snapshot = new Snapshot(...);
 * List<PartitionStatistics> stats = Arrays.asList(...);
 * CommitTableRequest request = new CommitTableRequest("table-123", snapshot, stats);
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CommitTableRequest implements RESTRequest {

    private static final String FIELD_TABLE_ID = "tableId";
    private static final String FIELD_SNAPSHOT = "snapshot";
    private static final String FIELD_STATISTICS = "statistics";

    /** 表的唯一标识符。 */
    @JsonProperty(FIELD_TABLE_ID)
    private final String tableId;

    /** 要提交的快照。 */
    @JsonProperty(FIELD_SNAPSHOT)
    private final Snapshot snapshot;

    /** 分区统计信息列表。 */
    @JsonProperty(FIELD_STATISTICS)
    private final List<PartitionStatistics> statistics;

    /**
     * 构造函数。
     *
     * @param tableId 表的唯一标识符
     * @param snapshot 要提交的快照
     * @param statistics 分区统计信息列表
     */
    @JsonCreator
    public CommitTableRequest(
            @JsonProperty(FIELD_TABLE_ID) String tableId,
            @JsonProperty(FIELD_SNAPSHOT) Snapshot snapshot,
            @JsonProperty(FIELD_STATISTICS) List<PartitionStatistics> statistics) {
        this.tableId = tableId;
        this.snapshot = snapshot;
        this.statistics = statistics;
    }

    /**
     * 获取表的唯一标识符。
     *
     * @return 表ID
     */
    @JsonGetter(FIELD_TABLE_ID)
    public String getTableId() {
        return tableId;
    }

    /**
     * 获取要提交的快照。
     *
     * @return 快照对象
     */
    @JsonGetter(FIELD_SNAPSHOT)
    public Snapshot getSnapshot() {
        return snapshot;
    }

    /**
     * 获取分区统计信息列表。
     *
     * @return 统计信息列表
     */
    @JsonGetter(FIELD_STATISTICS)
    public List<PartitionStatistics> getStatistics() {
        return statistics;
    }
}
