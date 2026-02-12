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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * 列出快照响应对象。
 *
 * <p>该类表示列出表所有快照的分页响应。
 *
 * <p>响应字段:
 * <ul>
 *   <li>snapshots - 快照对象列表
 *   <li>nextPageToken - 下一页令牌
 * </ul>
 *
 * @since 1.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ListSnapshotsResponse implements PagedResponse<Snapshot> {

    private static final String FIELD_SNAPSHOTS = "snapshots";
    private static final String FIELD_NEXT_PAGE_TOKEN = "nextPageToken";

    @JsonProperty(FIELD_SNAPSHOTS)
    private final List<Snapshot> snapshots;

    @JsonProperty(FIELD_NEXT_PAGE_TOKEN)
    private final String nextPageToken;

    public ListSnapshotsResponse(@JsonProperty(FIELD_SNAPSHOTS) List<Snapshot> snapshots) {
        this(snapshots, null);
    }

    @JsonCreator
    public ListSnapshotsResponse(
            @JsonProperty(FIELD_SNAPSHOTS) List<Snapshot> snapshots,
            @JsonProperty(FIELD_NEXT_PAGE_TOKEN) String nextPageToken) {
        this.snapshots = snapshots;
        this.nextPageToken = nextPageToken;
    }

    @JsonGetter(FIELD_SNAPSHOTS)
    public List<Snapshot> getSnapshots() {
        return this.snapshots;
    }

    @Override
    public List<Snapshot> data() {
        return snapshots;
    }

    @JsonGetter(FIELD_NEXT_PAGE_TOKEN)
    public String getNextPageToken() {
        return this.nextPageToken;
    }
}
