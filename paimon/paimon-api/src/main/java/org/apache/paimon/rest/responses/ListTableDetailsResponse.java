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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * 列出表详情响应对象。
 *
 * <p>该类表示列出表详细信息的分页响应,包含完整的表元数据。
 *
 * <p>响应字段:
 * <ul>
 *   <li>tableDetails - 表详细信息列表
 *   <li>nextPageToken - 下一页令牌
 * </ul>
 *
 * @since 1.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ListTableDetailsResponse implements PagedResponse<GetTableResponse> {

    private static final String FIELD_TABLE_DETAILS = "tableDetails";
    private static final String FIELD_NEXT_PAGE_TOKEN = "nextPageToken";

    @JsonProperty(FIELD_TABLE_DETAILS)
    private final List<GetTableResponse> tableDetails;

    @JsonProperty(FIELD_NEXT_PAGE_TOKEN)
    private final String nextPageToken;

    public ListTableDetailsResponse(
            @JsonProperty(FIELD_TABLE_DETAILS) List<GetTableResponse> tableDetails) {
        this(tableDetails, null);
    }

    @JsonCreator
    public ListTableDetailsResponse(
            @JsonProperty(FIELD_TABLE_DETAILS) List<GetTableResponse> tableDetails,
            @JsonProperty(FIELD_NEXT_PAGE_TOKEN) String nextPageToken) {
        this.tableDetails = tableDetails;
        this.nextPageToken = nextPageToken;
    }

    @JsonGetter(FIELD_TABLE_DETAILS)
    public List<GetTableResponse> getTableDetails() {
        return this.tableDetails;
    }

    @JsonGetter(FIELD_NEXT_PAGE_TOKEN)
    public String getNextPageToken() {
        return this.nextPageToken;
    }

    @Override
    public List<GetTableResponse> data() {
        return getTableDetails();
    }
}
