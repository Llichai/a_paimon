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

import javax.annotation.Nullable;

/**
 * 创建标签请求。
 *
 * <p>用于向 REST 服务器发送创建表快照标签的请求,可以为特定快照创建命名标签。
 *
 * <p>标签是快照的不可变命名引用,可用于版本控制和长期保留。
 *
 * <p>JSON 序列化格式:
 *
 * <pre>{@code
 * {
 *   "tagName": "v1.0.0",
 *   "snapshotId": 123,
 *   "timeRetained": "7d"
 * }
 * }</pre>
 *
 * <p>或者为最新快照创建标签:
 *
 * <pre>{@code
 * {
 *   "tagName": "latest",
 *   "snapshotId": null,
 *   "timeRetained": null
 * }
 * }</pre>
 *
 * <p>示例: 为特定快照创建标签
 *
 * <pre>{@code
 * CreateTagRequest request = new CreateTagRequest("release-1.0", 100L, "30d");
 * }</pre>
 *
 * <p>示例: 为最新快照创建标签
 *
 * <pre>{@code
 * CreateTagRequest request = new CreateTagRequest("latest", null, null);
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateTagRequest implements RESTRequest {

    private static final String FIELD_TAG_NAME = "tagName";
    private static final String FIELD_SNAPSHOT_ID = "snapshotId";
    private static final String FIELD_TIME_RETAINED = "timeRetained";

    /** 标签名称。 */
    @JsonProperty(FIELD_TAG_NAME)
    private final String tagName;

    /** 快照 ID,为 null 表示使用最新快照。 */
    @Nullable
    @JsonProperty(FIELD_SNAPSHOT_ID)
    private final Long snapshotId;

    /** 保留时间(如 "7d" 表示7天, "1h" 表示1小时),为 null 表示永久保留。 */
    @Nullable
    @JsonProperty(FIELD_TIME_RETAINED)
    private final String timeRetained;

    /**
     * 构造函数。
     *
     * @param tagName 标签名称
     * @param snapshotId 快照 ID,为 null 表示使用最新快照
     * @param timeRetained 保留时间,为 null 表示永久保留
     */
    @JsonCreator
    public CreateTagRequest(
            @JsonProperty(FIELD_TAG_NAME) String tagName,
            @Nullable @JsonProperty(FIELD_SNAPSHOT_ID) Long snapshotId,
            @Nullable @JsonProperty(FIELD_TIME_RETAINED) String timeRetained) {
        this.tagName = tagName;
        this.snapshotId = snapshotId;
        this.timeRetained = timeRetained;
    }

    /**
     * 获取标签名称。
     *
     * @return 标签名称
     */
    @JsonGetter(FIELD_TAG_NAME)
    public String tagName() {
        return tagName;
    }

    /**
     * 获取快照 ID。
     *
     * @return 快照 ID,为 null 表示使用最新快照
     */
    @Nullable
    @JsonGetter(FIELD_SNAPSHOT_ID)
    public Long snapshotId() {
        return snapshotId;
    }

    /**
     * 获取保留时间。
     *
     * @return 保留时间,为 null 表示永久保留
     */
    @Nullable
    @JsonGetter(FIELD_TIME_RETAINED)
    public String timeRetained() {
        return timeRetained;
    }
}
