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
import org.apache.paimon.table.Instant;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/**
 * 回滚表请求。
 *
 * <p>用于向 REST 服务器发送表回滚请求,将表恢复到指定的时间点或快照。
 *
 * <p>支持两种回滚方式:
 *
 * <ul>
 *   <li>基于时间戳: 回滚到指定时间点的状态
 *   <li>基于快照: 回滚到指定快照ID
 * </ul>
 *
 * <p>JSON 序列化格式 - 基于时间戳:
 *
 * <pre>{@code
 * {
 *   "instant": {
 *     "timestamp": 1640000000000,
 *     "watermark": null
 *   },
 *   "fromSnapshot": null
 * }
 * }</pre>
 *
 * <p>JSON 序列化格式 - 基于快照:
 *
 * <pre>{@code
 * {
 *   "instant": {
 *     "timestamp": null,
 *     "watermark": null
 *   },
 *   "fromSnapshot": 100
 * }
 * }</pre>
 *
 * <p>示例: 回滚到指定时间点
 *
 * <pre>{@code
 * Instant instant = new Instant(1640000000000L, null);
 * RollbackTableRequest request = new RollbackTableRequest(instant, null);
 * }</pre>
 *
 * <p>示例: 回滚到指定快照
 *
 * <pre>{@code
 * Instant instant = new Instant(null, null);
 * RollbackTableRequest request = new RollbackTableRequest(instant, 100L);
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RollbackTableRequest implements RESTRequest {

    private static final String FIELD_INSTANT = "instant";
    private static final String FIELD_FROM_SNAPSHOT = "fromSnapshot";

    /** 目标时间点。 */
    @JsonProperty(FIELD_INSTANT)
    private final Instant instant;

    /** 目标快照 ID,为 null 表示基于时间戳回滚。 */
    @JsonProperty(FIELD_FROM_SNAPSHOT)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final Long fromSnapshot;

    /**
     * 构造函数。
     *
     * @param instant 目标时间点
     * @param fromSnapshot 目标快照 ID,为 null 表示基于时间戳回滚
     */
    @JsonCreator
    public RollbackTableRequest(
            @JsonProperty(FIELD_INSTANT) Instant instant,
            @JsonProperty(FIELD_FROM_SNAPSHOT) @Nullable Long fromSnapshot) {
        this.instant = instant;
        this.fromSnapshot = fromSnapshot;
    }

    /**
     * 获取目标时间点。
     *
     * @return 时间点对象
     */
    @JsonGetter(FIELD_INSTANT)
    public Instant getInstant() {
        return instant;
    }

    /**
     * 获取目标快照 ID。
     *
     * @return 快照 ID,为 null 表示基于时间戳回滚
     */
    @JsonGetter(FIELD_FROM_SNAPSHOT)
    @Nullable
    public Long getFromSnapshot() {
        return fromSnapshot;
    }
}
