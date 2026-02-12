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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * 标记完成分区请求。
 *
 * <p>用于向 REST 服务器发送标记分区为"已完成"状态的请求。
 *
 * <p>此操作通常用于分区级别的数据质量管理,标记某些分区的数据已经准备就绪可供查询。
 *
 * <p>JSON 序列化格式:
 *
 * <pre>{@code
 * {
 *   "specs": [
 *     {
 *       "year": "2024",
 *       "month": "01",
 *       "day": "15"
 *     },
 *     {
 *       "year": "2024",
 *       "month": "01",
 *       "day": "16"
 *     }
 *   ]
 * }
 * }</pre>
 *
 * <p>示例: 标记多个分区为完成
 *
 * <pre>{@code
 * List<Map<String, String>> specs = Arrays.asList(
 *     Map.of("year", "2024", "month", "01", "day", "15"),
 *     Map.of("year", "2024", "month", "01", "day", "16")
 * );
 * MarkDonePartitionsRequest request = new MarkDonePartitionsRequest(specs);
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MarkDonePartitionsRequest extends BasePartitionsRequest {

    /**
     * 构造函数。
     *
     * @param partitionSpecs 要标记为完成的分区规格列表
     */
    @JsonCreator
    public MarkDonePartitionsRequest(
            @JsonProperty(FIELD_PARTITION_SPECS) List<Map<String, String>> partitionSpecs) {
        super(partitionSpecs);
    }
}
