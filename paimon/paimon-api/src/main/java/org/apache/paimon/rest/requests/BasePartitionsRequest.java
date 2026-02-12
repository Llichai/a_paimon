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

import java.util.List;
import java.util.Map;

/**
 * 分区操作基础请求。
 *
 * <p>所有分区相关操作请求的基类,包含分区规格列表。
 *
 * <p>分区规格是一个键值对映射,键是分区字段名,值是分区值。
 *
 * <p>JSON 序列化格式:
 *
 * <pre>{@code
 * {
 *   "specs": [
 *     {
 *       "year": "2024",
 *       "month": "01"
 *     },
 *     {
 *       "year": "2024",
 *       "month": "02"
 *     }
 *   ]
 * }
 * }</pre>
 *
 * <p>示例: 指定多个分区
 *
 * <pre>{@code
 * List<Map<String, String>> specs = Arrays.asList(
 *     Map.of("year", "2024", "month", "01", "day", "15"),
 *     Map.of("year", "2024", "month", "01", "day", "16")
 * );
 * BasePartitionsRequest request = new SomePartitionsRequest(specs);
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class BasePartitionsRequest implements RESTRequest {

    protected static final String FIELD_PARTITION_SPECS = "specs";

    /** 分区规格列表,每个规格是分区字段名到分区值的映射。 */
    @JsonProperty(FIELD_PARTITION_SPECS)
    private final List<Map<String, String>> partitionSpecs;

    /**
     * 构造函数。
     *
     * @param partitionSpecs 分区规格列表
     */
    @JsonCreator
    public BasePartitionsRequest(
            @JsonProperty(FIELD_PARTITION_SPECS) List<Map<String, String>> partitionSpecs) {
        this.partitionSpecs = partitionSpecs;
    }

    /**
     * 获取分区规格列表。
     *
     * @return 分区规格列表
     */
    @JsonGetter(FIELD_PARTITION_SPECS)
    public List<Map<String, String>> getPartitionSpecs() {
        return partitionSpecs;
    }
}
