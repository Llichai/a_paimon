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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.rest.RESTRequest;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 重命名表请求。
 *
 * <p>用于向 REST 服务器发送表重命名请求,支持在相同或不同数据库之间移动表。
 *
 * <p>JSON 序列化格式:
 *
 * <pre>{@code
 * {
 *   "source": {
 *     "database": "db1",
 *     "table": "old_table"
 *   },
 *   "destination": {
 *     "database": "db2",
 *     "table": "new_table"
 *   }
 * }
 * }</pre>
 *
 * <p>示例: 在同一数据库内重命名表
 *
 * <pre>{@code
 * Identifier source = Identifier.create("my_db", "old_name");
 * Identifier destination = Identifier.create("my_db", "new_name");
 * RenameTableRequest request = new RenameTableRequest(source, destination);
 * }</pre>
 *
 * <p>示例: 将表移动到不同数据库
 *
 * <pre>{@code
 * Identifier source = Identifier.create("db1", "table1");
 * Identifier destination = Identifier.create("db2", "table1");
 * RenameTableRequest request = new RenameTableRequest(source, destination);
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RenameTableRequest implements RESTRequest {

    private static final String FIELD_SOURCE = "source";
    private static final String FIELD_DESTINATION = "destination";

    /** 源表标识符。 */
    @JsonProperty(FIELD_SOURCE)
    private final Identifier source;

    /** 目标表标识符。 */
    @JsonProperty(FIELD_DESTINATION)
    private final Identifier destination;

    /**
     * 构造函数。
     *
     * @param source 源表标识符
     * @param destination 目标表标识符
     */
    @JsonCreator
    public RenameTableRequest(
            @JsonProperty(FIELD_SOURCE) Identifier source,
            @JsonProperty(FIELD_DESTINATION) Identifier destination) {
        this.source = source;
        this.destination = destination;
    }

    /**
     * 获取目标表标识符。
     *
     * @return 目标表标识符
     */
    @JsonGetter(FIELD_DESTINATION)
    public Identifier getDestination() {
        return destination;
    }

    /**
     * 获取源表标识符。
     *
     * @return 源表标识符
     */
    @JsonGetter(FIELD_SOURCE)
    public Identifier getSource() {
        return source;
    }
}
