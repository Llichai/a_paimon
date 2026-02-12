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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * 列出分支响应对象。
 *
 * <p>该类表示列出表所有分支的响应。
 *
 * <p>响应字段:
 * <ul>
 *   <li>branches - 分支名称列表
 * </ul>
 *
 * <p>JSON 格式示例:
 * <pre>
 * {
 *   "branches": ["main", "dev", "feature-x"]
 * }
 * </pre>
 *
 * @since 1.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ListBranchesResponse implements RESTResponse {

    public static final String FIELD_BRANCHES = "branches";

    @JsonProperty(FIELD_BRANCHES)
    private final List<String> branches;

    @JsonCreator
    public ListBranchesResponse(@JsonProperty(FIELD_BRANCHES) List<String> branches) {
        this.branches = branches;
    }

    @JsonGetter(FIELD_BRANCHES)
    public List<String> branches() {
        return branches;
    }
}
