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
 * 创建分支请求。
 *
 * <p>用于向 REST 服务器发送创建表分支的请求,可以从指定标签创建分支。
 *
 * <p>分支是表的一个命名快照,允许在不影响主线的情况下进行实验性操作。
 *
 * <p>JSON 序列化格式:
 *
 * <pre>{@code
 * {
 *   "branch": "feature-branch",
 *   "fromTag": "v1.0.0"
 * }
 * }</pre>
 *
 * <p>或者从当前状态创建分支:
 *
 * <pre>{@code
 * {
 *   "branch": "dev-branch",
 *   "fromTag": null
 * }
 * }</pre>
 *
 * <p>示例: 从标签创建分支
 *
 * <pre>{@code
 * CreateBranchRequest request = new CreateBranchRequest("hotfix-branch", "v2.1.0");
 * }</pre>
 *
 * <p>示例: 从当前状态创建分支
 *
 * <pre>{@code
 * CreateBranchRequest request = new CreateBranchRequest("dev-branch", null);
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateBranchRequest implements RESTRequest {

    private static final String FIELD_BRANCH = "branch";
    private static final String FIELD_FROM_TAG = "fromTag";

    /** 分支名称。 */
    @JsonProperty(FIELD_BRANCH)
    private final String branch;

    /** 源标签名称,为 null 表示从当前状态创建分支。 */
    @Nullable
    @JsonProperty(FIELD_FROM_TAG)
    private final String fromTag;

    /**
     * 构造函数。
     *
     * @param branch 分支名称
     * @param fromTag 源标签名称,为 null 表示从当前状态创建分支
     */
    @JsonCreator
    public CreateBranchRequest(
            @JsonProperty(FIELD_BRANCH) String branch,
            @Nullable @JsonProperty(FIELD_FROM_TAG) String fromTag) {
        this.branch = branch;
        this.fromTag = fromTag;
    }

    /**
     * 获取分支名称。
     *
     * @return 分支名称
     */
    @JsonGetter(FIELD_BRANCH)
    public String branch() {
        return branch;
    }

    /**
     * 获取源标签名称。
     *
     * @return 源标签名称,为 null 表示从当前状态创建分支
     */
    @Nullable
    @JsonGetter(FIELD_FROM_TAG)
    public String fromTag() {
        return fromTag;
    }
}
