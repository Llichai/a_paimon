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
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

/**
 * 配置响应对象。
 *
 * <p>该类表示获取配置的响应,包含默认配置和覆盖配置。
 *
 * <p>响应字段说明:
 * <ul>
 *   <li>defaults - 默认配置映射,优先级最低
 *   <li>overrides - 覆盖配置映射,优先级最高
 * </ul>
 *
 * <p>配置合并优先级(从低到高):
 * <ol>
 *   <li>defaults - 服务端默认配置
 *   <li>client properties - 客户端提供的配置
 *   <li>overrides - 服务端强制覆盖配置
 * </ol>
 *
 * <p>JSON 格式示例:
 * <pre>
 * {
 *   "defaults": {
 *     "option1": "default_value1",
 *     "option2": "default_value2"
 *   },
 *   "overrides": {
 *     "option1": "override_value1"
 *   }
 * }
 * </pre>
 *
 * <p>使用 {@link #merge(Map)} 方法将客户端配置与服务端配置合并。
 *
 * @since 1.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConfigResponse implements RESTResponse {

    private static final String FIELD_DEFAULTS = "defaults";
    private static final String FIELD_OVERRIDES = "overrides";

    @JsonProperty(FIELD_DEFAULTS)
    private final Map<String, String> defaults;

    @JsonProperty(FIELD_OVERRIDES)
    private final Map<String, String> overrides;

    /**
     * 构造配置响应对象。
     *
     * @param defaults 默认配置映射
     * @param overrides 覆盖配置映射
     */
    @JsonCreator
    public ConfigResponse(
            @JsonProperty(FIELD_DEFAULTS) Map<String, String> defaults,
            @JsonProperty(FIELD_OVERRIDES) Map<String, String> overrides) {
        this.defaults = defaults;
        this.overrides = overrides;
    }

    /**
     * 合并客户端配置与服务端配置。
     *
     * <p>合并顺序:
     * <ol>
     *   <li>从 defaults 开始
     *   <li>应用 clientProperties
     *   <li>应用 overrides(最高优先级)
     * </ol>
     *
     * @param clientProperties 客户端配置属性,不能为 null
     * @return 合并后的不可变配置映射,不包含 null 值
     * @throws NullPointerException 如果 clientProperties 为 null
     */
    public Map<String, String> merge(Map<String, String> clientProperties) {
        Preconditions.checkNotNull(
                clientProperties,
                "Cannot merge client properties with server-provided properties. Invalid client configuration: null");
        Map<String, String> merged =
                defaults != null ? Maps.newHashMap(defaults) : Maps.newHashMap();
        merged.putAll(clientProperties);

        if (overrides != null) {
            merged.putAll(overrides);
        }

        return ImmutableMap.copyOf(Maps.filterValues(merged, Objects::nonNull));
    }

    /**
     * 获取默认配置映射。
     *
     * @return 默认配置映射,可能为 null
     */
    @JsonGetter(FIELD_DEFAULTS)
    public Map<String, String> getDefaults() {
        return defaults;
    }

    /**
     * 获取覆盖配置映射。
     *
     * @return 覆盖配置映射,可能为 null
     */
    @JsonGetter(FIELD_OVERRIDES)
    public Map<String, String> getOverrides() {
        return overrides;
    }
}
