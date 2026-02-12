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

package org.apache.paimon.rest;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

/**
 * REST 请求和响应的对象映射器。
 *
 * <p>这是一个已废弃的类,用于提供 Jackson ObjectMapper 实例。
 *
 * <h2>废弃原因</h2>
 * <p>此类已被 {@link RESTApi} 取代。{@link RESTApi} 提供了更完善的 JSON 序列化和反序列化功能,
 * 包括更好的错误处理和配置选项。
 *
 * <h2>迁移指南</h2>
 * <pre>{@code
 * // 旧代码
 * ObjectMapper mapper = RESTObjectMapper.OBJECT_MAPPER;
 * String json = mapper.writeValueAsString(obj);
 *
 * // 新代码
 * String json = RESTApi.toJson(obj);
 * MyObject obj = RESTApi.fromJson(json, MyObject.class);
 * }</pre>
 *
 * @deprecated 使用 {@link RESTApi} 代替
 * @see RESTApi#toJson(Object)
 * @see RESTApi#fromJson(String, Class)
 */
@Deprecated
public class RESTObjectMapper {

    /**
     * Jackson ObjectMapper 实例。
     *
     * <p>直接引用 {@link RESTApi#OBJECT_MAPPER}。
     *
     * @deprecated 使用 {@link RESTApi#toJson(Object)} 和 {@link RESTApi#fromJson(String, Class)} 代替
     */
    public static final ObjectMapper OBJECT_MAPPER = RESTApi.OBJECT_MAPPER;
}
