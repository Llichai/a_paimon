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

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;

/**
 * REST Catalog 的内部配置选项。
 *
 * <p>这些配置选项仅供 REST Catalog 内部使用,不应由用户直接配置。
 * 它们通常由 REST 服务器在配置合并阶段返回,或由系统自动设置。
 *
 * <h2>主要用途</h2>
 * <ul>
 *   <li>存储服务器端返回的内部配置
 *   <li>在 REST API 和 Catalog 之间传递系统级配置
 *   <li>支持多租户和命名空间隔离
 * </ul>
 *
 * <h2>使用场景</h2>
 * <p>例如,REST 服务器可能返回一个 prefix 配置,用于在构建资源路径时添加前缀:
 * <pre>{@code
 * // 服务器返回的配置
 * {
 *   "prefix": "/api/v1/catalog/my-tenant"
 * }
 *
 * // 实际请求路径
 * /api/v1/catalog/my-tenant/databases/mydb
 * }</pre>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li>这些选项由系统自动管理,用户无需手动配置
 *   <li>配置值通常来自 REST 服务器的 /v1/config 接口
 *   <li>不要在客户端代码中直接设置这些选项
 * </ul>
 *
 * @see RESTCatalogOptions
 * @see RESTApi#RESTApi(org.apache.paimon.options.Options, boolean)
 */
public class RESTCatalogInternalOptions {

    /**
     * REST Catalog URI 的前缀。
     *
     * <p>此配置项用于在资源路径前添加统一的前缀,支持以下场景:
     * <ul>
     *   <li><b>多租户</b>: 不同租户使用不同的 URI 前缀
     *   <li><b>版本控制</b>: 通过前缀区分不同的 API 版本
     *   <li><b>命名空间</b>: 为不同的 catalog 实例分配独立的命名空间
     * </ul>
     *
     * <h2>工作原理</h2>
     * <p>当设置了 prefix 后,所有资源路径都会自动添加此前缀:
     * <pre>{@code
     * // 未设置 prefix
     * GET /v1/databases/mydb
     *
     * // 设置 prefix = "/tenant123"
     * GET /tenant123/v1/databases/mydb
     *
     * // 设置 prefix = "/api/v2/catalog"
     * GET /api/v2/catalog/v1/databases/mydb
     * }</pre>
     *
     * <h2>配置来源</h2>
     * <p>通常由 REST 服务器在响应 GET /v1/config 请求时返回:
     * <pre>{@code
     * {
     *   "defaults": {
     *     "prefix": "/api/v1/tenant/my-tenant"
     *   }
     * }
     * }</pre>
     *
     * @see org.apache.paimon.rest.ResourcePaths
     */
}
