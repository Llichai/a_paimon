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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogLoader;

/**
 * 用于创建 {@link RESTCatalog} 的加载器。
 *
 * <p>实现 {@link CatalogLoader} 接口，支持延迟加载和序列化传输 REST Catalog。
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li>分布式环境：将 Catalog 配置序列化后传输到不同节点
 *   <li>延迟初始化：仅在需要时才创建 Catalog 实例
 *   <li>资源管理：支持跨任务共享 Catalog 配置
 * </ul>
 *
 * <p><b>序列化支持：</b>
 * 实现了 Serializable 接口，可以在 Flink/Spark 等分布式框架中传输。
 *
 * @see RESTCatalog
 * @see RESTCatalogFactory
 */
public class RESTCatalogLoader implements CatalogLoader {

    private static final long serialVersionUID = 1L;

    /** Catalog 上下文，包含配置信息。 */
    private final CatalogContext context;

    /**
     * 构造函数。
     *
     * @param context Catalog 上下文
     */
    public RESTCatalogLoader(CatalogContext context) {
        this.context = context;
    }

    /**
     * 获取 Catalog 上下文。
     *
     * @return Catalog 上下文
     */
    public CatalogContext context() {
        return context;
    }

    /**
     * 加载并返回 REST Catalog 实例。
     *
     * <p>注意：此方法创建的实例不需要配置校验（configRequired=false），
     * 适用于已经验证过配置的场景。
     *
     * @return REST Catalog 实例
     */
    @Override
    public RESTCatalog load() {
        return new RESTCatalog(context, false);
    }
}
