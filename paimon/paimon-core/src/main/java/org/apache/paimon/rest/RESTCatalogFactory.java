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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;

/**
 * 创建 {@link RESTCatalog} 的工厂类。
 *
 * <p>实现 {@link CatalogFactory} 接口，用于通过 REST API 创建和管理 Catalog。
 *
 * <p><b>标识符：</b>
 * 使用 "rest" 作为唯一标识符，在配置中通过 {@code catalog.type=rest} 指定。
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li>远程 Catalog 服务：Catalog 元数据由独立的 REST 服务管理
 *   <li>多租户环境：通过 REST API 实现统一的元数据管理
 *   <li>云原生部署：与云服务集成，实现元数据的集中管理
 * </ul>
 *
 * @see RESTCatalog
 * @see RESTCatalogLoader
 */
public class RESTCatalogFactory implements CatalogFactory {
    /** 标识符常量，值为 "rest"。 */
    public static final String IDENTIFIER = "rest";

    /**
     * 返回工厂的标识符。
     *
     * @return "rest"
     */
    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    /**
     * 创建 REST Catalog 实例。
     *
     * @param context Catalog 上下文，包含配置信息
     * @return REST Catalog 实例
     */
    @Override
    public Catalog create(CatalogContext context) {
        return new RESTCatalog(context);
    }
}
