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

package org.apache.paimon.privilege;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;

/**
 * 用于创建 {@link PrivilegedCatalog} 的加载器。
 *
 * <p>该加载器实现了 {@link CatalogLoader} 接口,负责在加载Catalog时自动应用权限检查包装。
 *
 * <h2>工作原理</h2>
 * <ol>
 *   <li>使用 {@link CatalogLoader} 加载原始Catalog</li>
 *   <li>使用 {@link PrivilegeManagerLoader} 创建权限管理器</li>
 *   <li>将原始Catalog包装为 {@link PrivilegedCatalog}</li>
 * </ol>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建加载器
 * CatalogLoader catalogLoader = ...; // 原始Catalog加载器
 * PrivilegeManagerLoader privilegeLoader = ...; // 权限管理器加载器
 *
 * PrivilegedCatalogLoader loader = new PrivilegedCatalogLoader(
 *     catalogLoader,
 *     privilegeLoader
 * );
 *
 * // 加载带权限检查的Catalog
 * Catalog catalog = loader.load();
 * }</pre>
 *
 * <h2>序列化支持</h2>
 * <p>该类实现了 {@link java.io.Serializable},可以在分布式环境中序列化和传输。
 *
 * @see PrivilegedCatalog
 * @see PrivilegeManagerLoader
 * @see CatalogLoader
 */
public class PrivilegedCatalogLoader implements CatalogLoader {

    private static final long serialVersionUID = 1L;

    private final CatalogLoader catalogLoader;
    private final PrivilegeManagerLoader privilegeManagerLoader;

    /**
     * 构造权限Catalog加载器。
     *
     * @param catalogLoader 原始Catalog加载器
     * @param privilegeManagerLoader 权限管理器加载器
     */
    public PrivilegedCatalogLoader(
            CatalogLoader catalogLoader, PrivilegeManagerLoader privilegeManagerLoader) {
        this.catalogLoader = catalogLoader;
        this.privilegeManagerLoader = privilegeManagerLoader;
    }

    @Override
    public Catalog load() {
        Catalog catalog = catalogLoader.load();
        return new PrivilegedCatalog(catalog, privilegeManagerLoader);
    }
}
