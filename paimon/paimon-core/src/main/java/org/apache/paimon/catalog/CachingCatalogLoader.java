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

package org.apache.paimon.catalog;

import org.apache.paimon.options.Options;

/**
 * CachingCatalog 的加载器
 *
 * <p>用于创建 {@link CachingCatalog} 实例的加载器。会将底层的 Catalog 包装成带缓存的版本。
 *
 * <p>使用场景:
 * <ul>
 *   <li>Flink/Spark 分布式任务中需要序列化 Catalog
 *   <li>延迟初始化 Catalog（避免在客户端创建连接）
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建基础 Catalog 加载器
 * CatalogLoader baseLoader = new FileSystemCatalogLoader(fileIO, warehouse, context);
 *
 * // 包装成缓存 Catalog 加载器
 * Options options = new Options();
 * options.set(CACHE_ENABLED, true);
 * CatalogLoader cachingLoader = new CachingCatalogLoader(baseLoader, options);
 *
 * // 在任务节点上加载
 * Catalog catalog = cachingLoader.load();  // 返回 CachingCatalog
 * }</pre>
 *
 * @see CachingCatalog
 * @see CatalogLoader
 */
public class CachingCatalogLoader implements CatalogLoader {

    private static final long serialVersionUID = 1L;

    /** 底层 Catalog 加载器 */
    private final CatalogLoader catalogLoader;

    /** 缓存配置选项 */
    private final Options options;

    /**
     * 构造函数
     *
     * @param catalogLoader 底层 Catalog 加载器
     * @param options 缓存配置选项
     */
    public CachingCatalogLoader(CatalogLoader catalogLoader, Options options) {
        this.catalogLoader = catalogLoader;
        this.options = options;
    }

    /**
     * 加载 Catalog
     *
     * <p>先加载底层 Catalog,然后根据配置决定是否包装成 CachingCatalog。
     *
     * @return Catalog 实例（可能是 CachingCatalog）
     */
    @Override
    public Catalog load() {
        return CachingCatalog.tryToCreate(catalogLoader.load(), options);
    }
}
