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

package org.apache.paimon.jdbc;

import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

/**
 * JDBC Catalog 锁上下文.
 *
 * <p>封装了创建 JDBC 锁所需的上下文信息,包括连接池、Catalog 键和配置选项。
 * 实现了 {@link CatalogLockContext} 接口,用于锁工厂创建锁实例。
 *
 * <p>延迟初始化:
 * <ul>
 *   <li>connections 字段标记为 transient,不参与序列化</li>
 *   <li>首次访问时才创建连接池,避免过早初始化</li>
 *   <li>适合在分布式环境中序列化传输</li>
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>在锁工厂中创建锁实例时传递上下文</li>
 *   <li>在分布式环境中序列化传输锁上下文</li>
 * </ul>
 */
public class JdbcCatalogLockContext implements CatalogLockContext {

    /** JDBC 连接池,标记为 transient 避免序列化 */
    private transient JdbcClientPool connections;

    /** Catalog 键,用于多租户场景和生成锁 ID */
    private final String catalogKey;

    /** Catalog 配置选项 */
    private final Options options;

    /**
     * 构造 JDBC Catalog 锁上下文.
     *
     * @param catalogKey Catalog 键
     * @param options Catalog 配置选项
     */
    public JdbcCatalogLockContext(String catalogKey, Options options) {
        this.catalogKey = catalogKey;
        this.options = options;
    }

    /**
     * 获取配置选项.
     *
     * @return Catalog 配置选项
     */
    @Override
    public Options options() {
        return options;
    }

    /**
     * 获取 JDBC 连接池.
     *
     * <p>延迟初始化:首次访问时创建连接池。
     *
     * @return JDBC 连接池
     */
    public JdbcClientPool connections() {
        if (connections == null) {
            connections =
                    new JdbcClientPool(
                            options.get(CatalogOptions.CLIENT_POOL_SIZE),
                            options.get(CatalogOptions.URI.key()),
                            options.toMap());
        }
        return connections;
    }

    /**
     * 获取 Catalog 键.
     *
     * @return Catalog 键
     */
    public String catalogKey() {
        return catalogKey;
    }
}
