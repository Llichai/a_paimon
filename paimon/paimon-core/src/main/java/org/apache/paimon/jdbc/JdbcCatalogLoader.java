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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.fs.FileIO;

/**
 * JDBC Catalog 加载器.
 *
 * <p>实现了 {@link CatalogLoader} 接口,用于延迟加载和序列化传输 JDBC Catalog。
 * 在分布式环境中,可以将加载器序列化后传输到远程节点,然后在远程节点上重新创建 Catalog。
 *
 * <p>序列化支持:
 * <ul>
 *   <li>serialVersionUID = 2L: 版本控制,保证序列化兼容性</li>
 *   <li>所有字段都是可序列化的</li>
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>Flink/Spark 等分布式计算引擎中传递 Catalog</li>
 *   <li>延迟创建 Catalog,避免过早初始化连接池</li>
 * </ul>
 */
public class JdbcCatalogLoader implements CatalogLoader {

    /** 序列化版本号 */
    private static final long serialVersionUID = 2L;

    /** 文件 IO 实现 */
    private final FileIO fileIO;

    /** Catalog 键,用于多租户场景 */
    private final String catalogKey;

    /** Catalog 上下文,包含配置信息 */
    private final CatalogContext context;

    /** 仓库路径,表数据文件的根目录 */
    private final String warehouse;

    /**
     * 构造 JDBC Catalog 加载器.
     *
     * @param fileIO 文件 IO 实现
     * @param catalogKey Catalog 键
     * @param context Catalog 上下文
     * @param warehouse 仓库路径
     */
    public JdbcCatalogLoader(
            FileIO fileIO, String catalogKey, CatalogContext context, String warehouse) {
        this.fileIO = fileIO;
        this.catalogKey = catalogKey;
        this.context = context;
        this.warehouse = warehouse;
    }

    /**
     * 加载 JDBC Catalog.
     *
     * <p>每次调用都会创建一个新的 Catalog 实例。
     *
     * @return JDBC Catalog 实例
     */
    @Override
    public Catalog load() {
        return new JdbcCatalog(fileIO, catalogKey, context, warehouse);
    }
}
