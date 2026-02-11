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
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

/**
 * JDBC Catalog 工厂类.
 *
 * <p>用于创建 {@link JdbcCatalog} 实例。实现了 {@link CatalogFactory} 接口,
 * 通过 Java SPI 机制被自动发现和加载。
 *
 * <p>使用示例:
 * <pre>{@code
 * Map<String, String> options = new HashMap<>();
 * options.put("type", "jdbc");
 * options.put("uri", "jdbc:mysql://localhost:3306/paimon");
 * options.put("catalog-key", "my_catalog");
 *
 * CatalogContext context = CatalogContext.create(Options.fromMap(options));
 * Catalog catalog = factory.create(fileIO, warehouse, context);
 * }</pre>
 */
public class JdbcCatalogFactory implements CatalogFactory {

    /** Catalog 类型标识符,用于 SPI 发现 */
    public static final String IDENTIFIER = "jdbc";

    /**
     * 获取 Catalog 类型标识符.
     *
     * @return "jdbc"
     */
    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    /**
     * 创建 JDBC Catalog 实例.
     *
     * @param fileIO 文件 IO 实现,用于访问表数据文件
     * @param warehouse 仓库路径,表数据文件的根目录
     * @param context Catalog 上下文,包含配置信息
     * @return JDBC Catalog 实例
     */
    @Override
    public Catalog create(FileIO fileIO, Path warehouse, CatalogContext context) {
        Options options = context.options();
        // 获取 Catalog 键,用于多租户场景
        String catalogKey = options.get(JdbcCatalogOptions.CATALOG_KEY);
        return new JdbcCatalog(fileIO, catalogKey, context, warehouse.toString());
    }
}
