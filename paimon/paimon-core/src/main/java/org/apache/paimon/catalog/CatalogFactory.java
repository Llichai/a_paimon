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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.factories.Factory;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.privilege.PrivilegedCatalog;
import org.apache.paimon.utils.Preconditions;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.apache.paimon.options.CatalogOptions.METASTORE;
import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;

/**
 * CatalogFactory 接口 - 用于创建 {@link Catalog} 的工厂
 *
 * <p>CatalogFactory 基于 SPI（Service Provider Interface）机制,
 * 允许不同的 Catalog 实现通过统一的工厂方法创建。每个工厂都有唯一的标识符。
 *
 * <p>支持的 Catalog 类型:
 * <ul>
 *   <li><b>filesystem</b>: {@link FileSystemCatalog} - 基于文件系统（HDFS/S3/本地）
 *   <li><b>hive</b>: HiveCatalog - 基于 Hive Metastore（在 paimon-hive-connector 模块）
 *   <li><b>jdbc</b>: JdbcCatalog - 基于 JDBC（在 paimon-jdbc-connector 模块）
 *   <li><b>rest</b>: RestCatalog - 基于 REST API（在 paimon-rest-catalog 模块）
 * </ul>
 *
 * <p>工厂发现机制:
 * <p>通过 {@link FactoryUtil#discoverFactory} 从类路径中发现 CatalogFactory 实现:
 * <ol>
 *   <li>读取 META-INF/services/org.apache.paimon.catalog.CatalogFactory 文件
 *   <li>加载对应的实现类
 *   <li>调用 {@link Factory#identifier()} 匹配标识符
 *   <li>返回匹配的工厂实例
 * </ol>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 方式 1: 通过 CatalogContext 创建
 * Options options = new Options();
 * options.set("warehouse", "hdfs://warehouse");
 * options.set("metastore", "filesystem");
 * CatalogContext context = CatalogContext.create(options);
 * Catalog catalog = CatalogFactory.createCatalog(context);
 *
 * // 方式 2: 手动创建（自定义 ClassLoader）
 * Catalog catalog = CatalogFactory.createCatalog(context, customClassLoader);
 *
 * // 方式 3: 直接使用工厂（高级用法）
 * CatalogFactory factory = FactoryUtil.discoverFactory(
 *     classLoader,
 *     CatalogFactory.class,
 *     "filesystem"
 * );
 * Catalog catalog = factory.create(context);
 * }</pre>
 *
 * @since 0.4.0
 */
@Public
public interface CatalogFactory extends Factory {

    /**
     * 创建 Catalog 实例（旧版 API）
     *
     * <p>此方法用于向后兼容,新实现应该使用 {@link #create(CatalogContext)}。
     *
     * @param fileIO 文件 IO 实例
     * @param warehouse 仓库路径
     * @param context Catalog 上下文
     * @return Catalog 实例
     * @throws UnsupportedOperationException 如果实现不支持此方法
     */
    default Catalog create(FileIO fileIO, Path warehouse, CatalogContext context) {
        throw new UnsupportedOperationException(
                "Use  create(context) for " + this.getClass().getSimpleName());
    }

    /**
     * 创建 Catalog 实例（推荐 API）
     *
     * <p>从 CatalogContext 中提取必要的配置（如 warehouse、FileIO）并创建 Catalog。
     *
     * @param context Catalog 上下文,包含所有配置信息
     * @return Catalog 实例
     * @throws UnsupportedOperationException 如果实现不支持此方法
     */
    default Catalog create(CatalogContext context) {
        throw new UnsupportedOperationException(
                "Use create(fileIO, warehouse, context) for " + this.getClass().getSimpleName());
    }

    /**
     * 从 CatalogContext 中提取 warehouse 路径
     *
     * @param context Catalog 上下文
     * @return warehouse 路径
     * @throws NullPointerException 如果 warehouse 配置缺失
     */
    static Path warehouse(CatalogContext context) {
        String warehouse =
                Preconditions.checkNotNull(
                        context.options().get(WAREHOUSE),
                        "Paimon '" + WAREHOUSE.key() + "' path must be set");
        return new Path(warehouse);
    }

    /**
     * 创建 Catalog（使用当前线程的上下文 ClassLoader）
     *
     * <p>这是最常用的创建方法,自动处理:
     * <ul>
     *   <li>根据 metastore 配置发现对应的 CatalogFactory
     *   <li>创建底层 Catalog
     *   <li>包装 CachingCatalog（如果启用缓存）
     *   <li>包装 PrivilegedCatalog（如果启用权限控制）
     * </ul>
     *
     * @param options Catalog 配置上下文
     * @return 创建并包装后的 Catalog 实例
     */
    static Catalog createCatalog(CatalogContext options) {
        return createCatalog(options, CatalogFactory.class.getClassLoader());
    }

    /**
     * 创建 Catalog（使用指定的 ClassLoader）
     *
     * <p>此方法允许在特定的类加载环境中创建 Catalog,用于:
     * <ul>
     *   <li>插件化架构: 从插件 ClassLoader 加载 Catalog
     *   <li>类隔离: 避免不同版本依赖冲突
     *   <li>动态加载: 运行时加载新的 Catalog 实现
     * </ul>
     *
     * @param context Catalog 上下文
     * @param classLoader 用于加载 CatalogFactory 的 ClassLoader
     * @return 创建并包装后的 Catalog 实例
     */
    static Catalog createCatalog(CatalogContext context, ClassLoader classLoader) {
        // 创建底层 Catalog
        Catalog catalog = createUnwrappedCatalog(context, classLoader);
        Options options = context.options();
        // 包装缓存层
        catalog = CachingCatalog.tryToCreate(catalog, options);
        // 包装权限控制层
        return PrivilegedCatalog.tryToCreate(catalog, options);
    }

    /**
     * 创建未包装的 Catalog（不包含缓存和权限控制）
     *
     * <p>此方法仅创建底层 Catalog 实现,不添加任何包装层。
     * 主要用于内部使用和测试。
     *
     * @param context Catalog 上下文
     * @param classLoader 类加载器
     * @return 原始的 Catalog 实例
     */
    static Catalog createUnwrappedCatalog(CatalogContext context, ClassLoader classLoader) {
        Options options = context.options();
        String metastore = options.get(METASTORE);
        // 通过 SPI 发现对应的 CatalogFactory
        CatalogFactory catalogFactory =
                FactoryUtil.discoverFactory(classLoader, CatalogFactory.class, metastore);

        // 尝试新版 API
        try {
            return catalogFactory.create(context);
        } catch (UnsupportedOperationException ignore) {
        }

        // 降级到旧版 API
        // 手动验证配置（因为不同 Catalog 类型可能有不同的选项,无法在 optionalOptions() 中全部列出）
        String warehouse = warehouse(context).toString();

        Path warehousePath = new Path(warehouse);
        FileIO fileIO;

        try {
            fileIO = FileIO.get(warehousePath, context);
            fileIO.checkOrMkdirs(warehousePath);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return catalogFactory.create(fileIO, warehousePath, context);
    }
}
