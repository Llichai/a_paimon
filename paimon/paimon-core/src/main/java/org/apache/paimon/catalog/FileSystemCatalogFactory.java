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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.CatalogTableType;

import static org.apache.paimon.options.CatalogOptions.TABLE_TYPE;

/**
 * FileSystemCatalog 的工厂类
 *
 * <p>通过 SPI 机制创建 {@link FileSystemCatalog} 实例。
 *
 * <p>工厂标识符: {@code "filesystem"}
 *
 * <p>配置示例:
 * <pre>
 * # 使用文件系统 Catalog
 * catalog.type = filesystem
 * catalog.warehouse = hdfs://namenode:8020/warehouse
 * </pre>
 *
 * <p>限制:
 * <ul>
 *   <li>只支持托管表（managed table）
 *   <li>不支持外部表（external table）
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 通过工厂创建 Catalog
 * FileIO fileIO = ...;
 * Path warehouse = new Path("hdfs://namenode:8020/warehouse");
 * CatalogContext context = ...;
 *
 * CatalogFactory factory = new FileSystemCatalogFactory();
 * Catalog catalog = factory.create(fileIO, warehouse, context);
 * }</pre>
 *
 * @see FileSystemCatalog
 * @see CatalogFactory
 */
public class FileSystemCatalogFactory implements CatalogFactory {

    /** 工厂标识符 */
    public static final String IDENTIFIER = "filesystem";

    /**
     * 返回工厂标识符
     *
     * @return "filesystem"
     */
    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    /**
     * 创建 FileSystemCatalog
     *
     * @param fileIO 文件 I/O 接口
     * @param warehouse 仓库根路径
     * @param context Catalog 上下文
     * @return FileSystemCatalog 实例
     * @throws IllegalArgumentException 如果配置了外部表类型
     */
    @Override
    public Catalog create(FileIO fileIO, Path warehouse, CatalogContext context) {
        if (!CatalogTableType.MANAGED.equals(context.options().get(TABLE_TYPE))) {
            throw new IllegalArgumentException(
                    "Only managed table is supported in File system catalog.");
        }

        return new FileSystemCatalog(fileIO, warehouse, context);
    }
}
