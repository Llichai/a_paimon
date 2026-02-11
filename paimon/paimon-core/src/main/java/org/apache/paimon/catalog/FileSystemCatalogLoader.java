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

/**
 * FileSystemCatalog 的加载器
 *
 * <p>用于创建 {@link FileSystemCatalog} 实例的加载器。实现了 {@link CatalogLoader} 接口,
 * 支持序列化,可在 Flink/Spark 等分布式任务中使用。
 *
 * <p>使用场景:
 * <ul>
 *   <li>Flink/Spark 任务序列化 Catalog 配置
 *   <li>延迟初始化 Catalog（在任务节点上创建,而不是在客户端）
 *   <li>避免在客户端创建 FileIO 连接
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 1. 在客户端创建加载器
 * FileIO fileIO = ...;
 * Path warehouse = new Path("hdfs://namenode:8020/warehouse");
 * CatalogContext context = ...;
 * CatalogLoader loader = new FileSystemCatalogLoader(fileIO, warehouse, context);
 *
 * // 2. 序列化并发送到任务节点
 * byte[] bytes = SerializationUtils.serialize(loader);
 *
 * // 3. 在任务节点上反序列化并加载
 * CatalogLoader deserializedLoader = SerializationUtils.deserialize(bytes);
 * Catalog catalog = deserializedLoader.load();
 * }</pre>
 *
 * @see FileSystemCatalog
 * @see CatalogLoader
 */
public class FileSystemCatalogLoader implements CatalogLoader {

    private static final long serialVersionUID = 2L;

    /** 文件 I/O 接口 */
    private final FileIO fileIO;

    /** 仓库根路径 */
    private final Path warehouse;

    /** Catalog 上下文 */
    private final CatalogContext context;

    /**
     * 构造函数
     *
     * @param fileIO 文件 I/O 接口
     * @param warehouse 仓库根路径
     * @param context Catalog 上下文
     */
    public FileSystemCatalogLoader(FileIO fileIO, Path warehouse, CatalogContext context) {
        this.fileIO = fileIO;
        this.warehouse = warehouse;
        this.context = context;
    }

    /**
     * 加载 Catalog
     *
     * @return FileSystemCatalog 实例
     */
    @Override
    public Catalog load() {
        return new FileSystemCatalog(fileIO, warehouse, context);
    }
}
