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

import java.io.Serializable;

/**
 * CatalogLoader 接口 - 用于创建 {@link Catalog} 的加载器
 *
 * <p>CatalogLoader 是一个可序列化的函数式接口,用于延迟创建 Catalog 实例。
 * 这在分布式环境中特别有用,可以将 Catalog 的创建逻辑序列化并发送到远程节点。
 *
 * <p>使用场景:
 * <ul>
 *   <li><b>分布式任务</b>: 在 Flink/Spark 的 Task 节点上创建 Catalog
 *   <li><b>延迟初始化</b>: 推迟 Catalog 的创建到真正需要时
 *   <li><b>序列化传输</b>: 将 Catalog 创建逻辑通过网络传输
 * </ul>
 *
 * <p>实现类:
 * <ul>
 *   <li>{@link FileSystemCatalogLoader}: 文件系统 Catalog 加载器
 *   <li>{@link CachingCatalogLoader}: 缓存 Catalog 加载器
 *   <li>其他 Catalog 的对应加载器（HiveCatalogLoader、JdbcCatalogLoader 等）
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建 CatalogLoader（在 Driver 端）
 * CatalogLoader loader = () -> {
 *     FileIO fileIO = FileIO.get(warehouse);
 *     return new FileSystemCatalog(fileIO, warehouse, options);
 * };
 *
 * // 序列化并发送到 Executor 端
 * env.fromSource(source)
 *     .map(new MyMapFunction(loader))  // loader 被序列化
 *     .addSink(...);
 *
 * // 在 Executor 端加载 Catalog
 * public class MyMapFunction implements MapFunction<T, R> {
 *     private final CatalogLoader loader;
 *     private transient Catalog catalog;
 *
 *     public void open() {
 *         catalog = loader.load();  // 延迟创建
 *     }
 * }
 * }</pre>
 *
 * @since 1.1.0
 */
@Public
@FunctionalInterface
public interface CatalogLoader extends Serializable {

    /**
     * 加载并创建 Catalog 实例
     *
     * <p>此方法应该是幂等的,每次调用可能返回新的 Catalog 实例。
     *
     * @return 新创建的 Catalog 实例
     */
    Catalog load();
}
