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

package org.apache.paimon.utils;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexPathFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 索引文件路径工厂缓存
 *
 * <p>IndexFilePathFactories 是一个缓存类，用于缓存每个分区-Bucket 组合对应的 {@link IndexPathFactory}。
 *
 * <p>核心功能：
 * <ul>
 *   <li>缓存管理：使用 ConcurrentHashMap 缓存已创建的 IndexPathFactory 实例
 *   <li>延迟创建：只有在首次访问某个分区-Bucket 时才创建对应的工厂实例
 *   <li>线程安全：使用 computeIfAbsent 保证并发访问的正确性
 * </ul>
 *
 * <p>缓存键：
 * <ul>
 *   <li>使用 Pair<BinaryRow, Integer> 作为缓存键
 *   <li>BinaryRow：分区行数据
 *   <li>Integer：Bucket 编号
 * </ul>
 *
 * <p>索引文件存储位置：
 * <ul>
 *   <li>数据文件目录模式：索引文件与数据文件存储在同一目录（table_root/partition/bucket-N/）
 *   <li>全局索引目录模式：所有索引文件存储在全局索引目录（table_root/index/）
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>读取索引文件：根据索引元数据的分区和 Bucket 获取路径工厂
 *   <li>写入索引文件：在数据写入时生成索引文件路径
 *   <li>索引查找：在查询时根据分区和 Bucket 定位索引文件
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * FileStorePathFactory pathFactory = ...;
 * IndexFilePathFactories factories = new IndexFilePathFactories(pathFactory);
 *
 * // 获取特定分区和 Bucket 的索引文件路径工厂
 * BinaryRow partition = ...;
 * int bucket = 0;
 * IndexPathFactory factory = factories.get(partition, bucket);
 *
 * // 生成索引文件路径
 * Path indexFilePath = factory.newPath();
 * // 数据文件目录模式: table_root/dt=2024-01-01/bucket-0/index-uuid-0
 * // 全局索引目录模式: table_root/index/index-uuid-0
 * }</pre>
 *
 * @see IndexPathFactory
 * @see FileStorePathFactory
 */
public class IndexFilePathFactories {

    /** 分区-Bucket 到索引文件路径工厂的缓存 */
    private final Map<Pair<BinaryRow, Integer>, IndexPathFactory> cache = new ConcurrentHashMap<>();
    /** 文件存储路径工厂，用于创建新的索引文件路径工厂 */
    private final FileStorePathFactory pathFactory;

    /**
     * 构造索引文件路径工厂缓存
     *
     * @param pathFactory 文件存储路径工厂
     */
    public IndexFilePathFactories(FileStorePathFactory pathFactory) {
        this.pathFactory = pathFactory;
    }

    /**
     * 获取指定分区和 Bucket 的索引文件路径工厂
     *
     * <p>如果缓存中不存在，则创建新的工厂实例并缓存。
     *
     * @param partition 分区行数据
     * @param bucket Bucket 编号
     * @return 索引文件路径工厂
     */
    public IndexPathFactory get(BinaryRow partition, int bucket) {
        return cache.computeIfAbsent(
                Pair.of(partition, bucket),
                k -> pathFactory.indexFileFactory(k.getKey(), k.getValue()));
    }
}
