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
import org.apache.paimon.io.DataFilePathFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 数据文件路径工厂缓存
 *
 * <p>DataFilePathFactories 是一个缓存类，用于缓存每个分区-Bucket 组合对应的 {@link DataFilePathFactory}。
 *
 * <p>核心功能：
 * <ul>
 *   <li>缓存管理：使用 ConcurrentHashMap 缓存已创建的 DataFilePathFactory 实例
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
 * <p>使用场景：
 * <ul>
 *   <li>读取数据文件：根据文件元数据的分区和 Bucket 获取路径工厂
 *   <li>写入数据文件：根据目标分区和 Bucket 获取路径工厂
 *   <li>文件合并：在合并任务中复用相同分区-Bucket 的路径工厂
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * FileStorePathFactory pathFactory = ...;
 * DataFilePathFactories factories = new DataFilePathFactories(pathFactory);
 *
 * // 获取特定分区和 Bucket 的数据文件路径工厂
 * BinaryRow partition = ...;
 * int bucket = 0;
 * DataFilePathFactory factory = factories.get(partition, bucket);
 *
 * // 生成数据文件路径
 * Path dataFilePath = factory.toPath("data-uuid-123.orc");
 * // 结果: table_root/dt=2024-01-01/bucket-0/data-uuid-123.orc
 * }</pre>
 *
 * @see DataFilePathFactory
 * @see FileStorePathFactory
 */
public class DataFilePathFactories {

    /** 分区-Bucket 到数据文件路径工厂的缓存 */
    private final Map<Pair<BinaryRow, Integer>, DataFilePathFactory> cache =
            new ConcurrentHashMap<>();
    /** 文件存储路径工厂，用于创建新的数据文件路径工厂 */
    private final FileStorePathFactory pathFactory;

    /**
     * 构造数据文件路径工厂缓存
     *
     * @param pathFactory 文件存储路径工厂
     */
    public DataFilePathFactories(FileStorePathFactory pathFactory) {
        this.pathFactory = pathFactory;
    }

    /**
     * 获取指定分区和 Bucket 的数据文件路径工厂
     *
     * <p>如果缓存中不存在，则创建新的工厂实例并缓存。
     *
     * @param partition 分区行数据
     * @param bucket Bucket 编号
     * @return 数据文件路径工厂
     */
    public DataFilePathFactory get(BinaryRow partition, int bucket) {
        return cache.computeIfAbsent(
                Pair.of(partition, bucket),
                k -> pathFactory.createDataFilePathFactory(k.getKey(), k.getValue()));
    }
}
