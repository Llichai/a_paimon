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

package org.apache.paimon.lookup;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.sort.SortLookupStoreFactory;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.BloomFilter;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.function.Function;

/**
 * 查找存储工厂。
 *
 * <p>该接口定义了用于查找操作的键值存储工厂。键值存储应该是一个单一的二进制文件，
 * 写入一次后即可使用。这种设计适用于维度表查找等场景。
 *
 * <p>主要功能：
 * <ul>
 *   <li>创建写入器：一次性写入准备二进制文件
 *   <li>创建读取器：通过键字节查找值
 *   <li>支持布隆过滤器加速查找
 *   <li>支持数据压缩
 * </ul>
 *
 * <p>工作流程：
 * <pre>
 * 1. 写入阶段：
 *    - 创建 LookupStoreWriter
 *    - 按键有序写入键值对
 *    - 构建布隆过滤器（可选）
 *    - 关闭写入器，完成文件写入
 *
 * 2. 读取阶段：
 *    - 创建 LookupStoreReader
 *    - 通过键查找值
 *    - 关闭读取器
 * </pre>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建工厂
 * CoreOptions options = ...;
 * CacheManager cacheManager = ...;
 * Comparator<MemorySlice> keyComparator = ...;
 * LookupStoreFactory factory = LookupStoreFactory.create(
 *     options, cacheManager, keyComparator);
 *
 * // 写入阶段
 * File file = new File("/path/to/lookup.store");
 * BloomFilter.Builder bloomFilter = BloomFilter.builder(1000, 0.01);
 * try (LookupStoreWriter writer = factory.createWriter(file, bloomFilter)) {
 *     writer.put("key1".getBytes(), "value1".getBytes());
 *     writer.put("key2".getBytes(), "value2".getBytes());
 * }
 *
 * // 读取阶段
 * try (LookupStoreReader reader = factory.createReader(file)) {
 *     byte[] value = reader.lookup("key1".getBytes());
 *     System.out.println(new String(value)); // "value1"
 * }
 * }</pre>
 *
 * <p>实现：
 * <ul>
 *   <li>{@link SortLookupStoreFactory} - 基于排序的查找存储实现
 * </ul>
 *
 * <p>特点：
 * <ul>
 *   <li>一次写入，多次读取（Write-Once-Read-Many）
 *   <li>文件级别的存储单元
 *   <li>支持布隆过滤器快速判断键是否存在
 *   <li>支持多种压缩算法
 * </ul>
 */
public interface LookupStoreFactory {

    /**
     * 创建查找存储写入器。
     *
     * @param file 要写入的文件
     * @param bloomFilter 布隆过滤器构建器（可选）
     * @return 查找存储写入器
     * @throws IOException 如果创建写入器时发生错误
     */
    LookupStoreWriter createWriter(File file, @Nullable BloomFilter.Builder bloomFilter)
            throws IOException;

    /**
     * 创建查找存储读取器。
     *
     * @param file 要读取的文件
     * @return 查找存储读取器
     * @throws IOException 如果创建读取器时发生错误
     */
    LookupStoreReader createReader(File file) throws IOException;

    /**
     * 创建布隆过滤器生成器。
     *
     * @param options 配置选项
     * @return 布隆过滤器生成器函数
     */
    static Function<Long, BloomFilter.Builder> bfGenerator(Options options) {
        Function<Long, BloomFilter.Builder> bfGenerator = rowCount -> null;
        if (options.get(CoreOptions.LOOKUP_CACHE_BLOOM_FILTER_ENABLED)) {
            double bfFpp = options.get(CoreOptions.LOOKUP_CACHE_BLOOM_FILTER_FPP);
            bfGenerator =
                    rowCount -> {
                        if (rowCount > 0) {
                            return BloomFilter.builder(rowCount, bfFpp);
                        }
                        return null;
                    };
        }
        return bfGenerator;
    }

    /**
     * 创建查找存储工厂。
     *
     * @param options 核心配置选项
     * @param cacheManager 缓存管理器
     * @param keyComparator 键比较器
     * @return 查找存储工厂实例
     */
    static LookupStoreFactory create(
            CoreOptions options, CacheManager cacheManager, Comparator<MemorySlice> keyComparator) {
        CompressOptions compression = options.lookupCompressOptions();
        return new SortLookupStoreFactory(
                keyComparator, cacheManager, options.cachePageSize(), compression);
    }

    /**
     * 写入器和读取器之间的上下文。
     *
     * <p>用于在写入器和读取器之间传递元数据信息。
     */
    interface Context {}
}
