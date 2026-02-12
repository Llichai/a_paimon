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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.MemorySize;

/**
 * BTree 索引的配置选项。
 *
 * <p>该类定义了用于配置 BTree 全局索引的各种选项,包括压缩、块大小、缓存、并行度等。
 * BTree 索引是一种基于 B+ 树结构的有序索引,能够高效支持范围查询和点查。
 *
 * <h2>核心配置项</h2>
 * <ul>
 *   <li>压缩配置 - 控制索引文件的压缩算法和压缩级别
 *   <li>存储配置 - 设置块大小和每个索引文件的记录数
 *   <li>缓存配置 - 配置索引块的缓存大小和优先级池比例
 *   <li>并行度配置 - 控制索引构建的最大并行度
 * </ul>
 *
 * <h2>性能优化</h2>
 * <ul>
 *   <li>块大小 - 较大的块减少 I/O 次数,但增加内存占用
 *   <li>压缩 - 减少存储空间和 I/O 量,但增加 CPU 开销
 *   <li>缓存 - 将热点索引块缓存到内存中,加快查询速度
 *   <li>并行度 - 提高索引构建速度,适合大规模数据集
 * </ul>
 *
 * @see BTreeGlobalIndexer
 * @see BTreeIndexWriter
 * @see BTreeIndexReader
 */
public class BTreeIndexOptions {

    /** BTree 索引的压缩算法,默认为 none(不压缩),可选 lz4、zstd、snappy 等 */
    public static final ConfigOption<String> BTREE_INDEX_COMPRESSION =
            ConfigOptions.key("btree-index.compression")
                    .stringType()
                    .defaultValue("none")
                    .withDescription("The compression algorithm to use for BTreeIndex");

    /** 压缩算法的压缩级别,默认为 1(最快压缩),数值越大压缩率越高但速度越慢 */
    public static final ConfigOption<Integer> BTREE_INDEX_COMPRESSION_LEVEL =
            ConfigOptions.key("btree-index.compression-level")
                    .intType()
                    .defaultValue(1)
                    .withDescription("The compression level of the compression algorithm");

    /** BTree 索引的块大小,默认为 64 KiB,影响 I/O 粒度和内存占用 */
    public static final ConfigOption<MemorySize> BTREE_INDEX_BLOCK_SIZE =
            ConfigOptions.key("btree-index.block-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofKibiBytes(64))
                    .withDescription("The block size to use for BTreeIndex");

    /** BTree 索引的缓存大小,默认为 128 MiB,用于缓存热点索引块以加速查询 */
    public static final ConfigOption<MemorySize> BTREE_INDEX_CACHE_SIZE =
            ConfigOptions.key("btree-index.cache-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription("The cache size to use for BTreeIndex");

    /**
     * BTree 索引的高优先级缓存池比例,默认为 0.1(10%)。
     *
     * <p>高优先级池中的索引块不会被轻易淘汰,适合存放频繁访问的热点数据。
     */
    public static final ConfigOption<Double> BTREE_INDEX_HIGH_PRIORITY_POOL_RATIO =
            ConfigOptions.key("btree-index.high-priority-pool-ratio")
                    .doubleType()
                    .defaultValue(0.1)
                    .withDescription("The high priority pool ratio to use for BTreeIndex");

    /**
     * 每个 BTree 索引文件预期包含的记录数,默认为 1,000,000 条。
     *
     * <p>该参数影响索引文件的划分粒度。较大的值会产生更少但更大的索引文件,
     * 较小的值会产生更多但更小的索引文件。需要根据数据分布和查询模式调优。
     */
    public static final ConfigOption<Long> BTREE_INDEX_RECORDS_PER_RANGE =
            ConfigOptions.key("btree-index.records-per-range")
                    .longType()
                    .defaultValue(1000_000L)
                    .withDescription("The expected number of records per BTree Index File.");

    /**
     * 在 Flink/Spark 中构建 BTree 索引时的最大并行度,默认为 4096。
     *
     * <p>该参数限制索引构建任务的并行度上限,避免创建过多的小文件。
     * 实际并行度还受数据分区数和执行引擎配置的影响。
     */
    public static final ConfigOption<Integer> BTREE_INDEX_BUILD_MAX_PARALLELISM =
            ConfigOptions.key("btree-index.build.max-parallelism")
                    .intType()
                    .defaultValue(4096)
                    .withDescription("The max parallelism of Flink/Spark for building BTreeIndex.");
}
