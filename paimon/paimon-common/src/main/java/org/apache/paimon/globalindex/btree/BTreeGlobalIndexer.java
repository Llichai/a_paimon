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

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.LazyField;

import java.io.IOException;
import java.util.List;

/**
 * BTree 全局索引器。
 *
 * <p>与直接在内存中构建 B 树不同,该实现通过 SST 文件的多层元数据形成逻辑 B 树,
 * 显著降低了索引读取时的内存压力。
 *
 * <p>索引结构如下:
 * <pre>
 *                                             BTree-Index
 *                                             /         \
 *                                            /    ...    \
 *                                           /             \
 *     +--------------------------------------+           +------------+
 *     |               SST File               |           |            |
 *     +--------------------------------------+           |            |
 *     |              Root Index              |           |            |
 *     |             /   ...    \             |           |            |
 *     |     Leaf Index  ...  Leaf Index      |    ...    |  SST File  |
 *     |     /  ...   \       /  ...   \      |           |            |
 *     | DataBlock       ...        DataBlock |           |            |
 *     +--------------------------------------+           +------------+
 * </pre>
 *
 * <p>BTree 索引特点:
 * <ul>
 *   <li>支持范围查询 - WHERE column BETWEEN a AND b
 *   <li>支持排序扫描 - ORDER BY column
 *   <li>支持前缀查询 - WHERE column LIKE 'prefix%'
 *   <li>低内存占用 - 使用分层索引结构,按需加载
 * </ul>
 *
 * <p>工作原理:
 * <ol>
 *   <li>写入阶段: 将数据按键排序后写入 SST 文件,构建多层索引
 *   <li>查询阶段: 通过索引层次快速定位数据块,执行二分查找
 *   <li>缓存优化: 使用 {@link CacheManager} 缓存热点数据块
 * </ol>
 *
 * <p>性能优化:
 * <ul>
 *   <li>块缓存 - 缓存频繁访问的数据块和索引块
 *   <li>延迟加载 - 仅在需要时才读取索引和数据
 *   <li>压缩存储 - 支持多种压缩算法,减少 I/O
 * </ul>
 *
 * <p>配置选项:
 * <ul>
 *   <li>{@link BTreeIndexOptions#BTREE_INDEX_BLOCK_SIZE} - 数据块大小
 *   <li>{@link BTreeIndexOptions#BTREE_INDEX_COMPRESSION} - 压缩算法
 *   <li>{@link BTreeIndexOptions#BTREE_INDEX_CACHE_SIZE} - 缓存大小
 * </ul>
 */
public class BTreeGlobalIndexer implements GlobalIndexer {

    /** 键序列化器,用于序列化和反序列化索引键 */
    private final KeySerializer keySerializer;

    /** 索引配置选项 */
    private final Options options;

    /** 缓存管理器的延迟加载字段,用于缓存索引数据块 */
    private final LazyField<CacheManager> cacheManager;

    /**
     * 构造 BTree 全局索引器。
     *
     * @param dataField 要索引的数据字段
     * @param options 索引配置选项
     */
    public BTreeGlobalIndexer(DataField dataField, Options options) {
        this.keySerializer = KeySerializer.create(dataField.type());
        this.options = options;
        // TODO: cacheManager 可以为 null 以禁用数据缓存
        this.cacheManager =
                new LazyField<>(
                        () ->
                                new CacheManager(
                                        options.get(BTreeIndexOptions.BTREE_INDEX_CACHE_SIZE),
                                        options.get(
                                                BTreeIndexOptions
                                                        .BTREE_INDEX_HIGH_PRIORITY_POOL_RATIO)));
    }

    /**
     * 创建 BTree 索引写入器。
     *
     * @param fileWriter 文件写入器
     * @return BTree 索引写入器
     * @throws IOException 如果创建失败
     */
    @Override
    public BTreeIndexWriter createWriter(GlobalIndexFileWriter fileWriter) throws IOException {
        long blockSize = options.get(BTreeIndexOptions.BTREE_INDEX_BLOCK_SIZE).getBytes();
        CompressOptions compressOptions =
                new CompressOptions(
                        options.get(BTreeIndexOptions.BTREE_INDEX_COMPRESSION),
                        options.get(BTreeIndexOptions.BTREE_INDEX_COMPRESSION_LEVEL));
        return new BTreeIndexWriter(
                fileWriter,
                keySerializer,
                (int) blockSize,
                BlockCompressionFactory.create(compressOptions));
    }

    /**
     * 创建 BTree 索引读取器。
     *
     * @param fileReader 文件读取器
     * @param files 索引文件元数据列表
     * @return 全局索引读取器
     * @throws IOException 如果创建失败
     */
    @Override
    public GlobalIndexReader createReader(
            GlobalIndexFileReader fileReader, List<GlobalIndexIOMeta> files) throws IOException {
        return new LazyFilteredBTreeReader(files, keySerializer, fileReader, cacheManager.get());
    }
}
