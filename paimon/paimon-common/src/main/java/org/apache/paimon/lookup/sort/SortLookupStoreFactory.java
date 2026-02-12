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

package org.apache.paimon.lookup.sort;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.utils.BloomFilter;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;

/**
 * 基于排序的查找存储工厂。
 *
 * <p>该工厂实现基于 SST 文件格式的查找存储，使用排序索引在磁盘上查找记录。
 *
 * <p>主要特性：
 * <ul>
 *   <li>基于 SST 文件格式的有序存储
 *   <li>支持数据压缩
 *   <li>使用布隆过滤器加速查找
 *   <li>块缓存提高性能
 * </ul>
 *
 * <p>实现机制：
 * <ul>
 *   <li>写入器：{@link SortLookupStoreWriter} - 基于 {@link org.apache.paimon.sst.SstFileWriter}
 *   <li>读取器：{@link SortLookupStoreReader} - 基于 {@link org.apache.paimon.sst.SstFileReader}
 * </ul>
 */
public class SortLookupStoreFactory implements LookupStoreFactory {

    /** 键比较器 */
    private final Comparator<MemorySlice> comparator;

    /** 缓存管理器 */
    private final CacheManager cacheManager;

    /** 块大小 */
    private final int blockSize;

    /** 压缩工厂（可选） */
    @Nullable private final BlockCompressionFactory compressionFactory;

    /**
     * 构造排序查找存储工厂。
     *
     * @param comparator 键比较器
     * @param cacheManager 缓存管理器
     * @param blockSize 块大小
     * @param compression 压缩选项
     */
    public SortLookupStoreFactory(
            Comparator<MemorySlice> comparator,
            CacheManager cacheManager,
            int blockSize,
            CompressOptions compression) {
        this.comparator = comparator;
        this.cacheManager = cacheManager;
        this.blockSize = blockSize;
        this.compressionFactory = BlockCompressionFactory.create(compression);
    }

    @Override
    public SortLookupStoreReader createReader(File file) throws IOException {
        Path filePath = new Path(file.getAbsolutePath());
        SeekableInputStream input = LocalFileIO.INSTANCE.newInputStream(filePath);
        return new SortLookupStoreReader(comparator, filePath, file.length(), input, cacheManager);
    }

    @Override
    public SortLookupStoreWriter createWriter(File file, @Nullable BloomFilter.Builder bloomFilter)
            throws IOException {
        Path filePath = new Path(file.getAbsolutePath());
        PositionOutputStream out = LocalFileIO.INSTANCE.newOutputStream(filePath, true);
        return new SortLookupStoreWriter(out, blockSize, bloomFilter, compressionFactory);
    }
}
