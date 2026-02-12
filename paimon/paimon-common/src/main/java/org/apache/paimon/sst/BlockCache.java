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

package org.apache.paimon.sst;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.io.cache.CacheKey;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.io.cache.CacheManager.SegmentContainer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.utils.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * 块读取缓存。
 *
 * <p>该类为 SST 文件的块读取提供缓存支持,使用 {@link CacheManager} 来管理内存页。
 * 缓存可以显著减少磁盘 I/O,提高读取性能。
 *
 * <p>主要功能:
 * <ul>
 *   <li>缓存数据块和索引块
 *   <li>支持块级别的解压缩
 *   <li>自动刷新过期的缓存项
 *   <li>统一管理缓存生命周期
 * </ul>
 */
public class BlockCache implements Closeable {

    /** 文件路径 */
    private final Path filePath;

    /** 可定位输入流 */
    private final SeekableInputStream input;

    /** 缓存管理器 */
    private final CacheManager cacheManager;

    /** 块缓存映射,键为缓存键,值为内存段容器 */
    private final Map<CacheKey, SegmentContainer> blocks;

    /**
     * 构造块缓存。
     *
     * @param filePath 文件路径
     * @param input 可定位输入流
     * @param cacheManager 缓存管理器
     */
    public BlockCache(Path filePath, SeekableInputStream input, CacheManager cacheManager) {
        this.filePath = filePath;
        this.input = input;
        this.cacheManager = cacheManager;
        this.blocks = new HashMap<>();
    }

    /**
     * 从指定位置读取数据。
     *
     * @param offset 文件偏移量
     * @param length 数据长度
     * @return 读取的字节数组
     * @throws IOException 如果读取失败
     */
    private byte[] readFrom(long offset, int length) throws IOException {
        byte[] buffer = new byte[length];
        input.seek(offset);
        IOUtils.readFully(input, buffer);
        return buffer;
    }

    /**
     * 获取指定位置的块。
     *
     * <p>如果块已缓存且未过期,则直接返回缓存的内存段;
     * 否则从文件读取、解压缩并缓存。
     *
     * @param position 块在文件中的位置
     * @param length 块的长度
     * @param decompressFunc 解压缩函数
     * @param isIndex 是否为索引块
     * @return 包含块数据的内存段
     */
    public MemorySegment getBlock(
            long position, int length, Function<byte[], byte[]> decompressFunc, boolean isIndex) {
        CacheKey cacheKey = CacheKey.forPosition(filePath, position, length, isIndex);

        SegmentContainer container = blocks.get(cacheKey);
        if (container == null || container.getAccessCount() == CacheManager.REFRESH_COUNT) {
            MemorySegment segment =
                    cacheManager.getPage(
                            cacheKey,
                            key -> {
                                byte[] bytes = readFrom(position, length);
                                return decompressFunc.apply(bytes);
                            },
                            blocks::remove);
            container = new SegmentContainer(segment);
            blocks.put(cacheKey, container);
        }
        return container.access();
    }

    /**
     * 关闭缓存并释放所有缓存的块。
     *
     * @throws IOException 如果关闭失败
     */
    @Override
    public void close() throws IOException {
        Set<CacheKey> sets = new HashSet<>(blocks.keySet());
        for (CacheKey key : sets) {
            cacheManager.invalidPage(key);
        }
    }
}
