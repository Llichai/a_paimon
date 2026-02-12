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

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.BlockDecompressor;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.utils.FileBasedBloomFilter;
import org.apache.paimon.utils.MurmurHashUtils;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;

import static org.apache.paimon.sst.SstFileUtils.crc32c;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * SST 文件读取器。
 *
 * <p>该类提供 SST（Sorted String Table）文件的点查询和范围查询功能。SST 文件是一种有序的键值存储文件格式，
 * 广泛应用于 LSM-Tree 等存储引擎中。
 *
 * <p>主要功能：
 * <ul>
 *   <li>点查询：通过键快速查找对应的值
 *   <li>范围查询：通过迭代器遍历指定范围的键值对
 *   <li>Bloom Filter 加速：使用布隆过滤器快速判断键是否存在
 *   <li>块缓存：缓存数据块和索引块，提高查询性能
 * </ul>
 *
 * <p>SST 文件结构：
 * <pre>
 * +------------------+
 * | Data Block 1     | ← 存储实际的键值对数据
 * +------------------+
 * | Data Block 2     |
 * +------------------+
 * | ...              |
 * +------------------+
 * | Data Block N     |
 * +------------------+
 * | Index Block      | ← 索引块，存储数据块的位置信息
 * +------------------+
 * | Bloom Filter     | ← 可选的布隆过滤器
 * +------------------+
 * | Footer           | ← 文件尾部，存储索引块和布隆过滤器的位置
 * +------------------+
 * </pre>
 *
 * <p>查询流程：
 * <ol>
 *   <li>点查询：Bloom Filter 过滤 → 索引块定位 → 数据块查找
 *   <li>范围查询：创建迭代器 → Seek 到起始位置 → 批量读取数据块
 * </ol>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建 SST 文件读取器
 * Comparator<MemorySlice> comparator = ...;
 * BlockCache blockCache = ...;
 * BlockHandle indexBlockHandle = ...;
 * FileBasedBloomFilter bloomFilter = ...;
 *
 * SstFileReader reader = new SstFileReader(
 *     comparator, blockCache, indexBlockHandle, bloomFilter);
 *
 * // 点查询
 * byte[] key = "user_123".getBytes();
 * byte[] value = reader.lookup(key);
 * if (value != null) {
 *     System.out.println("Found: " + new String(value));
 * }
 *
 * // 范围查询
 * SstFileIterator iterator = reader.createIterator();
 * iterator.seekTo("user_100".getBytes());
 * BlockIterator batch;
 * while ((batch = iterator.readBatch()) != null) {
 *     while (batch.hasNext()) {
 *         BlockEntry entry = batch.next();
 *         // 处理键值对
 *     }
 * }
 *
 * // 关闭读取器
 * reader.close();
 * }</pre>
 *
 * <p>性能优化：
 * <ul>
 *   <li>使用 Bloom Filter 减少不必要的磁盘 I/O
 *   <li>使用块缓存避免重复读取相同数据
 *   <li>支持数据压缩，减少存储空间和 I/O
 *   <li>批量读取提高范围查询效率
 * </ul>
 *
 * <p>注意事项：
 * <ul>
 *   <li>该类不是线程安全的，多线程访问需要外部同步
 *   <li>使用完毕后必须调用 {@link #close()} 释放资源
 *   <li>Bloom Filter 可能产生误判（false positive），但不会产生漏判（false negative）
 * </ul>
 */
public class SstFileReader implements Closeable {

    /** 键的比较器，用于排序和查找 */
    private final Comparator<MemorySlice> comparator;

    /** 块缓存，用于缓存数据块和索引块 */
    private final BlockCache blockCache;

    /** 索引块读取器，用于定位数据块 */
    private final BlockReader indexBlock;

    /** 布隆过滤器，用于快速判断键是否存在（可选） */
    @Nullable private final FileBasedBloomFilter bloomFilter;

    /**
     * 构造 SST 文件读取器。
     *
     * @param comparator 键的比较器
     * @param blockCache 块缓存
     * @param indexBlockHandle 索引块的句柄
     * @param bloomFilter 布隆过滤器（可选）
     */
    public SstFileReader(
            Comparator<MemorySlice> comparator,
            BlockCache blockCache,
            BlockHandle indexBlockHandle,
            @Nullable FileBasedBloomFilter bloomFilter) {
        this.comparator = comparator;
        this.blockCache = blockCache;
        this.indexBlock = readBlock(indexBlockHandle, true);
        this.bloomFilter = bloomFilter;
    }

    /**
     * 在文件中查找指定的键。
     *
     * <p>查找流程：
     * <ol>
     *   <li>如果有布隆过滤器，先使用布隆过滤器判断键是否可能存在
     *   <li>在索引块中查找包含该键的数据块
     *   <li>在数据块中精确查找键
     * </ol>
     *
     * @param key 序列化的键
     * @return 对应的序列化值，如果未找到则返回 null
     * @throws IOException 如果读取文件时发生错误
     */
    @Nullable
    public byte[] lookup(byte[] key) throws IOException {
        if (bloomFilter != null && !bloomFilter.testHash(MurmurHashUtils.hashBytes(key))) {
            return null;
        }

        MemorySlice keySlice = MemorySlice.wrap(key);
        // 在索引中查找包含该键的数据块
        BlockIterator indexBlockIterator = indexBlock.iterator();
        indexBlockIterator.seekTo(keySlice);

        // 如果索引迭代器没有下一个元素，说明该键不存在
        if (indexBlockIterator.hasNext()) {
            // 在当前数据块中定位到该键
            BlockIterator current = getNextBlock(indexBlockIterator);
            if (current.seekTo(keySlice)) {
                return current.next().getValue().copyBytes();
            }
        }
        return null;
    }

    /**
     * 创建文件迭代器用于范围查询。
     *
     * @return SST 文件迭代器
     */
    public SstFileIterator createIterator() {
        return new SstFileIterator(indexBlock.iterator());
    }

    /**
     * 获取下一个数据块的迭代器。
     *
     * @param indexBlockIterator 索引块迭代器
     * @return 数据块迭代器
     */
    private BlockIterator getNextBlock(BlockIterator indexBlockIterator) {
        // 索引块的句柄，指向键值对的位置
        MemorySlice blockHandle = indexBlockIterator.next().getValue();
        BlockReader dataBlock =
                readBlock(BlockHandle.readBlockHandle(blockHandle.toInput()), false);
        return dataBlock.iterator();
    }

    /**
     * 读取指定的块。
     *
     * @param blockHandle 块句柄
     * @param index 是否作为索引块读取
     * @return 块读取器
     */
    private BlockReader readBlock(BlockHandle blockHandle, boolean index) {
        // 读取块尾部信息
        MemorySegment trailerData =
                blockCache.getBlock(
                        blockHandle.offset() + blockHandle.size(),
                        BlockTrailer.ENCODED_LENGTH,
                        b -> b,
                        true);
        BlockTrailer blockTrailer =
                BlockTrailer.readBlockTrailer(MemorySlice.wrap(trailerData).toInput());

        MemorySegment unCompressedBlock =
                blockCache.getBlock(
                        blockHandle.offset(),
                        blockHandle.size(),
                        bytes -> decompressBlock(bytes, blockTrailer),
                        index);
        return BlockReader.create(MemorySlice.wrap(unCompressedBlock), comparator);
    }

    /**
     * 解压缩数据块。
     *
     * @param compressedBytes 压缩的字节数据
     * @param blockTrailer 块尾部信息
     * @return 解压缩后的字节数据
     */
    private byte[] decompressBlock(byte[] compressedBytes, BlockTrailer blockTrailer) {
        MemorySegment compressed = MemorySegment.wrap(compressedBytes);
        int crc32cCode = crc32c(compressed, blockTrailer.getCompressionType());
        checkArgument(
                blockTrailer.getCrc32c() == crc32cCode,
                String.format(
                        "Expected CRC32C(%d) but found CRC32C(%d)",
                        blockTrailer.getCrc32c(), crc32cCode));

        // 解压缩数据
        BlockCompressionFactory compressionFactory =
                BlockCompressionFactory.create(blockTrailer.getCompressionType());
        if (compressionFactory == null) {
            return compressedBytes;
        } else {
            MemorySliceInput compressedInput = MemorySlice.wrap(compressed).toInput();
            byte[] uncompressed = new byte[compressedInput.readVarLenInt()];
            BlockDecompressor decompressor = compressionFactory.getDecompressor();
            int uncompressedLength =
                    decompressor.decompress(
                            compressed.getHeapMemory(),
                            compressedInput.position(),
                            compressedInput.available(),
                            uncompressed,
                            0);
            checkArgument(uncompressedLength == uncompressed.length);
            return uncompressed;
        }
    }

    /**
     * 关闭读取器并释放资源。
     *
     * @throws IOException 如果关闭资源时发生错误
     */
    @Override
    public void close() throws IOException {
        if (bloomFilter != null) {
            bloomFilter.close();
        }
        blockCache.close();
    }

    /**
     * 范围查询迭代器。
     *
     * <p>该迭代器支持：
     * <ul>
     *   <li>Seek 到指定位置
     *   <li>批量读取数据块
     *   <li>顺序遍历键值对
     * </ul>
     *
     * <p>使用示例：
     * <pre>{@code
     * SstFileIterator iterator = reader.createIterator();
     * // Seek 到起始键
     * iterator.seekTo("start_key".getBytes());
     *
     * // 批量读取
     * BlockIterator batch;
     * while ((batch = iterator.readBatch()) != null) {
     *     while (batch.hasNext()) {
     *         BlockEntry entry = batch.next();
     *         byte[] key = entry.getKey().copyBytes();
     *         byte[] value = entry.getValue().copyBytes();
     *         // 处理键值对
     *     }
     * }
     * }</pre>
     */
    public class SstFileIterator {

        /** 索引块迭代器 */
        private final BlockIterator indexIterator;

        /** 已定位的数据块迭代器 */
        private @Nullable BlockIterator seekedDataBlock = null;

        /**
         * 构造迭代器。
         *
         * @param indexBlockIterator 索引块迭代器
         */
        SstFileIterator(BlockIterator indexBlockIterator) {
            this.indexIterator = indexBlockIterator;
        }

        /**
         * 定位到键等于或大于指定键的记录位置。
         *
         * @param key 目标键
         */
        public void seekTo(byte[] key) {
            MemorySlice keySlice = MemorySlice.wrap(key);

            indexIterator.seekTo(keySlice);
            if (indexIterator.hasNext()) {
                seekedDataBlock = getNextBlock(indexIterator);
                // 索引块条目的键是对应数据块的最后一个键
                // 如果存在索引条目键 >= 目标键，相关的数据块必定也包含某个键 >= 目标键
                // 这意味着 seekedDataBlock.hasNext() 必定为 true
                seekedDataBlock.seekTo(keySlice);
                Preconditions.checkState(seekedDataBlock.hasNext());
            } else {
                seekedDataBlock = null;
            }
        }

        /**
         * 从 SST 文件中读取一批记录，并将当前记录位置移动到下一批。
         *
         * @return 当前批次的记录，如果到达文件末尾则返回 null
         * @throws IOException 如果读取文件时发生错误
         */
        public BlockIterator readBatch() throws IOException {
            if (seekedDataBlock != null) {
                BlockIterator result = seekedDataBlock;
                seekedDataBlock = null;
                return result;
            }

            if (!indexIterator.hasNext()) {
                return null;
            }

            return getNextBlock(indexIterator);
        }
    }
}
