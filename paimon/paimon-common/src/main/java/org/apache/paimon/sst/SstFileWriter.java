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
import org.apache.paimon.compression.BlockCompressionType;
import org.apache.paimon.compression.BlockCompressor;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.MurmurHashUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.paimon.memory.MemorySegmentUtils.allocateReuseBytes;
import static org.apache.paimon.sst.BlockHandle.writeBlockHandle;
import static org.apache.paimon.sst.SstFileUtils.crc32c;
import static org.apache.paimon.utils.VarLengthIntUtils.encodeInt;

/**
 * SST 文件写入器。
 *
 * <p>该类用于写入 SST（Sorted String Table）文件。SST 文件是面向行的格式，设计用于频繁的点查询和按键的范围查询。
 *
 * <p>SST 文件格式：
 * <pre>
 * +------------------+
 * | Data Block 1     | ← 存储实际的键值对数据（可压缩）
 * |   Entry 1        |   每个块包含多个键值对条目
 * |   Entry 2        |   块大小由 blockSize 参数控制
 * |   ...            |
 * +------------------+
 * | Block Trailer 1  | ← 块尾部（压缩类型 + CRC32 校验和）
 * +------------------+
 * | Data Block 2     |
 * +------------------+
 * | Block Trailer 2  |
 * +------------------+
 * | ...              |
 * +------------------+
 * | Index Block      | ← 索引块，存储每个数据块的最大键和位置信息
 * +------------------+
 * | Index Trailer    |
 * +------------------+
 * | Bloom Filter     | ← 可选的布隆过滤器（用于快速判断键是否存在）
 * +------------------+
 * | Footer           | ← 文件尾部，存储索引块和布隆过滤器的位置
 * +------------------+
 * </pre>
 *
 * <p>主要功能：
 * <ul>
 *   <li>按键有序写入键值对数据
 *   <li>自动分块管理（当块大小超过阈值时自动刷新）
 *   <li>数据压缩（可选，支持 LZ4、Snappy 等）
 *   <li>CRC32 校验确保数据完整性
 *   <li>构建布隆过滤器（可选）
 *   <li>构建索引块用于快速查找
 * </ul>
 *
 * <p>写入流程：
 * <ol>
 *   <li>创建写入器，配置块大小、压缩类型、布隆过滤器等
 *   <li>按键的升序顺序调用 {@link #put(byte[], byte[])} 写入数据
 *   <li>当数据块大小超过阈值时，自动调用 {@link #flush()} 刷新
 *   <li>写入完成后，调用 {@link #writeBloomFilter()} 和 {@link #writeIndexBlock()} 写入元数据
 * </ol>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建输出流
 * PositionOutputStream out = ...;
 *
 * // 配置参数
 * int blockSize = 64 * 1024; // 64KB
 * BloomFilter.Builder bloomFilter = BloomFilter.builder(10000, 0.01);
 * BlockCompressionFactory compressionFactory = BlockCompressionFactory.create(LZ4);
 *
 * // 创建写入器
 * SstFileWriter writer = new SstFileWriter(
 *     out, blockSize, bloomFilter, compressionFactory);
 *
 * // 写入数据（必须按键升序）
 * writer.put("key1".getBytes(), "value1".getBytes());
 * writer.put("key2".getBytes(), "value2".getBytes());
 * writer.put("key3".getBytes(), "value3".getBytes());
 *
 * // 刷新最后的数据块
 * writer.flush();
 *
 * // 写入布隆过滤器
 * BloomFilterHandle bloomFilterHandle = writer.writeBloomFilter();
 *
 * // 写入索引块
 * BlockHandle indexBlockHandle = writer.writeIndexBlock();
 *
 * // 写入文件尾部
 * // ... 写入 footer，包含 indexBlockHandle 和 bloomFilterHandle 的位置信息
 * }</pre>
 *
 * <p>性能优化特性：
 * <ul>
 *   <li>块压缩：只有压缩率超过 12.5% 时才使用压缩数据
 *   <li>预分配缓冲区：减少内存分配开销
 *   <li>批量写入：减少 I/O 次数
 *   <li>统计信息：记录压缩前后大小、记录数等
 * </ul>
 *
 * <p>重要约束：
 * <ul>
 *   <li>调用者必须保证写入的键是严格递增的
 *   <li>如果键的顺序错误，查找和范围查询的结果将是未定义的
 * </ul>
 */
public class SstFileWriter {

    private static final Logger LOG = LoggerFactory.getLogger(SstFileWriter.class.getName());

    /** 输出流 */
    private final PositionOutputStream out;

    /** 数据块大小阈值（字节） */
    private final int blockSize;

    /** 数据块写入器 */
    private final BlockWriter dataBlockWriter;

    /** 索引块写入器 */
    private final BlockWriter indexBlockWriter;

    /** 布隆过滤器构建器（可选） */
    @Nullable private final BloomFilter.Builder bloomFilter;

    /** 压缩类型 */
    private final BlockCompressionType compressionType;

    /** 块压缩器（可选） */
    @Nullable private final BlockCompressor blockCompressor;

    /** 最后写入的键 */
    private byte[] lastKey;

    /** 记录总数 */
    private long recordCount;

    /** 未压缩数据总大小 */
    private long totalUncompressedSize;

    /** 压缩后数据总大小 */
    private long totalCompressedSize;

    /**
     * 构造 SST 文件写入器。
     *
     * @param out 位置输出流
     * @param blockSize 数据块大小阈值（字节）
     * @param bloomFilter 布隆过滤器构建器（可选）
     * @param compressionFactory 压缩工厂（可选）
     */
    public SstFileWriter(
            PositionOutputStream out,
            int blockSize,
            @Nullable BloomFilter.Builder bloomFilter,
            @Nullable BlockCompressionFactory compressionFactory) {
        this.out = out;
        this.blockSize = blockSize;
        this.dataBlockWriter = new BlockWriter((int) (blockSize * 1.1));
        int expectedNumberOfBlocks = 1024;
        this.indexBlockWriter =
                new BlockWriter(BlockHandle.MAX_ENCODED_LENGTH * expectedNumberOfBlocks);
        this.bloomFilter = bloomFilter;
        if (compressionFactory == null) {
            this.compressionType = BlockCompressionType.NONE;
            this.blockCompressor = null;
        } else {
            this.compressionType = compressionFactory.getCompressionType();
            this.blockCompressor = compressionFactory.getCompressor();
        }
    }

    /**
     * 将序列化的键值对写入 SST 文件。
     *
     * <p>调用者必须保证输入的键是严格递增的（根据 {@link SstFileReader} 的比较器）。
     * 否则，查找和范围查询的结果将是未定义的。
     *
     * <p>当数据块大小超过阈值时，会自动调用 {@link #flush()} 刷新数据块。
     *
     * @param key 序列化的键
     * @param value 序列化的值
     * @throws IOException 如果写入文件时发生错误
     */
    public void put(byte[] key, byte[] value) throws IOException {
        dataBlockWriter.add(key, value);
        if (bloomFilter != null) {
            bloomFilter.addHash(MurmurHashUtils.hashBytes(key));
        }

        lastKey = key;

        if (dataBlockWriter.memory() > blockSize) {
            flush();
        }

        recordCount++;
    }

    /**
     * 刷新当前数据块。
     *
     * <p>将当前数据块写入文件，并在索引块中记录该数据块的最大键和位置信息。
     *
     * @throws IOException 如果写入文件时发生错误
     */
    public void flush() throws IOException {
        if (dataBlockWriter.size() == 0) {
            return;
        }

        BlockHandle blockHandle = writeBlock(dataBlockWriter);
        MemorySlice handleEncoding = writeBlockHandle(blockHandle);
        indexBlockWriter.add(lastKey, handleEncoding.copyBytes());
    }

    /**
     * 写入数据块。
     *
     * @param blockWriter 块写入器
     * @return 块句柄
     * @throws IOException 如果写入文件时发生错误
     */
    private BlockHandle writeBlock(BlockWriter blockWriter) throws IOException {
        // 完成块的构建
        MemorySlice block = blockWriter.finish();

        totalUncompressedSize += block.length();

        // 尝试压缩数据块
        BlockCompressionType blockCompressionType = BlockCompressionType.NONE;
        if (blockCompressor != null) {
            int maxCompressedSize = blockCompressor.getMaxCompressedSize(block.length());
            byte[] compressed = allocateReuseBytes(maxCompressedSize + 5);
            int offset = encodeInt(compressed, 0, block.length());
            int compressedSize =
                    offset
                            + blockCompressor.compress(
                                    block.getHeapMemory(),
                                    block.offset(),
                                    block.length(),
                                    compressed,
                                    offset);

            // 只有压缩率超过 12.5% 时才使用压缩数据
            if (compressedSize < block.length() - (block.length() / 8)) {
                block = new MemorySlice(MemorySegment.wrap(compressed), 0, compressedSize);
                blockCompressionType = this.compressionType;
            }
        }

        totalCompressedSize += block.length();

        // 创建块尾部信息
        BlockTrailer blockTrailer =
                new BlockTrailer(blockCompressionType, crc32c(block, blockCompressionType));
        MemorySlice trailer = BlockTrailer.writeBlockTrailer(blockTrailer);

        // 创建该块的句柄
        BlockHandle blockHandle = new BlockHandle(out.getPos(), block.length());

        // 写入数据
        writeSlice(block);

        // 写入尾部信息：5 字节
        writeSlice(trailer);

        // 清理状态
        blockWriter.reset();

        return blockHandle;
    }

    /**
     * 写入布隆过滤器。
     *
     * @return 布隆过滤器句柄，如果没有布隆过滤器则返回 null
     * @throws IOException 如果写入文件时发生错误
     */
    @Nullable
    public BloomFilterHandle writeBloomFilter() throws IOException {
        if (bloomFilter == null) {
            return null;
        }
        MemorySegment buffer = bloomFilter.getBuffer();
        BloomFilterHandle bloomFilterHandle =
                new BloomFilterHandle(out.getPos(), buffer.size(), bloomFilter.expectedEntries());
        writeSlice(MemorySlice.wrap(buffer));
        LOG.info("Bloom filter size: {} bytes", bloomFilter.getBuffer().size());
        return bloomFilterHandle;
    }

    /**
     * 写入索引块。
     *
     * @return 索引块句柄
     * @throws IOException 如果写入文件时发生错误
     */
    @Nullable
    public BlockHandle writeIndexBlock() throws IOException {
        BlockHandle indexBlock = writeBlock(indexBlockWriter);
        LOG.info("Number of record: {}", recordCount);
        LOG.info("totalUncompressedSize: {}", MemorySize.ofBytes(totalUncompressedSize));
        LOG.info("totalCompressedSize: {}", MemorySize.ofBytes(totalCompressedSize));
        return indexBlock;
    }

    /**
     * 写入内存切片到输出流。
     *
     * @param slice 要写入的内存切片
     * @throws IOException 如果写入文件时发生错误
     */
    public void writeSlice(MemorySlice slice) throws IOException {
        out.write(slice.getHeapMemory(), slice.offset(), slice.length());
    }
}
