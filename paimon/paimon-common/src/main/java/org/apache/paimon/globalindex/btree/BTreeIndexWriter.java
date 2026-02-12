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
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexParallelWriter;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceOutput;
import org.apache.paimon.sst.BlockHandle;
import org.apache.paimon.sst.BloomFilterHandle;
import org.apache.paimon.sst.SstFileWriter;
import org.apache.paimon.utils.LazyField;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.zip.CRC32;

/**
 * B-Tree 全局索引写入器（BTree Global Index Writer）实现
 *
 * <p><b>核心作用</b>：用于构建 B-Tree 索引，支持全局范围的键到行ID的映射，用于快速查询和范围扫描。
 *
 * <p><b>重要约束</b>：写入的键必须单调递增（不能重复写入更小的键），以支持高效的索引构建。
 *
 * <h2>文件格式和布局</h2>
 * <p>生成的索引文件采用分块存储，从低位到高位的布局如下：
 * <pre>
 *    +-----------------------------------+------+
 *    |             Footer                |      |
 *    +-----------------------------------+      |
 *    |           Index Block             |      +--> 打开时加载
 *    +-----------------------------------+      |
 *    |        Bloom Filter Block         |      |
 *    +-----------------------------------+------+
 *    |         Null Bitmap Block         |      |
 *    +-----------------------------------+      |
 *    |            Data Block             |      |
 *    +-----------------------------------+      +--> 按需加载
 *    |              ......               |      |
 *    +-----------------------------------+      |
 *    |            Data Block             |      |
 *    +-----------------------------------+------+
 * </pre>
 *
 * <h2>关键设计细节</h2>
 * <ul>
 *   <li><b>键合并（Key Merging）</b>：相同键的多个行ID合并为一个紧凑列表，减少索引大小</li>
 *   <li><b>空值位图（Null Bitmap）</b>：所有 NULL 键单独存储在位图中，与数据分离</li>
 *   <li><b>SST 格式</b>：底层使用 SST（Sorted String Table）格式存储，支持高效查询</li>
 *   <li><b>布隆过滤器</b>：可选的布隆过滤器加速等值和 IN 查询</li>
 * </ul>
 *
 * <h2>工作流程</h2>
 * <ol>
 *   <li>初始化：创建内存中间态数据结构（行ID列表、空值位图）</li>
 *   <li>写入：对每个键调用 write(key, rowId)，自动合并相同键</li>
 *   <li>冲洗：当检测到新键时，将前一个键的行ID列表序列化写入 SST</li>
 *   <li>完成：调用 finish() 完成索引构建，输出元数据</li>
 * </ol>
 *
 * <p><b>注意</b>：NULL 键的处理延迟到 finish() 阶段，以实现与其他键的分离存储。
 *
 * @see GlobalIndexSingletonWriter
 * @see SstFileWriter SST 文件写入器
 */
public class BTreeIndexWriter implements GlobalIndexParallelWriter {

    /**
     * 索引文件名
     */
    private final String fileName;
    /**
     * 输出流（用于写入索引数据到磁盘）
     */
    private final PositionOutputStream out;

    /**
     * SST 文件写入器（负责数据块、索引块等的写入）
     */
    private final SstFileWriter writer;
    /**
     * 键序列化器（用于将对象键序列化为字节）
     */
    private final KeySerializer keySerializer;
    /**
     * 键比较器（用于判断两个键是否相等）
     */
    private final Comparator<Object> comparator;
    /**
     * 当前处理的键对应的行ID列表（等待序列化）
     */
    private final List<Long> currentRowIds = new ArrayList<>();
    /**
     * 空值位图（用于存储所有 NULL 键对应的行ID）
     */
    private final LazyField<RoaringNavigableMap64> nullBitmap =
            new LazyField<>(RoaringNavigableMap64::new);

    /**
     * 第一个非空键（用于索引元数据）
     */
    private Object firstKey = null;
    /**
     * 最后一个非空键（用于索引元数据）
     */
    private Object lastKey = null;
    /**
     * 已处理的总行数（包括 NULL 键）
     */
    private long rowCount = 0;

    /**
     * 构造 B-Tree 索引写入器
     *
     * <p>初始化过程：
     * <ol>
     *   <li>创建新文件并获取输出流
     *   <li>初始化 SST 文件写入器（不支持布隆过滤器）
     *   <li>保存序列化器和比较器供后续使用
     * </ol>
     *
     * @param indexFileWriter    全局索引文件写入器（管理文件创建和输出流）
     * @param keySerializer      键序列化器（将键对象转换为字节数组）
     * @param blockSize          SST 数据块大小（字节数）
     * @param compressionFactory 压缩工厂（用于压缩数据块）
     * @throws IOException 如果文件创建失败
     */
    public BTreeIndexWriter(
            GlobalIndexFileWriter indexFileWriter,
            KeySerializer keySerializer,
            int blockSize,
            BlockCompressionFactory compressionFactory)
            throws IOException {
        this.fileName = indexFileWriter.newFileName(BTreeGlobalIndexerFactory.IDENTIFIER);
        this.out = indexFileWriter.newOutputStream(this.fileName);
        this.keySerializer = keySerializer;
        this.comparator = keySerializer.createComparator();
        // TODO: 未来可以启用布隆过滤器以加速等值和 IN 查询
        this.writer = new SstFileWriter(out, blockSize, null, compressionFactory);
    }

    /**
     * 写入单个行记录（键-行ID对）
     *
     * <p>工作流程：
     * <ol>
     *   <li>增加行计数
     *   <li>如果是 NULL 键，加入空值位图，直接返回
     *   <li>如果是新键（与上一个键不同），冲洗前一个键的行ID列表
     *   <li>将行ID加入当前键的行ID列表
     *   <li>更新统计信息（firstKey、lastKey）
     * </ol>
     *
     * <p><b>重要约束</b>：非 NULL 键必须单调递增（即 compare(key, lastKey) >= 0），
     * 违反此约束会导致索引构建失败。
     *
     * @param key   键（可为 null）
     * @param rowId 行ID（行在数据文件中的唯一标识）
     */
    @Override
    public void write(@Nullable Object key, long rowId) {
        rowCount++;
        if (key == null) {
            // 空值键单独处理，加入位图而不是数据块
            nullBitmap.get().add(rowId);
            return;
        }

        // 检测到新键，需要冲洗前一个键的数据
        if (lastKey != null && comparator.compare(key, lastKey) != 0) {
            try {
                flush();
            } catch (IOException e) {
                throw new RuntimeException("Error in writing btree index files.", e);
            }
        }
        lastKey = key;
        currentRowIds.add(rowId);

        // 更新统计信息（首次写入时记录第一个键）
        if (firstKey == null) {
            firstKey = key;
        }
    }

    /**
     * 冲洗当前键的行ID列表到 SST 文件
     *
     * <p>序列化过程：
     * <ol>
     *   <li>如果行ID列表为空，直接返回（没有数据可写）
     *   <li>创建 MemorySliceOutput 缓冲区
     *   <li>写入行ID数量（变长整数编码，节省空间）
     *   <li>逐个写入每个行ID（变长长整数编码）
     *   <li>清空当前行ID列表，准备接收下一个键的数据
     *   <li>序列化键并与行ID列表一起写入 SST
     * </ol>
     *
     * <p>行ID列表采用变长编码，计算空间需求时使用估算：
     * <pre>
     * 所需字节数 ≈ 行ID数 * 9 + 5
     * （最坏情况：每个行ID占9字节，数量字段占5字节）
     * </pre>
     *
     * @throws IOException 如果写入 SST 失败
     */
    private void flush() throws IOException {
        if (currentRowIds.isEmpty()) {
            return; // 当前键无行ID，无需冲洗
        }

        // 步骤1：创建输出缓冲区（估算大小）
        MemorySliceOutput sliceOutput = new MemorySliceOutput(currentRowIds.size() * 9 + 5);

        // 步骤2：写入行ID数量（变长编码）
        sliceOutput.writeVarLenInt(currentRowIds.size());

        // 步骤3：逐个写入行ID（变长编码）
        for (long currentRowId : currentRowIds) {
            sliceOutput.writeVarLenLong(currentRowId);
        }
        currentRowIds.clear(); // 清空列表，准备接收下一个键的数据

        // 步骤4：将键和行ID列表写入 SST
        writer.put(keySerializer.serialize(lastKey), sliceOutput.toSlice().copyBytes());
    }

    /**
     * 完成索引构建，返回最终的索引文件信息
     *
     * <p>索引完成过程：
     * <ol>
     *   <li>冲洗剩余数据（最后一个键的行ID列表）
     *   <li>冲洗 SST 写入器中的缓存数据块
     *   <li>写入空值位图块到文件末尾
     *   <li>写入布隆过滤器块（当前实现中为 null）
     *   <li>写入索引块（用于快速查询键）
     *   <li>写入文件页脚（包含各块的位置和元数据）
     *   <li>关闭输出流
     *   <li>生成并返回索引元数据
     * </ol>
     *
     * <p>元数据内容：
     * <ul>
     *   <li>文件名：索引文件的位置标识
     *   <li>行计数：文件中包含的总行数
     *   <li>元字节数：包含首键、末键、是否有空值位图的二进制编码
     * </ul>
     *
     * @return 包含文件信息的结果列表（通常只有一个元素）
     * @throws RuntimeException 如果索引构建失败（如构建空索引文件）
     */
    @Override
    public List<ResultEntry> finish() {
        try {
            // 步骤1：冲洗剩余的行ID列表
            flush();

            // 步骤2：冲洗 SST 写入器中的缓存数据块
            writer.flush();

            // 步骤3：写入空值位图块
            BlockHandle nullBitmapHandle = writeNullBitmap();

            // 步骤4：写入布隆过滤器块
            // 当前实现中不支持布隆过滤器，但保留接口以便未来扩展（用于加速等值和 IN 查询）
            BloomFilterHandle bloomFilterHandle = writer.writeBloomFilter();

            // 步骤5：写入索引块（用于快速查询键位置）
            BlockHandle indexBlockHandle = writer.writeIndexBlock();

            // 步骤6：写入文件页脚
            BTreeFileFooter footer =
                    new BTreeFileFooter(bloomFilterHandle, indexBlockHandle, nullBitmapHandle);
            MemorySlice footerEncoding = BTreeFileFooter.writeFooter(footer);
            writer.writeSlice(footerEncoding);

            // 步骤7：关闭输出流
            out.close();
        } catch (IOException e) {
            throw new RuntimeException("Error in closing BTree index writer", e);
        }

        // 验证索引非空（至少有一个非空键或一个空值）
        if (firstKey == null && !nullBitmap.initialized()) {
            throw new RuntimeException("Should never write an empty btree index file.");
        }

        // 步骤8：序列化索引元数据
        byte[] metaBytes =
                new BTreeIndexMeta(
                        // 序列化首键（null 表示没有非空键）
                        firstKey == null ? null : keySerializer.serialize(firstKey),
                        // 序列化末键（null 表示没有非空键）
                        lastKey == null ? null : keySerializer.serialize(lastKey),
                        // 是否存在空值位图
                        nullBitmap.initialized())
                        .serialize();

        // 返回包含文件信息的结果列表
        return Collections.singletonList(new ResultEntry(fileName, rowCount, metaBytes));
    }

    /**
     * 将空值位图写入文件
     *
     * <p>空值位图序列化过程：
     * <ol>
     *   <li>如果没有 NULL 键，返回 null（不需要位图）
     *   <li>序列化位图数据为字节数组
     *   <li>计算序列化数据的 CRC32 校验码
     *   <li>将位图数据和校验码一起写入文件
     *   <li>记录文件位置和大小信息
     * </ol>
     *
     * <p><b>数据完整性保证</b>：使用 CRC32 校验码验证数据在传输和存储过程中的完整性。
     *
     * @return 空值位图块的元数据（位置、大小），如果没有位图则返回 null
     * @throws IOException 如果写入失败
     */
    @Nullable
    private BlockHandle writeNullBitmap() throws IOException {
        if (!nullBitmap.initialized()) {
            return null; // 没有空值，不需要写入位图
        }

        // 步骤1：序列化位图数据
        byte[] serializedBitmap = nullBitmap.get().serialize();
        int length = serializedBitmap.length;

        // 步骤2：计算 CRC32 校验码（用于数据完整性验证）
        CRC32 crc32 = new CRC32();
        crc32.update(serializedBitmap, 0, length);

        // 步骤3：创建输出缓冲区（位图数据 + CRC 值 4 字节）
        MemorySliceOutput sliceOutput = new MemorySliceOutput(length + 4);
        sliceOutput.writeBytes(serializedBitmap);
        sliceOutput.writeInt((int) crc32.getValue());

        // 步骤4：写入文件并记录块信息
        BlockHandle nullBitmapHandle = new BlockHandle(out.getPos(), length);
        writer.writeSlice(sliceOutput.toSlice());

        return nullBitmapHandle;
    }
}
