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

import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.memory.MemorySliceOutput;
import org.apache.paimon.sst.BlockHandle;
import org.apache.paimon.sst.BloomFilterHandle;

import javax.annotation.Nullable;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * BTree 文件的页脚(Footer)结构。
 *
 * <p>Footer 是 BTree 索引文件的元数据区域,位于文件末尾,包含了定位其他关键区域的句柄信息。
 * 读取 BTree 索引文件时,首先读取 Footer 以获取其他区域的位置和大小。
 *
 * <h2>Footer 结构</h2>
 * <pre>
 * Footer 固定大小为 48 字节:
 * +----------------------+--------+
 * | BloomFilterHandle    | 20 字节 | (offset: 8, size: 4, expectedEntries: 8)
 * | IndexBlockHandle     | 12 字节 | (offset: 8, size: 4)
 * | NullBitmapHandle     | 12 字节 | (offset: 8, size: 4)
 * | Magic Number         | 4 字节  | 固定值 198732882,用于校验文件完整性
 * +----------------------+--------+
 * </pre>
 *
 * <h2>包含的句柄</h2>
 * <ul>
 *   <li>Bloom Filter 句柄 - 可选,用于快速判断键是否存在
 *   <li>Index Block 句柄 - 必需,指向索引块的位置
 *   <li>Null Bitmap 句柄 - 可选,存储 NULL 键的行 ID 位图
 * </ul>
 *
 * <h2>魔数校验</h2>
 * <p>魔数(Magic Number)用于验证文件格式的正确性。读取时如果魔数不匹配,
 * 说明文件已损坏或不是有效的 BTree 索引文件。
 *
 * @see BTreeIndexWriter
 * @see BTreeIndexReader
 */
public class BTreeFileFooter {

    /** 魔数,用于验证 BTree 文件的有效性 */
    public static final int MAGIC_NUMBER = 198732882;

    /** Footer 的固定编码长度,始终为 48 字节 */
    public static final int ENCODED_LENGTH = 48;

    /** Bloom Filter 句柄,如果未启用 Bloom Filter 则为 null */
    @Nullable private final BloomFilterHandle bloomFilterHandle;

    /** 索引块句柄,指向 BTree 索引块的位置和大小 */
    private final BlockHandle indexBlockHandle;

    /** Null 位图句柄,如果没有 NULL 键则为 null */
    @Nullable private final BlockHandle nullBitmapHandle;

    /**
     * 构造 BTree 文件页脚。
     *
     * @param bloomFilterHandle Bloom Filter 句柄,可为 null
     * @param indexBlockHandle 索引块句柄,不能为 null
     * @param nullBitmapHandle Null 位图句柄,可为 null
     */
    public BTreeFileFooter(
            @Nullable BloomFilterHandle bloomFilterHandle,
            BlockHandle indexBlockHandle,
            BlockHandle nullBitmapHandle) {
        this.bloomFilterHandle = bloomFilterHandle;
        this.indexBlockHandle = indexBlockHandle;
        this.nullBitmapHandle = nullBitmapHandle;
    }

    /** 获取 Bloom Filter 句柄。 */
    @Nullable
    public BloomFilterHandle getBloomFilterHandle() {
        return bloomFilterHandle;
    }

    /** 获取索引块句柄。 */
    public BlockHandle getIndexBlockHandle() {
        return indexBlockHandle;
    }

    /** 获取 Null 位图句柄。 */
    @Nullable
    public BlockHandle getNullBitmapHandle() {
        return nullBitmapHandle;
    }

    /**
     * 从输入流中读取 Footer。
     *
     * @param sliceInput 内存切片输入流
     * @return BTree 文件页脚对象
     * @throws IllegalArgumentException 如果魔数不匹配
     */
    public static BTreeFileFooter readFooter(MemorySliceInput sliceInput) {
        // 读取 Bloom Filter 句柄(offset: 8 字节, size: 4 字节, expectedEntries: 8 字节)
        @Nullable
        BloomFilterHandle bloomFilterHandle =
                new BloomFilterHandle(
                        sliceInput.readLong(), sliceInput.readInt(), sliceInput.readLong());
        // 如果所有字段都为 0,表示未启用 Bloom Filter
        if (bloomFilterHandle.offset() == 0
                && bloomFilterHandle.size() == 0
                && bloomFilterHandle.expectedEntries() == 0) {
            bloomFilterHandle = null;
        }

        // 读取索引块句柄(offset: 8 字节, size: 4 字节)
        BlockHandle indexBlockHandle = new BlockHandle(sliceInput.readLong(), sliceInput.readInt());

        // 读取 Null 位图句柄(offset: 8 字节, size: 4 字节)
        @Nullable
        BlockHandle nullBitmapHandle = new BlockHandle(sliceInput.readLong(), sliceInput.readInt());
        // 如果 offset 和 size 都为 0,表示没有 NULL 键
        if (nullBitmapHandle.offset() == 0 && nullBitmapHandle.size() == 0) {
            nullBitmapHandle = null;
        }

        // 跳过填充字节,定位到魔数位置
        sliceInput.setPosition(ENCODED_LENGTH - 4);

        // 验证魔数,确保文件格式正确
        int magicNumber = sliceInput.readInt();
        checkArgument(magicNumber == MAGIC_NUMBER, "File is not a table (bad magic number)");

        return new BTreeFileFooter(bloomFilterHandle, indexBlockHandle, nullBitmapHandle);
    }

    /**
     * 将 Footer 写入内存切片。
     *
     * @param footer BTree 文件页脚对象
     * @return 包含序列化 Footer 的内存切片
     */
    public static MemorySlice writeFooter(BTreeFileFooter footer) {
        MemorySliceOutput output = new MemorySliceOutput(ENCODED_LENGTH);
        writeFooter(footer, output);
        return output.toSlice();
    }

    /**
     * 将 Footer 写入输出流。
     *
     * @param footer BTree 文件页脚对象
     * @param sliceOutput 内存切片输出流
     */
    public static void writeFooter(BTreeFileFooter footer, MemorySliceOutput sliceOutput) {
        // 写入 Bloom Filter 句柄(20 字节)
        if (footer.bloomFilterHandle == null) {
            sliceOutput.writeLong(0);  // offset
            sliceOutput.writeInt(0);   // size
            sliceOutput.writeLong(0);  // expectedEntries
        } else {
            sliceOutput.writeLong(footer.bloomFilterHandle.offset());
            sliceOutput.writeInt(footer.bloomFilterHandle.size());
            sliceOutput.writeLong(footer.bloomFilterHandle.expectedEntries());
        }

        // 写入索引块句柄(12 字节)
        sliceOutput.writeLong(footer.indexBlockHandle.offset());
        sliceOutput.writeInt(footer.indexBlockHandle.size());

        // 写入 Null 位图句柄(12 字节)
        if (footer.nullBitmapHandle == null) {
            sliceOutput.writeLong(0);  // offset
            sliceOutput.writeInt(0);   // size
        } else {
            sliceOutput.writeLong(footer.nullBitmapHandle.offset());
            sliceOutput.writeInt(footer.nullBitmapHandle.size());
        }

        // 写入魔数(4 字节)
        sliceOutput.writeInt(MAGIC_NUMBER);
    }
}
