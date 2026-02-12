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

import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceOutput;
import org.apache.paimon.utils.IntArrayList;

import java.io.IOException;

import static org.apache.paimon.sst.BlockAlignedType.ALIGNED;
import static org.apache.paimon.sst.BlockAlignedType.UNALIGNED;

/**
 * 块写入器,用于构建 SST 文件中的数据块。
 *
 * <p>块的存储格式如下:
 * <pre>
 *     +---------------+
 *     | Block Trailer |  块尾部
 *     +------------------------------------------------+
 *     |       Block CRC32C      |     Compression      |
 *     +------------------------------------------------+
 *     +---------------+
 *     |  Block Data   |  块数据
 *     +---------------+--------------------------------+----+
 *     | key len | key bytes | value len | value bytes  |    |
 *     +------------------------------------------------+    |
 *     | key len | key bytes | value len | value bytes  |    +-> 键值对
 *     +------------------------------------------------+    |
 *     |                  ... ...                       |    |
 *     +------------------------------------------------+----+
 *     | entry pos | entry pos |     ...    | entry pos |    +-> 可选,用于非对齐块
 *     +------------------------------------------------+----+
 *     |   entry num  /  entry size   |   aligned type  |  元信息
 *     +------------------------------------------------+
 * </pre>
 *
 * <p>支持两种存储模式:
 * <ul>
 *   <li>对齐模式 - 所有键值对大小相同,无需索引
 *   <li>非对齐模式 - 键值对大小不同,需要位置索引
 * </ul>
 */
public class BlockWriter {

    /** 记录位置列表 */
    private final IntArrayList positions;

    /** 块数据输出流 */
    private final MemorySliceOutput block;

    /** 对齐记录的大小 */
    private int alignedSize;

    /** 是否为对齐模式 */
    private boolean aligned;

    /**
     * 构造块写入器。
     *
     * @param blockSize 预期的块大小
     */
    public BlockWriter(int blockSize) {
        this.positions = new IntArrayList(32);
        this.block = new MemorySliceOutput(blockSize + 128);
        this.alignedSize = 0;
        this.aligned = true;
    }

    /** 重置写入器状态。 */
    public void reset() {
        this.positions.clear();
        this.block.reset();
        this.alignedSize = 0;
        this.aligned = true;
    }

    /**
     * 添加键值对。
     *
     * @param key 键字节数组
     * @param value 值字节数组
     */
    public void add(byte[] key, byte[] value) {
        int startPosition = block.size();
        block.writeVarLenInt(key.length);
        block.writeBytes(key);
        block.writeVarLenInt(value.length);
        block.writeBytes(value);
        int endPosition = block.size();

        positions.add(startPosition);
        if (aligned) {
            int currentSize = endPosition - startPosition;
            if (alignedSize == 0) {
                alignedSize = currentSize;
            } else {
                aligned = alignedSize == currentSize;
            }
        }
    }

    /**
     * 返回已添加的记录数量。
     *
     * @return 记录数量
     */
    public int size() {
        return positions.size();
    }

    /**
     * 返回当前内存占用(字节)。
     *
     * @return 内存占用大小
     */
    public int memory() {
        int memory = block.size() + 5;
        if (!aligned) {
            memory += positions.size() * 4;
        }
        return memory;
    }

    /**
     * 完成块写入并返回块数据。
     *
     * @return 包含块数据的内存切片
     * @throws IOException 如果写入失败
     */
    public MemorySlice finish() throws IOException {
        if (positions.isEmpty()) {
            // Do not use alignment mode, as it is impossible to calculate how many records are
            // inside when reading
            aligned = false;
        }

        if (aligned) {
            block.writeInt(alignedSize);
        } else {
            for (int i = 0; i < positions.size(); i++) {
                block.writeInt(positions.get(i));
            }
            block.writeInt(positions.size());
        }
        block.writeByte(aligned ? ALIGNED.toByte() : UNALIGNED.toByte());
        return block.toSlice();
    }
}
