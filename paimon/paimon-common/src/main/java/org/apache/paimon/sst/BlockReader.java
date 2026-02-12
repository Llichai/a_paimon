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
import org.apache.paimon.memory.MemorySliceInput;

import java.util.Comparator;

import static org.apache.paimon.sst.BlockAlignedType.ALIGNED;

/**
 * 块读取器。
 *
 * <p>用于读取和解析 SST 文件中的数据块,支持对齐和非对齐两种存储格式。
 *
 * <p>子类:
 * <ul>
 *   <li>{@link AlignedBlockReader} - 对齐块读取器,所有记录大小相同
 *   <li>{@link UnalignedBlockReader} - 非对齐块读取器,记录大小不同,使用索引定位
 * </ul>
 */
public abstract class BlockReader {

    /** 块数据 */
    private final MemorySlice block;

    /** 记录数量 */
    private final int recordCount;

    /** 键比较器 */
    private final Comparator<MemorySlice> comparator;

    private BlockReader(MemorySlice block, int recordCount, Comparator<MemorySlice> comparator) {
        this.block = block;
        this.recordCount = recordCount;
        this.comparator = comparator;
    }

    /** 返回块输入流。 */
    public MemorySliceInput blockInput() {
        return block.toInput();
    }

    /** 返回记录数量。 */
    public int recordCount() {
        return recordCount;
    }

    /** 返回键比较器。 */
    public Comparator<MemorySlice> comparator() {
        return comparator;
    }

    /** 返回块迭代器。 */
    public BlockIterator iterator() {
        return new BlockIterator(this);
    }

    /**
     * 根据记录位置定位到切片位置。
     *
     * @param recordPosition 记录位置(从0开始)
     * @return 切片位置(字节偏移量)
     */
    public abstract int seekTo(int recordPosition);

    /**
     * 创建块读取器。
     *
     * @param block 块数据
     * @param comparator 键比较器
     * @return 块读取器实例
     */
    public static BlockReader create(MemorySlice block, Comparator<MemorySlice> comparator) {
        BlockAlignedType alignedType =
                BlockAlignedType.fromByte(block.readByte(block.length() - 1));
        int intValue = block.readInt(block.length() - 5);
        if (alignedType == ALIGNED) {
            return new AlignedBlockReader(block.slice(0, block.length() - 5), intValue, comparator);
        } else {
            int indexLength = intValue * 4;
            int indexOffset = block.length() - 5 - indexLength;
            MemorySlice data = block.slice(0, indexOffset);
            MemorySlice index = block.slice(indexOffset, indexLength);
            return new UnalignedBlockReader(data, index, comparator);
        }
    }

    /** 对齐块读取器,所有记录大小相同。 */
    private static class AlignedBlockReader extends BlockReader {

        /** 记录大小 */
        private final int recordSize;

        public AlignedBlockReader(
                MemorySlice data, int recordSize, Comparator<MemorySlice> comparator) {
            super(data, data.length() / recordSize, comparator);
            this.recordSize = recordSize;
        }

        @Override
        public int seekTo(int recordPosition) {
            return recordPosition * recordSize;
        }
    }

    /** 非对齐块读取器,记录大小不同,使用索引定位。 */
    private static class UnalignedBlockReader extends BlockReader {

        /** 位置索引 */
        private final MemorySlice index;

        public UnalignedBlockReader(
                MemorySlice data, MemorySlice index, Comparator<MemorySlice> comparator) {
            super(data, index.length() / 4, comparator);
            this.index = index;
        }

        @Override
        public int seekTo(int recordPosition) {
            return index.readInt(recordPosition * 4);
        }
    }
}
