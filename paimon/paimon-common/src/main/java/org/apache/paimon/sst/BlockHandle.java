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
import org.apache.paimon.memory.MemorySliceOutput;

/**
 * 块句柄,用于定位块在文件中的位置和大小。
 *
 * <p>该类封装了块的物理位置信息,包括文件偏移量和块大小。
 * 使用变长编码来节省存储空间。
 *
 * <p>编码格式:
 * <ul>
 *   <li>offset - 使用变长 long 编码(最多 9 字节)
 *   <li>size - 使用变长 int 编码(最多 5 字节)
 * </ul>
 */
public class BlockHandle {

    /** 最大编码长度(字节) */
    public static final int MAX_ENCODED_LENGTH = 9 + 5;

    /** 块在文件中的偏移量 */
    private final long offset;

    /** 块的大小(不包括尾部) */
    private final int size;

    /**
     * 构造块句柄。
     *
     * @param offset 文件偏移量
     * @param size 块大小
     */
    public BlockHandle(long offset, int size) {
        this.offset = offset;
        this.size = size;
    }

    /**
     * 返回块的文件偏移量。
     *
     * @return 偏移量
     */
    public long offset() {
        return offset;
    }

    /**
     * 返回块的大小。
     *
     * @return 块大小(字节)
     */
    public int size() {
        return size;
    }

    /**
     * 返回完整块的大小(包括尾部)。
     *
     * @return 完整块大小
     */
    public int getFullBlockSize() {
        return size + BlockTrailer.ENCODED_LENGTH;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BlockHandle that = (BlockHandle) o;

        if (size != that.size) {
            return false;
        }
        if (offset != that.offset) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(offset);
        result = 31 * result + size;
        return result;
    }

    @Override
    public String toString() {
        return "BlockHandle" + "{offset=" + offset + ", size=" + size + '}';
    }

    /**
     * 从输入流读取块句柄。
     *
     * @param sliceInput 内存切片输入流
     * @return 块句柄
     */
    public static BlockHandle readBlockHandle(MemorySliceInput sliceInput) {
        long offset = sliceInput.readVarLenLong();
        int size = sliceInput.readVarLenInt();
        return new BlockHandle(offset, size);
    }

    /**
     * 将块句柄写入内存切片。
     *
     * @param blockHandle 块句柄
     * @return 包含编码数据的内存切片
     */
    public static MemorySlice writeBlockHandle(BlockHandle blockHandle) {
        MemorySliceOutput sliceOutput = new MemorySliceOutput(MAX_ENCODED_LENGTH);
        writeBlockHandleTo(blockHandle, sliceOutput);
        return sliceOutput.toSlice();
    }

    /**
     * 将块句柄写入输出流。
     *
     * @param blockHandle 块句柄
     * @param sliceOutput 内存切片输出流
     */
    public static void writeBlockHandleTo(BlockHandle blockHandle, MemorySliceOutput sliceOutput) {
        sliceOutput.writeVarLenLong(blockHandle.offset);
        sliceOutput.writeVarLenInt(blockHandle.size);
    }
}
