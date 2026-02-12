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

import org.apache.paimon.compression.BlockCompressionType;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.memory.MemorySliceOutput;

import static java.util.Objects.requireNonNull;

/**
 * 块尾部信息。
 *
 * <p>包含块的压缩类型和 CRC32C 校验码,用于数据完整性验证和解压缩。
 */
public class BlockTrailer {

    /** 编码长度(字节) */
    public static final int ENCODED_LENGTH = 5;

    /** 压缩类型 */
    private final BlockCompressionType compressionType;

    /** CRC32C 校验码 */
    private final int crc32c;

    /**
     * 构造块尾部。
     *
     * @param compressionType 压缩类型
     * @param crc32c CRC32C 校验码
     */
    public BlockTrailer(BlockCompressionType compressionType, int crc32c) {
        requireNonNull(compressionType, "compressionType is null");

        this.compressionType = compressionType;
        this.crc32c = crc32c;
    }

    /** 返回压缩类型。 */
    public BlockCompressionType getCompressionType() {
        return compressionType;
    }

    /** 返回 CRC32C 校验码。 */
    public int getCrc32c() {
        return crc32c;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BlockTrailer that = (BlockTrailer) o;
        if (crc32c != that.crc32c) {
            return false;
        }
        return compressionType == that.compressionType;
    }

    @Override
    public int hashCode() {
        int result = compressionType.hashCode();
        result = 31 * result + crc32c;
        return result;
    }

    @Override
    public String toString() {
        return "BlockTrailer"
                + "{compressionType="
                + compressionType
                + ", crc32c=0x"
                + Integer.toHexString(crc32c)
                + '}';
    }

    /**
     * 从输入流读取块尾部。
     *
     * @param input 输入流
     * @return 块尾部
     */
    public static BlockTrailer readBlockTrailer(MemorySliceInput input) {
        BlockCompressionType compressionType =
                BlockCompressionType.getCompressionTypeByPersistentId(input.readUnsignedByte());
        int crc32c = input.readInt();
        return new BlockTrailer(compressionType, crc32c);
    }

    /**
     * 将块尾部写入内存切片。
     *
     * @param blockTrailer 块尾部
     * @return 内存切片
     */
    public static MemorySlice writeBlockTrailer(BlockTrailer blockTrailer) {
        MemorySliceOutput output = new MemorySliceOutput(ENCODED_LENGTH);
        writeBlockTrailer(blockTrailer, output);
        return output.toSlice();
    }

    /**
     * 将块尾部写入输出流。
     *
     * @param blockTrailer 块尾部
     * @param sliceOutput 输出流
     */
    public static void writeBlockTrailer(BlockTrailer blockTrailer, MemorySliceOutput sliceOutput) {
        sliceOutput.writeByte(blockTrailer.getCompressionType().persistentId());
        sliceOutput.writeInt(blockTrailer.getCrc32c());
    }
}
