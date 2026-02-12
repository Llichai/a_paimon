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

package org.apache.paimon.compression;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import static org.apache.paimon.compression.CompressorUtils.HEADER_LENGTH;
import static org.apache.paimon.compression.CompressorUtils.writeIntLE;

/**
 * LZ4 块压缩器。
 *
 * <p>将数据编码为 LZ4 格式(不兼容 LZ4 Frame 格式)。它从外部提供的字节数组读取和写入数据,
 * 从而减少了复制时间。
 *
 * <p>该类从 {@link net.jpountz.lz4.LZ4BlockOutputStream} 复制并修改而来。
 *
 * <p>特点:
 * <ul>
 *   <li>使用 LZ4 快速压缩模式</li>
 *   <li>在数据前添加8字节头部(压缩长度和原始长度)</li>
 *   <li>零拷贝设计,直接使用外部缓冲区</li>
 * </ul>
 */
public class Lz4BlockCompressor implements BlockCompressor {

    /** LZ4 压缩器实例 */
    private final LZ4Compressor compressor;

    /**
     * 创建 LZ4 块压缩器。
     *
     * <p>使用 LZ4 最快的压缩模式。
     */
    public Lz4BlockCompressor() {
        this.compressor = LZ4Factory.fastestInstance().fastCompressor();
    }

    @Override
    public int getMaxCompressedSize(int srcSize) {
        return HEADER_LENGTH + compressor.maxCompressedLength(srcSize);
    }

    @Override
    public int compress(byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff)
            throws BufferCompressionException {
        try {
            int compressedLength =
                    compressor.compress(src, srcOff, srcLen, dst, dstOff + HEADER_LENGTH);
            writeIntLE(compressedLength, dst, dstOff);
            writeIntLE(srcLen, dst, dstOff + 4);
            return HEADER_LENGTH + compressedLength;
        } catch (Exception e) {
            throw new BufferCompressionException(e);
        }
    }
}
