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

import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

import static org.apache.paimon.compression.CompressorUtils.HEADER_LENGTH;
import static org.apache.paimon.compression.CompressorUtils.readIntLE;
import static org.apache.paimon.compression.CompressorUtils.validateLength;

/**
 * LZ4 块解压缩器。
 *
 * <p>解码由 {@link Lz4BlockCompressor} 写入的数据。它从外部提供的字节数组读取和写入数据,
 * 从而减少了复制时间。
 *
 * <p>该类从 {@link net.jpountz.lz4.LZ4BlockInputStream} 复制并修改而来。
 *
 * <p>特点:
 * <ul>
 *   <li>使用 LZ4 安全解压缩模式,防止缓冲区溢出</li>
 *   <li>读取并验证8字节头部信息</li>
 *   <li>验证解压缩后的数据长度</li>
 *   <li>零拷贝设计,直接使用外部缓冲区</li>
 * </ul>
 */
public class Lz4BlockDecompressor implements BlockDecompressor {

    /** LZ4 安全解压缩器实例 */
    private final LZ4SafeDecompressor decompressor;

    /**
     * 创建 LZ4 块解压缩器。
     *
     * <p>使用 LZ4 最快的安全解压缩模式。
     */
    public Lz4BlockDecompressor() {
        this.decompressor = LZ4Factory.fastestInstance().safeDecompressor();
    }

    @Override
    public int decompress(byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff)
            throws BufferDecompressionException {
        final int compressedLen = readIntLE(src, srcOff);
        final int originalLen = readIntLE(src, srcOff + 4);
        validateLength(compressedLen, originalLen);

        if (dst.length - dstOff < originalLen) {
            throw new BufferDecompressionException("Buffer length too small");
        }

        if (src.length - srcOff - HEADER_LENGTH < compressedLen) {
            throw new BufferDecompressionException(
                    "Source data is not integral for decompression.");
        }

        try {
            final int originalLenByLz4 =
                    decompressor.decompress(
                            src, srcOff + HEADER_LENGTH, compressedLen, dst, dstOff);
            if (originalLen != originalLenByLz4) {
                throw new BufferDecompressionException("Input is corrupted");
            }
        } catch (LZ4Exception e) {
            throw new BufferDecompressionException("Input is corrupted", e);
        }

        return originalLen;
    }
}
