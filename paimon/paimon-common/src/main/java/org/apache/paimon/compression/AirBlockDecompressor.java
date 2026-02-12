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

import io.airlift.compress.Decompressor;
import io.airlift.compress.MalformedInputException;

import static org.apache.paimon.compression.CompressorUtils.HEADER_LENGTH;
import static org.apache.paimon.compression.CompressorUtils.readIntLE;
import static org.apache.paimon.compression.CompressorUtils.validateLength;

/**
 * Airlift 解压缩器的 {@link BlockDecompressor} 实现。
 *
 * <p>该类将 Airlift 解压缩器适配到 Paimon 的块解压缩接口,负责:
 * <ul>
 *   <li>读取并验证压缩块头部信息</li>
 *   <li>检查缓冲区大小是否足够</li>
 *   <li>调用 Airlift 解压缩器执行实际解压缩</li>
 *   <li>验证解压缩结果的正确性</li>
 * </ul>
 */
public class AirBlockDecompressor implements BlockDecompressor {

    /** Airlift 内部解压缩器 */
    private final Decompressor internalDecompressor;

    /**
     * 创建 Airlift 块解压缩器。
     *
     * @param internalDecompressor Airlift 解压缩器实例
     */
    public AirBlockDecompressor(Decompressor internalDecompressor) {
        this.internalDecompressor = internalDecompressor;
    }

    @Override
    public int decompress(byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff)
            throws BufferDecompressionException {
        int compressedLen = readIntLE(src, srcOff);
        int originalLen = readIntLE(src, srcOff + 4);
        validateLength(compressedLen, originalLen);

        if (dst.length - dstOff < originalLen) {
            throw new BufferDecompressionException("Buffer length too small");
        }

        if (src.length - srcOff - HEADER_LENGTH < compressedLen) {
            throw new BufferDecompressionException(
                    "Source data is not integral for decompression.");
        }

        try {
            final int decompressedLen =
                    internalDecompressor.decompress(
                            src, srcOff + HEADER_LENGTH, compressedLen, dst, dstOff, originalLen);
            if (originalLen != decompressedLen) {
                throw new BufferDecompressionException("Input is corrupted");
            }
        } catch (MalformedInputException e) {
            throw new BufferDecompressionException("Input is corrupted", e);
        }

        return originalLen;
    }
}
