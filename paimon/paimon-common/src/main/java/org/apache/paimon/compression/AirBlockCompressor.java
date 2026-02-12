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

import io.airlift.compress.Compressor;

import static org.apache.paimon.compression.CompressorUtils.HEADER_LENGTH;
import static org.apache.paimon.compression.CompressorUtils.writeIntLE;

/**
 * Airlift 压缩器的 {@link BlockCompressor} 实现。
 *
 * <p>该类将 Airlift 压缩器适配到 Paimon 的块压缩接口,负责:
 * <ul>
 *   <li>添加压缩块头部信息(压缩长度和原始长度)</li>
 *   <li>调用 Airlift 压缩器执行实际压缩</li>
 *   <li>处理压缩异常</li>
 * </ul>
 */
public class AirBlockCompressor implements BlockCompressor {

    /** Airlift 内部压缩器 */
    private final Compressor internalCompressor;

    /**
     * 创建 Airlift 块压缩器。
     *
     * @param internalCompressor Airlift 压缩器实例
     */
    public AirBlockCompressor(Compressor internalCompressor) {
        this.internalCompressor = internalCompressor;
    }

    @Override
    public int getMaxCompressedSize(int srcSize) {
        return HEADER_LENGTH + internalCompressor.maxCompressedLength(srcSize);
    }

    @Override
    public int compress(byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff)
            throws BufferCompressionException {
        try {
            if (dst.length < dstOff + getMaxCompressedSize(srcLen)) {
                throw new ArrayIndexOutOfBoundsException();
            }

            int compressedLength =
                    internalCompressor.compress(
                            src,
                            srcOff,
                            srcLen,
                            dst,
                            dstOff + HEADER_LENGTH,
                            internalCompressor.maxCompressedLength(srcLen));
            writeIntLE(compressedLength, dst, dstOff);
            writeIntLE(srcLen, dst, dstOff + 4);
            return HEADER_LENGTH + compressedLength;
        } catch (Exception e) {
            throw new BufferCompressionException(e);
        }
    }
}
