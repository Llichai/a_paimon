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

import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.ZstdOutputStream;

import java.io.IOException;
import java.io.OutputStream;

import static org.apache.paimon.compression.CompressorUtils.HEADER_LENGTH;

/**
 * Zstd 块压缩器。
 *
 * <p>使用 Zstandard 算法压缩数据块。Zstd 提供高压缩率和可配置的压缩级别。
 *
 * <p>实现细节:
 * <ul>
 *   <li>使用 zstd-jni 库的双参数构造函数避免冲突</li>
 *   <li>支持自定义压缩级别</li>
 *   <li>使用单线程压缩(workers=0)以保证性能可预测</li>
 *   <li>通过自定义 OutputStream 避免额外的数据复制</li>
 * </ul>
 */
public class ZstdBlockCompressor implements BlockCompressor {

    /** 最大块大小,用于计算压缩后的最大可能大小 */
    private static final int MAX_BLOCK_SIZE = 128 * 1024;

    /** 压缩级别 */
    private final int level;

    /**
     * 创建 Zstd 块压缩器。
     *
     * @param level 压缩级别,范围1-22。推荐值为1-3
     */
    public ZstdBlockCompressor(int level) {
        this.level = level;
    }

    @Override
    public int getMaxCompressedSize(int srcSize) {
        return HEADER_LENGTH + zstdMaxCompressedLength(srcSize);
    }

    /**
     * 计算 Zstd 压缩后的最大可能长度。
     *
     * <p>参考 io.airlift.compress.zstd.ZstdCompressor 的实现。
     *
     * @param uncompressedSize 未压缩数据的大小
     * @return 压缩后的最大可能大小
     */
    private int zstdMaxCompressedLength(int uncompressedSize) {
        // refer to io.airlift.compress.zstd.ZstdCompressor
        int result = uncompressedSize + (uncompressedSize >>> 8);
        if (uncompressedSize < MAX_BLOCK_SIZE) {
            result += (MAX_BLOCK_SIZE - uncompressedSize) >>> 11;
        }
        return result;
    }

    @Override
    public int compress(byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff)
            throws BufferCompressionException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream(dst, dstOff);
        // 使用双参数构造函数避免 zstd-jni 冲突
        try (ZstdOutputStream zstdStream =
                new ZstdOutputStream(stream, RecyclingBufferPool.INSTANCE)) {
            zstdStream.setLevel(level);
            zstdStream.setWorkers(0); // 使用单线程压缩
            zstdStream.write(src, srcOff, srcLen);
        } catch (IOException e) {
            throw new BufferCompressionException(e);
        }
        return stream.position() - dstOff;
    }

    /**
     * 自定义字节数组输出流。
     *
     * <p>直接写入到提供的字节数组,避免额外的缓冲区复制。
     */
    private static class ByteArrayOutputStream extends OutputStream {

        /** 目标字节数组 */
        private final byte[] buf;
        /** 当前写入位置 */
        private int position;

        /**
         * 创建字节数组输出流。
         *
         * @param buf 目标字节数组
         * @param position 起始写入位置
         */
        public ByteArrayOutputStream(byte[] buf, int position) {
            this.buf = buf;
            this.position = position;
        }

        @Override
        public void write(int b) {
            buf[position] = (byte) b;
            position += 1;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (b == null || buf == null) {
                throw new NullPointerException("Input array or buffer is null");
            }
            if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) - b.length > 0)) {
                throw new IndexOutOfBoundsException("Invalid offset or length");
            }
            if (b.length == 0) {
                return;
            }
            try {
                System.arraycopy(b, off, buf, position, len);
            } catch (IndexOutOfBoundsException e) {
                throw new IOException(e);
            }
            position += len;
        }

        /**
         * 获取当前写入位置。
         *
         * @return 当前位置
         */
        int position() {
            return position;
        }
    }
}
