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

/**
 * {@link BlockCompressor} 的工具类。
 *
 * <p>提供压缩和解压缩过程中使用的通用工具方法,包括:
 * <ul>
 *   <li>整数的小端序读写操作</li>
 *   <li>压缩块头部信息管理</li>
 *   <li>长度验证</li>
 * </ul>
 *
 * <p>压缩块格式:
 * <pre>
 * [头部8字节] [压缩数据]
 * 头部结构:
 *   前4字节: 压缩后的长度(小端序)
 *   后4字节: 原始数据长度(小端序)
 * </pre>
 */
public class CompressorUtils {
    /**
     * 头部长度。
     *
     * <p>我们在每个压缩块前放置两个整数,第一个整数表示块的压缩长度,
     * 第二个整数表示块的原始长度。
     */
    public static final int HEADER_LENGTH = 8;

    /**
     * 以小端序方式写入整数。
     *
     * @param i 要写入的整数
     * @param buf 目标缓冲区
     * @param offset 写入偏移量
     */
    public static void writeIntLE(int i, byte[] buf, int offset) {
        buf[offset++] = (byte) i;
        buf[offset++] = (byte) (i >>> 8);
        buf[offset++] = (byte) (i >>> 16);
        buf[offset] = (byte) (i >>> 24);
    }

    /**
     * 以小端序方式读取整数。
     *
     * @param buf 源缓冲区
     * @param i 读取偏移量
     * @return 读取的整数
     */
    public static int readIntLE(byte[] buf, int i) {
        return (buf[i] & 0xFF)
                | ((buf[i + 1] & 0xFF) << 8)
                | ((buf[i + 2] & 0xFF) << 16)
                | ((buf[i + 3] & 0xFF) << 24);
    }

    /**
     * 验证压缩长度和原始长度的有效性。
     *
     * <p>检查长度是否满足以下条件:
     * <ul>
     *   <li>长度不能为负数</li>
     *   <li>原始长度为0时,压缩长度必须为0</li>
     *   <li>原始长度不为0时,压缩长度不能为0</li>
     * </ul>
     *
     * @param compressedLen 压缩后的长度
     * @param originalLen 原始数据长度
     * @throws BufferDecompressionException 如果长度验证失败
     */
    public static void validateLength(int compressedLen, int originalLen)
            throws BufferDecompressionException {
        if (originalLen < 0
                || compressedLen < 0
                || (originalLen == 0 && compressedLen != 0)
                || (originalLen != 0 && compressedLen == 0)) {
            throw new BufferDecompressionException("Input is corrupted, invalid length.");
        }
    }
}
