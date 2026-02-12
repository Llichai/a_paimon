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

package org.apache.paimon.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * Delta 编码和 Varints 编码的组合压缩器。
 *
 * <p>适用于递增或差异不大的整数序列压缩,通过以下步骤实现高效压缩:
 * <ol>
 *   <li>Delta 编码 - 存储相邻元素之间的差值而非原始值
 *   <li>ZigZag 变换 - 将有符号整数映射为无符号整数,优化负数编码
 *   <li>Varints 编码 - 使用变长编码,小数字占用更少字节
 * </ol>
 *
 * <p>特别适合以下场景:
 * <ul>
 *   <li>单调递增的序列(如时间戳、ID序列)
 *   <li>相邻值差异较小的序列
 *   <li>需要节省存储空间的整数数组
 * </ul>
 */
public class DeltaVarintCompressor {

    /**
     * 压缩长整型数组。
     *
     * <p>使用 Delta 编码、ZigZag 变换和 Varints 编码对数组进行压缩。
     * 压缩过程:
     * <ol>
     *   <li>保存第一个元素
     *   <li>计算相邻元素的差值(Delta)
     *   <li>对每个差值应用 ZigZag 和 Varints 编码
     * </ol>
     *
     * @param data 待压缩的长整型数组
     * @return 压缩后的字节数组
     */
    public static byte[] compress(long[] data) {
        if (data == null || data.length == 0) {
            return new byte[0];
        }

        LongArrayList deltas = new LongArrayList(data.length);
        // Store the first element
        deltas.add(data[0]);
        for (int i = 1; i < data.length; i++) {
            // Compute delta
            deltas.add(data[i] - data[i - 1]);
        }

        // Pre-allocate space
        ByteArrayOutputStream out = new ByteArrayOutputStream(data.length * 10);
        for (int i = 0; i < deltas.size(); i++) {
            // Apply ZigZag and Varints
            encodeVarint(deltas.get(i), out);
        }
        return out.toByteArray();
    }

    /**
     * 解压缩字节数组,恢复原始长整型数组。
     *
     * <p>解压缩过程:
     * <ol>
     *   <li>解码 Varints 并反向 ZigZag 变换,获得差值序列
     *   <li>恢复第一个元素
     *   <li>通过累加差值重建原始序列
     * </ol>
     *
     * @param compressed 压缩后的字节数组
     * @return 解压后的长整型数组
     */
    public static long[] decompress(byte[] compressed) {
        if (compressed == null || compressed.length == 0) {
            return new long[0];
        }

        ByteArrayInputStream in = new ByteArrayInputStream(compressed);
        // Pre-allocate space
        LongArrayList deltas = new LongArrayList(compressed.length);
        while (in.available() > 0) {
            // Decode Varints and reverse ZigZag
            deltas.add(decodeVarint(in));
        }

        long[] result = new long[deltas.size()];
        // Restore the first element
        result[0] = deltas.get(0);
        for (int i = 1; i < result.length; i++) {
            // Reconstruct using deltas
            result[i] = result[i - 1] + deltas.get(i);
        }
        return result;
    }

    /**
     * 使用 ZigZag 和 Varints 编码长整型值。
     *
     * <p>ZigZag 变换将有符号数映射为无符号数:
     * <ul>
     *   <li>0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4, ...</li>
     *   <li>使负数也能高效编码</li>
     * </ul>
     *
     * <p>Varints 编码使用最高位(MSB)作为继续标志:
     * <ul>
     *   <li>MSB=1 表示后续还有字节</li>
     *   <li>MSB=0 表示这是最后一个字节</li>
     *   <li>每个字节存储7位数据</li>
     * </ul>
     *
     * @param value 待编码的值
     * @param out 输出流
     */
    private static void encodeVarint(long value, ByteArrayOutputStream out) {
        // ZigZag transformation for long
        long tmp = (value << 1) ^ (value >> 63);
        // Check if multiple bytes are needed
        while ((tmp & ~0x7FL) != 0) {
            // Set MSB to 1 (continuation)
            out.write(((int) tmp & 0x7F) | 0x80);
            // Unsigned right shift
            tmp >>>= 7;
        }
        // Final byte with MSB set to 0
        out.write((byte) tmp);
    }

    /**
     * 解码 Varints 编码的值并反向 ZigZag 变换。
     *
     * <p>解码过程:
     * <ol>
     *   <li>读取字节,每个字节提取7位数据</li>
     *   <li>检查 MSB,如果为1则继续读取下一字节</li>
     *   <li>如果 MSB 为0,表示编码结束</li>
     *   <li>反向 ZigZag 变换恢复有符号数</li>
     * </ol>
     *
     * @param in 输入流
     * @return 解码后的值
     * @throws RuntimeException 如果输入提前结束或发生 Varint 溢出
     */
    private static long decodeVarint(ByteArrayInputStream in) {
        long result = 0;
        int shift = 0;
        while (true) {
            long b = in.read();
            if (b == -1) {
                throw new RuntimeException("Unexpected end of input");
            }
            // Extract 7 bits
            result |= (b & 0x7F) << shift;
            // MSB is 0, end of encoding
            if ((b & 0x80) == 0) {
                break;
            }
            shift += 7;
            if (shift > 63) {
                throw new RuntimeException("Varint overflow");
            }
        }
        // Reverse ZigZag transformation
        long zigzag = result >>> 1;
        return (result & 1) == 0 ? zigzag : (~zigzag);
    }
}
