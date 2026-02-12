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

package org.apache.paimon.sort.zorder;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Z-Order 字节工具类。
 *
 * <p>该工具类提供了将各种数据类型转换为字典序可比较字节表示的方法，以及比特位交错功能。
 * 这些方法是实现 Z-Order 索引的核心基础。
 *
 * <h2>核心功能</h2>
 * <ul>
 *   <li><b>有序字节转换</b>：将各种类型转换为字典序可比较的字节数组</li>
 *   <li><b>比特位交错</b>：将多列的字节按位交错组合成 Z-Order 索引</li>
 *   <li><b>字节填充/截断</b>：处理变长类型使其符合固定长度要求</li>
 *   <li><b>缓冲区重用</b>：提供 ByteBuffer 重用机制减少内存分配</li>
 * </ul>
 *
 * <h2>字节转换原理</h2>
 *
 * <h3>1. 有符号整数转换</h3>
 * <p>有符号整数的字节表示不满足字典序，因为符号位导致负数的字节值大于正数。
 * 解决方法是翻转符号位，使得：
 * <pre>
 * 原始范围: [MIN_VALUE ... -1, 0 ... MAX_VALUE]
 * 翻转后:   [0 ... MAX_VALUE, MIN_VALUE ... -1]
 * 结果:     负数 < 正数，且大小关系保持
 * </pre>
 *
 * <h3>2. 浮点数转换</h3>
 * <p>根据 IEEE 754 标准："如果两个浮点数以相同格式排序（如 x < y），
 * 当它们的位被重新解释为符号-幅度整数时，它们的顺序保持不变。"
 * <p>转换步骤：
 * <ol>
 *   <li>将浮点数转换为 long 类型的位表示</li>
 *   <li>对符号位和符号-幅度位进行异或操作</li>
 *   <li>结果满足字典序可比性</li>
 * </ol>
 *
 * <h3>3. 字符串转换</h3>
 * <p>字符串本身是字典序可比的，但不同长度会破坏 Z-Order 的要求（每列必须贡献相同字节数）。
 * 解决方法：
 * <ul>
 *   <li>设置固定长度（如 8 字节）</li>
 *   <li>短字符串：右侧填充 0x00</li>
 *   <li>长字符串：截断到固定长度</li>
 * </ul>
 *
 * <h2>比特位交错原理</h2>
 * <p>Z-Order 通过交错多列的比特位来生成一维索引。算法流程：
 * <pre>
 * 输入：columnsBinary = [[col1_bytes], [col2_bytes], ...]
 * 过程：
 *   1. 从每列的最高位开始
 *   2. 依次取各列的当前位，写入输出
 *   3. 移动到下一位，重复直到所有位处理完
 * 输出：交错后的字节数组
 *
 * 示例（2列，每列2位）：
 *   col1 = [1,0]  (二进制)
 *   col2 = [1,1]  (二进制)
 *   交错结果 = [1,1,0,1] (按 col1[0], col2[0], col1[1], col2[1] 顺序)
 * </pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 整数转有序字节
 * ByteBuffer buffer = ByteBuffer.allocate(8);
 * ByteBuffer result = ZOrderByteUtils.intToOrderedBytes(42, buffer);
 *
 * // 2. 浮点数转有序字节
 * result = ZOrderByteUtils.doubleToOrderedBytes(3.14, buffer);
 *
 * // 3. 字符串转固定长度字节
 * result = ZOrderByteUtils.stringToOrderedBytes("hello", 8, buffer);
 *
 * // 4. 字节填充/截断
 * byte[] data = new byte[]{1, 2, 3};
 * result = ZOrderByteUtils.byteTruncateOrFill(data, 8, buffer);
 *
 * // 5. 比特位交错
 * byte[][] columns = {
 *     {0x12, 0x34},  // 列1
 *     {0x56, 0x78}   // 列2
 * };
 * byte[] zorder = ZOrderByteUtils.interleaveBits(columns, 4);
 * }</pre>
 *
 * <h2>性能优化</h2>
 * <ul>
 *   <li><b>缓冲区重用</b>：所有方法支持传入 ByteBuffer 重用</li>
 *   <li><b>ThreadLocal</b>：字符串编码器使用 ThreadLocal 避免重复创建</li>
 *   <li><b>原地操作</b>：尽可能在提供的缓冲区上原地操作</li>
 *   <li><b>固定长度</b>：原始类型固定使用 8 字节，便于优化</li>
 * </ul>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li>所有字节比较应按无符号字节、大端序进行</li>
 *   <li>NULL 值统一表示为全 0 字节</li>
 *   <li>变长类型的截断可能导致信息损失</li>
 *   <li>比特交错要求每列贡献相同字节数</li>
 * </ul>
 *
 * <p>本文件基于 Iceberg 项目的源代码（http://iceberg.apache.org/），
 * 由 Apache Software Foundation (ASF) 根据 Apache License 2.0 授权。
 * 详见随本作品分发的 NOTICE 文件以获取其他版权所有权信息。
 *
 * <p>大部分技术来源于：
 * https://aws.amazon.com/blogs/database/z-order-indexing-for-multifaceted-queries-in-amazon-dynamodb-part-2/
 *
 * @see ZIndexer
 */
public class ZOrderByteUtils {

    /** 原始类型的缓冲区大小（8字节）。 */
    public static final int PRIMITIVE_BUFFER_SIZE = 8;

    /** NULL 值的字节表示（8个0字节）。 */
    public static final byte[] NULL_BYTES = new byte[PRIMITIVE_BUFFER_SIZE];

    /** 线程本地的字符集编码器，用于字符串转字节。 */
    private static final ThreadLocal<CharsetEncoder> ENCODER = new ThreadLocal<>();

    static {
        Arrays.fill(NULL_BYTES, (byte) 0x00);
    }

    private ZOrderByteUtils() {}

    static ByteBuffer allocatePrimitiveBuffer() {
        return ByteBuffer.allocate(PRIMITIVE_BUFFER_SIZE);
    }

    /**
     * 将有符号整数转换为有序字节表示。
     *
     * <p>有符号整数的字节不是按幅度顺序排列的，因为符号位的存在。
     * 为了修正这个问题，翻转符号位使所有负数排在正数之前。
     * 这本质上是移动了 0 值的位置，避免在跨越新 0 值时破坏排序。
     *
     * @param val 要转换的整数值
     * @param reuse 重用的 ByteBuffer
     * @return 包含有序字节的 ByteBuffer
     */
    public static ByteBuffer intToOrderedBytes(int val, ByteBuffer reuse) {
        ByteBuffer bytes = reuse(reuse, PRIMITIVE_BUFFER_SIZE);
        bytes.putLong(((long) val) ^ 0x8000000000000000L);
        return bytes;
    }

    /**
     * 将有符号长整数转换为有序字节表示。
     *
     * <p>处理方式与 {@link #intToOrderedBytes(int, ByteBuffer)} 相同。
     *
     * @param val 要转换的长整数值
     * @param reuse 重用的 ByteBuffer
     * @return 包含有序字节的 ByteBuffer
     */
    public static ByteBuffer longToOrderedBytes(long val, ByteBuffer reuse) {
        ByteBuffer bytes = reuse(reuse, PRIMITIVE_BUFFER_SIZE);
        bytes.putLong(val ^ 0x8000000000000000L);
        return bytes;
    }

    /**
     * Signed shorts are treated the same as the signed ints in {@link #intToOrderedBytes(int,
     * ByteBuffer)}.
     */
    public static ByteBuffer shortToOrderedBytes(short val, ByteBuffer reuse) {
        ByteBuffer bytes = reuse(reuse, PRIMITIVE_BUFFER_SIZE);
        bytes.putLong(((long) val) ^ 0x8000000000000000L);
        return bytes;
    }

    /**
     * Signed tiny ints are treated the same as the signed ints in {@link #intToOrderedBytes(int,
     * ByteBuffer)}.
     */
    public static ByteBuffer tinyintToOrderedBytes(byte val, ByteBuffer reuse) {
        ByteBuffer bytes = reuse(reuse, PRIMITIVE_BUFFER_SIZE);
        bytes.putLong(((long) val) ^ 0x8000000000000000L);
        return bytes;
    }

    /**
     * IEEE 754 : “If two floating-point numbers in the same format are ordered (say, x {@literal <}
     * y), they are ordered the same way when their bits are reinterpreted as sign-magnitude
     * integers.”
     *
     * <p>Which means floats can be treated as sign magnitude integers which can then be converted
     * into lexicographically comparable bytes.
     */
    public static ByteBuffer floatToOrderedBytes(float val, ByteBuffer reuse) {
        ByteBuffer bytes = reuse(reuse, PRIMITIVE_BUFFER_SIZE);
        long lval = Double.doubleToLongBits(val);
        lval ^= ((lval >> (Integer.SIZE - 1)) | Long.MIN_VALUE);
        bytes.putLong(lval);
        return bytes;
    }

    /**
     * Doubles are treated the same as floats in {@link #floatToOrderedBytes(float, ByteBuffer)}.
     */
    public static ByteBuffer doubleToOrderedBytes(double val, ByteBuffer reuse) {
        ByteBuffer bytes = reuse(reuse, PRIMITIVE_BUFFER_SIZE);
        long lval = Double.doubleToLongBits(val);
        lval ^= ((lval >> (Integer.SIZE - 1)) | Long.MIN_VALUE);
        bytes.putLong(lval);
        return bytes;
    }

    /**
     * Strings are lexicographically sortable BUT if different byte array lengths will ruin the
     * Z-Ordering. (ZOrder requires that a given column contribute the same number of bytes every
     * time). This implementation just uses a set size to for all output byte representations.
     * Truncating longer strings and right padding 0 for shorter strings.
     */
    @SuppressWarnings("ByteBufferBackingArray")
    public static ByteBuffer stringToOrderedBytes(String val, int length, ByteBuffer reuse) {
        CharsetEncoder encoder = ENCODER.get();
        if (encoder == null) {
            encoder = StandardCharsets.UTF_8.newEncoder();
            ENCODER.set(encoder);
        }

        ByteBuffer bytes = reuse(reuse, length);
        Arrays.fill(bytes.array(), 0, length, (byte) 0x00);
        if (val != null) {
            CharBuffer inputBuffer = CharBuffer.wrap(val);
            encoder.encode(inputBuffer, bytes, true);
        }
        return bytes;
    }

    /**
     * Return a bytebuffer with the given bytes truncated to length, or filled with 0's to length
     * depending on whether the given bytes are larger or smaller than the given length.
     */
    @SuppressWarnings("ByteBufferBackingArray")
    public static ByteBuffer byteTruncateOrFill(byte[] val, int length, ByteBuffer reuse) {
        ByteBuffer bytes = reuse(reuse, length);
        if (val.length < length) {
            bytes.put(val, 0, val.length);
            Arrays.fill(bytes.array(), val.length, length, (byte) 0x00);
        } else {
            bytes.put(val, 0, length);
        }
        return bytes;
    }

    public static byte[] interleaveBits(byte[][] columnsBinary, int interleavedSize) {
        return interleaveBits(columnsBinary, interleavedSize, ByteBuffer.allocate(interleavedSize));
    }

    /**
     * Interleave bits using a naive loop. Variable length inputs are allowed but to get a
     * consistent ordering it is required that every column contribute the same number of bytes in
     * each invocation. Bits are interleaved from all columns that have a bit available at that
     * position. Once a Column has no more bits to produce it is skipped in the interleaving.
     *
     * @param columnsBinary an array of ordered byte representations of the columns being ZOrdered
     * @param interleavedSize the number of bytes to use in the output
     * @return the columnbytes interleaved
     */
    // NarrowingCompoundAssignment is intended here. See
    // https://github.com/apache/iceberg/pull/5200#issuecomment-1176226163
    @SuppressWarnings({"ByteBufferBackingArray", "NarrowingCompoundAssignment"})
    public static byte[] interleaveBits(
            byte[][] columnsBinary, int interleavedSize, ByteBuffer reuse) {
        byte[] interleavedBytes = reuse.array();
        Arrays.fill(interleavedBytes, 0, interleavedSize, (byte) 0x00);

        int sourceColumn = 0;
        int sourceByte = 0;
        int sourceBit = 7;
        int interleaveByte = 0;
        int interleaveBit = 7;

        while (interleaveByte < interleavedSize) {
            // Take the source bit from source byte and move it to the output bit position
            interleavedBytes[interleaveByte] |=
                    (columnsBinary[sourceColumn][sourceByte] & 1 << sourceBit)
                            >>> sourceBit
                            << interleaveBit;
            --interleaveBit;

            // Check if an output byte has been completed
            if (interleaveBit == -1) {
                // Move to the next output byte
                interleaveByte++;
                // Move to the highest order bit of the new output byte
                interleaveBit = 7;
            }

            // Check if the last output byte has been completed
            if (interleaveByte == interleavedSize) {
                break;
            }

            // Find the next source bit to interleave
            do {
                // Move to next column
                ++sourceColumn;
                if (sourceColumn == columnsBinary.length) {
                    // If the last source column was used, reset to next bit of first column
                    sourceColumn = 0;
                    --sourceBit;
                    if (sourceBit == -1) {
                        // If the last bit of the source byte was used, reset to the highest bit of
                        // the next
                        // byte
                        sourceByte++;
                        sourceBit = 7;
                    }
                }
            } while (columnsBinary[sourceColumn].length <= sourceByte);
        }
        return interleavedBytes;
    }

    public static ByteBuffer reuse(ByteBuffer reuse, int length) {
        reuse.position(0);
        reuse.limit(length);
        return reuse;
    }
}
