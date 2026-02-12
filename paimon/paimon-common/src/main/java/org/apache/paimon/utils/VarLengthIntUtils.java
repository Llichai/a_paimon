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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 可变长度整数编解码工具类。
 *
 * <p>提供 int 和 long 类型的可变长度编码和解码功能，用于节省存储空间。
 *
 * <p>编码原理：
 * <ul>
 *   <li>使用 7 位存储数据，第 8 位作为继续标志
 *   <li>小数值使用更少的字节，大数值使用更多的字节
 *   <li>最多使用 9 字节编码 long，5 字节编码 int
 * </ul>
 *
 * <p>主要功能：
 * <ul>
 *   <li>Long 编码 - 将 long 值编码为可变长度字节
 *   <li>Long 解码 - 将可变长度字节解码为 long 值
 *   <li>Int 编码 - 将 int 值编码为可变长度字节
 *   <li>Int 解码 - 将可变长度字节解码为 int 值
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>数据压缩 - 减少整数的存储空间
 *   <li>序列化 - 高效的整数序列化
 *   <li>索引存储 - 紧凑的索引数据存储
 * </ul>
 *
 * <p>注意：此实现基于 PalDB 项目的 LongPacker，遵循 Apache License 2.0。
 *
 * @see DataInput
 * @see DataOutput
 */
public final class VarLengthIntUtils {

    /** Long 类型的最大可变长度大小（字节）。 */
    public static final int MAX_VAR_LONG_SIZE = 9;

    /** Int 类型的最大可变长度大小（字节）。 */
    public static final int MAX_VAR_INT_SIZE = 5;

    /**
     * 将 long 值编码到 DataOutput。
     *
     * <p>使用可变长度编码，小值使用更少的字节。
     *
     * @param os 输出流
     * @param value 要编码的 long 值（必须为非负数）
     * @return 编码的字节长度
     * @throws IOException 如果发生 I/O 错误
     * @throws IllegalArgumentException 如果值为负数
     */
    public static int encodeLong(DataOutput os, long value) throws IOException {

        if (value < 0) {
            throw new IllegalArgumentException("negative value: v=" + value);
        }

        int i = 1;
        while ((value & ~0x7FL) != 0) {
            os.write((((int) value & 0x7F) | 0x80));
            value >>>= 7;
            i++;
        }
        os.write((byte) value);
        return i;
    }

    /**
     * 将 long 值编码到字节数组。
     *
     * <p>使用可变长度编码，小值使用更少的字节。
     *
     * @param bytes 目标字节数组
     * @param value 要编码的 long 值（必须为非负数）
     * @return 编码的字节长度
     * @throws IllegalArgumentException 如果值为负数
     */
    public static int encodeLong(byte[] bytes, long value) {

        if (value < 0) {
            throw new IllegalArgumentException("negative value: v=" + value);
        }

        int i = 1;
        while ((value & ~0x7FL) != 0) {
            bytes[i - 1] = (byte) (((int) value & 0x7F) | 0x80);
            value >>>= 7;
            i++;
        }
        bytes[i - 1] = (byte) value;
        return i;
    }

    /**
     * 从 DataInput 解码 long 值。
     *
     * @param is 输入流
     * @return 解码的 long 值
     * @throws IOException 如果发生 I/O 错误或数据格式错误
     */
    public static long decodeLong(DataInput is) throws IOException {

        long result = 0;
        for (int offset = 0; offset < 64; offset += 7) {
            long b = is.readUnsignedByte();
            result |= (b & 0x7F) << offset;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new Error("Malformed long.");
    }

    /**
     * 从字节数组解码 long 值。
     *
     * @param ba 字节数组
     * @param index 起始索引
     * @return 解码的 long 值
     * @throws Error 如果数据格式错误
     */
    public static long decodeLong(byte[] ba, int index) {
        long result = 0;
        for (int offset = 0; offset < 64; offset += 7) {
            long b = ba[index++];
            result |= (b & 0x7F) << offset;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new Error("Malformed long.");
    }

    /**
     * 将 int 值编码到字节数组的指定偏移位置。
     *
     * <p>使用可变长度编码，小值使用更少的字节。
     *
     * @param bytes 目标字节数组
     * @param offset 起始偏移量
     * @param value 要编码的 int 值（必须为非负数）
     * @return 编码的字节长度
     * @throws IllegalArgumentException 如果值为负数
     */
    public static int encodeInt(byte[] bytes, int offset, int value) {

        if (value < 0) {
            throw new IllegalArgumentException("negative value: v=" + value);
        }

        int i = 1;
        while ((value & ~0x7F) != 0) {
            bytes[i + offset - 1] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
            i++;
        }
        bytes[i + offset - 1] = (byte) value;
        return i;
    }

    /**
     * 将 int 值编码到 DataOutput。
     *
     * <p>使用可变长度编码，小值使用更少的字节。
     *
     * @param os 输出流
     * @param value 要编码的 int 值（必须为非负数）
     * @return 编码的字节长度
     * @throws IOException 如果发生 I/O 错误
     * @throws IllegalArgumentException 如果值为负数
     */
    public static int encodeInt(DataOutput os, int value) throws IOException {

        if (value < 0) {
            throw new IllegalArgumentException("negative value: v=" + value);
        }

        int i = 1;
        while ((value & ~0x7F) != 0) {
            os.write(((value & 0x7F) | 0x80));
            value >>>= 7;
            i++;
        }

        os.write((byte) value);
        return i;
    }

    /**
     * 从 DataInput 解码 int 值。
     *
     * @param is 输入流
     * @return 解码的 int 值
     * @throws IOException 如果发生 I/O 错误或数据格式错误
     */
    public static int decodeInt(DataInput is) throws IOException {
        for (int offset = 0, result = 0; offset < 32; offset += 7) {
            int b = is.readUnsignedByte();
            result |= (b & 0x7F) << offset;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new Error("Malformed integer.");
    }
}
