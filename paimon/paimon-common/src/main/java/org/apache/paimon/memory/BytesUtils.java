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

package org.apache.paimon.memory;

import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.Variant;

import static org.apache.paimon.data.BinarySection.HIGHEST_FIRST_BIT;
import static org.apache.paimon.data.BinarySection.HIGHEST_SECOND_TO_EIGHTH_BIT;
import static org.apache.paimon.memory.MemorySegment.LITTLE_ENDIAN;

/**
 * 字节数组工具类。
 *
 * <p>提供从 byte 数组中读取基本类型数据的工具方法。
 *
 * <h2>主要功能</h2>
 *
 * <ul>
 *   <li>从 byte 数组读取 int、short、long 等基本类型
 *   <li>读取变长二进制数据(支持内联存储优化)
 *   <li>读取 Variant 数据类型
 * </ul>
 *
 * <h2>字节序</h2>
 *
 * <p>所有方法使用小端字节序进行数据读取。
 *
 * <h2>性能优化</h2>
 *
 * <p>对于小于 8 字节的二进制数据,支持内联存储在 long 值中,减少间接访问开销。
 */
public class BytesUtils {

    /**
     * 从字节数组读取 int 值(小端字节序)。
     *
     * @param bytes 源字节数组
     * @param offset 起始偏移量
     * @return int 值
     */
    public static int getInt(byte[] bytes, int offset) {
        return (bytes[offset + 3] << 24)
                | ((bytes[offset + 2] & 0xff) << 16)
                | ((bytes[offset + 1] & 0xff) << 8)
                | (bytes[offset] & 0xff);
    }

    /**
     * 从字节数组读取 short 值(小端字节序)。
     *
     * @param bytes 源字节数组
     * @param offset 起始偏移量
     * @return short 值
     */
    public static short getShort(byte[] bytes, int offset) {
        return (short) ((bytes[offset + 1] << 8) | (bytes[offset] & 0xff));
    }

    /**
     * 从字节数组读取 long 值(小端字节序)。
     *
     * @param bytes 源字节数组
     * @param offset 起始偏移量
     * @return long 值
     */
    public static long getLong(byte[] bytes, int offset) {
        return ((long) bytes[offset + 7] << 56)
                | (((long) bytes[offset + 6] & 0xff) << 48)
                | (((long) bytes[offset + 5] & 0xff) << 40)
                | (((long) bytes[offset + 4] & 0xff) << 32)
                | (((long) bytes[offset + 3] & 0xff) << 24)
                | (((long) bytes[offset + 2] & 0xff) << 16)
                | (((long) bytes[offset + 1] & 0xff) << 8)
                | ((long) bytes[offset] & 0xff);
    }

    /**
     * 读取二进制数据。
     *
     * <p>支持两种存储模式:
     *
     * <ul>
     *   <li>内联模式:长度小于 8 字节时,数据直接存储在 variablePartOffsetAndLen 中
     *   <li>引用模式:长度大于等于 8 字节时,variablePartOffsetAndLen 存储偏移量和长度
     * </ul>
     *
     * @param bytes 源字节数组
     * @param baseOffset 基础偏移量
     * @param fieldOffset 字段起始偏移量
     * @param variablePartOffsetAndLen 偏移量和长度的组合值,或内联数据
     * @return 读取的字节数组
     */
    public static byte[] readBinary(
            byte[] bytes, int baseOffset, int fieldOffset, long variablePartOffsetAndLen) {
        long mark = variablePartOffsetAndLen & HIGHEST_FIRST_BIT;
        if (mark == 0) {
            final int subOffset = (int) (variablePartOffsetAndLen >> 32);
            final int len = (int) variablePartOffsetAndLen;
            byte[] ret = new byte[len];
            System.arraycopy(bytes, baseOffset + subOffset, ret, 0, len);
            return ret;
        } else {
            int len = (int) ((variablePartOffsetAndLen & HIGHEST_SECOND_TO_EIGHTH_BIT) >>> 56);
            byte[] ret = new byte[len];
            if (LITTLE_ENDIAN) {
                System.arraycopy(bytes, fieldOffset, ret, 0, len);
            } else {
                System.arraycopy(bytes, fieldOffset + 1, ret, 0, len);
            }
            return ret;
        }
    }

    /**
     * 从字节数组读取 Variant 数据。
     *
     * <p>Variant 数据由值部分和元数据部分组成,存储格式为:[值大小(4字节)][值数据][元数据]。
     *
     * @param bytes 源字节数组
     * @param baseOffset 基础偏移量
     * @param offsetAndLen 偏移量和总长度的组合值
     * @return 读取的 Variant 对象
     */
    public static Variant readVariant(byte[] bytes, int baseOffset, long offsetAndLen) {
        int offset = baseOffset + (int) (offsetAndLen >> 32);
        int totalSize = (int) offsetAndLen;
        int valueSize = getInt(bytes, offset);
        int metadataSize = totalSize - 4 - valueSize;
        byte[] value = new byte[valueSize];
        byte[] metadata = new byte[metadataSize];
        System.arraycopy(bytes, offset + 4, value, 0, valueSize);
        System.arraycopy(bytes, offset + 4 + valueSize, metadata, 0, metadataSize);
        return new GenericVariant(value, metadata);
    }
}
