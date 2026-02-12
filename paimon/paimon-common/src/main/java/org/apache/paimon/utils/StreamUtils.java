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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * I/O 流工具类，提供字节序转换等辅助功能。
 *
 * <p>StreamUtils 提供了一组静态方法，用于在不同字节序之间进行数据转换，
 * 特别是将 Java 基本类型转换为小端序（Little-Endian）字节数组。
 *
 * <p>字节序说明：
 * <ul>
 *   <li>小端序（Little-Endian）：最低有效字节存储在最低地址
 *   <li>大端序（Big-Endian）：最高有效字节存储在最低地址
 *   <li>网络字节序通常是大端序
 *   <li>x86/x64 架构通常使用小端序
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 将整数转换为小端序字节数组
 * int value = 0x12345678;
 * byte[] bytes = StreamUtils.intToLittleEndian(value);
 * // bytes = {0x78, 0x56, 0x34, 0x12}
 *
 * // 将长整数转换为小端序字节数组
 * long lValue = 0x123456789ABCDEF0L;
 * byte[] lBytes = StreamUtils.longToLittleEndian(lValue);
 * // lBytes = {0xF0, 0xDE, 0xBC, 0x9A, 0x78, 0x56, 0x34, 0x12}
 * }</pre>
 *
 * <p>应用场景：
 * <ul>
 *   <li>文件格式处理：许多文件格式（如 Parquet、ORC）使用小端序
 *   <li>网络协议：某些协议要求特定的字节序
 *   <li>跨平台数据交换：统一字节序以保证数据一致性
 *   <li>二进制序列化：确保序列化后的数据可在不同平台间交换
 * </ul>
 *
 * <p>性能特性：
 * <ul>
 *   <li>使用 {@link ByteBuffer} 进行转换，性能优于手动位操作
 *   <li>每次调用都会分配新的字节数组
 *   <li>时间复杂度：O(1)
 *   <li>空间复杂度：O(1)
 * </ul>
 *
 * <p>注意事项：
 * <ul>
 *   <li>返回的字节数组大小固定：int 为 4 字节，long 为 8 字节
 *   <li>此类仅处理小端序转换，不处理大端序
 *   <li>如需读取小端序数据，应使用对应的读取方法
 * </ul>
 */
public class StreamUtils {

    /**
     * 将 int 值转换为小端序字节数组。
     *
     * <p>将 32 位整数按照小端序（最低有效字节在前）编码为 4 字节数组。
     *
     * <p>示例：
     * <pre>{@code
     * int value = 0x12345678;
     * byte[] bytes = intToLittleEndian(value);
     * // bytes[0] = 0x78 (最低字节)
     * // bytes[1] = 0x56
     * // bytes[2] = 0x34
     * // bytes[3] = 0x12 (最高字节)
     * }</pre>
     *
     * @param value 要转换的整数值
     * @return 包含小端序编码的 4 字节数组
     */
    public static byte[] intToLittleEndian(int value) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(value);
        return buffer.array();
    }

    /**
     * 将 long 值转换为小端序字节数组。
     *
     * <p>将 64 位长整数按照小端序（最低有效字节在前）编码为 8 字节数组。
     *
     * <p>示例：
     * <pre>{@code
     * long value = 0x123456789ABCDEF0L;
     * byte[] bytes = longToLittleEndian(value);
     * // bytes[0] = 0xF0 (最低字节)
     * // bytes[1] = 0xDE
     * // bytes[2] = 0xBC
     * // bytes[3] = 0x9A
     * // bytes[4] = 0x78
     * // bytes[5] = 0x56
     * // bytes[6] = 0x34
     * // bytes[7] = 0x12 (最高字节)
     * }</pre>
     *
     * @param value 要转换的长整数值
     * @return 包含小端序编码的 8 字节数组
     */
    public static byte[] longToLittleEndian(long value) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(value);
        return buffer.array();
    }
}
