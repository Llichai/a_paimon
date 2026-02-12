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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.memory.MemorySegment;

/**
 * {@link BinaryRow} 的工具类。
 *
 * <p>该类中的许多方法用于代码生成。这是从 {@link BinaryRowDataUtil} 直接复制过来的。
 */
public class BinaryRowDataUtil {

    /** Unsafe 实例,用于直接内存操作 */
    public static final sun.misc.Unsafe UNSAFE = MemorySegment.UNSAFE;

    /** 字节数组的基础偏移量 */
    public static final int BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    /** 空行常量,表示一个没有任何字段的二进制行 */
    public static final BinaryRow EMPTY_ROW = new BinaryRow(0);

    static {
        int size = EMPTY_ROW.getFixedLengthPartSize();
        byte[] bytes = new byte[size];
        EMPTY_ROW.pointTo(MemorySegment.wrap(bytes), 0, size);
    }

    /**
     * 比较两个字节数组是否相等。
     *
     * @param left 左侧字节数组
     * @param right 右侧字节数组
     * @param length 要比较的长度
     * @return 如果指定长度内的字节相等则返回 true
     */
    public static boolean byteArrayEquals(byte[] left, byte[] right, int length) {
        return byteArrayEquals(left, BYTE_ARRAY_BASE_OFFSET, right, BYTE_ARRAY_BASE_OFFSET, length);
    }

    /**
     * 比较两个对象中指定偏移位置的字节序列是否相等。
     *
     * <p>使用 Unsafe 进行高效的内存比较,首先按 8 字节(long)进行比较,
     * 然后按单字节比较剩余部分。
     *
     * @param left 左侧对象
     * @param leftOffset 左侧对象的偏移量
     * @param right 右侧对象
     * @param rightOffset 右侧对象的偏移量
     * @param length 要比较的字节长度
     * @return 如果指定长度内的字节相等则返回 true
     */
    public static boolean byteArrayEquals(
            Object left, long leftOffset, Object right, long rightOffset, int length) {
        int i = 0;

        // 按 8 字节进行批量比较
        while (i <= length - 8) {
            if (UNSAFE.getLong(left, leftOffset + i) != UNSAFE.getLong(right, rightOffset + i)) {
                return false;
            }
            i += 8;
        }

        // 比较剩余的字节
        while (i < length) {
            if (UNSAFE.getByte(left, leftOffset + i) != UNSAFE.getByte(right, rightOffset + i)) {
                return false;
            }
            i += 1;
        }
        return true;
    }
}
