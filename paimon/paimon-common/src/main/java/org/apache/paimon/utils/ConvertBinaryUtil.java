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

import java.nio.charset.StandardCharsets;

/**
 * 二进制转换工具类,用于将值转换为二进制格式。
 *
 * <p>提供字符串、字节数组与长整型之间的转换功能,
 * 支持填充和UTF-8编码处理。
 */
public class ConvertBinaryUtil {

    private ConvertBinaryUtil() {}

    /**
     * 将字节数组填充到8字节。
     *
     * @param data 原始字节数组
     * @return 填充后的8字节数组
     */
    public static byte[] paddingTo8Byte(byte[] data) {
        return paddingToNByte(data, 8);
    }

    /**
     * 将字节数组填充到指定字节数。
     *
     * <p>如果原数组长度等于目标长度,直接返回原数组;
     * 如果超过目标长度,则截断;如果不足,则在前面补0。
     *
     * @param data 原始字节数组
     * @param paddingNum 目标字节数
     * @return 填充后的字节数组
     */
    public static byte[] paddingToNByte(byte[] data, int paddingNum) {
        if (data.length == paddingNum) {
            return data;
        }
        if (data.length > paddingNum) {
            byte[] result = new byte[paddingNum];
            System.arraycopy(data, 0, result, 0, paddingNum);
            return result;
        }
        int paddingSize = paddingNum - data.length;
        byte[] result = new byte[paddingNum];
        for (int i = 0; i < paddingSize; i++) {
            result[i] = 0;
        }
        System.arraycopy(data, 0, result, paddingSize, data.length);

        return result;
    }

    /**
     * 将 UTF-8 字符串转换为8字节数组。
     *
     * @param data UTF-8 字符串
     * @return 8字节数组
     */
    public static byte[] utf8To8Byte(String data) {
        return paddingTo8Byte(data.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 将字符串转换为 Long 值。
     *
     * @param data 字符串
     * @return Long 值
     */
    public static Long convertStringToLong(String data) {
        byte[] bytes = utf8To8Byte(data);
        return convertBytesToLong(bytes);
    }

    /**
     * 将字节数组转换为 long 值。
     *
     * <p>使用大端字节序将8个字节组合成一个 long 值。
     *
     * @param bytes 字节数组
     * @return long 值
     */
    public static long convertBytesToLong(byte[] bytes) {
        byte[] paddedBytes = paddingTo8Byte(bytes);
        long temp = 0L;
        for (int i = 7; i >= 0; i--) {
            temp = temp | (((long) paddedBytes[i] & 0xff) << (7 - i) * 8);
        }
        return temp;
    }
}
