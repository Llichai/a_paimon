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

import java.io.ByteArrayOutputStream;

/**
 * 可复用字节数组的 {@link ByteArrayOutputStream}。
 *
 * <p>允许重用底层字节数组,避免频繁的数组分配和GC开销。
 *
 * <p>特性:
 * <ul>
 *   <li>支持设置和获取底层缓冲区
 *   <li>固定长度的字节数组输出流
 *   <li>当缓冲区满时停止写入(不自动扩容)
 * </ul>
 */
public class FixLenByteArrayOutputStream {

    /** 底层字节缓冲区 */
    private byte[] buf;

    /** 已写入的字节数 */
    private int count;

    /**
     * 设置底层缓冲区。
     *
     * @param buffer 字节缓冲区
     */
    public void setBuffer(byte[] buffer) {
        this.buf = buffer;
    }

    /**
     * 获取底层缓冲区。
     *
     * @return 字节缓冲区
     */
    public byte[] getBuffer() {
        return buf;
    }

    /**
     * 写入字节数组的一部分。
     *
     * @param b 源字节数组
     * @param off 起始偏移量
     * @param len 要写入的长度
     * @return 实际写入的字节数
     * @throws IndexOutOfBoundsException 如果参数越界
     */
    public int write(byte[] b, int off, int len) {
        if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) - b.length > 0)) {
            throw new IndexOutOfBoundsException();
        }
        int writeLen = Math.min(len, buf.length - count);
        System.arraycopy(b, off, buf, count, writeLen);
        count += writeLen;
        return writeLen;
    }

    /**
     * 获取已写入的字节数。
     *
     * @return 已写入的字节数
     */
    public int getCount() {
        return count;
    }

    /**
     * 写入单个字节。
     *
     * @param b 要写入的字节
     * @return 实际写入的字节数(0或1)
     */
    public int write(byte b) {
        if (count < buf.length) {
            buf[count] = b;
            count += 1;
            return 1;
        }
        return 0;
    }

    /**
     * 设置已写入的字节数。
     *
     * @param count 字节数
     */
    public void setCount(int count) {
        this.count = count;
    }
}
