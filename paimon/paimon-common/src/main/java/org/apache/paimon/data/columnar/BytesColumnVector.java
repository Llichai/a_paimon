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

package org.apache.paimon.data.columnar;

/**
 * 字节数组列向量接口,用于访问变长字节数组数据。
 *
 * <p>此接口扩展了 {@link ColumnVector} 基础接口,提供对字节数组的访问方法。
 * 与其他固定长度类型不同,字节数组是变长类型,通过 {@link Bytes} 对象返回,
 * 包含实际数据、偏移量和长度信息。
 *
 * <h2>设计特点</h2>
 * <ul>
 *   <li><b>零拷贝设计:</b> 通过 Bytes 对象返回引用,避免不必要的数据复制
 *   <li><b>内存复用:</b> Bytes 对象中的数据可能被多个调用复用
 *   <li><b>灵活切片:</b> 通过 offset 和 len 支持对大块内存的切片访问
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>存储 VARCHAR/CHAR 类型的字符串数据
 *   <li>存储 VARBINARY/BINARY 类型的二进制数据
 *   <li>存储 DECIMAL 类型的大数值(以字节数组表示)
 * </ul>
 *
 * <h2>内存模型</h2>
 * <pre>
 * 底层字节数组: [unused|data1|unused|data2|unused]
 *                       ↑           ↑
 *                  offset=5     offset=15
 *                  len=8        len=10
 * </pre>
 *
 * <p><b>警告:</b> 返回的 {@link Bytes} 对象中的数据可能在下次调用时被覆盖,
 * 如果需要保留数据,必须调用 {@link Bytes#getBytes()} 复制一份。
 *
 * @see ColumnVector 列向量基础接口
 * @see Bytes 字节数组数据的包装类
 * @see org.apache.paimon.data.columnar.heap.HeapBytesVector 堆内存实现
 * @see org.apache.paimon.data.columnar.writable.WritableBytesVector 可写实现
 */
public interface BytesColumnVector extends ColumnVector {
    /**
     * 获取指定位置的字节数组引用。
     *
     * <p><b>注意:</b> 返回的 Bytes 对象可能在下次调用时被复用,
     * 数据可能被覆盖。如需保留数据,请调用 {@link Bytes#getBytes()} 复制。
     *
     * @param i 行索引(从0开始)
     * @return 字节数组引用,包含数据、偏移量和长度
     */
    Bytes getBytes(int i);

    /**
     * 字节数组数据的包装类,包含数据引用、偏移量和长度。
     *
     * <p>此类用于零拷贝地返回字节数组数据,避免每次都分配新的数组。
     * 数据可能存储在一个大的共享缓冲区中,通过 offset 和 len 定位实际数据。
     *
     * <h2>使用注意</h2>
     * <ul>
     *   <li>data 数组可能包含多个值的数据,不要假设整个数组都是有效数据
     *   <li>实际数据范围: data[offset] 到 data[offset + len - 1]
     *   <li>如需独立的数组副本,调用 {@link #getBytes()} 方法
     * </ul>
     */
    class Bytes {
        /** 底层字节数组(可能包含多个值的数据) */
        public final byte[] data;
        /** 此值在数组中的起始偏移量 */
        public final int offset;
        /** 此值的字节长度 */
        public final int len;

        /**
         * 构造字节数组引用。
         *
         * @param data 底层字节数组
         * @param offset 数据起始偏移量
         * @param len 数据长度
         */
        public Bytes(byte[] data, int offset, int len) {
            this.data = data;
            this.offset = offset;
            this.len = len;
        }

        /**
         * 获取此值的独立字节数组副本。
         *
         * <p>如果 offset 为 0 且 len 等于 data 长度,直接返回 data 数组;
         * 否则创建一个新数组并复制指定范围的数据。
         *
         * @return 独立的字节数组副本
         */
        public byte[] getBytes() {
            if (offset == 0 && len == data.length) {
                return data;
            }
            byte[] res = new byte[len];
            System.arraycopy(data, offset, res, 0, len);
            return res;
        }
    }
}
