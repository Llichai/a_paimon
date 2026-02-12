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

package org.apache.paimon.data.columnar.writable;

import org.apache.paimon.data.columnar.LongColumnVector;

/**
 * 可写长整数列向量接口。
 *
 * <p>WritableLongVector 扩展了只读的 {@link LongColumnVector},提供64位长整数数据的写入能力。
 * 适用于存储大整数、时间戳(毫秒)、ID等需要长整数类型的场景。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li><b>单值写入</b>: 通过setLong()在指定位置写入长整数
 *   <li><b>二进制导入</b>: 通过setLongsFromBinary()从字节数组高效导入数据
 *   <li><b>追加写入</b>: 通过appendLong()追加长整数值
 *   <li><b>填充操作</b>: 通过fill()用指定值填充整个向量
 * </ul>
 *
 * <h2>典型应用场景</h2>
 * <ul>
 *   <li>存储大整数值(超过32位范围)
 *   <li>存储时间戳(毫秒或纳秒精度)
 *   <li>存储全局唯一ID(如雪花ID)
 *   <li>存储计数器或累加器值
 * </ul>
 *
 * <h2>二进制导入</h2>
 * setLongsFromBinary()方法支持从字节数组直接导入长整数数据:
 * <ul>
 *   <li>使用UNSAFE实现高效内存复制
 *   <li>避免逐个元素复制的开销
 *   <li>字节数组大小应为 count * 8 字节
 *   <li>适合从序列化数据快速恢复
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建容量为100的长整数向量
 * WritableLongVector vector = new HeapLongVector(100);
 *
 * // 单值写入
 * vector.setLong(0, 1234567890123456L);
 * vector.setLong(1, System.currentTimeMillis());
 *
 * // 追加时间戳
 * vector.appendLong(System.currentTimeMillis());
 * vector.appendLong(System.nanoTime());
 *
 * // 从二进制数据导入(例如从Parquet文件读取)
 * byte[] binaryData = ...; // 包含100个长整数的字节数组(800字节)
 * vector.setLongsFromBinary(0, 100, binaryData, 0);
 *
 * // 填充整个向量为0
 * vector.fill(0L);
 *
 * // 存储雪花ID
 * long snowflakeId = generateSnowflakeId();
 * vector.appendLong(snowflakeId);
 * }</pre>
 *
 * <h2>性能优化建议</h2>
 * <ul>
 *   <li>对于批量数据,使用setLongsFromBinary()比循环setLong()快得多
 *   <li>追加操作前预先reserve()足够容量,避免频繁扩容
 *   <li>使用fill()初始化比循环setLong()更高效
 * </ul>
 *
 * <h2>与WritableIntVector的区别</h2>
 * <ul>
 *   <li>WritableLongVector存储64位长整数,WritableIntVector存储32位整数
 *   <li>WritableLongVector占用空间是WritableIntVector的2倍
 *   <li>WritableLongVector支持更大的数值范围(-2^63 到 2^63-1)
 *   <li>WritableLongVector常用于时间戳,WritableIntVector常用于普通计数
 * </ul>
 *
 * <h2>线程安全性</h2>
 * WritableLongVector <b>不是线程安全的</b>。多线程环境下需要外部同步。
 *
 * @see LongColumnVector 只读长整数列向量接口
 * @see WritableColumnVector 可写列向量基础接口
 */
public interface WritableLongVector extends WritableColumnVector, LongColumnVector {

    /**
     * 在指定位置设置长整数值。
     *
     * @param rowId 行ID,范围 [0, capacity)
     * @param value 要设置的长整数值
     */
    void setLong(int rowId, long value);

    /**
     * 从二进制数据设置长整数值,使用UNSAFE进行高效复制。
     *
     * <p>该方法直接从字节数组复制数据到向量,避免逐个元素复制的开销,
     * 特别适合从Parquet、ORC等列式存储格式反序列化数据。
     *
     * @param rowId 起始行ID
     * @param count 长整数数量,字节大小为 count * 8
     * @param src 源字节数组
     * @param srcIndex 源数组的字节索引(不是长整数索引)
     */
    void setLongsFromBinary(int rowId, int count, byte[] src, int srcIndex);

    /**
     * 追加一个长整数值。
     *
     * <p>在当前追加位置写入值,并自动增加elementsAppended计数器。如果容量不足会自动扩容。
     *
     * @param v 要追加的长整数值
     */
    void appendLong(long v);

    /**
     * 用指定值填充整个列向量。
     *
     * @param value 填充值
     */
    void fill(long value);
}
