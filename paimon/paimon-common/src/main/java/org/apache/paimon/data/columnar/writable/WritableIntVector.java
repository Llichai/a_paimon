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

import org.apache.paimon.data.columnar.IntColumnVector;

/**
 * 可写整数列向量接口。
 *
 * <p>WritableIntVector 扩展了只读的 {@link IntColumnVector},提供整数数据的写入能力。
 * 支持单值写入、批量写入、二进制数据导入等多种写入方式。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li><b>单值写入</b>: 通过setInt()在指定位置写入整数
 *   <li><b>批量写入</b>: 通过setInts()批量设置重复值或数组值
 *   <li><b>二进制导入</b>: 通过setIntsFromBinary()从字节数组高效导入数据
 *   <li><b>追加写入</b>: 通过appendInt()追加单个或多个整数
 *   <li><b>填充操作</b>: 通过fill()用指定值填充整个向量
 * </ul>
 *
 * <h2>写入模式</h2>
 * <ul>
 *   <li><b>随机写入</b>: setInt(rowId, value) - 在任意位置写入
 *   <li><b>顺序追加</b>: appendInt(value) - 在末尾追加,自动管理位置
 *   <li><b>批量设置</b>: setInts() - 高效设置多个连续位置的值
 * </ul>
 *
 * <h2>二进制导入</h2>
 * setIntsFromBinary()方法支持从字节数组直接导入整数数据,通过UNSAFE实现高效复制:
 * <ul>
 *   <li>避免逐个元素复制的开销
 *   <li>适合从序列化数据快速恢复
 *   <li>字节数组大小应为 count * 4 字节
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建容量为100的整数向量
 * WritableIntVector vector = new HeapIntVector(100);
 *
 * // 单值写入
 * vector.setInt(0, 42);
 * vector.setInt(1, 100);
 *
 * // 批量写入相同值
 * vector.setInts(2, 5, 999);  // 位置2-6都设置为999
 *
 * // 批量写入数组值
 * int[] values = {1, 2, 3, 4, 5};
 * vector.setInts(10, 5, values, 0);  // 将values[0-4]写入位置10-14
 *
 * // 追加写入
 * vector.appendInt(200);
 * vector.appendInts(3, 300);  // 追加3个300
 *
 * // 从二进制数据导入
 * byte[] binaryData = ...; // 包含20个整数的字节数组(80字节)
 * vector.setIntsFromBinary(20, 20, binaryData, 0);
 *
 * // 填充整个向量
 * vector.fill(0);  // 所有位置设置为0
 * }</pre>
 *
 * <h2>性能优化建议</h2>
 * <ul>
 *   <li>批量操作(setInts)比循环调用setInt()更高效
 *   <li>二进制导入(setIntsFromBinary)比逐个setInt()快得多
 *   <li>追加操作前预先reserve()足够容量,避免频繁扩容
 *   <li>使用fill()初始化比循环setInt()更快
 * </ul>
 *
 * <h2>线程安全性</h2>
 * WritableIntVector <b>不是线程安全的</b>。多线程环境下需要外部同步。
 *
 * @see IntColumnVector 只读整数列向量接口
 * @see WritableColumnVector 可写列向量基础接口
 */
public interface WritableIntVector extends WritableColumnVector, IntColumnVector {

    /**
     * 在指定位置设置整数值。
     *
     * @param rowId 行ID,范围 [0, capacity)
     * @param value 要设置的整数值
     */
    void setInt(int rowId, int value);

    /**
     * 从二进制数据设置整数值,使用UNSAFE进行高效复制。
     *
     * <p>该方法直接从字节数组复制数据到向量,避免逐个元素复制的开销,特别适合反序列化场景。
     *
     * @param rowId 起始行ID
     * @param count 整数数量,字节大小为 count * 4
     * @param src 源字节数组
     * @param srcIndex 源数组的字节索引(不是整数索引)
     */
    void setIntsFromBinary(int rowId, int count, byte[] src, int srcIndex);

    /**
     * 批量设置重复值。
     *
     * <p>将[rowId, rowId + count)范围内的所有位置设置为相同的值,适合初始化或填充连续数据。
     *
     * @param rowId 起始行ID
     * @param count 设置的数量
     * @param value 要设置的值
     */
    void setInts(int rowId, int count, int value);

    /**
     * 批量设置数组值。
     *
     * <p>将src数组中[srcIndex, srcIndex + count)范围的值复制到向量的[rowId, rowId + count)位置。
     *
     * @param rowId 目标起始行ID
     * @param count 复制的数量
     * @param src 源整数数组
     * @param srcIndex 源数组起始索引
     */
    void setInts(int rowId, int count, int[] src, int srcIndex);

    /**
     * 用指定值填充整个列向量。
     *
     * @param value 填充值
     */
    void fill(int value);

    /**
     * 追加一个整数值。
     *
     * <p>在当前追加位置写入值,并自动增加elementsAppended计数器。如果容量不足会自动扩容。
     *
     * @param v 要追加的整数值
     */
    void appendInt(int v);

    /**
     * 追加多个相同的整数值。
     *
     * <p>在当前追加位置连续写入count个相同的值,自动管理位置和扩容。
     *
     * @param count 追加的数量
     * @param v 要追加的整数值
     */
    void appendInts(int count, int v);
}
