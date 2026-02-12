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

package org.apache.paimon.data.columnar.heap;

import org.apache.paimon.data.columnar.writable.WritableByteVector;

import java.util.Arrays;

/**
 * 堆字节列向量实现类。
 *
 * <p>这个类表示一个可空的字节列向量，用于在列式存储中高效地存储和访问字节类型的数据（TINYINT）。
 * 它使用 byte 数组作为底层存储，支持字典编码以进一步压缩数据。
 *
 * <h2>内存布局</h2>
 * <pre>
 * HeapByteVector 结构（无字典编码）:
 * ┌─────────────────────┐
 * │ Null Bitmap (可选)  │  每个 bit 表示一个值是否为 null
 * │ [0, 1, 0, 0, ...]   │
 * ├─────────────────────┤
 * │ Byte Array          │  实际的字节值数组
 * │ [10, -5, 127, ...]  │
 * └─────────────────────┘
 *
 * HeapByteVector 结构（使用字典编码）:
 * ┌─────────────────────┐
 * │ Dictionary          │  唯一值的字典
 * │ [10, -5, 127]       │
 * ├─────────────────────┤
 * │ Dictionary IDs      │  索引到字典的 ID
 * │ [0, 1, 0, 2, 0,...] │
 * └─────────────────────┘
 * </pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建一个可以存储1000个字节值的列向量
 * HeapByteVector vector = new HeapByteVector(1000);
 *
 * // 设置单个字节值
 * vector.setByte(0, (byte) 10);
 * vector.setByte(1, (byte) -5);
 *
 * // 批量填充相同的值
 * vector.fill((byte) 0);
 *
 * // 追加字节值
 * vector.appendByte((byte) 127);
 *
 * // 读取字节值
 * byte value = vector.getByte(0);
 *
 * // 重置向量以便重用
 * vector.reset();
 * }</pre>
 *
 * <h2>字典编码示例</h2>
 * <pre>{@code
 * // 假设有大量重复的字节值
 * HeapByteVector vector = new HeapByteVector(10000);
 * // 内部可以使用字典编码: [10, -5, 127] 三个唯一值
 * // 然后存储索引: [0, 1, 0, 2, 0, 1, ...] 代替原始值
 * // 当读取时: getByte(0) 会自动从字典解码返回 10
 * }</pre>
 *
 * <h2>性能特点</h2>
 * <ul>
 *   <li><b>内存效率</b>: 每个字节值占用1字节存储</li>
 *   <li><b>字典压缩</b>: 对于重复值多的数据，字典编码可以显著减少内存使用</li>
 *   <li><b>访问速度</b>: O(1) 时间复杂度的随机访问（字典编码会增加一次间接查找）</li>
 *   <li><b>批量操作</b>: 支持高效的批量填充操作</li>
 *   <li><b>动态扩容</b>: 自动扩展容量以支持追加操作</li>
 * </ul>
 *
 * <h2>适用场景</h2>
 * <ul>
 *   <li>存储 SQL TINYINT 类型（-128 到 127）</li>
 *   <li>存储状态码、标志位等小整数值</li>
 *   <li>存储枚举值的整数表示</li>
 * </ul>
 *
 * <h2>线程安全性</h2>
 * 此类不是线程安全的，如果多线程访问需要外部同步。
 *
 * @see AbstractHeapVector 堆向量的基类
 * @see WritableByteVector 可写字节向量接口
 */
public class HeapByteVector extends AbstractHeapVector implements WritableByteVector {

    private static final long serialVersionUID = 7216045902943789034L;

    /** 存储字节值的数组。 */
    public byte[] vector;

    /**
     * 构造一个堆字节列向量。
     *
     * <p>注意: 除了测试目的外，不要直接使用此构造函数。应该通过工厂方法或构建器来创建。
     *
     * @param len 向量的容量（可以存储的字节值数量）
     */
    public HeapByteVector(int len) {
        super(len);
        vector = new byte[len];
    }

    /**
     * 获取指定位置的字节值。
     *
     * <p>如果使用了字典编码，则会自动从字典中解码。
     *
     * @param i 索引位置
     * @return 该位置的字节值
     */
    @Override
    public byte getByte(int i) {
        if (dictionary == null) {
            return vector[i];
        } else {
            return (byte) dictionary.decodeToInt(dictionaryIds.vector[i]);
        }
    }

    /**
     * 设置指定位置的字节值。
     *
     * @param i 索引位置
     * @param value 要设置的字节值
     */
    @Override
    public void setByte(int i, byte value) {
        vector[i] = value;
    }

    /**
     * 用指定的字节值填充整个向量。
     *
     * @param value 填充值
     */
    @Override
    public void fill(byte value) {
        Arrays.fill(vector, value);
    }

    /**
     * 追加一个字节值到向量末尾。
     *
     * <p>如果容量不足，会自动扩展向量。
     *
     * @param v 要追加的字节值
     */
    @Override
    public void appendByte(byte v) {
        reserve(elementsAppended + 1);
        setByte(elementsAppended, v);
        elementsAppended++;
    }

    /**
     * 为堆向量预留空间。
     *
     * <p>如果需要的容量大于当前数组大小，则扩展底层字节数组。
     *
     * @param newCapacity 新的容量大小
     */
    @Override
    void reserveForHeapVector(int newCapacity) {
        if (vector.length < newCapacity) {
            vector = Arrays.copyOf(vector, newCapacity);
        }
    }

    /**
     * 重置向量到初始状态。
     *
     * <p>清空所有数据，重置元素计数器。如果数组大小与容量不匹配，则重新分配数组。
     */
    @Override
    public void reset() {
        super.reset();
        if (vector.length != capacity) {
            vector = new byte[capacity];
        } else {
            Arrays.fill(vector, (byte) 0);
        }
    }
}
