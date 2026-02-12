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

import org.apache.paimon.data.columnar.writable.WritableBooleanVector;

import java.util.Arrays;

/**
 * 堆布尔列向量实现类。
 *
 * <p>这个类表示一个可空的堆布尔列向量，用于在列式存储中高效地存储和访问布尔类型的数据。
 * 它使用 boolean 数组作为底层存储，并提供了丰富的写入和批量操作方法。
 *
 * <h2>内存布局</h2>
 * <pre>
 * HeapBooleanVector 结构:
 * ┌─────────────────────┐
 * │ Null Bitmap (可选)  │  每个 bit 表示一个值是否为 null
 * │ [0, 1, 0, 0, ...]   │
 * ├─────────────────────┤
 * │ Boolean Array       │  实际的布尔值数组
 * │ [T, F, T, T, ...]   │  T=true, F=false
 * └─────────────────────┘
 * </pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建一个可以存储1000个布尔值的列向量
 * HeapBooleanVector vector = new HeapBooleanVector(1000);
 *
 * // 设置单个布尔值
 * vector.setBoolean(0, true);
 * vector.setBoolean(1, false);
 *
 * // 批量设置相同的值
 * vector.setBooleans(10, 5, true);  // 从索引10开始设置5个true
 *
 * // 从字节中解包布尔值（8个布尔值打包在一个字节中）
 * byte packed = 0b10110001;  // 最低位是第0个布尔值
 * vector.setBooleans(20, packed);  // 从索引20开始设置8个布尔值
 *
 * // 追加布尔值
 * vector.appendBoolean(true);
 *
 * // 填充整个向量
 * vector.fill(false);
 *
 * // 读取布尔值
 * boolean value = vector.getBoolean(0);
 *
 * // 重置向量以便重用
 * vector.reset();
 * }</pre>
 *
 * <h2>位打包特性</h2>
 * <p>虽然内部使用 boolean[] 存储（每个布尔值占用1字节），但提供了从打包字节中批量设置的方法，
 * 这在从紧凑的二进制格式读取数据时很有用：
 * <pre>{@code
 * // 从字节中提取布尔值
 * byte b = 0b10110001;
 * // setBooleans(rowId, count, src, srcIndex)
 * vector.setBooleans(0, 8, b, 0);
 * // 结果: [true, false, false, false, true, true, false, true]
 * //       位0    位1    位2    位3    位4   位5   位6   位7
 * }</pre>
 *
 * <h2>性能特点</h2>
 * <ul>
 *   <li><b>内存效率</b>: 每个布尔值占用1字节（Java boolean 数组的标准大小）</li>
 *   <li><b>访问速度</b>: O(1) 时间复杂度的随机访问</li>
 *   <li><b>批量操作</b>: 支持高效的批量设置和填充操作</li>
 *   <li><b>位打包</b>: 支持从紧凑的字节表示中批量解包</li>
 *   <li><b>动态扩容</b>: 自动扩展容量以支持追加操作</li>
 * </ul>
 *
 * <h2>字典编码</h2>
 * 布尔向量不支持字典编码，调用 {@code reserveDictionaryIds} 或 {@code getDictionaryIds} 会抛出异常。
 *
 * <h2>线程安全性</h2>
 * 此类不是线程安全的，如果多线程访问需要外部同步。
 *
 * @see AbstractHeapVector 堆向量的基类
 * @see WritableBooleanVector 可写布尔向量接口
 */
public class HeapBooleanVector extends AbstractHeapVector implements WritableBooleanVector {

    private static final long serialVersionUID = 4131239076731313596L;

    /** 存储布尔值的数组。 */
    /** 存储布尔值的数组。 */
    public boolean[] vector;

    /**
     * 构造一个堆布尔列向量。
     *
     * @param len 向量的容量（可以存储的布尔值数量）
     */
    public HeapBooleanVector(int len) {
        super(len);
        vector = new boolean[len];
    }

    /**
     * 为字典 ID 预留空间。
     *
     * <p>布尔向量不支持字典编码，调用此方法会抛出异常。
     *
     * @param capacity 字典容量
     * @return 不会返回，总是抛出异常
     * @throws RuntimeException 总是抛出，因为布尔向量没有字典
     */
    @Override
    public HeapIntVector reserveDictionaryIds(int capacity) {
        throw new RuntimeException("HeapBooleanVector has no dictionary.");
    }

    /**
     * 获取字典 ID 向量。
     *
     * <p>布尔向量不支持字典编码，调用此方法会抛出异常。
     *
     * @return 不会返回，总是抛出异常
     * @throws RuntimeException 总是抛出，因为布尔向量没有字典
     */
    @Override
    public HeapIntVector getDictionaryIds() {
        throw new RuntimeException("HeapBooleanVector has no dictionary.");
    }

    /**
     * 为堆向量预留空间。
     *
     * <p>如果需要的容量大于当前数组大小，则扩展底层布尔数组。
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
     * 获取指定位置的布尔值。
     *
     * @param i 索引位置
     * @return 该位置的布尔值
     */
    @Override
    public boolean getBoolean(int i) {
        return vector[i];
    }

    /**
     * 设置指定位置的布尔值。
     *
     * @param i 索引位置
     * @param value 要设置的布尔值
     */
    @Override
    public void setBoolean(int i, boolean value) {
        vector[i] = value;
    }

    /**
     * 批量设置相同的布尔值。
     *
     * <p>从 rowId 位置开始，连续设置 count 个相同的布尔值。
     *
     * @param rowId 起始位置
     * @param count 要设置的数量
     * @param value 要设置的布尔值
     */
    @Override
    public void setBooleans(int rowId, int count, boolean value) {
        for (int i = 0; i < count; ++i) {
            vector[i + rowId] = value;
        }
    }

    /**
     * 从字节中解包布尔值并批量设置。
     *
     * <p>从字节 src 的指定位开始，提取 count 个布尔值并设置到向量中。
     * 字节中的每个位代表一个布尔值，最低位（位0）对应第一个布尔值。
     *
     * <p>例如，字节 0b10110001：
     * <ul>
     *   <li>位0 (最低位) = 1 → true</li>
     *   <li>位1 = 0 → false</li>
     *   <li>位2 = 0 → false</li>
     *   <li>位3 = 0 → false</li>
     *   <li>位4 = 1 → true</li>
     *   <li>位5 = 1 → true</li>
     *   <li>位6 = 0 → false</li>
     *   <li>位7 = 1 → true</li>
     * </ul>
     *
     * @param rowId 向量中的起始位置
     * @param count 要设置的布尔值数量
     * @param src 源字节
     * @param srcIndex 源字节中的起始位索引（0-7）
     * @throws AssertionError 如果 count + srcIndex > 8
     */
    @Override
    public void setBooleans(int rowId, int count, byte src, int srcIndex) {
        assert (count + srcIndex <= 8);
        for (int i = 0; i < count; i++) {
            vector[i + rowId] = (byte) (src >>> (i + srcIndex) & 1) == 1;
        }
    }

    /**
     * 从字节的所有8个位中解包布尔值。
     *
     * <p>这是 {@link #setBooleans(int, int, byte, int)} 的便捷方法，
     * 从字节的位0开始提取全部8个布尔值。
     *
     * @param rowId 向量中的起始位置
     * @param src 源字节
     */
    @Override
    public void setBooleans(int rowId, byte src) {
        setBooleans(rowId, 8, src, 0);
    }

    /**
     * 追加一个布尔值到向量末尾。
     *
     * <p>如果容量不足，会自动扩展向量。
     *
     * @param v 要追加的布尔值
     */
    @Override
    public void appendBoolean(boolean v) {
        reserve(elementsAppended + 1);
        setBoolean(elementsAppended, v);
        elementsAppended++;
    }

    /**
     * 用指定的布尔值填充整个向量。
     *
     * @param value 填充值
     */
    @Override
    public void fill(boolean value) {
        Arrays.fill(vector, value);
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
            vector = new boolean[capacity];
        } else {
            Arrays.fill(vector, false);
        }
    }
}
