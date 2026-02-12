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

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.columnar.ArrayColumnVector;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarArray;

/**
 * 堆数组列向量实现类。
 *
 * <p>这个类表示一个可空的堆数组列向量，用于在列式存储中管理数组类型的数据。它通过维护偏移量和长度信息，
 * 支持高效地访问数组元素，每个数组可以包含不同数量的元素。
 *
 * <h2>内存布局</h2>
 * <pre>
 * HeapArrayVector 结构:
 * ┌─────────────────────┐
 * │ Null Bitmap (可选)  │  每个 bit 表示一个数组是否为 null
 * ├─────────────────────┤
 * │ Offsets Array       │  每个数组在子列向量中的起始位置
 * │ [0, 3, 7, 10, ...]  │
 * ├─────────────────────┤
 * │ Lengths Array       │  每个数组的元素个数
 * │ [3, 4, 3, 5, ...]   │
 * ├─────────────────────┤
 * │ Child ColumnVector  │  实际存储数组元素的列向量
 * │ [a, b, c, d, e,...] │
 * └─────────────────────┘
 * </pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建一个可以存储整数数组的列向量
 * HeapIntVector elementVector = new HeapIntVector(100);  // 可以存储100个整数元素
 * HeapArrayVector arrayVector = new HeapArrayVector(10, elementVector);  // 可以存储10个数组
 *
 * // 设置第0个数组: [1, 2, 3]
 * arrayVector.setOffsets(0, 0);  // 从位置0开始
 * arrayVector.setLengths(0, 3);  // 长度为3
 * elementVector.setInt(0, 1);
 * elementVector.setInt(1, 2);
 * elementVector.setInt(2, 3);
 *
 * // 读取第0个数组
 * InternalArray array = arrayVector.getArray(0);
 * // array.size() == 3
 * // array.getInt(0) == 1
 * // array.getInt(1) == 2
 * // array.getInt(2) == 3
 * }</pre>
 *
 * <h2>性能特点</h2>
 * <ul>
 *   <li><b>内存效率</b>: 所有数组的元素连续存储在子列向量中，减少内存碎片</li>
 *   <li><b>访问效率</b>: O(1) 时间复杂度访问任意数组，通过 offset 和 length 快速定位</li>
 *   <li><b>可变长度</b>: 支持每个数组有不同的长度</li>
 *   <li><b>空值支持</b>: 通过 null bitmap 高效地表示空数组</li>
 *   <li><b>批量处理</b>: 适合列式存储和向量化计算</li>
 * </ul>
 *
 * <h2>线程安全性</h2>
 * 此类不是线程安全的，如果多线程访问需要外部同步。
 *
 * @see AbstractArrayBasedVector 基于数组的向量基类
 * @see ArrayColumnVector 数组列向量接口
 * @see ColumnarArray 列式存储的数组表示
 */
public class HeapArrayVector extends AbstractArrayBasedVector implements ArrayColumnVector {

    /**
     * 构造一个堆数组列向量。
     *
     * @param len 数组的数量（行数）
     * @param vector 用于存储所有数组元素的子列向量
     */
    public HeapArrayVector(int len, ColumnVector vector) {
        super(len, new ColumnVector[] {vector});
    }

    /**
     * 设置子列向量。
     *
     * <p>用于替换当前用于存储数组元素的列向量，这在需要更新底层存储或进行类型转换时很有用。
     *
     * @param child 新的子列向量
     */
    public void setChild(ColumnVector child) {
        children[0] = child;
    }

    /**
     * 获取指定位置的数组。
     *
     * <p>通过偏移量和长度信息，从子列向量中提取出对应的数组视图。返回的数组是一个轻量级的视图，
     * 不会复制底层数据。
     *
     * @param i 数组的索引位置
     * @return 指定位置的内部数组表示，如果该位置为 null 则应先通过 isNullAt(i) 检查
     */
    @Override
    public InternalArray getArray(int i) {
        long offset = offsets[i];
        long length = lengths[i];
        return new ColumnarArray(children[0], (int) offset, (int) length);
    }

    /**
     * 获取存储数组元素的子列向量。
     *
     * @return 子列向量实例
     */
    @Override
    public ColumnVector getColumnVector() {
        return children[0];
    }
}
