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

import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarMap;
import org.apache.paimon.data.columnar.MapColumnVector;

/**
 * 堆Map列向量实现类。
 *
 * <p>这个类表示一个可空的堆Map列向量，用于在列式存储中管理Map类型的数据。
 * Map由键列向量和值列向量组成，通过维护偏移量和长度信息来表示每个Map的边界。
 *
 * <h2>内存布局</h2>
 * <pre>
 * HeapMapVector 结构:
 * ┌─────────────────────┐
 * │ Null Bitmap (可选)  │  每个 bit 表示一个 Map 是否为 null
 * ├─────────────────────┤
 * │ Offsets Array       │  每个 Map 在键/值列向量中的起始位置
 * │ [0, 3, 5, 8, ...]   │
 * ├─────────────────────┤
 * │ Lengths Array       │  每个 Map 的键值对数量
 * │ [3, 2, 3, 4, ...]   │
 * ├─────────────────────┤
 * │ Keys ColumnVector   │  存储所有 Map 的键
 * │ [k1,k2,k3,k4, ...]  │
 * ├─────────────────────┤
 * │ Values ColumnVector │  存储所有 Map 的值
 * │ [v1,v2,v3,v4, ...]  │
 * └─────────────────────┘
 * </pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建键和值的列向量
 * HeapIntVector keysVector = new HeapIntVector(100);
 * HeapBytesVector valuesVector = new HeapBytesVector(100);
 *
 * // 创建 Map 列向量，可以存储10个Map
 * HeapMapVector mapVector = new HeapMapVector(10, keysVector, valuesVector);
 *
 * // 设置第0个Map: {1->"a", 2->"b", 3->"c"}
 * mapVector.setOffsets(0, 0);  // 从位置0开始
 * mapVector.setLengths(0, 3);  // 包含3个键值对
 * keysVector.setInt(0, 1);
 * keysVector.setInt(1, 2);
 * keysVector.setInt(2, 3);
 * valuesVector.putByteArray(0, "a".getBytes(), 0, 1);
 * valuesVector.putByteArray(1, "b".getBytes(), 0, 1);
 * valuesVector.putByteArray(2, "c".getBytes(), 0, 1);
 *
 * // 读取第0个Map
 * InternalMap map = mapVector.getMap(0);
 * // map.size() == 3
 * }</pre>
 *
 * <h2>性能特点</h2>
 * <ul>
 *   <li><b>内存效率</b>: 所有Map的键和值分别连续存储，减少内存碎片</li>
 *   <li><b>访问效率</b>: O(1) 时间复杂度访问任意Map</li>
 *   <li><b>可变大小</b>: 每个Map可以包含不同数量的键值对</li>
 *   <li><b>类型灵活</b>: 键和值可以是任意类型的列向量</li>
 * </ul>
 *
 * @see AbstractArrayBasedVector 基于数组的向量基类
 * @see MapColumnVector Map列向量接口
 * @see ColumnarMap 列式存储的Map表示
 */
public class HeapMapVector extends AbstractArrayBasedVector implements MapColumnVector {

    /**
     * 构造一个堆Map列向量。
     *
     * @param capacity Map的数量（行数）
     * @param keys 存储所有Map键的列向量
     * @param values 存储所有Map值的列向量
     */
    public HeapMapVector(int capacity, ColumnVector keys, ColumnVector values) {
        super(capacity, new ColumnVector[] {keys, values});
    }

    /**
     * 设置键列向量。
     *
     * @param keys 新的键列向量
     */
    public void setKeys(ColumnVector keys) {
        children[0] = keys;
    }

    /**
     * 获取键列向量。
     *
     * @return 键列向量
     */
    public ColumnVector getKeys() {
        return children[0];
    }

    /**
     * 设置值列向量。
     *
     * @param values 新的值列向量
     */
    public void setValues(ColumnVector values) {
        children[1] = values;
    }

    /**
     * 获取值列向量。
     *
     * @return 值列向量
     */
    public ColumnVector getValues() {
        return children[1];
    }

    /**
     * 获取指定位置的Map。
     *
     * <p>通过偏移量和长度信息，从键和值列向量中提取出对应的Map视图。
     * 返回的Map是一个轻量级的视图，不会复制底层数据。
     *
     * @param i Map的索引位置
     * @return 指定位置的内部Map表示
     */
    @Override
    public InternalMap getMap(int i) {
        long offset = offsets[i];
        long length = lengths[i];
        return new ColumnarMap(children[0], children[1], (int) offset, (int) length);
    }
}
