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

import org.apache.paimon.data.columnar.writable.WritableDoubleVector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * 堆双精度浮点列向量实现类。
 *
 * <p>这个类表示一个可空的双精度浮点列向量，用于在列式存储中高效地存储和访问双精度浮点数（DOUBLE）。
 * 它使用 64 位 double 数组作为底层存储，支持字典编码和批量二进制设置操作。
 *
 * <h2>内存布局</h2>
 * <pre>
 * HeapDoubleVector 结构:
 * ┌─────────────────────┐
 * │ Null Bitmap (可选)  │  每个 bit 表示一个值是否为 null
 * ├─────────────────────┤
 * │ Double Array        │  64位浮点数数组
 * │ [1.5, 2.7, -3.14...]│
 * └─────────────────────┘
 * </pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * HeapDoubleVector vector = new HeapDoubleVector(1000);
 * vector.setDouble(0, 3.14159);
 * vector.appendDouble(2.71828);
 * vector.fill(0.0);
 * double value = vector.getDouble(0);
 * vector.reset();
 * }</pre>
 *
 * <h2>性能特点</h2>
 * <ul>
 *   <li><b>内存效率</b>: 每个双精度浮点值占用8字节</li>
 *   <li><b>字典压缩</b>: 对于重复值多的数据，支持字典编码</li>
 *   <li><b>批量操作</b>: 支持从二进制数据批量设置，利用 UNSAFE 优化</li>
 *   <li><b>访问速度</b>: O(1) 时间复杂度的随机访问</li>
 * </ul>
 *
 * @see AbstractHeapVector 堆向量的基类
 * @see WritableDoubleVector 可写双精度浮点向量接口
 */
public class HeapDoubleVector extends AbstractHeapVector implements WritableDoubleVector {

    private static final long serialVersionUID = 6193940154117411328L;

    /** 存储双精度浮点值的数组。 */
    public double[] vector;

    /**
     * 构造一个堆双精度浮点列向量。
     *
     * <p>注意: 除了测试目的外，不要直接使用此构造函数。
     *
     * @param len 向量的容量
     */
    public HeapDoubleVector(int len) {
        super(len);
        vector = new double[len];
    }

    @Override
    void reserveForHeapVector(int newCapacity) {
        if (vector.length < newCapacity) {
            vector = Arrays.copyOf(vector, newCapacity);
        }
    }

    @Override
    public double getDouble(int i) {
        if (dictionary == null) {
            return vector[i];
        } else {
            return dictionary.decodeToDouble(dictionaryIds.vector[i]);
        }
    }

    @Override
    public void setDouble(int i, double value) {
        vector[i] = value;
    }

    @Override
    public void setDoublesFromBinary(int rowId, int count, byte[] src, int srcIndex) {
        if (rowId + count > vector.length || srcIndex + count * 8L > src.length) {
            throw new IndexOutOfBoundsException(
                    String.format(
                            "Index out of bounds, row id is %s, count is %s, binary src index is %s, binary"
                                    + " length is %s, double array src index is %s, double array length is %s.",
                            rowId, count, srcIndex, src.length, rowId, vector.length));
        }
        if (LITTLE_ENDIAN) {
            UNSAFE.copyMemory(
                    src,
                    BYTE_ARRAY_OFFSET + srcIndex,
                    vector,
                    DOUBLE_ARRAY_OFFSET + rowId * 8L,
                    count * 8L);
        } else {
            ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.BIG_ENDIAN);
            for (int i = 0; i < count; ++i) {
                vector[i + rowId] = bb.getDouble(srcIndex + (8 * i));
            }
        }
    }

    @Override
    public void appendDouble(double v) {
        reserve(elementsAppended + 1);
        setDouble(elementsAppended, v);
        elementsAppended++;
    }

    @Override
    public void fill(double value) {
        Arrays.fill(vector, value);
    }

    @Override
    public void reset() {
        super.reset();
        if (vector.length != capacity) {
            vector = new double[capacity];
        } else {
            Arrays.fill(vector, 0);
        }
    }
}
