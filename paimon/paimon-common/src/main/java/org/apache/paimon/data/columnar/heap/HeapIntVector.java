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

import org.apache.paimon.data.columnar.writable.WritableIntVector;

import java.util.Arrays;

/**
 * 堆整数列向量实现类。
 *
 * <p>这个类表示一个可空的整数列向量，用于在列式存储中高效地存储和访问整数类型的数据（INT）。
 * 它使用 32 位 int 数组作为底层存储，支持字典编码和多种批量操作。
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * HeapIntVector vector = new HeapIntVector(1000);
 * vector.setInt(0, 42);
 * vector.setInts(10, 5, 100);  // 从索引10开始设置5个100
 * vector.appendInt(200);
 * int value = vector.getInt(0);
 * }</pre>
 *
 * <h2>性能特点</h2>
 * <ul>
 *   <li><b>内存效率</b>: 每个整数值占用4字节</li>
 *   <li><b>字典压缩</b>: 对于重复值多的数据，支持字典编码</li>
 *   <li><b>批量操作</b>: 支持从二进制数据和数组批量设置，利用 UNSAFE 和 System.arraycopy 优化</li>
 * </ul>
 *
 * @see AbstractHeapVector 堆向量的基类
 * @see WritableIntVector 可写整数向量接口
 */
public class HeapIntVector extends AbstractHeapVector implements WritableIntVector {

    private static final long serialVersionUID = -2749499358889718254L;

    /** 存储整数值的数组。 */
    public int[] vector;

    /**
     * 构造一个堆整数列向量。
     *
     * <p>注意: 除了测试目的外，不要直接使用此构造函数。
     *
     * @param len 向量的容量
     */
    public HeapIntVector(int len) {
        super(len);
        vector = new int[len];
    }

    @Override
    public void setNullAt(int i) {
        super.setNullAt(i);
    }

    @Override
    void reserveForHeapVector(int newCapacity) {
        if (vector.length < newCapacity) {
            vector = Arrays.copyOf(vector, newCapacity);
        }
    }

    @Override
    public int getInt(int i) {
        if (dictionary == null) {
            return vector[i];
        } else {
            return dictionary.decodeToInt(dictionaryIds.vector[i]);
        }
    }

    @Override
    public void setInt(int i, int value) {
        vector[i] = value;
    }

    @Override
    public void setIntsFromBinary(int rowId, int count, byte[] src, int srcIndex) {
        if (rowId + count > vector.length || srcIndex + count * 4L > src.length) {
            throw new IndexOutOfBoundsException(
                    String.format(
                            "Index out of bounds, row id is %s, count is %s, binary src index is %s, binary"
                                    + " length is %s, int array src index is %s, int array length is %s.",
                            rowId, count, srcIndex, src.length, rowId, vector.length));
        }
        if (LITTLE_ENDIAN) {
            UNSAFE.copyMemory(
                    src,
                    BYTE_ARRAY_OFFSET + srcIndex,
                    vector,
                    INT_ARRAY_OFFSET + rowId * 4L,
                    count * 4L);
        } else {
            long srcOffset = srcIndex + BYTE_ARRAY_OFFSET;
            for (int i = 0; i < count; ++i, srcOffset += 4) {
                vector[i + rowId] = Integer.reverseBytes(UNSAFE.getInt(src, srcOffset));
            }
        }
    }

    @Override
    public void setInts(int rowId, int count, int value) {
        for (int i = 0; i < count; ++i) {
            vector[i + rowId] = value;
        }
    }

    @Override
    public void setInts(int rowId, int count, int[] src, int srcIndex) {
        System.arraycopy(src, srcIndex, vector, rowId, count);
    }

    @Override
    public void fill(int value) {
        Arrays.fill(vector, value);
    }

    @Override
    public void appendInt(int v) {
        reserve(elementsAppended + 1);
        setInt(elementsAppended, v);
        elementsAppended++;
    }

    @Override
    public void appendInts(int count, int v) {
        reserve(elementsAppended + count);
        int result = elementsAppended;
        setInts(elementsAppended, count, v);
        elementsAppended += count;
    }

    @Override
    public void reset() {
        super.reset();
        if (vector.length != capacity) {
            vector = new int[capacity];
        } else {
            Arrays.fill(vector, 0);
        }
    }
}
