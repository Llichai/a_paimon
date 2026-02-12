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

import org.apache.paimon.data.columnar.writable.WritableFloatVector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * 堆单精度浮点列向量实现类。
 *
 * <p>这个类表示一个可空的单精度浮点列向量，用于在列式存储中高效地存储和访问单精度浮点数（FLOAT）。
 * 它使用 32 位 float 数组作为底层存储，支持字典编码和批量二进制设置操作。
 *
 * <h2>性能特点</h2>
 * <ul>
 *   <li><b>内存效率</b>: 每个单精度浮点值占用4字节，是 double 的一半</li>
 *   <li><b>字典压缩</b>: 对于重复值多的数据，支持字典编码</li>
 *   <li><b>批量操作</b>: 支持从二进制数据批量设置，利用 UNSAFE 优化</li>
 * </ul>
 *
 * @see AbstractHeapVector 堆向量的基类
 * @see WritableFloatVector 可写单精度浮点向量接口
 */
public class HeapFloatVector extends AbstractHeapVector implements WritableFloatVector {

    private static final long serialVersionUID = 8928878923550041110L;

    /** 存储单精度浮点值的数组。 */
    public float[] vector;

    /**
     * 构造一个堆单精度浮点列向量。
     *
     * <p>注意: 除了测试目的外，不要直接使用此构造函数。
     *
     * @param len 向量的容量
     */
    public HeapFloatVector(int len) {
        super(len);
        vector = new float[len];
    }

    @Override
    void reserveForHeapVector(int newCapacity) {
        if (vector.length < newCapacity) {
            vector = Arrays.copyOf(vector, newCapacity);
        }
    }

    @Override
    public float getFloat(int i) {
        if (dictionary == null) {
            return vector[i];
        } else {
            return dictionary.decodeToFloat(dictionaryIds.vector[i]);
        }
    }

    @Override
    public void setFloat(int i, float value) {
        vector[i] = value;
    }

    @Override
    public void setFloatsFromBinary(int rowId, int count, byte[] src, int srcIndex) {
        if (rowId + count > vector.length || srcIndex + count * 4L > src.length) {
            throw new IndexOutOfBoundsException(
                    String.format(
                            "Index out of bounds, row id is %s, count is %s, binary src index is %s, binary"
                                    + " length is %s, float array src index is %s, float array length is %s.",
                            rowId, count, srcIndex, src.length, rowId, vector.length));
        }
        if (LITTLE_ENDIAN) {
            UNSAFE.copyMemory(
                    src,
                    BYTE_ARRAY_OFFSET + srcIndex,
                    vector,
                    FLOAT_ARRAY_OFFSET + rowId * 4L,
                    count * 4L);
        } else {
            ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.BIG_ENDIAN);
            for (int i = 0; i < count; ++i) {
                vector[i + rowId] = bb.getFloat(srcIndex + (4 * i));
            }
        }
    }

    @Override
    public void appendFloat(float v) {
        reserve(elementsAppended + 1);
        setFloat(elementsAppended, v);
        elementsAppended++;
    }

    @Override
    public void fill(float value) {
        Arrays.fill(vector, value);
    }

    @Override
    public void reset() {
        super.reset();
        if (vector.length != capacity) {
            vector = new float[capacity];
        } else {
            Arrays.fill(vector, 0);
        }
    }
}
