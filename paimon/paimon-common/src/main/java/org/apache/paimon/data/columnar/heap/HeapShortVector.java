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

import org.apache.paimon.data.columnar.writable.WritableShortVector;

import java.util.Arrays;

/**
 * 堆短整数列向量实现类。
 *
 * <p>这个类表示一个可空的短整数列向量，用于在列式存储中高效地存储和访问短整数类型的数据（SMALLINT）。
 * 它使用 16 位 short 数组作为底层存储，支持字典编码。
 *
 * <h2>性能特点</h2>
 * <ul>
 *   <li><b>内存效率</b>: 每个短整数值占用2字节，是 int 的一半</li>
 *   <li><b>字典压缩</b>: 对于重复值多的数据，支持字典编码</li>
 * </ul>
 *
 * @see AbstractHeapVector 堆向量的基类
 * @see WritableShortVector 可写短整数向量接口
 */
public class HeapShortVector extends AbstractHeapVector implements WritableShortVector {

    private static final long serialVersionUID = -8278486456144676292L;

    /** 存储短整数值的数组。 */
    public short[] vector;

    /**
     * 构造一个堆短整数列向量。
     *
     * <p>注意: 除了测试目的外，不要直接使用此构造函数。
     *
     * @param len 向量的容量
     */
    public HeapShortVector(int len) {
        super(len);
        vector = new short[len];
    }

    @Override
    void reserveForHeapVector(int newCapacity) {
        if (vector.length < newCapacity) {
            vector = Arrays.copyOf(vector, newCapacity);
        }
    }

    @Override
    public short getShort(int i) {
        if (dictionary == null) {
            return vector[i];
        } else {
            return (short) dictionary.decodeToInt(dictionaryIds.vector[i]);
        }
    }

    @Override
    public void setShort(int i, short value) {
        vector[i] = value;
    }

    @Override
    public void appendShort(short v) {
        reserve(elementsAppended + 1);
        setShort(elementsAppended, v);
        elementsAppended++;
    }

    @Override
    public void fill(short value) {
        Arrays.fill(vector, value);
    }

    @Override
    public void reset() {
        super.reset();
        if (vector.length != capacity) {
            vector = new short[capacity];
        } else {
            Arrays.fill(vector, (short) 0);
        }
    }
}
