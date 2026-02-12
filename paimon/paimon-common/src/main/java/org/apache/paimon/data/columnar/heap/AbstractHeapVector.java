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

import org.apache.paimon.data.columnar.writable.AbstractWritableVector;
import org.apache.paimon.memory.MemorySegment;

import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * 堆向量抽象基类,为所有堆内存实现的列向量提供通用功能。
 *
 * <p>此类是所有 heap 包中列向量实现的基础,提供了 NULL 值管理、字典编码支持、
 * 以及基于 Java 堆内存的数据存储机制。
 *
 * <h2>设计特点</h2>
 * <ul>
 *   <li><b>堆内存存储:</b> 数据存储在 Java 堆内存中,由 GC 自动管理
 *   <li><b>NULL 值管理:</b> 使用 boolean 数组 isNull 跟踪每个位置的 NULL 状态
 *   <li><b>字典编码:</b> 支持可选的字典编码以压缩存储空间
 *   <li><b>UNSAFE 优化:</b> 使用 sun.misc.Unsafe 进行高效的内存操作
 * </ul>
 *
 * <h2>NULL 值处理</h2>
 * <ul>
 *   <li>noNulls=true: 该列没有 NULL 值,可跳过 NULL 检查
 *   <li>isAllNull=true: 该列全是 NULL 值
 *   <li>isNull[i]=true: 第 i 个位置是 NULL
 * </ul>
 *
 * <h2>字典编码支持</h2>
 * <p>当启用字典编码时:
 * <pre>
 * 实际数据: ["Beijing", "Shanghai", "Beijing", ...]
 * 字典: 0->"Beijing", 1->"Shanghai"
 * 存储: [0, 1, 0, ...] (仅存储 ID)
 * </pre>
 *
 * <h2>UNSAFE 常量</h2>
 * <p>提供各种数组类型的基础偏移量,用于高效的批量内存操作:
 * <ul>
 *   <li>BYTE_ARRAY_OFFSET: byte数组的起始偏移
 *   <li>INT_ARRAY_OFFSET: int数组的起始偏移
 *   <li>LONG_ARRAY_OFFSET: long数组的起始偏移
 *   <li>等等
 * </ul>
 *
 * <p><b>注意:</b> 使用 UNSAFE 操作时需要特别小心,错误的使用可能导致 JVM 崩溃。
 *
 * @see AbstractWritableVector 可写向量基类
 * @see ElementCountable 元素计数接口
 * @see org.apache.paimon.data.columnar.writable.WritableColumnVector 可写列向量接口
 */
public abstract class AbstractHeapVector extends AbstractWritableVector
        implements ElementCountable {

    public static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

    public static final sun.misc.Unsafe UNSAFE = MemorySegment.UNSAFE;
    public static final int BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
    public static final int INT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(int[].class);
    public static final int LONG_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(long[].class);
    public static final int FLOAT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(float[].class);
    public static final int DOUBLE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(double[].class);

    /*
     * If hasNulls is true, then this array contains true if the value
     * is null, otherwise false. The array is always allocated, so a batch can be re-used
     * later and nulls added.
     */
    protected boolean[] isNull;

    /** Reusable column for ids of dictionary. */
    protected HeapIntVector dictionaryIds;

    public AbstractHeapVector(int capacity) {
        super(capacity);
        isNull = new boolean[capacity];
    }

    /** Resets the column to default state. - fills the isNull array with false. */
    @Override
    public void reset() {
        super.reset();
        if (isNull.length != capacity) {
            isNull = new boolean[capacity];
        } else {
            Arrays.fill(isNull, false);
        }
        if (dictionaryIds != null) {
            dictionaryIds.reset();
        }
    }

    @Override
    public void setNullAt(int i) {
        isNull[i] = true;
        noNulls = false;
    }

    @Override
    public void setNulls(int i, int count) {
        for (int j = 0; j < count; j++) {
            isNull[i + j] = true;
        }
        if (count > 0) {
            noNulls = false;
        }
    }

    @Override
    public void fillWithNulls() {
        this.noNulls = false;
        Arrays.fill(isNull, true);
    }

    @Override
    public boolean isNullAt(int i) {
        return isAllNull || (!noNulls && isNull[i]);
    }

    @Override
    public HeapIntVector reserveDictionaryIds(int capacity) {
        if (dictionaryIds == null) {
            dictionaryIds = new HeapIntVector(capacity);
        } else {
            if (capacity > dictionaryIds.vector.length) {
                int current = dictionaryIds.vector.length;
                while (current < capacity) {
                    current <<= 1;
                }
                dictionaryIds = new HeapIntVector(current);
            } else {
                dictionaryIds.reset();
            }
        }
        return dictionaryIds;
    }

    /** Returns the underlying integer column for ids of dictionary. */
    @Override
    public HeapIntVector getDictionaryIds() {
        return dictionaryIds;
    }

    @Override
    protected void reserveInternal(int newCapacity) {
        if (isNull.length < newCapacity) {
            isNull = Arrays.copyOf(isNull, newCapacity);
        }
        reserveForHeapVector(newCapacity);
    }

    abstract void reserveForHeapVector(int newCapacity);
}
