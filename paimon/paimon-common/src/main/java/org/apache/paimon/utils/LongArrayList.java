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

package org.apache.paimon.utils;

import java.util.Arrays;

/**
 * 基于数组的 long 类型列表的最小实现。
 *
 * <p>提供了一个高性能的 long 类型列表实现，避免了自动装箱的开销。
 * 适用于需要频繁操作长整数列表的场景，如存储时间戳、ID 等。
 *
 * <h3>性能优化:</h3>
 * <ul>
 *   <li>直接使用 long[] 数组存储，避免装箱为 Long
 *   <li>数组自动扩容，扩容策略为 2 倍增长
 *   <li>最大容量限制为 Integer.MAX_VALUE - 8
 * </ul>
 *
 * <h3>使用示例:</h3>
 * <pre>{@code
 * LongArrayList list = new LongArrayList(10);
 * list.add(100L);
 * list.add(200L);
 * list.add(300L);
 * long value = list.get(0);  // 返回 100L
 * long removed = list.removeLong(1);  // 移除并返回 200L
 * }</pre>
 */
public class LongArrayList {

    /** 列表中元素的数量 */
    private int size;

    /** 存储元素的数组 */
    private long[] array;

    /**
     * 创建一个指定初始容量的 LongArrayList。
     *
     * @param capacity 初始容量
     */
    public LongArrayList(int capacity) {
        this.size = 0;
        this.array = new long[capacity];
    }

    /**
     * 获取列表中元素的数量。
     *
     * @return 元素数量
     */
    public int size() {
        return size;
    }

    /**
     * 向列表末尾添加一个长整数。
     *
     * <p>如果数组容量不足，会自动扩容。
     *
     * @param number 要添加的长整数
     * @return 总是返回 true
     */
    public boolean add(long number) {
        grow(size + 1);
        array[size++] = number;
        return true;
    }

    /**
     * 获取指定索引处的元素。
     *
     * @param index 元素索引
     * @return 指定索引处的元素值
     * @throws IndexOutOfBoundsException 如果索引超出范围
     */
    public long get(int index) {
        rangeCheck(index);
        return array[index];
    }

    /**
     * 移除并返回指定索引处的元素。
     *
     * <p>移除元素后，后面的元素会向前移动。
     *
     * @param index 要移除的元素索引
     * @return 被移除的元素值
     * @throws IndexOutOfBoundsException 如果索引超出范围
     */
    public long removeLong(int index) {
        if (index >= size) {
            throw new IndexOutOfBoundsException(
                    "Index (" + index + ") is greater than or equal to list size (" + size + ")");
        }
        final long old = array[index];
        size--;
        if (index != size) {
            System.arraycopy(array, index + 1, array, index, size - index);
        }
        return old;
    }

    /**
     * 清空列表中的所有元素。
     *
     * <p>注意：此操作不会释放底层数组的内存。
     */
    public void clear() {
        size = 0;
    }

    /**
     * 检查列表是否为空。
     *
     * @return 如果列表为空则返回 true，否则返回 false
     */
    public boolean isEmpty() {
        return (size == 0);
    }

    /**
     * 将列表转换为 long 数组。
     *
     * @return 包含列表所有元素的新数组
     */
    public long[] toArray() {
        return Arrays.copyOf(array, size);
    }

    /**
     * 确保数组容量足够。
     *
     * <p>扩容策略：
     * <ul>
     *   <li>新容量为当前容量的 2 倍
     *   <li>最大容量为 Integer.MAX_VALUE - 8
     *   <li>如果 2 倍容量不足，则使用所需的最小容量
     * </ul>
     *
     * @param length 所需的最小容量
     */
    private void grow(int length) {
        if (length > array.length) {
            final int newLength =
                    (int) Math.max(Math.min(2L * array.length, Integer.MAX_VALUE - 8), length);
            final long[] t = new long[newLength];
            System.arraycopy(array, 0, t, 0, size);
            array = t;
        }
    }

    /**
     * 检查索引是否在有效范围内。
     *
     * @param index 要检查的索引
     * @throws IndexOutOfBoundsException 如果索引超出范围
     */
    private void rangeCheck(int index) {
        if (index >= size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
    }
}
