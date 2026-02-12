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
import java.util.NoSuchElementException;

/**
 * 基于数组的 int 类型列表的最小实现。
 *
 * <p>提供了一个高性能的 int 类型列表实现，避免了自动装箱的开销。
 * 适用于需要频繁操作整数列表的场景。
 *
 * <h3>性能优化:</h3>
 * <ul>
 *   <li>直接使用 int[] 数组存储，避免装箱为 Integer
 *   <li>数组自动扩容，扩容策略为 2 倍增长
 *   <li>最大容量限制为 Integer.MAX_VALUE - 8
 * </ul>
 *
 * <h3>使用示例:</h3>
 * <pre>{@code
 * IntArrayList list = new IntArrayList(10);
 * list.add(1);
 * list.add(2);
 * list.add(3);
 * int value = list.get(0);  // 返回 1
 * int last = list.removeLast();  // 返回 3
 * }</pre>
 */
public class IntArrayList {

    /** 列表中元素的数量 */
    private int size;

    /** 存储元素的数组 */
    private int[] array;

    /**
     * 创建一个指定初始容量的 IntArrayList。
     *
     * @param capacity 初始容量
     */
    public IntArrayList(final int capacity) {
        this.size = 0;
        this.array = new int[capacity];
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
     * 向列表末尾添加一个整数。
     *
     * <p>如果数组容量不足，会自动扩容。
     *
     * @param number 要添加的整数
     * @return 总是返回 true
     */
    public boolean add(final int number) {
        grow(size + 1);
        array[size++] = number;
        return true;
    }

    /**
     * 移除并返回列表中的最后一个元素。
     *
     * @return 被移除的最后一个元素
     * @throws NoSuchElementException 如果列表为空
     */
    public int removeLast() {
        if (size == 0) {
            throw new NoSuchElementException();
        }
        --size;
        return array[size];
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
        return size == 0;
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
    private void grow(final int length) {
        if (length > array.length) {
            final int newLength =
                    (int) Math.max(Math.min(2L * array.length, Integer.MAX_VALUE - 8), length);
            final int[] t = new int[newLength];
            System.arraycopy(array, 0, t, 0, size);
            array = t;
        }
    }

    /**
     * 将列表转换为 int 数组。
     *
     * @return 包含列表所有元素的新数组
     */
    public int[] toArray() {
        return Arrays.copyOf(array, size);
    }

    /**
     * 获取指定索引处的元素。
     *
     * @param i 元素索引
     * @return 指定索引处的元素值
     */
    public int get(int i) {
        return array[i];
    }

    /**
     * 空列表常量。
     *
     * <p>这是一个不可修改的空列表，任何添加或移除操作都会抛出 UnsupportedOperationException。
     */
    public static final IntArrayList EMPTY =
            new IntArrayList(0) {

                @Override
                public boolean add(int number) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public int removeLast() {
                    throw new UnsupportedOperationException();
                }
            };
}
