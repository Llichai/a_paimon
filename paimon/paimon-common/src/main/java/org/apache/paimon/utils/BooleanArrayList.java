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
 * 基于数组的布尔值列表的最小实现。
 *
 * <p>提供动态扩容的布尔数组,相比使用包装类型的 ArrayList,可以节省内存开销。
 */
public class BooleanArrayList {
    /** 当前列表大小 */
    private int size;

    /** 存储布尔值的数组 */
    private boolean[] array;

    /**
     * 构造指定初始容量的布尔列表。
     *
     * @param capacity 初始容量
     */
    public BooleanArrayList(int capacity) {
        this.size = 0;
        this.array = new boolean[capacity];
    }

    /**
     * 返回列表大小。
     *
     * @return 列表中的元素数量
     */
    public int size() {
        return size;
    }

    /**
     * 向列表末尾添加元素。
     *
     * @param element 要添加的布尔值
     * @return 总是返回 true
     */
    public boolean add(boolean element) {
        grow(size + 1);
        array[size++] = element;
        return true;
    }

    /**
     * 清空列表。
     */
    public void clear() {
        size = 0;
    }

    /**
     * 判断列表是否为空。
     *
     * @return 如果列表为空则返回 true
     */
    public boolean isEmpty() {
        return (size == 0);
    }

    /**
     * 将列表转换为数组。
     *
     * @return 包含列表所有元素的布尔数组
     */
    public boolean[] toArray() {
        return Arrays.copyOf(array, size);
    }

    /**
     * 确保数组容量足够,必要时进行扩容。
     *
     * @param length 需要的最小长度
     */
    private void grow(int length) {
        if (length > array.length) {
            final int newLength =
                    (int) Math.max(Math.min(2L * array.length, Integer.MAX_VALUE - 8), length);
            final boolean[] t = new boolean[newLength];
            System.arraycopy(array, 0, t, 0, size);
            array = t;
        }
    }
}
