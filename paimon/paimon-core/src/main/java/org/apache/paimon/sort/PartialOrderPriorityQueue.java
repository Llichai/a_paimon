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

package org.apache.paimon.sort;

import java.util.AbstractQueue;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Queue;

/**
 * 部分排序优先级队列实现。
 *
 * <p>维护元素的部分排序,使得最小元素始终可以在常量时间内找到。
 * put() 和 pop() 操作需要 log(size) 时间。
 */
public class PartialOrderPriorityQueue<T> extends AbstractQueue<T> implements Queue<T> {
    /** 堆,以数组形式组织 */
    private final T[] heap;

    /** 用于在流之间建立顺序的比较器 */
    private final Comparator<T> comparator;

    /** 堆的最大大小 */
    private final int capacity;

    /** 队列中的当前元素数量 */
    private int size;

    /**
     * 构造部分排序优先级队列。
     *
     * @param comparator 比较器
     * @param capacity 容量
     */
    @SuppressWarnings("unchecked")
    public PartialOrderPriorityQueue(Comparator<T> comparator, int capacity) {
        this.comparator = comparator;
        this.capacity = capacity + 1;
        this.size = 0;
        this.heap = (T[]) new Object[this.capacity];
    }

    /**
     * 确定此优先级队列中对象的顺序。
     *
     * @param a 第一个元素
     * @param b 第二个元素
     * @return a < b 返回true,否则返回false
     */
    private boolean lessThan(T a, T b) {
        return comparator.compare(a, b) < 0;
    }

    /**
     * 返回后备数组的剩余容量。
     *
     * @return 剩余容量
     */
    public int remainingCapacity() {
        return capacity - size;
    }

    /**
     * 在 log(size) 时间内向优先级队列添加缓冲区。
     *
     * <p>如果尝试添加超过 maxSize 的对象,会抛出 RuntimeException (ArrayIndexOutOfBound)。
     *
     * @param element 要添加的元素
     */
    public final void put(T element) {
        size++;
        heap[size] = element;
        upHeap();
    }

    /**
     * 在 log(size) 时间内向优先级队列添加元素。
     *
     * <p>仅在优先级队列未满,或元素不小于 top() 时添加。
     *
     * @param element 要插入的元素
     * @return 添加成功返回true,否则返回false
     */
    public boolean offer(T element) {
        if (size < capacity) {
            put(element);
            return true;
        } else if (size > 0 && !lessThan(element, peek())) {
            heap[1] = element;
            adjustTop();
            return true;
        } else {
            return false;
        }
    }

    /**
     * 在常量时间内返回优先级队列的最小元素,但不从队列中移除它。
     *
     * @return 最小元素
     */
    public final T peek() {
        if (size > 0) {
            return heap[1];
        } else {
            return null;
        }
    }

    /**
     * 在 log(size) 时间内移除并返回优先级队列的最小元素。
     *
     * @return 最小元素
     */
    public final T poll() {
        if (size > 0) {
            T result = heap[1]; // save first value
            heap[1] = heap[size]; // move last to first
            heap[size] = null; // permit GC of objects
            size--;
            downHeap(); // adjust heap
            return result;
        } else {
            return null;
        }
    }

    /**
     * 当顶部对象的值发生变化时应调用此方法。
     */
    public final void adjustTop() {
        downHeap();
    }

    /**
     * 返回当前存储在优先级队列中的元素数量。
     *
     * @return 队列中的元素数量
     */
    public final int size() {
        return size;
    }

    /**
     * 从优先级队列中移除所有条目。
     */
    public final void clear() {
        for (int i = 0; i <= size; i++) {
            heap[i] = null;
        }
        size = 0;
    }

    /**
     * 向上调整堆。
     */
    private void upHeap() {
        int i = size;
        T node = heap[i]; // save bottom node
        int j = i >>> 1;
        while (j > 0 && lessThan(node, heap[j])) {
            heap[i] = heap[j]; // shift parents down
            i = j;
            j = j >>> 1;
        }
        heap[i] = node; // install saved node
    }

    /**
     * 向下调整堆。
     */
    private void downHeap() {
        int i = 1;
        T node = heap[i]; // save top node
        int j = i << 1; // find smaller child
        int k = j + 1;
        if (k <= size && lessThan(heap[k], heap[j])) {
            j = k;
        }

        while (j <= size && lessThan(heap[j], node)) {
            heap[i] = heap[j]; // shift up child
            i = j;
            j = i << 1;
            k = j + 1;
            if (k <= size && lessThan(heap[k], heap[j])) {
                j = k;
            }
        }

        heap[i] = node; // install saved node
    }

    @Override
    public Iterator<T> iterator() {
        return Arrays.asList(heap).iterator();
    }
}
