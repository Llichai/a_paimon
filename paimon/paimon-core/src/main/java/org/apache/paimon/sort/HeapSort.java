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

/**
 * 堆排序实现。
 *
 * <p>基于 Hadoop 项目的源代码。
 */
public final class HeapSort implements IndexedSorter {

    /**
     * 向下调整堆。
     *
     * @param s 可排序对象
     * @param b 基础索引
     * @param i 起始索引
     * @param n 堆大小
     */
    private static void downHeap(final IndexedSortable s, final int b, int i, final int n) {
        for (int idx = i << 1; idx < n; idx = i << 1) {
            if (idx + 1 < n && s.compare(b + idx, b + idx + 1) < 0) {
                if (s.compare(b + i, b + idx + 1) < 0) {
                    s.swap(b + i, b + idx + 1);
                } else {
                    return;
                }
                i = idx + 1;
            } else if (s.compare(b + i, b + idx) < 0) {
                s.swap(b + i, b + idx);
                i = idx;
            } else {
                return;
            }
        }
    }

    /**
     * 对指定范围进行排序。
     *
     * @param s 可排序对象
     * @param p 起始位置
     * @param r 结束位置
     */
    public void sort(final IndexedSortable s, final int p, final int r) {
        final int n = r - p;
        // build heap w/ reverse comparator, then write in-place from end
        final int t = Integer.highestOneBit(n);
        for (int i = t; i > 1; i >>>= 1) {
            for (int j = i >>> 1; j < i; ++j) {
                downHeap(s, p - 1, j, n + 1);
            }
        }
        for (int i = r - 1; i > p; --i) {
            s.swap(p, i);
            downHeap(s, p - 1, 1, i - p + 1);
        }
    }

    @Override
    public void sort(IndexedSortable s) {
        sort(s, 0, s.size());
    }
}
