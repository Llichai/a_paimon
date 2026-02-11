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

import org.apache.paimon.utils.MutableObjectIterator;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * {@code MergeIterator} 的二进制版本。
 *
 * <p>使用 {@code RecordComparator} 来比较记录。
 */
public class BinaryMergeIterator<Entry> implements MutableObjectIterator<Entry> {

    // heap over the head elements of the stream
    /** 流头元素的堆 */
    private final PartialOrderPriorityQueue<HeadStream<Entry>> heap;
    /** 当前头流 */
    private HeadStream<Entry> currHead;

    /**
     * 构造二进制合并迭代器。
     *
     * @param iterators 迭代器列表
     * @param reusableEntries 可重用条目列表
     * @param comparator 比较器
     * @throws IOException 如果遇到IO问题
     */
    public BinaryMergeIterator(
            List<MutableObjectIterator<Entry>> iterators,
            List<Entry> reusableEntries,
            Comparator<Entry> comparator)
            throws IOException {
        checkArgument(iterators.size() == reusableEntries.size());
        this.heap =
                new PartialOrderPriorityQueue<>(
                        (o1, o2) -> comparator.compare(o1.getHead(), o2.getHead()),
                        iterators.size());
        for (int i = 0; i < iterators.size(); i++) {
            this.heap.add(new HeadStream<>(iterators.get(i), reusableEntries.get(i)));
        }
    }

    @Override
    public Entry next(Entry reuse) throws IOException {
        // Ignore reuse, because each HeadStream has its own reuse BinaryRow.
        return next();
    }

    @Override
    public Entry next() throws IOException {
        if (currHead != null) {
            if (currHead.noMoreHead()) {
                this.heap.poll();
            } else {
                this.heap.adjustTop();
            }
        }

        if (this.heap.size() > 0) {
            currHead = this.heap.peek();
            return currHead.getHead();
        } else {
            return null;
        }
    }

    /**
     * 头流,保存迭代器和当前头元素。
     *
     * @param <Entry> 条目类型
     */
    private static final class HeadStream<Entry> {

        /** 条目迭代器 */
        private final MutableObjectIterator<Entry> iterator;
        /** 头元素 */
        private Entry head;

        /**
         * 构造头流。
         *
         * @param iterator 条目迭代器
         * @param head 头元素
         * @throws IOException 如果遇到IO问题
         */
        private HeadStream(MutableObjectIterator<Entry> iterator, Entry head) throws IOException {
            this.iterator = iterator;
            this.head = head;
            if (noMoreHead()) {
                throw new IllegalStateException();
            }
        }

        /**
         * 获取头元素。
         *
         * @return 头元素
         */
        private Entry getHead() {
            return this.head;
        }

        /**
         * 检查是否没有更多头元素。
         *
         * @return 没有更多元素返回true
         * @throws IOException 如果遇到IO问题
         */
        private boolean noMoreHead() throws IOException {
            return (this.head = this.iterator.next(head)) == null;
        }
    }
}
