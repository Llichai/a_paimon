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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * 基于最小堆实现的归并排序读取器
 *
 * <p>使用 Java 的 {@link PriorityQueue} 实现多路归并排序。
 *
 * <p>特点：
 * <ul>
 *   <li>实现简单：使用标准的优先队列
 *   <li>批次管理：每个批次需要管理读取器状态
 *   <li>适合少量输入：当输入 reader 数量少时性能良好
 * </ul>
 *
 * <p>工作原理：
 * <pre>
 * 1. 初始化：从每个 reader 读取第一个元素，加入最小堆
 * 2. 读取：从最小堆中取出最小的键
 * 3. 合并：取出所有相同键的元素进行合并
 * 4. 更新：从对应 reader 读取下一个元素，重新加入堆
 * </pre>
 *
 * @param <T> 输出类型（KeyValue 或 ChangelogResult）
 */
public class SortMergeReaderWithMinHeap<T> implements SortMergeReader<T> {

    /** 下一批次的读取器列表（需要重新读取的 reader） */
    private final List<RecordReader<KeyValue>> nextBatchReaders;
    /** 用户键比较器 */
    private final Comparator<InternalRow> userKeyComparator;
    /** 合并函数包装器 */
    private final MergeFunctionWrapper<T> mergeFunctionWrapper;

    /** 最小堆（优先队列） */
    private final PriorityQueue<Element> minHeap;
    /** 已弹出的元素列表（待更新） */
    private final List<Element> polled;

    /**
     * 构造最小堆归并排序读取器
     *
     * @param readers 输入读取器列表
     * @param userKeyComparator 用户键比较器
     * @param userDefinedSeqComparator 用户自定义序列号比较器
     * @param mergeFunctionWrapper 合并函数包装器
     */
    public SortMergeReaderWithMinHeap(
            List<RecordReader<KeyValue>> readers,
            Comparator<InternalRow> userKeyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunctionWrapper) {
        this.nextBatchReaders = new ArrayList<>(readers);
        this.userKeyComparator = userKeyComparator;
        this.mergeFunctionWrapper = mergeFunctionWrapper;

        // 创建最小堆（优先队列）
        this.minHeap =
                new PriorityQueue<>(
                        (e1, e2) -> {
                            // 第一级：比较用户键
                            int result = userKeyComparator.compare(e1.kv.key(), e2.kv.key());
                            if (result != 0) {
                                return result;
                            }
                            // 第二级：比较用户自定义序列字段（如果有）
                            if (userDefinedSeqComparator != null) {
                                result =
                                        userDefinedSeqComparator.compare(
                                                e1.kv.value(), e2.kv.value());
                                if (result != 0) {
                                    return result;
                                }
                            }
                            // 第三级：比较序列号
                            return Long.compare(e1.kv.sequenceNumber(), e2.kv.sequenceNumber());
                        });
        this.polled = new ArrayList<>();
    }

    /**
     * 读取下一批记录
     *
     * <p>工作流程：
     * <ol>
     *   <li>从每个 reader 读取第一个有效的 KeyValue
     *   <li>将每个 KeyValue 封装为 Element 并加入最小堆
     *   <li>清空 nextBatchReaders（等待后续批次填充）
     * </ol>
     *
     * @return 记录迭代器或 null（已读完）
     * @throws IOException IO 异常
     */
    @Nullable
    @Override
    public RecordIterator<T> readBatch() throws IOException {
        for (RecordReader<KeyValue> reader : nextBatchReaders) {
            while (true) {
                RecordIterator<KeyValue> iterator = reader.readBatch();
                if (iterator == null) {
                    // 没有更多批次，永久移除此 reader
                    reader.close();
                    break;
                }
                KeyValue kv = iterator.next();
                if (kv == null) {
                    // 空迭代器，清理并尝试下一批次
                    iterator.releaseBatch();
                } else {
                    // 找到下一个 kv，加入最小堆
                    minHeap.offer(new Element(kv, iterator, reader));
                    break;
                }
            }
        }
        nextBatchReaders.clear();

        return minHeap.isEmpty() ? null : new SortMergeIterator();
    }

    /**
     * 关闭读取器
     *
     * @throws IOException IO 异常
     */
    @Override
    public void close() throws IOException {
        for (RecordReader<KeyValue> reader : nextBatchReaders) {
            reader.close();
        }
        for (Element element : minHeap) {
            element.iterator.releaseBatch();
            element.reader.close();
        }
        for (Element element : polled) {
            element.iterator.releaseBatch();
            element.reader.close();
        }
    }

    /**
     * 归并排序迭代器
     *
     * <p>迭代 {@link SortMergeReaderWithMinHeap} 中的合并结果
     */
    private class SortMergeIterator implements RecordIterator<T> {

        /** 是否已释放 */
        private boolean released = false;

        /**
         * 获取下一个合并结果
         *
         * <p>循环处理直到找到有效结果或全部读完
         *
         * @return 合并结果或 null（已读完）
         * @throws IOException IO 异常
         */
        @Override
        public T next() throws IOException {
            while (true) {
                boolean hasMore = nextImpl(); // 处理下一个键
                if (!hasMore) {
                    return null; // 当前批次已读完
                }
                T result = mergeFunctionWrapper.getResult();
                if (result != null) {
                    return result; // 有结果，返回
                }
                // result 为 null（例如被过滤），继续下一个键
            }
        }

        /**
         * 处理下一个键的实现
         *
         * <p>工作流程：
         * <ol>
         *   <li>更新之前弹出的元素（从对应 iterator 读取下一个值）
         *   <li>如果某个 reader 的批次读完，标记需要读取下一批次
         *   <li>如果有 reader 需要读取下一批次，结束当前批次
         *   <li>从堆中取出所有相同键的元素进行合并
         * </ol>
         *
         * @return 是否有更多数据（false 表示当前批次已结束）
         * @throws IOException IO 异常
         */
        private boolean nextImpl() throws IOException {
            Preconditions.checkState(
                    !released, "SortMergeIterator#advanceNext is called after release");
            Preconditions.checkState(
                    nextBatchReaders.isEmpty(),
                    "SortMergeIterator#advanceNext is called even if the last call returns null. "
                            + "This is a bug.");

            // 步骤1：将之前弹出的元素更新后重新加入堆
            for (Element element : polled) {
                if (element.update()) {
                    // 还有 kv，重新加入优先队列
                    minHeap.offer(element);
                } else {
                    // 到达批次末尾，清理并标记需要读取下一批次
                    element.iterator.releaseBatch();
                    nextBatchReaders.add(element.reader);
                }
            }
            polled.clear();

            // 步骤2：如果有 reader 到达批次末尾，结束当前批次
            if (!nextBatchReaders.isEmpty()) {
                return false;
            }

            // 步骤3：从堆中取出所有相同键的元素
            mergeFunctionWrapper.reset();
            InternalRow key =
                    Preconditions.checkNotNull(minHeap.peek(), "Min heap is empty. This is a bug.")
                            .kv
                            .key();

            // 获取所有相同键的元素
            // 注意：同一个 iterator 不应该产生相同的键，所以这段代码是正确的
            while (!minHeap.isEmpty()) {
                Element element = minHeap.peek();
                if (userKeyComparator.compare(key, element.kv.key()) != 0) {
                    break; // 键不同，停止
                }
                minHeap.poll(); // 弹出元素
                mergeFunctionWrapper.add(element.kv); // 添加到合并函数
                polled.add(element); // 记录已弹出的元素
            }
            return true;
        }

        /**
         * 释放批次
         */
        @Override
        public void releaseBatch() {
            released = true;
        }
    }

    /**
     * 堆元素
     *
     * <p>封装 KeyValue、迭代器和读取器
     */
    private static class Element {
        /** 当前 KeyValue */
        private KeyValue kv;
        /** 批次迭代器 */
        private final RecordIterator<KeyValue> iterator;
        /** 记录读取器 */
        private final RecordReader<KeyValue> reader;

        /**
         * 构造元素
         *
         * @param kv 当前 KeyValue
         * @param iterator 批次迭代器
         * @param reader 记录读取器
         */
        private Element(
                KeyValue kv, RecordIterator<KeyValue> iterator, RecordReader<KeyValue> reader) {
            this.kv = kv;
            this.iterator = iterator;
            this.reader = reader;
        }

        /**
         * 更新到下一个 KeyValue
         *
         * <p>重要：不能对优先队列中的元素调用此方法！
         *
         * @return 是否有下一个值
         * @throws IOException IO 异常
         */
        private boolean update() throws IOException {
            KeyValue nextKv = iterator.next();
            if (nextKv == null) {
                return false; // 批次已读完
            }
            kv = nextKv;
            return true;
        }
    }
}
