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
import java.util.Comparator;
import java.util.List;

/**
 * 基于败者树实现的归并排序读取器
 *
 * <p>使用 {@link LoserTree} 实现多路归并排序。
 *
 * <p>特点：
 * <ul>
 *   <li>性能优：败者树的调整时间复杂度为 O(log n)
 *   <li>一次批次：与堆排序不同，败者树只产生一个批次
 *   <li>适合大量输入：当输入 reader 数量多时性能优于最小堆
 * </ul>
 *
 * <p>工作原理：
 * <pre>
 * 1. 初始化：构建败者树
 * 2. 读取：从败者树中获取全局胜者（最小的键）
 * 3. 合并：合并所有相同键的记录
 * 4. 调整：调整败者树，准备下一个键
 * </pre>
 *
 * @param <T> 输出类型（KeyValue 或 ChangelogResult）
 */
public class SortMergeReaderWithLoserTree<T> implements SortMergeReader<T> {

    /**
     * 合并函数包装器
     */
    private final MergeFunctionWrapper<T> mergeFunctionWrapper;
    /**
     * 败者树（用于多路归并）
     */
    private final LoserTree<KeyValue> loserTree;

    /**
     * 构造败者树归并排序读取器
     *
     * @param readers                  输入读取器列表
     * @param userKeyComparator        用户键比较器
     * @param userDefinedSeqComparator 用户自定义序列号比较器
     * @param mergeFunctionWrapper     合并函数包装器
     */
    public SortMergeReaderWithLoserTree(
            List<RecordReader<KeyValue>> readers,
            Comparator<InternalRow> userKeyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunctionWrapper) {
        this.mergeFunctionWrapper = mergeFunctionWrapper;
        this.loserTree =
                new LoserTree<>(
                        readers,
                        // 第一级比较器：比较用户键（注意：e2 和 e1 顺序相反，实现升序）
                        (e1, e2) -> userKeyComparator.compare(e2.key(), e1.key()),
                        // 第二级比较器：比较序列号
                        createSequenceComparator(userDefinedSeqComparator));
    }

    /**
     * 创建序列号比较器
     *
     * <p>如果有用户自定义序列号比较器，优先使用；否则使用默认的序列号比较。
     *
     * @param userDefinedSeqComparator 用户自定义序列号比较器
     * @return 序列号比较器
     */
    private Comparator<KeyValue> createSequenceComparator(
            @Nullable FieldsComparator userDefinedSeqComparator) {
        if (userDefinedSeqComparator == null) {
            // 默认比较序列号（降序：e2 > e1 返回正值）
            return (e1, e2) -> Long.compare(e2.sequenceNumber(), e1.sequenceNumber());
        }

        // 自定义比较器：先比较用户定义的字段，再比较序列号
        return (o1, o2) -> {
            int result = userDefinedSeqComparator.compare(o2.value(), o1.value());
            if (result != 0) {
                return result;
            }
            return Long.compare(o2.sequenceNumber(), o1.sequenceNumber());
        };
    }

    /**
     * 读取下一批记录
     *
     * <p>与堆排序不同，{@link LoserTree} 只产生一个批次（整个归并过程）
     *
     * @return 记录迭代器或 null（已读完）
     * @throws IOException IO 异常
     */
    @Nullable
    @Override
    public RecordIterator<T> readBatch() throws IOException {
        loserTree.initializeIfNeeded(); // 初始化败者树
        return loserTree.peekWinner() == null ? null : new SortMergeIterator();
    }

    /**
     * 关闭读取器
     *
     * @throws IOException IO 异常
     */
    @Override
    public void close() throws IOException {
        loserTree.close();
    }

    /**
     * 归并排序迭代器
     *
     * <p>迭代 {@link SortMergeReaderWithLoserTree} 中的合并结果
     */
    private class SortMergeIterator implements RecordIterator<T> {

        /**
         * 是否已释放
         */
        private boolean released = false;

        /**
         * 获取下一个合并结果
         *
         * <p>工作流程：
         * <ol>
         *   <li>调整败者树到下一个键
         *   <li>弹出全局胜者
         *   <li>合并所有相同键的记录
         *   <li>返回合并结果（可能为 null，会继续循环）
         * </ol>
         *
         * @return 合并结果或 null（已读完）
         * @throws IOException IO 异常
         */
        @Nullable
        @Override
        public T next() throws IOException {
            while (true) {
                loserTree.adjustForNextLoop(); // 调整到下一个不同的键
                KeyValue winner = loserTree.popWinner(); // 弹出全局胜者
                if (winner == null) {
                    return null; // 所有记录已处理
                }
                mergeFunctionWrapper.reset(); // 重置合并函数
                mergeFunctionWrapper.add(winner); // 添加第一个记录

                T result = merge(); // 合并所有相同键的记录
                if (result != null) {
                    return result; // 有结果，返回
                }
                // result 为 null（例如被过滤），继续下一个键
            }
        }

        /**
         * 合并所有相同键的记录
         *
         * <p>从败者树中弹出所有相同键的 KeyValue，通过合并函数进行合并
         *
         * @return 合并结果
         */
        private T merge() {
            Preconditions.checkState(
                    !released, "SortMergeIterator#nextImpl is called after release");

            // 弹出所有相同键的记录
            while (loserTree.peekWinner() != null) {
                mergeFunctionWrapper.add(loserTree.popWinner());
            }
            return mergeFunctionWrapper.getResult();
        }

        /**
         * 释放批次
         */
        @Override
        public void releaseBatch() {
            released = true;
        }
    }
}
