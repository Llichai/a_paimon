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

import org.apache.paimon.CoreOptions.SortEngine;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.FieldsComparator;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.List;

/**
 * 多路归并排序读取器
 *
 * <p>读取一组已按键和序列号排序的 {@link RecordReader}，执行归并排序算法。
 * 相同键的 {@link KeyValue} 会在归并过程中通过 {@link MergeFunctionWrapper} 进行合并。
 *
 * <p>重要约束：
 * <ul>
 *   <li>同一个 {@link RecordReader} 中的 {@link KeyValue} 不能包含相同的键
 *   <li>输入的 RecordReader 列表已按键和序列号排序
 * </ul>
 *
 * <p>两种实现：
 * <ul>
 *   <li>{@link SortMergeReaderWithMinHeap}：基于最小堆的归并实现
 *   <li>{@link SortMergeReaderWithLoserTree}：基于败者树的归并实现（性能更优）
 * </ul>
 *
 * <p>工作原理：
 * <pre>
 * 1. 从多个 RecordReader 中读取记录
 * 2. 使用排序算法（最小堆/败者树）找到最小的键
 * 3. 合并相同键的所有版本（使用 MergeFunctionWrapper）
 * 4. 输出合并后的结果
 * </pre>
 *
 * @param <T> 输出类型（KeyValue 或 ChangelogResult）
 */
public interface SortMergeReader<T> extends RecordReader<T> {

    /**
     * 创建归并排序读取器
     *
     * <p>根据配置的排序引擎选择不同的实现：
     * <ul>
     *   <li>MIN_HEAP：最小堆实现（适合少量输入）
     *   <li>LOSER_TREE：败者树实现（适合大量输入，性能更优）
     * </ul>
     *
     * @param readers 输入的记录读取器列表（已排序）
     * @param userKeyComparator 用户键比较器
     * @param userDefinedSeqComparator 用户自定义序列号比较器（可选）
     * @param mergeFunctionWrapper 合并函数包装器
     * @param sortEngine 排序引擎类型
     * @param <T> 输出类型
     * @return 归并排序读取器
     */
    static <T> SortMergeReader<T> createSortMergeReader(
            List<RecordReader<KeyValue>> readers,
            Comparator<InternalRow> userKeyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunctionWrapper,
            SortEngine sortEngine) {
        switch (sortEngine) {
            case MIN_HEAP:
                return new SortMergeReaderWithMinHeap<>(
                        readers, userKeyComparator, userDefinedSeqComparator, mergeFunctionWrapper);
            case LOSER_TREE:
                return new SortMergeReaderWithLoserTree<>(
                        readers, userKeyComparator, userDefinedSeqComparator, mergeFunctionWrapper);
            default:
                throw new UnsupportedOperationException("Unsupported sort engine: " + sortEngine);
        }
    }
}
