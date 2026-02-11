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

package org.apache.paimon.crosspartition;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.RowIterator;

import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * 跳过新记录处理器
 *
 * <p>{@link ExistingProcessor} 的实现，用于跳过新记录，保留旧记录。
 *
 * <p>使用场景：
 * <ul>
 *   <li>FIRST_ROW 合并引擎：主键相同的记录，保留第一条（最早的）记录
 *   <li>跨分区更新：主键相同但分区不同时，保留旧分区的记录
 * </ul>
 *
 * <p>处理流程：
 * <pre>
 * 新记录（分区B）
 *     ↓ 检测到主键已存在于旧分区A
 *     ↓ processExists()
 * 返回 false，跳过新记录
 * （旧记录保留在旧分区A、旧桶）
 * </pre>
 *
 * <p>批量加载：
 * <ul>
 *   <li>使用升序排序，保证每个主键的第一条记录排在前面
 *   <li>采用"收集优先"策略，每个主键只输出第一条遇到的记录
 * </ul>
 */
public class SkipNewExistingProcessor implements ExistingProcessor {

    /** 记录收集器，接收处理后的记录和桶号 */
    private final BiConsumer<InternalRow, Integer> collector;

    /**
     * 构造跳过新记录处理器
     *
     * @param collector 记录收集器
     */
    public SkipNewExistingProcessor(BiConsumer<InternalRow, Integer> collector) {
        this.collector = collector;
    }

    /**
     * 处理已存在的主键
     *
     * <p>FIRST_ROW 模式：保留旧记录，跳过新记录。
     *
     * @param newRow 新记录（被跳过）
     * @param previousPart 旧记录所在的分区（保留）
     * @param previousBucket 旧记录所在的桶（保留）
     * @return false，表示应该跳过新记录
     */
    @Override
    public boolean processExists(InternalRow newRow, BinaryRow previousPart, int previousBucket) {
        // 跳过新记录，保留旧记录
        return false;
    }

    /**
     * 批量加载新记录
     *
     * <p>处理策略：
     * <ul>
     *   <li>使用升序排序，保证每个主键的最早记录排在前面
     *   <li>对于每个主键，只收集第一条遇到的记录（最早的）
     * </ul>
     *
     * @param iteratorFunction 排序迭代器函数
     * @param extractPrimary 提取主键的函数
     * @param extractPartition 提取分区的函数
     * @param assignBucket 分配桶的函数
     */
    @Override
    public void bulkLoadNewRecords(
            Function<SortOrder, RowIterator> iteratorFunction,
            Function<InternalRow, BinaryRow> extractPrimary,
            Function<InternalRow, BinaryRow> extractPartition,
            Function<BinaryRow, Integer> assignBucket) {
        // 使用升序排序，每个主键只保留第一条记录
        ExistingProcessor.bulkLoadCollectFirst(
                collector,
                iteratorFunction.apply(SortOrder.ASCENDING),
                extractPrimary,
                extractPartition,
                assignBucket);
    }
}
