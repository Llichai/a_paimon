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
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.ProjectToRowFunction;
import org.apache.paimon.utils.RowIterator;

import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * 删除现有记录处理器
 *
 * <p>{@link ExistingProcessor} 的实现，用于删除旧分区的记录。
 *
 * <p>使用场景：
 * <ul>
 *   <li>DEDUPLICATE 合并引擎：主键相同的记录，删除旧记录，保留新记录
 *   <li>跨分区更新：主键相同但分区不同时，删除旧分区的记录
 * </ul>
 *
 * <p>处理流程：
 * <pre>
 * 新记录（分区B）
 *     ↓ 检测到主键已存在于旧分区A
 *     ↓ processExists()
 * 1. 生成删除记录（-D）发送到旧分区A、旧桶
 * 2. 减少旧桶的记录计数
 * 3. 返回 true，继续处理新记录
 * </pre>
 *
 * <p>批量加载：
 * <ul>
 *   <li>使用降序排序，保证每个主键只保留最后一条记录
 *   <li>采用"收集优先"策略，每个主键只输出第一条遇到的记录
 * </ul>
 */
public class DeleteExistingProcessor implements ExistingProcessor {

    /** 设置分区字段的函数 */
    private final ProjectToRowFunction setPartition;

    /** 桶分配器，用于管理桶的记录计数 */
    private final BucketAssigner bucketAssigner;

    /** 记录收集器，接收处理后的记录和桶号 */
    private final BiConsumer<InternalRow, Integer> collector;

    /**
     * 构造删除现有记录处理器
     *
     * @param setPartition 设置分区字段的函数
     * @param bucketAssigner 桶分配器
     * @param collector 记录收集器
     */
    public DeleteExistingProcessor(
            ProjectToRowFunction setPartition,
            BucketAssigner bucketAssigner,
            BiConsumer<InternalRow, Integer> collector) {
        this.setPartition = setPartition;
        this.bucketAssigner = bucketAssigner;
        this.collector = collector;
    }

    /**
     * 处理已存在的主键
     *
     * <p>执行逻辑：
     * <ol>
     *   <li>将新记录的分区设置为旧分区
     *   <li>设置 RowKind 为 DELETE
     *   <li>发送删除记录到旧分区、旧桶
     *   <li>减少旧桶的记录计数
     *   <li>返回 true，表示需要继续处理新记录
     * </ol>
     *
     * @param newRow 新记录
     * @param previousPart 旧记录所在的分区
     * @param previousBucket 旧记录所在的桶
     * @return true，表示应该继续处理新记录
     */
    @Override
    public boolean processExists(InternalRow newRow, BinaryRow previousPart, int previousBucket) {
        // 生成删除记录：将新记录的分区设置为旧分区
        InternalRow retract = setPartition.apply(newRow, previousPart);
        retract.setRowKind(RowKind.DELETE);
        collector.accept(retract, previousBucket);

        // 减少旧桶的记录计数
        bucketAssigner.decrement(previousPart, previousBucket);

        // 返回 true，继续处理新记录
        return true;
    }

    /**
     * 批量加载新记录
     *
     * <p>处理策略：
     * <ul>
     *   <li>使用降序排序，保证每个主键的最新记录排在前面
     *   <li>对于每个主键，只收集第一条遇到的记录（最新的）
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
        // 使用降序排序，每个主键只保留最后一条记录
        ExistingProcessor.bulkLoadCollectFirst(
                collector,
                iteratorFunction.apply(SortOrder.DESCENDING),
                extractPrimary,
                extractPartition,
                assignBucket);
    }
}
