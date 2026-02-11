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

import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.ProjectToRowFunction;
import org.apache.paimon.utils.RowIterator;

import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * 现有记录处理器接口
 *
 * <p>处理跨分区更新场景中主键已存在的记录。
 *
 * <p>核心功能：
 * <ul>
 *   <li>冲突处理：当新记录的主键已存在于其他分区时，决定如何处理
 *   <li>批量加载：在没有索引的情况下，批量处理新记录
 * </ul>
 *
 * <p>实现策略（根据合并引擎）：
 * <ul>
 *   <li>{@link DeleteExistingProcessor}：DEDUPLICATE 模式，删除旧记录
 *   <li>{@link UseOldExistingProcessor}：PARTIAL_UPDATE/AGGREGATE 模式，使用旧分区
 *   <li>{@link SkipNewExistingProcessor}：FIRST_ROW 模式，跳过新记录
 * </ul>
 *
 * <p>处理场景：
 * <pre>
 * 场景1：有索引（在线处理）
 *   新记录 -> 查询索引 -> 发现主键存在 -> processExists()
 *
 * 场景2：无索引（批量加载）
 *   所有记录 -> 排序 -> bulkLoadNewRecords() -> 去重/合并
 * </pre>
 */
public interface ExistingProcessor {

    /**
     * 处理已存在的主键
     *
     * <p>当检测到新记录的主键已存在于其他分区时调用。
     *
     * <p>不同实现的处理方式：
     * <ul>
     *   <li>DEDUPLICATE：删除旧记录，返回 true 继续处理新记录
     *   <li>PARTIAL_UPDATE/AGGREGATE：使用旧分区和桶，返回 false 跳过新记录
     *   <li>FIRST_ROW：保留旧记录，返回 false 跳过新记录
     * </ul>
     *
     * @param newRow 新记录
     * @param previousPart 旧记录所在的分区
     * @param previousBucket 旧记录所在的桶
     * @return true 如果应该继续处理新记录，false 如果应该跳过新记录
     */
    boolean processExists(InternalRow newRow, BinaryRow previousPart, int previousBucket);

    /**
     * 批量加载新记录（无索引场景）
     *
     * <p>在没有索引的情况下，通过排序和去重来处理新记录。
     *
     * <p>执行流程：
     * <ol>
     *   <li>根据排序顺序（升序/降序）获取记录迭代器
     *   <li>按主键分组处理记录
     *   <li>根据策略决定每个主键保留哪条记录
     *   <li>分配分区和桶
     *   <li>输出最终记录
     * </ol>
     *
     * @param iteratorFunction 排序迭代器函数，根据 SortOrder 返回相应排序的迭代器
     * @param extractPrimary 提取主键的函数
     * @param extractPartition 提取分区的函数
     * @param assignBucket 分配桶的函数
     */
    void bulkLoadNewRecords(
            Function<SortOrder, RowIterator> iteratorFunction,
            Function<InternalRow, BinaryRow> extractPrimary,
            Function<InternalRow, BinaryRow> extractPartition,
            Function<BinaryRow, Integer> assignBucket);

    /**
     * 批量加载时"收集优先"策略
     *
     * <p>对于每个主键，只收集第一条遇到的记录。
     *
     * <p>适用场景：
     * <ul>
     *   <li>DEDUPLICATE + 降序：保留最新记录
     *   <li>FIRST_ROW + 升序：保留最早记录
     * </ul>
     *
     * <p>执行逻辑：
     * <ol>
     *   <li>遍历排序后的记录
     *   <li>对于每个新主键，提取分区并分配桶
     *   <li>收集记录（主键已存在则跳过）
     * </ol>
     *
     * @param collector 记录收集器
     * @param iterator 记录迭代器（已排序）
     * @param extractPrimary 提取主键的函数
     * @param extractPartition 提取分区的函数
     * @param assignBucket 分配桶的函数
     */
    static void bulkLoadCollectFirst(
            BiConsumer<InternalRow, Integer> collector,
            RowIterator iterator,
            Function<InternalRow, BinaryRow> extractPrimary,
            Function<InternalRow, BinaryRow> extractPartition,
            Function<BinaryRow, Integer> assignBucket) {
        InternalRow row;
        BinaryRow currentKey = null;
        while ((row = iterator.next()) != null) {
            BinaryRow primaryKey = extractPrimary.apply(row);
            // 主键不同，说明是新主键，收集记录
            if (currentKey == null || !currentKey.equals(primaryKey)) {
                collector.accept(row, assignBucket.apply(extractPartition.apply(row)));
                currentKey = primaryKey.copy();
            }
            // 主键相同，跳过（已经收集了第一条）
        }
    }

    /**
     * 创建现有记录处理器
     *
     * <p>根据合并引擎类型选择合适的处理器实现。
     *
     * @param mergeEngine 合并引擎类型
     * @param setPartition 设置分区字段的函数
     * @param bucketAssigner 桶分配器
     * @param collector 记录收集器
     * @return 对应的处理器实现
     * @throws UnsupportedOperationException 如果合并引擎不支持
     */
    static ExistingProcessor create(
            MergeEngine mergeEngine,
            ProjectToRowFunction setPartition,
            BucketAssigner bucketAssigner,
            BiConsumer<InternalRow, Integer> collector) {
        switch (mergeEngine) {
            case DEDUPLICATE:
                // 去重模式：删除旧记录
                return new DeleteExistingProcessor(setPartition, bucketAssigner, collector);
            case PARTIAL_UPDATE:
            case AGGREGATE:
                // 部分更新/聚合模式：使用旧分区
                return new UseOldExistingProcessor(setPartition, collector);
            case FIRST_ROW:
                // 首行模式：跳过新记录
                return new SkipNewExistingProcessor(collector);
            default:
                throw new UnsupportedOperationException("Unsupported engine: " + mergeEngine);
        }
    }

    /**
     * 输入排序顺序枚举
     *
     * <p>用于批量加载时指定记录的排序方式。
     */
    enum SortOrder {
        /** 升序：最早的记录排在前面 */
        ASCENDING,
        /** 降序：最新的记录排在前面 */
        DESCENDING,
    }
}
