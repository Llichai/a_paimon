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
import org.apache.paimon.utils.ProjectToRowFunction;
import org.apache.paimon.utils.RowIterator;

import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * 使用旧分区处理器
 *
 * <p>{@link ExistingProcessor} 的实现，用于将新记录路由到旧分区和旧桶。
 *
 * <p>使用场景：
 * <ul>
 *   <li>PARTIAL_UPDATE 合并引擎：主键相同的记录，合并到旧分区
 *   <li>AGGREGATE 合并引擎：主键相同的记录，聚合到旧分区
 *   <li>跨分区更新：主键相同但分区不同时，使用旧分区和旧桶
 * </ul>
 *
 * <p>处理流程：
 * <pre>
 * 新记录（分区B，字段更新）
 *     ↓ 检测到主键已存在于旧分区A
 *     ↓ processExists()
 * 1. 将新记录的分区设置为旧分区A
 * 2. 发送到旧桶
 * 3. 返回 false，不处理新分区的记录
 * （合并引擎会在旧分区合并新旧数据）
 * </pre>
 *
 * <p>批量加载：
 * <ul>
 *   <li>使用升序排序，保证记录顺序
 *   <li>每个主键的第一条记录分配新分区和桶
 *   <li>相同主键的后续记录路由到第一条记录的分区和桶
 * </ul>
 */
public class UseOldExistingProcessor implements ExistingProcessor {

    /** 设置分区字段的函数 */
    private final ProjectToRowFunction setPartition;

    /** 记录收集器，接收处理后的记录和桶号 */
    private final BiConsumer<InternalRow, Integer> collector;

    /**
     * 构造使用旧分区处理器
     *
     * @param setPartition 设置分区字段的函数
     * @param collector 记录收集器
     */
    public UseOldExistingProcessor(
            ProjectToRowFunction setPartition, BiConsumer<InternalRow, Integer> collector) {
        this.setPartition = setPartition;
        this.collector = collector;
    }

    /**
     * 处理已存在的主键
     *
     * <p>执行逻辑：
     * <ol>
     *   <li>将新记录的分区设置为旧分区
     *   <li>发送新记录到旧分区、旧桶
     *   <li>返回 false，表示不需要继续处理新记录
     * </ol>
     *
     * <p>合并引擎会在旧分区合并新旧数据：
     * <ul>
     *   <li>PARTIAL_UPDATE：新字段覆盖旧字段，null 字段保留旧值
     *   <li>AGGREGATE：使用聚合函数合并新旧值
     * </ul>
     *
     * @param newRow 新记录
     * @param previousPart 旧记录所在的分区
     * @param previousBucket 旧记录所在的桶
     * @return false，表示不需要处理新记录
     */
    @Override
    public boolean processExists(InternalRow newRow, BinaryRow previousPart, int previousBucket) {
        // 将新记录的分区设置为旧分区
        InternalRow newValue = setPartition.apply(newRow, previousPart);
        // 发送到旧桶
        collector.accept(newValue, previousBucket);
        // 返回 false，不处理新分区的记录
        return false;
    }

    /**
     * 批量加载新记录
     *
     * <p>处理策略：
     * <ol>
     *   <li>使用升序排序遍历记录
     *   <li>对于每个主键的第一条记录：
     *       <ul>
     *         <li>提取分区并分配新桶
     *         <li>记录分区和桶信息
     *         <li>收集记录
     *       </ul>
     *   <li>对于相同主键的后续记录：
     *       <ul>
     *         <li>将分区设置为第一条记录的分区
     *         <li>路由到第一条记录的桶
     *         <li>收集记录
     *       </ul>
     * </ol>
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
        RowIterator iterator = iteratorFunction.apply(SortOrder.ASCENDING);
        InternalRow row;
        BinaryRow currentKey = null;
        BinaryRow currentPartition = null;
        int currentBucket = -1;
        while ((row = iterator.next()) != null) {
            BinaryRow primaryKey = extractPrimary.apply(row);
            BinaryRow partition = extractPartition.apply(row);
            if (currentKey == null || !currentKey.equals(primaryKey)) {
                // 新主键，分配新分区和桶
                int bucket = assignBucket.apply(partition);
                collector.accept(row, bucket);
                currentKey = primaryKey.copy();
                currentPartition = partition.copy();
                currentBucket = bucket;
            } else {
                // 相同主键，路由到第一条记录的分区和桶
                InternalRow newRow = setPartition.apply(row, currentPartition);
                collector.accept(newRow, currentBucket);
            }
        }
    }
}
