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

package org.apache.paimon.table.source;

/**
 * 数据表扫描接口，用于扫描数据表（主键表和追加表）。
 *
 * <p>DataTableScan 是数据表的专用扫描接口，扩展了 {@link InnerTableScan}，
 * 提供了分片（shard）功能，用于并行读取场景。
 *
 * <h3>与 InnerTableScan 的关系</h3>
 * <ul>
 *   <li><b>InnerTableScan</b>: 通用扫描接口，提供过滤、分区等基础功能</li>
 *   <li><b>DataTableScan</b>: 数据表扫描，增加了分片（shard）功能，用于并行任务</li>
 * </ul>
 *
 * <h3>分片（Shard）功能</h3>
 * <p>分片用于将扫描工作分配给多个并行任务：
 * <ul>
 *   <li>每个任务负责扫描部分桶（bucket）</li>
 *   <li>通过 {@link #withShard(int, int)} 指定当前任务的分片信息</li>
 *   <li>适用于 Flink、Spark 等分布式计算引擎</li>
 * </ul>
 *
 * <h3>实现类</h3>
 * <ul>
 *   <li>{@link DataTableBatchScan}: 批量扫描实现（一次性读取完整快照）</li>
 *   <li>{@link DataTableStreamScan}: 流式扫描实现（持续跟踪新快照）</li>
 *   <li>{@link AbstractDataTableScan}: 抽象基类，提供通用功能</li>
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 批量扫描（在并行任务中）
 * DataTableScan scan = table.newReadBuilder()
 *     .withFilter(predicate)
 *     .newScan();
 *
 * // 设置分片信息（任务 2/10）
 * scan.withShard(2, 10);
 *
 * // 生成计划（只包含分配给任务 2 的桶）
 * Plan plan = scan.plan();
 * }</pre>
 *
 * @see InnerTableScan 基础扫描接口
 * @see DataTableBatchScan 批量扫描实现
 * @see DataTableStreamScan 流式扫描实现
 */
public interface DataTableScan extends InnerTableScan {

    /**
     * 指定要读取的分片（shard），分配分片的文件供读取。
     *
     * <p>在分布式计算中，多个并行任务共同读取一个表，每个任务负责一部分数据。
     * 该方法用于指定当前任务的分片信息，确保每个任务只读取分配给它的桶。
     *
     * <h3>分片分配逻辑</h3>
     * <p>桶（bucket）按照以下规则分配给任务：
     * <pre>
     * 如果 bucket % numberOfParallelSubtasks == indexOfThisSubtask，
     * 则该桶分配给当前任务
     * </pre>
     *
     * <h3>示例</h3>
     * <pre>{@code
     * // 表有 10 个桶（bucket 0-9），有 3 个并行任务
     * // 任务 0: 读取桶 0, 3, 6, 9
     * scan.withShard(0, 3);
     *
     * // 任务 1: 读取桶 1, 4, 7
     * scan.withShard(1, 3);
     *
     * // 任务 2: 读取桶 2, 5, 8
     * scan.withShard(2, 3);
     * }</pre>
     *
     * @param indexOfThisSubtask 当前任务的索引（从 0 开始）
     * @param numberOfParallelSubtasks 并行任务的总数
     * @return this
     */
    DataTableScan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks);
}
