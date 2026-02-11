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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.table.Table;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 表扫描接口，用于生成读取分片（{@link Split}）的扫描计划。
 *
 * <p>TableScan 是 Table 层的顶层扫描接口，位于 {@link org.apache.paimon.operation.FileStoreScan} 之上，
 * 提供了更高级别的抽象和用户友好的 API。
 *
 * <h3>与 FileStoreScan 的关系</h3>
 * <ul>
 *   <li><b>FileStoreScan</b>: 底层接口，直接操作 Manifest 和数据文件，返回 {@link org.apache.paimon.manifest.ManifestEntry}</li>
 *   <li><b>TableScan</b>: 高层接口，封装 FileStoreScan，返回用户友好的 {@link Split}（包含分区、桶、数据文件等信息）</li>
 * </ul>
 *
 * <h3>主要功能</h3>
 * <ul>
 *   <li>生成扫描计划（{@link Plan}），包含多个 {@link Split}</li>
 *   <li>每个 Split 可被独立读取（用于并行读取）</li>
 *   <li>支持分区列举功能</li>
 *   <li>支持监控指标收集</li>
 * </ul>
 *
 * <h3>使用流程</h3>
 * <pre>{@code
 * TableScan scan = table.newReadBuilder()
 *     .withFilter(predicate)           // 设置过滤条件
 *     .newScan();
 *
 * Plan plan = scan.plan();             // 生成扫描计划
 * List<Split> splits = plan.splits();  // 获取所有分片
 *
 * // 每个 split 可独立读取
 * for (Split split : splits) {
 *     RecordReader reader = tableRead.createReader(split);
 *     // 读取数据...
 * }
 * }</pre>
 *
 * <h3>实现类</h3>
 * <ul>
 *   <li>{@link DataTableScan}: 数据表扫描（主键表和追加表）</li>
 *   <li>{@link StreamTableScan}: 流式表扫描（支持持续读取）</li>
 *   <li>{@link ReadOnceTableScan}: 一次性读取扫描</li>
 * </ul>
 *
 * @see Split 扫描生成的分片
 * @see Plan 扫描计划
 * @see TableRead 用于读取 Split 的接口
 * @see org.apache.paimon.operation.FileStoreScan 底层文件存储扫描接口
 * @since 0.4.0
 */
@Public
public interface TableScan {

    /**
     * 设置监控指标注册器，用于收集扫描过程中的性能指标。
     *
     * @param registry 指标注册器
     * @return this
     */
    TableScan withMetricRegistry(MetricRegistry registry);

    /**
     * 生成扫描计划（包含要读取的所有分片）。
     *
     * <p>该方法会根据配置的过滤条件、分区等参数，生成最终的扫描计划。
     * 扫描计划包含一系列 {@link Split}，每个 Split 可被独立读取。
     *
     * <h3>异常说明</h3>
     * <ul>
     *   <li>如果扫描已结束（流式扫描场景），抛出 {@link EndOfScanException}</li>
     *   <li>批量扫描通常不会抛出此异常</li>
     * </ul>
     *
     * @return 扫描计划，包含所有要读取的分片
     * @throws EndOfScanException 如果扫描已结束（仅流式扫描）
     */
    Plan plan();

    /**
     * 列举所有要扫描的分区。
     *
     * <p>该方法是 {@link #listPartitionEntries()} 的便捷方法，
     * 只返回分区键（{@link BinaryRow}），不包含分区的统计信息。
     *
     * @return 分区键列表
     */
    default List<BinaryRow> listPartitions() {
        return listPartitionEntries().stream()
                .map(PartitionEntry::partition)
                .collect(Collectors.toList());
    }

    /**
     * 列举所有要扫描的分区条目（包含分区键和统计信息）。
     *
     * <p>分区条目（{@link PartitionEntry}）包含：
     * <ul>
     *   <li>分区键（partition）</li>
     *   <li>记录数（recordCount）</li>
     *   <li>文件数（fileCount）</li>
     *   <li>文件大小（fileSizeInBytes）</li>
     * </ul>
     *
     * @return 分区条目列表
     */
    List<PartitionEntry> listPartitionEntries();

    /**
     * 扫描计划，包含所有要读取的分片。
     *
     * <p>Plan 是 {@link #plan()} 方法的返回值，包含一系列 {@link Split}。
     * 每个 Split 代表一个可独立读取的数据单元（通常对应一个桶的一部分数据）。
     *
     * <h3>并行读取</h3>
     * <pre>{@code
     * Plan plan = scan.plan();
     * List<Split> splits = plan.splits();
     *
     * // 可以将 splits 分配给多个任务并行读取
     * splits.parallelStream().forEach(split -> {
     *     RecordReader reader = tableRead.createReader(split);
     *     // 处理数据...
     * });
     * }</pre>
     *
     * @since 0.4.0
     */
    @Public
    interface Plan {
        /**
         * 获取扫描计划中的所有分片。
         *
         * @return 分片列表，每个分片可独立读取
         */
        List<Split> splits();
    }
}
