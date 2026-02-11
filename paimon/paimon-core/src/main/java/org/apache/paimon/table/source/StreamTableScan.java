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
import org.apache.paimon.utils.Restorable;

import javax.annotation.Nullable;

/**
 * 流式表扫描接口，支持检查点（checkpoint）和恢复（restore）功能。
 *
 * <p>StreamTableScan 扩展了 {@link TableScan}，增加了流式处理所需的状态管理功能，
 * 主要用于持续读取新产生的数据（如 Flink 流式处理）。
 *
 * <h3>核心功能</h3>
 * <ul>
 *   <li><b>检查点（Checkpoint）</b>: 保存当前读取进度（下一个要读取的快照 ID）</li>
 *   <li><b>恢复（Restore）</b>: 从上次检查点恢复读取进度</li>
 *   <li><b>水位线（Watermark）</b>: 获取当前消费快照的水位线（时间戳）</li>
 * </ul>
 *
 * <h3>检查点机制</h3>
 * <p>流式扫描需要记录读取进度，以便故障恢复：
 * <ul>
 *   <li><b>checkpoint()</b>: 返回下一个要读取的快照 ID（不是当前正在读取的）</li>
 *   <li><b>restore(nextSnapshotId)</b>: 从指定的快照 ID 恢复读取</li>
 *   <li><b>notifyCheckpointComplete(nextSnapshotId)</b>: 通知检查点完成（用于清理）</li>
 * </ul>
 *
 * <h3>使用流程</h3>
 * <pre>{@code
 * // 1. 创建流式扫描
 * StreamTableScan scan = table.newStreamScan();
 *
 * // 2. 恢复上次进度（如果有）
 * Long savedNextSnapshot = loadCheckpoint();
 * if (savedNextSnapshot != null) {
 *     scan.restore(savedNextSnapshot);
 * }
 *
 * // 3. 持续读取新数据
 * while (true) {
 *     Plan plan = scan.plan();
 *     processPlan(plan);
 *
 *     // 4. 定期保存检查点
 *     Long nextSnapshot = scan.checkpoint();
 *     saveCheckpoint(nextSnapshot);
 *     scan.notifyCheckpointComplete(nextSnapshot);
 * }
 * }</pre>
 *
 * <h3>与批量扫描的区别</h3>
 * <ul>
 *   <li><b>批量扫描</b>: 一次性读取完整快照，无状态</li>
 *   <li><b>流式扫描</b>: 持续跟踪新快照，有状态（需要检查点）</li>
 * </ul>
 *
 * <h3>实现类</h3>
 * <ul>
 *   <li>{@link DataTableStreamScan}: 数据表的流式扫描实现</li>
 *   <li>{@link StreamDataTableScan}: 流式数据表扫描接口</li>
 * </ul>
 *
 * <h3>检查点说明</h3>
 * <p>注意：{@link #checkpoint()} 返回的是<b>下一个要读取的快照 ID</b>，
 * 而不是当前正在读取的快照 ID。这样设计是为了避免重复消费。
 *
 * @see TableScan 表扫描接口
 * @see DataTableStreamScan 流式扫描实现
 * @see Restorable 可恢复接口
 * @since 0.4.0
 */
@Public
public interface StreamTableScan extends TableScan, Restorable<Long> {

    /**
     * 获取当前消费快照的水位线（时间戳）。
     *
     * <p>水位线用于表示数据的事件时间，通常用于：
     * <ul>
     *   <li>窗口聚合（根据事件时间触发窗口）</li>
     *   <li>迟到数据处理</li>
     *   <li>时间语义计算</li>
     * </ul>
     *
     * @return 当前快照的水位线（毫秒时间戳），如果没有水位线则返回 null
     */
    @Nullable
    Long watermark();

    /**
     * 从检查点恢复（恢复到下一个要读取的快照 ID）。
     *
     * <p>在流式处理中，当任务重启或故障恢复时，需要从上次检查点恢复读取进度。
     *
     * <h3>恢复逻辑</h3>
     * <ul>
     *   <li>如果 nextSnapshotId 不为 null，从该快照开始读取</li>
     *   <li>如果 nextSnapshotId 为 null，从初始位置开始读取（如最新快照）</li>
     * </ul>
     *
     * @param nextSnapshotId 下一个要读取的快照 ID（由 {@link #checkpoint()} 返回）
     */
    @Override
    void restore(@Nullable Long nextSnapshotId);

    /**
     * 执行检查点，返回下一个要读取的快照 ID。
     *
     * <p>检查点用于保存读取进度，以便故障恢复。返回的是<b>下一个要读取的快照 ID</b>，
     * 而不是当前正在读取的快照 ID。
     *
     * <h3>返回值说明</h3>
     * <ul>
     *   <li>如果已经读取了快照 10，返回 11（下一个要读取的快照）</li>
     *   <li>如果还没有读取任何快照，返回初始快照 ID</li>
     *   <li>如果扫描已结束，返回 null</li>
     * </ul>
     *
     * @return 下一个要读取的快照 ID，如果扫描已结束则返回 null
     */
    @Nullable
    @Override
    Long checkpoint();

    /**
     * 通知检查点完成（用于清理和优化）。
     *
     * <p>当分布式系统确认检查点已成功保存后，会调用此方法通知扫描器。
     * 扫描器可以利用这个信息进行优化，如清理不再需要的状态。
     *
     * <h3>使用场景</h3>
     * <ul>
     *   <li>清理已确认的消费进度</li>
     *   <li>更新 Consumer 记录（持久化消费位置）</li>
     *   <li>触发快照过期清理</li>
     * </ul>
     *
     * @param nextSnapshot 已确认的下一个快照 ID（与 {@link #checkpoint()} 返回值相同）
     */
    void notifyCheckpointComplete(@Nullable Long nextSnapshot);
}
