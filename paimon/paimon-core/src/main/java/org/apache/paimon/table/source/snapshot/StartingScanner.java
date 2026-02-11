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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;

import javax.annotation.Nullable;

import java.util.List;

/**
 * 起始扫描器接口
 *
 * <p>该接口用于 {@link org.apache.paimon.table.source.TableScan} 的首次规划，负责确定流式或批量读取的起点。
 *
 * <p><b>核心职责：</b>
 * <ol>
 *   <li>根据不同的启动模式（StartupMode）确定扫描起点
 *   <li>执行第一次扫描，返回初始数据或下一个快照 ID
 *   <li>提供起始上下文信息供后续使用
 * </ol>
 *
 * <p><b>扫描结果类型：</b>
 * <table border="1">
 *   <tr>
 *     <th>结果类型</th>
 *     <th>说明</th>
 *     <th>使用场景</th>
 *   </tr>
 *   <tr>
 *     <td>{@link NoSnapshot}</td>
 *     <td>当前没有快照，需要等待</td>
 *     <td>表刚创建，还没有数据写入</td>
 *   </tr>
 *   <tr>
 *     <td>{@link ScannedResult}</td>
 *     <td>已扫描快照，包含分片列表</td>
 *     <td>批量读取或流式读取的初始全量扫描</td>
 *   </tr>
 *   <tr>
 *     <td>{@link NextSnapshot}</td>
 *     <td>不扫描当前快照，返回下一个快照 ID</td>
 *     <td>流式读取跳过历史数据，从指定快照后开始</td>
 *   </tr>
 * </table>
 *
 * <p><b>实现类分类：</b>
 * <table border="1">
 *   <tr>
 *     <th>类别</th>
 *     <th>实现类</th>
 *     <th>启动模式</th>
 *   </tr>
 *   <tr>
 *     <td rowspan="2">全量扫描</td>
 *     <td>FullStartingScanner</td>
 *     <td>LATEST_FULL</td>
 *   </tr>
 *   <tr>
 *     <td>FullCompactedStartingScanner</td>
 *     <td>COMPACTED_FULL</td>
 *   </tr>
 *   <tr>
 *     <td rowspan="5">静态扫描</td>
 *     <td>StaticFromSnapshotStartingScanner</td>
 *     <td>FROM_SNAPSHOT</td>
 *   </tr>
 *   <tr>
 *     <td>StaticFromTimestampStartingScanner</td>
 *     <td>FROM_TIMESTAMP</td>
 *   </tr>
 *   <tr>
 *     <td>StaticFromTagStartingScanner</td>
 *     <td>FROM_TAG</td>
 *   </tr>
 *   <tr>
 *     <td>StaticFromWatermarkStartingScanner</td>
 *     <td>FROM_WATERMARK</td>
 *   </tr>
 *   <tr>
 *     <td>CompactedStartingScanner</td>
 *     <td>COMPACTED_FULL</td>
 *   </tr>
 *   <tr>
 *     <td rowspan="4">连续扫描</td>
 *     <td>ContinuousLatestStartingScanner</td>
 *     <td>LATEST</td>
 *   </tr>
 *   <tr>
 *     <td>ContinuousFromSnapshotStartingScanner</td>
 *     <td>FROM_SNAPSHOT（流式）</td>
 *   </tr>
 *   <tr>
 *     <td>ContinuousFromSnapshotFullStartingScanner</td>
 *     <td>FROM_SNAPSHOT_FULL</td>
 *   </tr>
 *   <tr>
 *     <td>ContinuousFromTimestampStartingScanner</td>
 *     <td>FROM_TIMESTAMP（流式）</td>
 *   </tr>
 *   <tr>
 *     <td rowspan="2">增量扫描</td>
 *     <td>IncrementalDeltaStartingScanner</td>
 *     <td>INCREMENTAL（Delta）</td>
 *   </tr>
 *   <tr>
 *     <td>IncrementalDiffStartingScanner</td>
 *     <td>INCREMENTAL（Diff）</td>
 *   </tr>
 *   <tr>
 *     <td>其他</td>
 *     <td>ContinuousCompactorStartingScanner</td>
 *     <td>压缩作业专用</td>
 *   </tr>
 * </table>
 *
 * <p><b>使用示例：</b>
 * <pre>
 * // 1. 创建 StartingScanner
 * StartingScanner scanner;
 * if (startupMode == StartupMode.LATEST_FULL) {
 *     scanner = new FullStartingScanner(snapshotManager);
 * } else if (startupMode == StartupMode.FROM_SNAPSHOT) {
 *     if (streaming) {
 *         scanner = new ContinuousFromSnapshotStartingScanner(...);
 *     } else {
 *         scanner = new StaticFromSnapshotStartingScanner(...);
 *     }
 * }
 *
 * // 2. 执行扫描
 * StartingScanner.Result result = scanner.scan(snapshotReader);
 *
 * // 3. 处理结果
 * if (result instanceof NoSnapshot) {
 *     // 等待快照生成
 * } else if (result instanceof ScannedResult) {
 *     ScannedResult scanned = (ScannedResult) result;
 *     List<Split> splits = scanned.splits();
 *     // 处理分片...
 * } else if (result instanceof NextSnapshot) {
 *     long nextSnapshotId = ((NextSnapshot) result).nextSnapshotId();
 *     // 从 nextSnapshotId 开始流式读取
 * }
 * </pre>
 *
 * @see FollowUpScanner
 * @see org.apache.paimon.table.source.TableScan
 * @see AbstractStartingScanner
 */
public interface StartingScanner {

    /** 获取起始上下文信息（包含起始快照 ID 和是否全量扫描标志） */
    StartingContext startingContext();

    /**
     * 执行扫描
     *
     * @param snapshotReader 快照读取器
     * @return 扫描结果（NoSnapshot/ScannedResult/NextSnapshot 之一）
     */
    Result scan(SnapshotReader snapshotReader);

    /**
     * 扫描分区信息
     *
     * @param snapshotReader 快照读取器
     * @return 分区条目列表
     */
    List<PartitionEntry> scanPartitions(SnapshotReader snapshotReader);

    /** 扫描结果基接口 */
    interface Result {}

    /**
     * 无快照结果
     *
     * <p>表示当前没有快照可供扫描，需要等待快照生成。
     */
    class NoSnapshot implements Result {}

    /** 从计划创建扫描结果的工厂方法 */
    static ScannedResult fromPlan(SnapshotReader.Plan plan) {
        return new ScannedResult(plan);
    }

    /**
     * 已扫描结果
     *
     * <p>包含扫描到的快照数据和分片列表，下一个快照应该是 (currentSnapshotId + 1)。
     */
    class ScannedResult implements Result {

        private final SnapshotReader.Plan plan;

        public ScannedResult(SnapshotReader.Plan plan) {
            this.plan = plan;
        }

        /** 获取当前快照 ID */
        public long currentSnapshotId() {
            return plan.snapshotId();
        }

        /** 获取当前水位线（如果有） */
        @Nullable
        public Long currentWatermark() {
            return plan.watermark();
        }

        /** 获取分片列表 */
        public List<Split> splits() {
            return plan.splits();
        }

        /** 获取完整的读取计划 */
        public SnapshotReader.Plan plan() {
            return plan;
        }
    }

    /**
     * 下一个快照结果
     *
     * <p>表示不扫描当前快照（甚至当前快照可能不存在），而是从指定的下一个快照 ID 开始扫描。
     * 主要用于流式读取场景，跳过历史数据。
     */
    class NextSnapshot implements Result {

        private final long nextSnapshotId;

        public NextSnapshot(long nextSnapshotId) {
            this.nextSnapshotId = nextSnapshotId;
        }

        /** 获取下一个要扫描的快照 ID */
        public long nextSnapshotId() {
            return nextSnapshotId;
        }
    }
}
