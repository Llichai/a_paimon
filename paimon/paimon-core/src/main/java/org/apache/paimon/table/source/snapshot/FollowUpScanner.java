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

import org.apache.paimon.Snapshot;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader.Plan;

/**
 * 后续扫描器接口
 *
 * <p>该接口用于 {@link org.apache.paimon.table.source.StreamTableScan} 的持续规划，
 * 负责在流式读取中跟踪新的快照并生成增量读取计划。
 *
 * <p><b>核心职责：</b>
 * <ol>
 *   <li>判断快照是否应该被扫描（过滤不相关的快照）
 *   <li>扫描新快照并生成读取计划
 *   <li>处理覆盖写（OVERWRITE）场景
 * </ol>
 *
 * <p><b>与 StartingScanner 的关系：</b>
 * <pre>
 * 流式读取流程：
 * 1. StartingScanner.scan()        → 初始扫描（可能返回全量数据或起始点）
 * 2. FollowUpScanner.shouldScanSnapshot() → 检查每个新快照
 * 3. FollowUpScanner.scan()        → 扫描新快照，返回增量数据
 * 4. 重复步骤 2-3                    → 持续跟踪新快照
 * </pre>
 *
 * <p><b>实现类：</b>
 * <table border="1">
 *   <tr>
 *     <th>实现类</th>
 *     <th>使用场景</th>
 *     <th>扫描策略</th>
 *   </tr>
 *   <tr>
 *     <td>{@link DeltaFollowUpScanner}</td>
 *     <td>无 changelog producer 的表</td>
 *     <td>只扫描 APPEND 类型快照，读取 deltaManifestList</td>
 *   </tr>
 *   <tr>
 *     <td>{@link ChangelogFollowUpScanner}</td>
 *     <td>有 changelog producer 的表</td>
 *     <td>只扫描有 changelogManifestList 的快照</td>
 *   </tr>
 *   <tr>
 *     <td>{@link AllDeltaFollowUpScanner}</td>
 *     <td>读取所有文件变更</td>
 *     <td>扫描所有快照，使用 readChanges()</td>
 *   </tr>
 * </table>
 *
 * <p><b>使用示例：</b>
 * <pre>
 * // 1. 创建 FollowUpScanner
 * FollowUpScanner scanner;
 * if (hasChangelogProducer) {
 *     scanner = new ChangelogFollowUpScanner();
 * } else {
 *     scanner = new DeltaFollowUpScanner();
 * }
 *
 * // 2. 流式读取循环
 * long nextSnapshotId = startingSnapshotId + 1;
 * while (running) {
 *     // 等待新快照
 *     Snapshot snapshot = waitForSnapshot(nextSnapshotId);
 *
 *     // 检查是否应该扫描
 *     if (scanner.shouldScanSnapshot(snapshot)) {
 *         // 扫描快照
 *         Plan plan = scanner.scan(snapshot, snapshotReader);
 *         // 处理数据...
 *         processData(plan);
 *     }
 *
 *     nextSnapshotId++;
 * }
 * </pre>
 *
 * @see StartingScanner
 * @see DeltaFollowUpScanner
 * @see ChangelogFollowUpScanner
 * @see AllDeltaFollowUpScanner
 */
public interface FollowUpScanner {

    /**
     * 判断是否应该扫描该快照
     *
     * <p>用于过滤不需要读取的快照，例如：
     * <ul>
     *   <li>DeltaFollowUpScanner：只扫描 APPEND 类型快照，跳过 COMPACT/OVERWRITE
     *   <li>ChangelogFollowUpScanner：只扫描有 changelogManifestList 的快照
     *   <li>AllDeltaFollowUpScanner：扫描所有快照
     * </ul>
     *
     * @param snapshot 待检查的快照
     * @return true 如果应该扫描该快照，false 否则
     */
    boolean shouldScanSnapshot(Snapshot snapshot);

    /**
     * 扫描快照并生成读取计划
     *
     * <p>根据不同的实现，使用不同的扫描模式：
     * <ul>
     *   <li>DeltaFollowUpScanner：ScanMode.DELTA
     *   <li>ChangelogFollowUpScanner：ScanMode.CHANGELOG
     *   <li>AllDeltaFollowUpScanner：readChanges()
     * </ul>
     *
     * @param snapshot 要扫描的快照
     * @param snapshotReader 快照读取器
     * @return 读取计划
     */
    Plan scan(Snapshot snapshot, SnapshotReader snapshotReader);

    /**
     * 获取覆盖写场景的读取计划
     *
     * <p>覆盖写（OVERWRITE）会删除旧数据并写入新数据，需要特殊处理：
     * <ul>
     *   <li>如果是 APPEND 模式（isAppend=true）：使用 DELTA 模式读取新增文件
     *   <li>如果是非 APPEND 模式：使用 readChanges() 读取完整变更（before/after）
     * </ul>
     *
     * @param snapshot 覆盖写快照
     * @param snapshotReader 快照读取器
     * @param isAppend 是否为 APPEND 模式
     * @return 读取计划
     */
    default Plan getOverwriteChangesPlan(
            Snapshot snapshot, SnapshotReader snapshotReader, boolean isAppend) {
        if (isAppend) {
            return snapshotReader.withSnapshot(snapshot).withMode(ScanMode.DELTA).read();
        } else {
            return snapshotReader.withSnapshot(snapshot).readChanges();
        }
    }
}
