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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Changelog 跟踪扫描器
 *
 * <p>该类用于流式读取场景，负责持续扫描新产生的 changelog 文件。
 *
 * <p><b>工作原理：</b>
 * <ol>
 *   <li>在流式读取中，首先通过 {@link StartingScanner} 读取初始快照
 *   <li>然后切换到 {@link FollowUpScanner} 持续跟踪新的快照
 *   <li>该类作为 FollowUpScanner 的实现，专门用于读取有 changelog 的表
 * </ol>
 *
 * <p><b>适用场景：</b>
 * <ul>
 *   <li>表配置了 changelog-producer（INPUT/FULL_COMPACTION/LOOKUP 任一模式）
 *   <li>流式读取模式（streaming read）
 *   <li>需要读取增量变更记录（INSERT/DELETE/UPDATE）
 * </ul>
 *
 * <p><b>关键逻辑：</b>
 * <ul>
 *   <li>{@link #shouldScanSnapshot}：检查 snapshot 是否有 changelog
 *       <ul>
 *         <li>如果 snapshot.changelogManifestList() 不为 null，说明有 changelog，返回 true
 *         <li>否则跳过该 snapshot，继续检查下一个
 *       </ul>
 *   <li>{@link #scan}：使用 CHANGELOG 模式读取 snapshot
 *       <ul>
 *         <li>读取 changelogManifestList 中的 changelog 文件
 *         <li>返回完整的变更记录（带 RowKind）
 *       </ul>
 * </ul>
 *
 * <p><b>与其他 Scanner 的对比：</b>
 * <table border="1">
 *   <tr>
 *     <th>Scanner</th>
 *     <th>使用场景</th>
 *     <th>读取内容</th>
 *   </tr>
 *   <tr>
 *     <td>ChangelogFollowUpScanner</td>
 *     <td>流式读取 changelog 表</td>
 *     <td>读取 changelogManifestList</td>
 *   </tr>
 *   <tr>
 *     <td>DeltaFollowUpScanner</td>
 *     <td>流式读取无 changelog 的表</td>
 *     <td>读取 deltaManifestList</td>
 *   </tr>
 *   <tr>
 *     <td>SnapshotStartingScanner</td>
 *     <td>初始快照读取</td>
 *     <td>读取完整快照</td>
 *   </tr>
 * </table>
 *
 * <p><b>使用示例：</b>
 * <pre>
 * // 在 TableScan 中使用
 * if (有 changelog producer) {
 *     followUpScanner = new ChangelogFollowUpScanner();
 * } else {
 *     followUpScanner = new DeltaFollowUpScanner();
 * }
 *
 * // 流式读取循环
 * while (running) {
 *     Snapshot nextSnapshot = getNextSnapshot();
 *     if (followUpScanner.shouldScanSnapshot(nextSnapshot)) {
 *         Plan plan = followUpScanner.scan(nextSnapshot, reader);
 *         processChangelog(plan);
 *     }
 * }
 * </pre>
 *
 * @see FollowUpScanner
 * @see org.apache.paimon.table.source.snapshot.DeltaFollowUpScanner
 * @see org.apache.paimon.table.source.snapshot.StartingScanner
 */
public class ChangelogFollowUpScanner implements FollowUpScanner {

    private static final Logger LOG = LoggerFactory.getLogger(ChangelogFollowUpScanner.class);

    /**
     * 判断是否应该扫描该 snapshot
     *
     * <p>检查逻辑：
     * <ol>
     *   <li>检查 snapshot.changelogManifestList() 是否为 null
     *   <li>如果不为 null，说明该 snapshot 有 changelog 文件，返回 true
     *   <li>如果为 null，说明该 snapshot 没有 changelog（可能是压缩中间状态），跳过
     * </ol>
     *
     * <p><b>为什么会出现没有 changelog 的 snapshot？</b>
     * <ul>
     *   <li>FULL_COMPACTION 模式：只在全量压缩时生成 changelog，普通压缩不生成
     *   <li>LOOKUP 模式：只在 Level-0 压缩时生成 changelog，高层级压缩不生成
     *   <li>写入失败：某些异常情况下 changelog 可能未生成成功
     * </ul>
     *
     * @param snapshot 待检查的 snapshot
     * @return true 如果该 snapshot 有 changelog 并且应该被扫描，false 否则
     */
    @Override
    public boolean shouldScanSnapshot(Snapshot snapshot) {
        if (snapshot.changelogManifestList() != null) {
            // 有 changelog，应该扫描
            return true;
        }

        // 没有 changelog，跳过并记录日志
        LOG.debug("Next snapshot id {} has no changelog, check next one.", snapshot.id());
        return false;
    }

    /**
     * 扫描 snapshot 并返回读取计划
     *
     * <p>使用 {@link ScanMode#CHANGELOG} 模式读取：
     * <ul>
     *   <li>读取 changelogManifestList 指向的 changelog 文件
     *   <li>返回完整的变更记录（INSERT/DELETE/UPDATE_BEFORE/UPDATE_AFTER）
     *   <li>每条记录都带有 RowKind 标识
     * </ul>
     *
     * <p><b>读取过程：</b>
     * <pre>
     * 1. snapshotReader.withMode(ScanMode.CHANGELOG)  → 设置扫描模式为 CHANGELOG
     * 2. .withSnapshot(snapshot)                       → 设置要扫描的 snapshot
     * 3. .read()                                       → 执行扫描，返回 Plan
     * 4. Plan 包含：
     *    - DataSplit 列表：每个 split 对应一个或多个 changelog 文件
     *    - Split 中包含 changelog 文件的路径和元信息
     * </pre>
     *
     * <p><b>与 DELTA 模式的区别：</b>
     * <ul>
     *   <li>CHANGELOG 模式：读取 changelogManifestList，返回完整的变更记录（带 RowKind）
     *   <li>DELTA 模式：读取 deltaManifestList，返回合并后的数据（可能没有 RowKind）
     * </ul>
     *
     * @param snapshot 要扫描的 snapshot
     * @param snapshotReader snapshot 读取器
     * @return 扫描计划（包含要读取的文件列表）
     */
    @Override
    public SnapshotReader.Plan scan(Snapshot snapshot, SnapshotReader snapshotReader) {
        return snapshotReader.withMode(ScanMode.CHANGELOG).withSnapshot(snapshot).read();
    }
}
