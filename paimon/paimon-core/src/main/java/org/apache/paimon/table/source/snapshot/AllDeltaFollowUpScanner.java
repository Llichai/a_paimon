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

/**
 * 全量 Delta 后续扫描器
 *
 * <p>读取所有类型快照的文件变更。
 *
 * <p><b>功能：</b>
 * <ul>
 *   <li>扫描所有快照（APPEND/COMPACT/OVERWRITE 都扫描）
 *   <li>使用 {@link SnapshotReader#readChanges()} 读取文件变更
 *   <li>返回 IncrementalSplit（包含 before/after 文件）
 * </ul>
 *
 * <p><b>与其他 Scanner 的区别：</b>
 * <table border="1">
 *   <tr>
 *     <th>Scanner</th>
 *     <th>扫描快照类型</th>
 *     <th>读取方法</th>
 *   </tr>
 *   <tr>
 *     <td>DeltaFollowUpScanner</td>
 *     <td>只扫描 APPEND</td>
 *     <td>read() with DELTA</td>
 *   </tr>
 *   <tr>
 *     <td>ChangelogFollowUpScanner</td>
 *     <td>有 changelog 的快照</td>
 *     <td>read() with CHANGELOG</td>
 *   </tr>
 *   <tr>
 *     <td>AllDeltaFollowUpScanner</td>
 *     <td>所有快照</td>
 *     <td>readChanges()</td>
 *   </tr>
 * </table>
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li>需要完整的文件级别变更信息
 *   <li>CDC（Change Data Capture）场景
 *   <li>审计和数据对比
 * </ul>
 *
 * @see FollowUpScanner
 * @see SnapshotReader#readChanges()
 */
public class AllDeltaFollowUpScanner implements FollowUpScanner {

    @Override
    public boolean shouldScanSnapshot(Snapshot snapshot) {
        return true;
    }

    @Override
    public SnapshotReader.Plan scan(Snapshot snapshot, SnapshotReader snapshotReader) {
        return snapshotReader.withMode(ScanMode.DELTA).withSnapshot(snapshot).readChanges();
    }
}
