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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.SnapshotManager;

/**
 * 连续全量快照起始扫描器
 *
 * <p>对应批量读取的 {@link CoreOptions.StartupMode#FROM_SNAPSHOT_FULL} 启动模式。
 *
 * <p><b>功能：</b>
 * <ul>
 *   <li>先全量读取指定快照的数据
 *   <li>扫描模式：ScanMode.ALL
 *   <li>如果指定快照已过期，使用最早快照
 * </ul>
 *
 * <p><b>与 StaticFromSnapshotStartingScanner 的区别：</b>
 * <ul>
 *   <li>Static：快照不存在时抛异常
 *   <li>Continuous：快照不存在时使用最早快照（容错）
 * </ul>
 *
 * @see CoreOptions.StartupMode#FROM_SNAPSHOT_FULL
 */
public class ContinuousFromSnapshotFullStartingScanner extends ReadPlanStartingScanner {

    public ContinuousFromSnapshotFullStartingScanner(
            SnapshotManager snapshotManager, long snapshotId) {
        super(snapshotManager);
        this.startingSnapshotId = snapshotId;
    }

    @Override
    public ScanMode startingScanMode() {
        return ScanMode.ALL;
    }

    @Override
    public SnapshotReader configure(SnapshotReader snapshotReader) {
        Long earliestSnapshotId = snapshotManager.earliestSnapshotId();
        if (earliestSnapshotId == null) {
            return null;
        }
        long ceiledSnapshotId = Math.max(startingSnapshotId, earliestSnapshotId);
        return snapshotReader.withMode(ScanMode.ALL).withSnapshot(ceiledSnapshotId);
    }
}
