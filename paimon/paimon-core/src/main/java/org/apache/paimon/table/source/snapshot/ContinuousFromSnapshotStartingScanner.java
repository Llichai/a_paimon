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
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.SnapshotManager;

/**
 * 连续快照起始扫描器
 *
 * <p>对应流式读取的 {@link CoreOptions.StartupMode#FROM_SNAPSHOT} 启动模式。
 *
 * <p><b>功能：</b>
 * <ul>
 *   <li>从指定快照 ID 之后开始流式读取
 *   <li>不读取指定快照，从下一个快照开始
 *   <li>支持 changelog 解耦模式
 * </ul>
 *
 * <p><b>Changelog 解耦：</b>
 * <ul>
 *   <li>如果 changelogDecoupled=true，会同时检查 changelog 文件
 *   <li>起点可能是 changelog 的最早 ID 或 snapshot 的最早 ID
 * </ul>
 *
 * <p><b>返回逻辑：</b>
 * <pre>
 * 返回 NextSnapshot(max(startingSnapshotId, earliestId))
 * - 如果指定的快照已过期，从最早快照开始
 * - 否则从指定快照的下一个开始
 * </pre>
 *
 * @see CoreOptions.StartupMode#FROM_SNAPSHOT
 */
public class ContinuousFromSnapshotStartingScanner extends AbstractStartingScanner {

    private final boolean changelogDecoupled;
    private final ChangelogManager changelogManager;

    public ContinuousFromSnapshotStartingScanner(
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            long snapshotId,
            boolean changelogDecoupled) {
        super(snapshotManager);
        this.changelogManager = changelogManager;
        this.startingSnapshotId = snapshotId;
        this.changelogDecoupled = changelogDecoupled;
    }

    @Override
    public Result scan(SnapshotReader snapshotReader) {
        Long earliestId = getEarliestId();
        if (earliestId == null) {
            return new NoSnapshot();
        }
        // We should return the specified snapshot as next snapshot to indicate to scan delta data
        // from it. If the snapshotId < earliestSnapshotId, start from the earliest.
        return new NextSnapshot(Math.max(startingSnapshotId, earliestId));
    }

    private Long getEarliestId() {
        Long earliestId;
        if (changelogDecoupled) {
            Long earliestChangelogId = changelogManager.earliestLongLivedChangelogId();
            earliestId =
                    earliestChangelogId == null
                            ? snapshotManager.earliestSnapshotId()
                            : earliestChangelogId;
        } else {
            earliestId = snapshotManager.earliestSnapshotId();
        }
        return earliestId;
    }
}
