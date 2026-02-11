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
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 连续压缩器起始扫描器
 *
 * <p>专门用于独立的流式压缩作业（Compaction Job）。
 *
 * <p><b>功能：</b>
 * <ul>
 *   <li>查找最新的压缩快照（commitKind == COMPACT）
 *   <li>从该压缩快照的下一个开始读取
 *   <li>如果没有压缩快照，从最早快照开始
 * </ul>
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li>流式压缩作业需要跟踪最新的压缩进度
 *   <li>避免重复压缩已压缩的数据
 * </ul>
 *
 * <p><b>查找逻辑：</b>
 * <pre>
 * 1. 从 latestSnapshotId 向前遍历到 earliestSnapshotId
 * 2. 找到第一个 commitKind == COMPACT 的快照
 * 3. 返回 NextSnapshot(compactSnapshotId + 1)
 * 4. 如果没找到，返回 NextSnapshot(earliestSnapshotId)
 * </pre>
 */
public class ContinuousCompactorStartingScanner extends AbstractStartingScanner {

    private static final Logger LOG =
            LoggerFactory.getLogger(ContinuousCompactorStartingScanner.class);

    public ContinuousCompactorStartingScanner(SnapshotManager snapshotManager) {
        super(snapshotManager);
        this.startingSnapshotId = snapshotManager.earliestSnapshotId();
    }

    @Override
    public Result scan(SnapshotReader snapshotReader) {
        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        Long earliestSnapshotId = snapshotManager.earliestSnapshotId();
        if (latestSnapshotId == null || earliestSnapshotId == null) {
            LOG.debug("There is currently no snapshot. Wait for the snapshot generation.");
            return new NoSnapshot();
        }

        for (long id = latestSnapshotId; id >= earliestSnapshotId; id--) {
            Snapshot snapshot = snapshotManager.snapshot(id);
            if (snapshot.commitKind() == Snapshot.CommitKind.COMPACT) {
                LOG.debug("Found latest compact snapshot {}, reading from the next snapshot.", id);
                return new NextSnapshot(id + 1);
            }
        }

        LOG.debug(
                "No compact snapshot found, reading from the earliest snapshot {}.",
                earliestSnapshotId);
        return new NextSnapshot(earliestSnapshotId);
    }
}
