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

import org.apache.paimon.CoreOptions.StartupMode;
import org.apache.paimon.Snapshot;
import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * 全量压缩起始扫描器（带 Delta 提交计数）
 *
 * <p>对应 {@link StartupMode#COMPACTED_FULL} 启动模式，配合 'full-compaction.delta-commits' 参数。
 *
 * <p><b>功能：</b>
 * <ul>
 *   <li>查找最新的全量压缩快照（commitIdentifier % deltaCommits == 0）
 *   <li>全量压缩是经过多次 delta 提交后的大规模压缩
 *   <li>如果没有全量压缩快照，使用最新快照
 * </ul>
 *
 * <p><b>全量压缩识别：</b>
 * <ul>
 *   <li>commitKind == COMPACT
 *   <li>commitIdentifier % deltaCommits == 0（周期性全量压缩）
 *   <li>或 commitIdentifier == Long.MAX_VALUE（手动触发的全量压缩）
 * </ul>
 *
 * @see StartupMode#COMPACTED_FULL
 * @see CoreOptions#FULL_COMPACTION_DELTA_COMMITS
 */
public class FullCompactedStartingScanner extends ReadPlanStartingScanner {

    private static final Logger LOG = LoggerFactory.getLogger(FullCompactedStartingScanner.class);

    private final int deltaCommits;

    public FullCompactedStartingScanner(SnapshotManager snapshotManager, int deltaCommits) {
        super(snapshotManager);
        this.deltaCommits = deltaCommits;
        this.startingSnapshotId = pick();
    }

    @Nullable
    protected Long pick() {
        return snapshotManager.pickOrLatest(this::picked);
    }

    private boolean picked(Snapshot snapshot) {
        long identifier = snapshot.commitIdentifier();
        return snapshot.commitKind() == CommitKind.COMPACT
                && isFullCompactedIdentifier(identifier, deltaCommits);
    }

    @Override
    public ScanMode startingScanMode() {
        return ScanMode.ALL;
    }

    @Override
    public SnapshotReader configure(SnapshotReader snapshotReader) {
        Long startingSnapshotId = pick();
        if (startingSnapshotId == null) {
            startingSnapshotId = snapshotManager.latestSnapshotId();
            if (startingSnapshotId == null) {
                LOG.debug("There is currently no snapshot. Wait for the snapshot generation.");
                return null;
            } else {
                LOG.debug(
                        "No compact snapshot found, reading from the latest snapshot {}.",
                        startingSnapshotId);
            }
        }

        return snapshotReader.withMode(ScanMode.ALL).withSnapshot(startingSnapshotId);
    }

    public static boolean isFullCompactedIdentifier(long identifier, int deltaCommits) {
        return identifier % deltaCommits == 0 || identifier == Long.MAX_VALUE;
    }
}
