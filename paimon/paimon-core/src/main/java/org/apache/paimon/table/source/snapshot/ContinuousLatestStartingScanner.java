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
import org.apache.paimon.Snapshot;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 连续最新起始扫描器
 *
 * <p>对应流式读取的 {@link CoreOptions.StartupMode#LATEST} 启动模式。
 *
 * <p><b>功能：</b>
 * <ul>
 *   <li>从最新快照之后开始流式读取
 *   <li>不读取历史数据，只读取新写入的数据
 *   <li>返回 NextSnapshot（下一个要读取的快照 ID）
 * </ul>
 *
 * <p><b>逻辑：</b>
 * <pre>
 * 1. 如果启动时有快照 → 返回 NextSnapshot(latestSnapshotId + 1)
 * 2. 如果启动时没有快照 → 等待第一个快照出现，返回 NextSnapshot(1)
 * </pre>
 *
 * @see CoreOptions.StartupMode#LATEST
 */
public class ContinuousLatestStartingScanner extends AbstractStartingScanner {

    private static final Logger LOG =
            LoggerFactory.getLogger(ContinuousLatestStartingScanner.class);

    public ContinuousLatestStartingScanner(SnapshotManager snapshotManager) {
        super(snapshotManager);
        this.startingSnapshotId = snapshotManager.latestSnapshotId();
    }

    @Override
    public Result scan(SnapshotReader snapshotReader) {
        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        if (latestSnapshotId == null) {
            LOG.debug("There is currently no snapshot. Wait for the snapshot generation.");
            return new NoSnapshot();
        }

        // If there's no snapshot before the reading job starts,
        // then the first snapshot should be considered as an incremental snapshot
        long nextSnapshot =
                startingSnapshotId == null ? Snapshot.FIRST_SNAPSHOT_ID : latestSnapshotId + 1;
        return new NextSnapshot(nextSnapshot);
    }
}
