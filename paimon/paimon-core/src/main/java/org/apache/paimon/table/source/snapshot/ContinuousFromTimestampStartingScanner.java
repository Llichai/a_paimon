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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 连续时间戳起始扫描器
 *
 * <p>对应流式读取的 {@link CoreOptions.StartupMode#FROM_TIMESTAMP} 启动模式。
 *
 * <p><b>功能：</b>
 * <ul>
 *   <li>从指定时间戳之后开始流式读取
 *   <li>查找早于指定时间的最新快照
 *   <li>从该快照的下一个开始读取
 *   <li>支持 changelog 解耦模式
 * </ul>
 *
 * <p><b>查找逻辑：</b>
 * <pre>
 * 1. earlierThanTimeMills() 查找 snapshot.timeMillis() < startupMillis 的最大快照 ID
 * 2. 返回 NextSnapshot(snapshotId + 1)
 * </pre>
 *
 * @see CoreOptions.StartupMode#FROM_TIMESTAMP
 * @see CoreOptions#SCAN_TIMESTAMP
 * @see CoreOptions#SCAN_TIMESTAMP_MILLIS
 */
public class ContinuousFromTimestampStartingScanner extends AbstractStartingScanner {

    private static final Logger LOG =
            LoggerFactory.getLogger(ContinuousFromTimestampStartingScanner.class);

    private final ChangelogManager changelogManager;
    private final long startupMillis;
    private final boolean startFromChangelog;

    public ContinuousFromTimestampStartingScanner(
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            long startupMillis,
            boolean changelogDecoupled) {
        super(snapshotManager);
        this.changelogManager = changelogManager;
        this.startupMillis = startupMillis;
        this.startFromChangelog = changelogDecoupled;
        this.startingSnapshotId =
                TimeTravelUtil.earlierThanTimeMills(
                        snapshotManager,
                        changelogManager,
                        startupMillis,
                        startFromChangelog,
                        false);
    }

    @Override
    public StartingContext startingContext() {
        if (startingSnapshotId == null) {
            return StartingContext.EMPTY;
        } else {
            return new StartingContext(startingSnapshotId + 1, false);
        }
    }

    @Override
    public Result scan(SnapshotReader snapshotReader) {
        if (startingSnapshotId == null) {
            startingSnapshotId =
                    TimeTravelUtil.earlierThanTimeMills(
                            snapshotManager,
                            changelogManager,
                            startupMillis,
                            startFromChangelog,
                            false);
        }
        if (startingSnapshotId == null) {
            LOG.debug("There is currently no snapshot. Waiting for snapshot generation.");
            return new NoSnapshot();
        }
        return new NextSnapshot(startingSnapshotId + 1);
    }
}
