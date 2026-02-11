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
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * 静态水位线起始扫描器
 *
 * <p>对应批量读取的水位线查询（{@link CoreOptions#SCAN_WATERMARK}）。
 *
 * <p><b>功能：</b>
 * <ul>
 *   <li>查找水位线大于等于指定值的最早快照
 *   <li>扫描模式：ScanMode.ALL
 *   <li>如果所有快照的水位线都小于指定值，抛出异常
 * </ul>
 *
 * <p><b>水位线匹配逻辑：</b>
 * <ul>
 *   <li>找到 snapshot.watermark() >= watermark 的最小快照 ID
 *   <li>水位线通常表示事件时间的进度
 * </ul>
 *
 * @see CoreOptions#SCAN_WATERMARK
 * @see Snapshot#watermark()
 */
public class StaticFromWatermarkStartingScanner extends ReadPlanStartingScanner {

    private static final Logger LOG =
            LoggerFactory.getLogger(StaticFromWatermarkStartingScanner.class);

    private final Snapshot snapshot;

    public StaticFromWatermarkStartingScanner(SnapshotManager snapshotManager, long watermark) {
        super(snapshotManager);
        this.snapshot = timeTravelToWatermark(snapshotManager, watermark);
        if (snapshot == null) {
            LOG.warn(
                    "There is currently no snapshot later than or equal to watermark[{}]",
                    watermark);
            throw new RuntimeException(
                    String.format(
                            "There is currently no snapshot later than or equal to "
                                    + "watermark[%d]",
                            watermark));
        }
        this.startingSnapshotId = snapshot.id();
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

    @Override
    public ScanMode startingScanMode() {
        return ScanMode.ALL;
    }

    @Override
    public SnapshotReader configure(SnapshotReader snapshotReader) {
        return snapshotReader.withMode(ScanMode.ALL).withSnapshot(snapshot);
    }

    @Nullable
    public static Snapshot timeTravelToWatermark(SnapshotManager snapshotManager, long watermark) {
        return snapshotManager.laterOrEqualWatermark(watermark);
    }
}
