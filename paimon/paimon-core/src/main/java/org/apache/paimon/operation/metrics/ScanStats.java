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

package org.apache.paimon.operation.metrics;

import org.apache.paimon.annotation.VisibleForTesting;

/**
 * 扫描操作统计信息
 *
 * <p>封装单次扫描操作的统计数据。
 *
 * <h2>统计内容</h2>
 * <p>该类记录以下扫描统计信息：
 * <ul>
 *   <li><b>扫描耗时</b>：扫描操作的持续时间（毫秒）
 *   <li><b>扫描快照ID</b>：扫描的快照版本号
 *   <li><b>扫描的Manifest文件数</b>：读取的Manifest文件数量
 *   <li><b>跳过的表文件数</b>：通过过滤器跳过的数据文件数量
 *   <li><b>返回的表文件数</b>：实际需要读取的数据文件数量
 * </ul>
 *
 * <h2>用途</h2>
 * <p>该统计信息用于：
 * <ul>
 *   <li>性能监控：跟踪扫描操作的性能
 *   <li>效果分析：评估过滤器的效果（跳过文件数/总文件数）
 *   <li>性能优化：识别慢扫描并优化
 * </ul>
 *
 * @see ScanMetrics 扫描指标
 */
public class ScanStats {

    /** 扫描持续时间（毫秒） */
    private final long duration;

    /** 扫描的快照ID */
    private final long scannedSnapshotId;

    /** 扫描的Manifest文件数 */
    private final long scannedManifests;

    /** 跳过的表文件数 */
    private final long skippedTableFiles;

    /** 返回的表文件数 */
    private final long resultedTableFiles;

    /**
     * 构造扫描统计信息
     *
     * @param duration 扫描持续时间（毫秒）
     * @param scannedSnapshotId 扫描的快照ID
     * @param scannedManifests 扫描的Manifest文件数
     * @param skippedTableFiles 跳过的表文件数
     * @param resultedTableFiles 返回的表文件数
     */
    public ScanStats(
            long duration,
            long scannedSnapshotId,
            long scannedManifests,
            long skippedTableFiles,
            long resultedTableFiles) {
        this.duration = duration;
        this.scannedSnapshotId = scannedSnapshotId;
        this.scannedManifests = scannedManifests;
        this.skippedTableFiles = skippedTableFiles;
        this.resultedTableFiles = resultedTableFiles;
    }

    /**
     * 获取扫描的快照ID
     *
     * @return 快照ID
     */
    @VisibleForTesting
    protected long getScannedSnapshotId() {
        return scannedSnapshotId;
    }

    /**
     * 获取扫描的Manifest文件数
     *
     * @return Manifest文件数
     */
    @VisibleForTesting
    protected long getScannedManifests() {
        return scannedManifests;
    }

    /**
     * 获取跳过的表文件数
     *
     * @return 跳过的表文件数
     */
    @VisibleForTesting
    protected long getSkippedTableFiles() {
        return skippedTableFiles;
    }

    /**
     * 获取返回的表文件数
     *
     * @return 返回的表文件数
     */
    @VisibleForTesting
    protected long getResultedTableFiles() {
        return resultedTableFiles;
    }

    /**
     * 获取扫描持续时间
     *
     * @return 持续时间（毫秒）
     */
    @VisibleForTesting
    protected long getDuration() {
        return duration;
    }
}
