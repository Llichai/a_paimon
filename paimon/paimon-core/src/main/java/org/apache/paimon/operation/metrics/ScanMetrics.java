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
import org.apache.paimon.metrics.Histogram;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.MetricRegistry;

/**
 * 扫描指标类
 *
 * <p>用于测量和报告表扫描操作的性能指标。
 *
 * <h2>扫描性能监控</h2>
 * <p>该类收集扫描操作的关键指标，包括：
 * <ul>
 *   <li><b>扫描耗时</b>：单次扫描的持续时间（毫秒）
 *   <li><b>扫描快照ID</b>：扫描的快照版本
 *   <li><b>Manifest文件数</b>：扫描的Manifest文件数量
 *   <li><b>跳过的表文件数</b>：通过过滤跳过的数据文件数
 *   <li><b>返回的表文件数</b>：实际需要读取的数据文件数
 *   <li><b>缓存命中率</b>：Manifest和删除向量元数据的缓存命中情况
 * </ul>
 *
 * <h2>缓存指标监控</h2>
 * <p>该类集成了两种缓存的监控：
 * <ul>
 *   <li><b>Manifest缓存</b>：缓存Manifest文件内容，避免重复读取
 *       <ul>
 *         <li>manifestHitCache - Manifest缓存命中次数
 *         <li>manifestMissedCache - Manifest缓存未命中次数
 *       </ul>
 *   <li><b>删除向量元数据缓存</b>：缓存删除向量的元数据
 *       <ul>
 *         <li>dvMetaHitCache - 删除向量元数据缓存命中次数
 *         <li>dvMetaMissedCache - 删除向量元数据缓存未命中次数
 *       </ul>
 * </ul>
 *
 * <h2>指标名称</h2>
 * <p>所有指标都注册在 "scan" 指标组下：
 * <ul>
 *   <li>lastScanDuration - 最后一次扫描的耗时（毫秒）
 *   <li>scanDuration - 扫描耗时的直方图
 *   <li>lastScannedSnapshotId - 最后一次扫描的快照ID
 *   <li>lastScannedManifests - 最后一次扫描的Manifest文件数
 *   <li>lastScanSkippedTableFiles - 最后一次扫描跳过的表文件数
 *   <li>lastScanResultedTableFiles - 最后一次扫描返回的表文件数
 *   <li>manifestHitCache - Manifest缓存命中次数
 *   <li>manifestMissedCache - Manifest缓存未命中次数
 *   <li>dvMetaHitCache - 删除向量元数据缓存命中次数
 *   <li>dvMetaMissedCache - 删除向量元数据缓存未命中次数
 * </ul>
 *
 * @see ScanStats 扫描统计数据
 * @see CacheMetrics 缓存指标
 */
public class ScanMetrics {

    /** 直方图窗口大小，保留最近100次扫描的统计 */
    private static final int HISTOGRAM_WINDOW_SIZE = 100;

    /** 指标组名称 */
    public static final String GROUP_NAME = "scan";

    // ==================== 指标名称常量 ====================

    /** 最后一次扫描的持续时间（毫秒） */
    public static final String LAST_SCAN_DURATION = "lastScanDuration";

    /** 扫描持续时间的直方图 */
    public static final String SCAN_DURATION = "scanDuration";

    /** 最后一次扫描的快照ID */
    public static final String LAST_SCANNED_SNAPSHOT_ID = "lastScannedSnapshotId";

    /** 最后一次扫描的Manifest文件数 */
    public static final String LAST_SCANNED_MANIFESTS = "lastScannedManifests";

    /** 最后一次扫描跳过的表文件数 */
    public static final String LAST_SCAN_SKIPPED_TABLE_FILES = "lastScanSkippedTableFiles";

    /** 最后一次扫描返回的表文件数 */
    public static final String LAST_SCAN_RESULTED_TABLE_FILES = "lastScanResultedTableFiles";

    /** Manifest缓存命中次数 */
    public static final String MANIFEST_HIT_CACHE = "manifestHitCache";

    /** Manifest缓存未命中次数 */
    public static final String MANIFEST_MISSED_CACHE = "manifestMissedCache";

    /** 删除向量元数据缓存命中次数 */
    public static final String DVMETA_HIT_CACHE = "dvMetaHitCache";

    /** 删除向量元数据缓存未命中次数 */
    public static final String DVMETA_MISSED_CACHE = "dvMetaMissedCache";

    /** 指标组，用于注册和管理所有扫描相关指标 */
    private final MetricGroup metricGroup;

    /** 扫描耗时直方图 */
    private final Histogram durationHistogram;

    /** Manifest缓存指标 */
    private final CacheMetrics cacheMetrics;

    /** 删除向量元数据缓存指标 */
    private final CacheMetrics dvMetaCacheMetrics;

    /** 最新扫描的统计信息 */
    private ScanStats latestScan;

    /**
     * 构造扫描指标收集器
     *
     * @param registry 指标注册器
     * @param tableName 表名
     */
    public ScanMetrics(MetricRegistry registry, String tableName) {
        metricGroup = registry.createTableMetricGroup(GROUP_NAME, tableName);
        metricGroup.gauge(
                LAST_SCAN_DURATION, () -> latestScan == null ? 0L : latestScan.getDuration());
        durationHistogram = metricGroup.histogram(SCAN_DURATION, HISTOGRAM_WINDOW_SIZE);
        cacheMetrics = new CacheMetrics();
        dvMetaCacheMetrics = new CacheMetrics();
        metricGroup.gauge(
                LAST_SCANNED_SNAPSHOT_ID,
                () -> latestScan == null ? 0L : latestScan.getScannedSnapshotId());
        metricGroup.gauge(
                LAST_SCANNED_MANIFESTS,
                () -> latestScan == null ? 0L : latestScan.getScannedManifests());
        metricGroup.gauge(
                LAST_SCAN_SKIPPED_TABLE_FILES,
                () -> latestScan == null ? 0L : latestScan.getSkippedTableFiles());
        metricGroup.gauge(
                LAST_SCAN_RESULTED_TABLE_FILES,
                () -> latestScan == null ? 0L : latestScan.getResultedTableFiles());
        metricGroup.gauge(MANIFEST_HIT_CACHE, () -> cacheMetrics.getHitObject().get());
        metricGroup.gauge(MANIFEST_MISSED_CACHE, () -> cacheMetrics.getMissedObject().get());
        metricGroup.gauge(DVMETA_HIT_CACHE, () -> dvMetaCacheMetrics.getHitObject().get());
        metricGroup.gauge(DVMETA_MISSED_CACHE, () -> dvMetaCacheMetrics.getMissedObject().get());
    }

    /**
     * 获取指标组（仅用于测试）
     *
     * @return 指标组对象
     */
    @VisibleForTesting
    MetricGroup getMetricGroup() {
        return metricGroup;
    }

    /**
     * 报告扫描统计信息
     *
     * <p>更新最新扫描的统计信息，并将耗时添加到直方图中。
     *
     * @param scanStats 扫描统计数据
     */
    public void reportScan(ScanStats scanStats) {
        latestScan = scanStats;
        durationHistogram.update(scanStats.getDuration());
    }

    /**
     * 获取Manifest缓存指标
     *
     * @return Manifest缓存指标对象
     */
    public CacheMetrics getCacheMetrics() {
        return cacheMetrics;
    }

    /**
     * 获取删除向量元数据缓存指标
     *
     * @return 删除向量元数据缓存指标对象
     */
    public CacheMetrics getDvMetaCacheMetrics() {
        return dvMetaCacheMetrics;
    }
}
