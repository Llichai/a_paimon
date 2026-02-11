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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.options.MemorySize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * 提前全量压缩策略
 *
 * <p>在特定条件下提前触发全量压缩，无需等待 UniversalCompaction 的默认触发条件。
 *
 * <p>三种触发条件（满足任意一个即触发）：
 * <ul>
 *   <li><b>时间间隔</b>（fullCompactionInterval）：
 *       距离上次全量压缩的时间超过阈值
 *   <li><b>总大小阈值</b>（totalSizeThreshold）：
 *       所有文件总大小小于阈值（小数据集快速合并）
 *   <li><b>增量大小阈值</b>（incrementalSizeThreshold）：
 *       非最高层级文件总大小超过阈值（增量数据过多）
 * </ul>
 *
 * <p>适用场景：
 * <ul>
 *   <li>定期全量压缩：保证 changelog 质量和查询性能
 *   <li>小数据集优化：快速合并到最高层级
 *   <li>增量数据控制：防止低层级文件累积过多
 * </ul>
 *
 * <p>配置示例：
 * <pre>
 * compaction.optimization-interval = 1h  # 每小时全量压缩
 * compaction.total-size-threshold = 100MB  # 小于100MB立即合并
 * compaction.incremental-size-threshold = 1GB  # 增量超过1GB触发
 * </pre>
 */
public class EarlyFullCompaction {

    private static final Logger LOG = LoggerFactory.getLogger(EarlyFullCompaction.class);

    /** 全量压缩时间间隔（毫秒，null 表示不启用） */
    @Nullable private final Long fullCompactionInterval;
    /** 总大小阈值（字节，null 表示不启用） */
    @Nullable private final Long totalSizeThreshold;
    /** 增量大小阈值（字节，null 表示不启用） */
    @Nullable private final Long incrementalSizeThreshold;

    /** 上次全量压缩的时间戳（毫秒） */
    @Nullable private Long lastFullCompaction;

    /**
     * 构造提前全量压缩策略
     *
     * @param fullCompactionInterval 全量压缩时间间隔（毫秒）
     * @param totalSizeThreshold 总大小阈值（字节）
     * @param incrementalSizeThreshold 增量大小阈值（字节）
     */
    public EarlyFullCompaction(
            @Nullable Long fullCompactionInterval,
            @Nullable Long totalSizeThreshold,
            @Nullable Long incrementalSizeThreshold) {
        this.fullCompactionInterval = fullCompactionInterval;
        this.totalSizeThreshold = totalSizeThreshold;
        this.incrementalSizeThreshold = incrementalSizeThreshold;
    }

    /**
     * 从配置创建提前全量压缩策略
     *
     * <p>如果所有阈值都未配置，返回 null（不启用提前全量压缩）
     *
     * @param options 配置选项
     * @return 提前全量压缩策略或 null
     */
    @Nullable
    public static EarlyFullCompaction create(CoreOptions options) {
        Duration interval = options.optimizedCompactionInterval();
        MemorySize totalThreshold = options.compactionTotalSizeThreshold();
        MemorySize incrementalThreshold = options.compactionIncrementalSizeThreshold();
        if (interval == null && totalThreshold == null && incrementalThreshold == null) {
            return null; // 所有阈值都未配置，不启用
        }
        return new EarlyFullCompaction(
                interval == null ? null : interval.toMillis(),
                totalThreshold == null ? null : totalThreshold.getBytes(),
                incrementalThreshold == null ? null : incrementalThreshold.getBytes());
    }

    /**
     * 尝试触发全量压缩
     *
     * <p>按顺序检查三个条件，满足任意一个即触发全量压缩：
     * <ol>
     *   <li>时间间隔：距离上次全量压缩的时间超过阈值
     *   <li>总大小阈值：所有文件总大小小于阈值
     *   <li>增量大小阈值：非最高层级文件总大小超过阈值
     * </ol>
     *
     * @param numLevels 总层级数
     * @param runs 当前所有文件
     * @return 压缩单元（全量压缩）或 empty
     */
    public Optional<CompactUnit> tryFullCompact(int numLevels, List<LevelSortedRun> runs) {
        if (runs.size() == 1) {
            return Optional.empty(); // 只有一个文件，无需压缩
        }

        int maxLevel = numLevels - 1;
        // 条件1：检查时间间隔
        if (fullCompactionInterval != null) {
            if (lastFullCompaction == null
                    || currentTimeMillis() - lastFullCompaction > fullCompactionInterval) {
                LOG.debug("Universal compaction due to full compaction interval");
                updateLastFullCompaction();
                return Optional.of(CompactUnit.fromLevelRuns(maxLevel, runs));
            }
        }
        // 条件2：检查总大小阈值
        if (totalSizeThreshold != null) {
            long totalSize = 0;
            for (LevelSortedRun run : runs) {
                totalSize += run.run().totalSize();
            }
            if (totalSize < totalSizeThreshold) {
                // 总大小小于阈值，快速合并到最高层级
                return Optional.of(CompactUnit.fromLevelRuns(maxLevel, runs));
            }
        }
        // 条件3：检查增量大小阈值
        if (incrementalSizeThreshold != null) {
            long incrementalSize = 0;
            for (LevelSortedRun run : runs) {
                if (run.level() != maxLevel) {
                    // 累加非最高层级的文件大小
                    incrementalSize += run.run().totalSize();
                }
            }
            if (incrementalSize > incrementalSizeThreshold) {
                // 增量数据过多，触发全量压缩
                return Optional.of(CompactUnit.fromLevelRuns(maxLevel, runs));
            }
        }
        return Optional.empty();
    }

    /**
     * 更新最后一次全量压缩的时间戳
     */
    public void updateLastFullCompaction() {
        lastFullCompaction = currentTimeMillis();
    }

    /**
     * 获取当前时间戳（毫秒）
     *
     * <p>提取为方法以便测试时 mock
     *
     * @return 当前时间戳
     */
    @VisibleForTesting
    long currentTimeMillis() {
        return System.currentTimeMillis();
    }
}
