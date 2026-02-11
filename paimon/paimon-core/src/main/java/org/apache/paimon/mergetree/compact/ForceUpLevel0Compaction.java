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

import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.mergetree.LevelSortedRun;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 强制提升 Level-0 文件的压缩策略
 *
 * <p>包装 {@link UniversalCompaction}，在特定条件下强制压缩 Level-0 文件。
 *
 * <p>核心功能：
 * <ul>
 *   <li>优先使用 UniversalCompaction 的常规策略
 *   <li>如果常规策略未触发压缩，强制选择 Level-0 文件
 *   <li>可配置压缩间隔，避免过于频繁的压缩
 * </ul>
 *
 * <p>适用场景：
 * <ul>
 *   <li>LOOKUP changelog 模式：确保 Level-0 文件及时压缩以生成 changelog
 *   <li>防止 Level-0 文件累积过多影响查询性能
 * </ul>
 *
 * <p>压缩触发逻辑：
 * <pre>
 * 1. 尝试常规压缩（UniversalCompaction）
 * 2. 如果常规压缩未触发：
 *    - 无间隔限制：立即强制压缩 Level-0
 *    - 有间隔限制：检查计数器是否达到间隔阈值
 *      → 达到：强制压缩并重置计数器
 *      → 未达到：跳过压缩，计数器加1
 * </pre>
 */
public class ForceUpLevel0Compaction implements CompactStrategy {

    /** 内部的通用压缩策略 */
    private final UniversalCompaction universal;
    /** 最大压缩间隔（null 表示无限制） */
    @Nullable private final Integer maxCompactInterval;
    /** 压缩触发计数器（用于控制间隔） */
    @Nullable private final AtomicInteger compactTriggerCount;

    /**
     * 构造强制 Level-0 压缩策略
     *
     * @param universal 通用压缩策略
     * @param maxCompactInterval 最大压缩间隔（null 表示每次都强制压缩）
     */
    public ForceUpLevel0Compaction(
            UniversalCompaction universal, @Nullable Integer maxCompactInterval) {
        this.universal = universal;
        this.maxCompactInterval = maxCompactInterval;
        this.compactTriggerCount = maxCompactInterval == null ? null : new AtomicInteger(0);
    }

    /**
     * 获取最大压缩间隔配置
     *
     * @return 最大压缩间隔
     */
    @Nullable
    public Integer maxCompactInterval() {
        return maxCompactInterval;
    }

    /**
     * 选择压缩单元
     *
     * <p>两阶段策略：
     * <ol>
     *   <li>阶段1：尝试常规的 UniversalCompaction
     *   <li>阶段2：如果常规策略未触发，强制选择 Level-0 文件
     * </ol>
     *
     * @param numLevels 总层级数
     * @param runs 当前所有文件
     * @return 压缩单元（如果需要压缩）
     */
    @Override
    public Optional<CompactUnit> pick(int numLevels, List<LevelSortedRun> runs) {
        // 阶段1：尝试常规压缩
        Optional<CompactUnit> pick = universal.pick(numLevels, runs);
        if (pick.isPresent()) {
            return pick; // 常规策略已触发，直接返回
        }

        // 阶段2：强制 Level-0 压缩
        if (maxCompactInterval == null || compactTriggerCount == null) {
            // 无间隔限制，每次都强制压缩
            return universal.forcePickL0(numLevels, runs);
        }

        // 有间隔限制，检查计数器
        compactTriggerCount.getAndIncrement(); // 计数器加1
        if (compactTriggerCount.compareAndSet(maxCompactInterval, 0)) {
            // 达到间隔阈值，强制压缩并重置计数器
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Universal compaction due to max lookup compaction interval {}.",
                        maxCompactInterval);
            }
            return universal.forcePickL0(numLevels, runs);
        } else {
            // 未达到间隔阈值，跳过压缩
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Skip universal compaction due to lookup compaction trigger count {} is less than the max interval {}.",
                        compactTriggerCount.get(),
                        maxCompactInterval);
            }
            return Optional.empty();
        }
    }
}
