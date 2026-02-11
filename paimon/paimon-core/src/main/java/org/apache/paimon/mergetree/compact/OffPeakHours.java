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

import javax.annotation.Nullable;

/**
 * 非峰值时段配置
 *
 * <p>用于在非峰值时段调整压缩策略，使压缩更激进（更容易触发）。
 *
 * <p>核心功能：
 * <ul>
 *   <li>定义非峰值时段的时间范围（startHour 到 endHour）
 *   <li>在非峰值时段提供额外的压缩比例（compactOffPeakRatio）
 *   <li>在峰值时段返回0，使用默认压缩策略
 * </ul>
 *
 * <p>工作原理：
 * <pre>
 * UniversalCompaction 的大小比例检查：
 * candidateSize * (100 + sizeRatio + offPeakRatio) / 100 < nextFileSize
 *
 * - 峰值时段：offPeakRatio = 0，使用默认 sizeRatio
 * - 非峰值时段：offPeakRatio = compactOffPeakRatio，增加比例使压缩更容易触发
 * </pre>
 *
 * <p>使用场景：
 * <ul>
 *   <li>在业务低峰期更激进地压缩，减少文件数量
 *   <li>在业务高峰期保守压缩，减少资源占用
 * </ul>
 *
 * <p>配置示例：
 * <pre>
 * compaction.off-peak-start-hour = 22  # 晚上10点开始
 * compaction.off-peak-end-hour = 6     # 早上6点结束
 * compaction.off-peak-ratio = 10       # 增加10%的比例
 * </pre>
 */
public class OffPeakHours {

    /** 非峰值时段开始小时（0-23） */
    private final int startHour;
    /** 非峰值时段结束小时（0-23） */
    private final int endHour;
    /** 非峰值时段的压缩比例增量 */
    private final int compactOffPeakRatio;

    /**
     * 构造非峰值时段配置
     *
     * @param startHour 开始小时（0-23）
     * @param endHour 结束小时（0-23）
     * @param compactOffPeakRatio 压缩比例增量
     */
    private OffPeakHours(int startHour, int endHour, int compactOffPeakRatio) {
        this.startHour = startHour;
        this.endHour = endHour;
        this.compactOffPeakRatio = compactOffPeakRatio;
    }

    /**
     * 获取目标小时的压缩比例增量
     *
     * <p>判断是否为非峰值时段：
     * <ul>
     *   <li>正常范围（startHour <= endHour）：
     *       startHour <= targetHour < endHour → 非峰值
     *   <li>跨天范围（startHour > endHour）：
     *       targetHour < endHour || targetHour >= startHour → 非峰值
     * </ul>
     *
     * <p>示例：
     * <pre>
     * startHour=22, endHour=6, compactOffPeakRatio=10
     * - targetHour=23 → 非峰值 → 返回10
     * - targetHour=2  → 非峰值 → 返回10
     * - targetHour=12 → 峰值 → 返回0
     * </pre>
     *
     * @param targetHour 目标小时（0-23）
     * @return 压缩比例增量（非峰值时段返回 compactOffPeakRatio，否则返回0）
     */
    public int currentRatio(int targetHour) {
        boolean isOffPeak;
        if (startHour <= endHour) {
            // 正常范围（例如：8点到18点）
            isOffPeak = startHour <= targetHour && targetHour < endHour;
        } else {
            // 跨天范围（例如：22点到次日6点）
            isOffPeak = targetHour < endHour || startHour <= targetHour;
        }
        return isOffPeak ? compactOffPeakRatio : 0;
    }

    /**
     * 从配置创建非峰值时段配置
     *
     * @param options 配置选项
     * @return 非峰值时段配置或 null
     */
    @Nullable
    public static OffPeakHours create(CoreOptions options) {
        return create(
                options.compactOffPeakStartHour(),
                options.compactOffPeakEndHour(),
                options.compactOffPeakRatio());
    }

    /**
     * 创建非峰值时段配置
     *
     * <p>如果未配置（startHour 或 endHour 为 -1）或配置无效（startHour == endHour），
     * 返回 null 表示不启用非峰值时段优化。
     *
     * @param startHour 开始小时（-1 表示未配置）
     * @param endHour 结束小时（-1 表示未配置）
     * @param compactOffPeakRatio 压缩比例增量
     * @return 非峰值时段配置或 null
     */
    public static OffPeakHours create(int startHour, int endHour, int compactOffPeakRatio) {
        if (startHour == -1 || endHour == -1) {
            return null; // 未配置，不启用
        }

        if (startHour == endHour) {
            return null; // 开始和结束相同，无效配置
        }

        return new OffPeakHours(startHour, endHour, compactOffPeakRatio);
    }
}
