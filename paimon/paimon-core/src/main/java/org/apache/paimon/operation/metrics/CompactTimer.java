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
import org.apache.paimon.utils.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

import java.util.LinkedList;

/**
 * 压缩计时器
 *
 * <p>支持在O(1)摊销时间复杂度内完成以下操作的计时器：
 * <ul>
 *   <li>启动计时器
 *   <li>停止计时器
 *   <li>查询最近 queryLengthMillis 毫秒内计时器运行了多长时间
 * </ul>
 *
 * <h2>设计目标</h2>
 * <p>该计时器用于跟踪压缩线程的繁忙度：
 * <ul>
 *   <li><b>滑动窗口</b>：只关注最近一段时间（如60秒）内的运行时间
 *   <li><b>高效查询</b>：通过缓存中间值实现O(1)查询
 *   <li><b>线程安全</b>：使用synchronized保证并发安全
 * </ul>
 *
 * <h2>数据结构</h2>
 * <p>使用时间区间列表记录计时器的运行历史：
 * <ul>
 *   <li><b>TimeInterval</b>：记录单次运行的开始和结束时间
 *   <li><b>LinkedList</b>：按时间顺序存储所有区间
 *   <li><b>innerSum</b>：缓存中间区间的总时长，避免重复计算
 * </ul>
 *
 * <h2>优化策略</h2>
 * <p>为了实现O(1)查询，使用以下优化：
 * <ul>
 *   <li>只有第一个和最后一个区间可能不完整（跨越窗口边界）
 *   <li>中间区间的总时长被缓存在 innerSum 中
 *   <li>查询时只需计算首尾两个区间 + innerSum
 *   <li>过期区间被及时移除
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * CompactTimer timer = new CompactTimer(60_000); // 60秒窗口
 * timer.start();
 * // 执行压缩
 * timer.finish();
 * long busyTime = timer.calculateLength(); // 最近60秒的繁忙时间
 * }</pre>
 *
 * @see CompactionMetrics 压缩指标
 */
@ThreadSafe
public class CompactTimer {

    /** 查询窗口长度（毫秒） */
    private final long queryLengthMillis;

    /** 时间区间列表 */
    private final LinkedList<TimeInterval> intervals;

    /** 中间区间的总时长（不包括第一个和最后一个区间） */
    private long innerSum;

    /** 最后一次调用的时间戳，用于检测时间倒退 */
    private long lastCallMillis;

    /**
     * 构造压缩计时器
     *
     * @param queryLengthMillis 查询窗口长度（毫秒）
     */
    public CompactTimer(long queryLengthMillis) {
        this.queryLengthMillis = queryLengthMillis;
        this.intervals = new LinkedList<>();
        this.innerSum = 0;
        this.lastCallMillis = -1;
    }

    /**
     * 启动计时器
     *
     * <p>使用当前系统时间启动计时器。
     */
    public void start() {
        start(System.currentTimeMillis());
    }

    /**
     * 启动计时器（测试用）
     *
     * @param millis 启动时间戳（毫秒）
     */
    @VisibleForTesting
    void start(long millis) {
        synchronized (intervals) {
            // 移除过期的区间
            removeExpiredIntervals(millis - queryLengthMillis);
            Preconditions.checkArgument(
                    intervals.isEmpty() || intervals.getLast().finished(),
                    "There is an unfinished interval. This is unexpected.");
            Preconditions.checkArgument(lastCallMillis <= millis, "millis must not decrease.");
            lastCallMillis = millis;

            // 如果列表中有多个区间，将最后一个区间加入innerSum
            if (intervals.size() > 1) {
                innerSum += intervals.getLast().totalLength();
            }
            // 添加新的区间
            intervals.add(new TimeInterval(millis));
        }
    }

    /**
     * 停止计时器
     *
     * <p>使用当前系统时间停止计时器。
     */
    public void finish() {
        finish(System.currentTimeMillis());
    }

    /**
     * 停止计时器（测试用）
     *
     * @param millis 停止时间戳（毫秒）
     */
    @VisibleForTesting
    void finish(long millis) {
        synchronized (intervals) {
            // 移除过期的区间
            removeExpiredIntervals(millis - queryLengthMillis);
            Preconditions.checkArgument(
                    intervals.size() > 0 && !intervals.getLast().finished(),
                    "There is no unfinished interval. This is unexpected.");
            Preconditions.checkArgument(lastCallMillis <= millis, "millis must not decrease.");
            lastCallMillis = millis;

            // 结束最后一个区间
            intervals.getLast().finish(millis);
        }
    }

    /**
     * 计算运行时长
     *
     * <p>计算最近 queryLengthMillis 毫秒内计时器运行的总时长。
     *
     * @return 运行时长（毫秒）
     */
    public long calculateLength() {
        return calculateLength(System.currentTimeMillis());
    }

    /**
     * 计算运行时长（测试用）
     *
     * @param toMillis 结束时间戳（毫秒）
     * @return 运行时长（毫秒）
     */
    @VisibleForTesting
    long calculateLength(long toMillis) {
        synchronized (intervals) {
            Preconditions.checkArgument(lastCallMillis <= toMillis, "millis must not decrease.");
            lastCallMillis = toMillis;

            long fromMillis = toMillis - queryLengthMillis;
            // 移除过期的区间
            removeExpiredIntervals(fromMillis);

            if (intervals.isEmpty()) {
                return 0;
            } else if (intervals.size() == 1) {
                // 只有一个区间，直接计算
                return intervals.getFirst().calculateLength(fromMillis, toMillis);
            } else {
                // 多个区间：第一个 + 中间所有 + 最后一个
                return innerSum
                        + intervals.getFirst().calculateLength(fromMillis, toMillis)
                        + intervals.getLast().calculateLength(fromMillis, toMillis);
            }
        }
    }

    /**
     * 移除过期的时间区间
     *
     * <p>移除所有在过期时间之前结束的区间。
     *
     * @param expireMillis 过期时间戳（毫秒）
     */
    private void removeExpiredIntervals(long expireMillis) {
        while (intervals.size() > 0
                && intervals.getFirst().finished()
                && intervals.getFirst().finishMillis <= expireMillis) {
            intervals.removeFirst();
            // 更新innerSum：如果移除后还有多个区间，减去新的第一个区间
            if (intervals.size() > 1) {
                innerSum -= intervals.getFirst().totalLength();
            }
        }
    }

    /**
     * 时间区间
     *
     * <p>记录单次计时器运行的时间区间。
     */
    private static class TimeInterval {

        /** 开始时间戳（毫秒） */
        private final long startMillis;

        /** 结束时间戳（毫秒），null表示未结束 */
        private Long finishMillis;

        /**
         * 构造时间区间
         *
         * @param startMillis 开始时间戳
         */
        private TimeInterval(long startMillis) {
            this.startMillis = startMillis;
            this.finishMillis = null;
        }

        /**
         * 结束区间
         *
         * @param finishMillis 结束时间戳
         */
        private void finish(long finishMillis) {
            this.finishMillis = finishMillis;
        }

        /**
         * 判断区间是否已结束
         *
         * @return true表示已结束
         */
        private boolean finished() {
            return finishMillis != null;
        }

        /**
         * 计算区间总时长
         *
         * @return 总时长（毫秒）
         */
        private long totalLength() {
            return finishMillis - startMillis;
        }

        /**
         * 计算区间在指定时间范围内的重叠时长
         *
         * @param fromMillis 开始时间戳
         * @param toMillis 结束时间戳
         * @return 重叠时长（毫秒）
         */
        private long calculateLength(long fromMillis, long toMillis) {
            if (finishMillis == null) {
                // 区间未结束，计算从max(start, from)到to的时长
                return toMillis - Math.min(Math.max(startMillis, fromMillis), toMillis);
            } else {
                // 区间已结束，计算重叠部分
                long l = Math.max(fromMillis, startMillis);
                long r = Math.min(toMillis, finishMillis);
                return Math.max(0, r - l);
            }
        }
    }
}
