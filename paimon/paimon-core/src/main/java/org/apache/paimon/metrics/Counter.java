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

package org.apache.paimon.metrics;

import org.apache.paimon.annotation.Public;

/**
 * 计数器指标接口。
 *
 * <p>计数器是一个 {@link Metric}，用于测量计数值，支持增加和减少操作。
 *
 * <h3>核心功能：</h3>
 * <ul>
 *   <li>递增计数（按1或指定值）
 *   <li>递减计数（按1或指定值）
 *   <li>获取当前计数值
 * </ul>
 *
 * <h3>使用场景：</h3>
 * <ul>
 *   <li>统计提交次数
 *   <li>统计写入记录数
 *   <li>统计文件数量
 *   <li>统计错误次数
 * </ul>
 *
 * <h3>实现类：</h3>
 * <ul>
 *   <li>{@link SimpleCounter}: 简单的低开销实现
 * </ul>
 *
 * @since 0.5.0
 */
@Public
public interface Counter extends Metric {

    /**
     * 递增计数1。
     */
    void inc();

    /**
     * 按指定值递增计数。
     *
     * @param n 递增值
     */
    void inc(long n);

    /**
     * 递减计数1。
     */
    void dec();

    /**
     * 按指定值递减计数。
     *
     * @param n 递减值
     */
    void dec(long n);

    /**
     * 获取当前计数值。
     *
     * @return 当前计数
     */
    long getCount();

    @Override
    default MetricType getMetricType() {
        return MetricType.COUNTER;
    }
}
