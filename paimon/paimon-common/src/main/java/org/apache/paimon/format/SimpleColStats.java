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

package org.apache.paimon.format;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * 简单的列统计信息类，支持以下统计指标：
 *
 * <ul>
 *   <li>min: 列的最小值
 *   <li>max: 列的最大值
 *   <li>nullCount: 空值数量
 * </ul>
 *
 * <p>这些统计信息可用于查询优化（如数据跳过）、元数据管理等场景。
 */
public class SimpleColStats {

    /** 表示没有统计信息的常量 */
    public static final SimpleColStats NONE = new SimpleColStats(null, null, null);

    /** 列的最小值 */
    @Nullable private final Object min;
    /** 列的最大值 */
    @Nullable private final Object max;
    /** 空值数量 */
    private final Long nullCount;

    /**
     * 构造函数。
     *
     * @param min 列的最小值，可为 null
     * @param max 列的最大值，可为 null
     * @param nullCount 空值数量，可为 null
     */
    public SimpleColStats(@Nullable Object min, @Nullable Object max, @Nullable Long nullCount) {
        this.min = min;
        this.max = max;
        this.nullCount = nullCount;
    }

    /**
     * 获取列的最小值。
     *
     * @return 最小值，可能为 null
     */
    @Nullable
    public Object min() {
        return min;
    }

    /**
     * 获取列的最大值。
     *
     * @return 最大值，可能为 null
     */
    @Nullable
    public Object max() {
        return max;
    }

    /**
     * 获取空值数量。
     *
     * @return 空值数量，可能为 null
     */
    @Nullable
    public Long nullCount() {
        return nullCount;
    }

    /**
     * 判断是否没有任何统计信息。
     *
     * @return 如果所有统计指标都为 null 返回 true，否则返回 false
     */
    public boolean isNone() {
        return min == null && max == null && nullCount == null;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SimpleColStats)) {
            return false;
        }
        SimpleColStats that = (SimpleColStats) o;
        return Objects.equals(min, that.min)
                && Objects.equals(max, that.max)
                && Objects.equals(nullCount, that.nullCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(min, max, nullCount);
    }

    @Override
    public String toString() {
        return String.format("{%s, %s, %d}", min, max, nullCount);
    }
}
