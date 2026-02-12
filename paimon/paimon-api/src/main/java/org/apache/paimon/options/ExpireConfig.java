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

package org.apache.paimon.options;

import java.time.Duration;

/**
 * 快照和变更日志(changelog)过期配置类。
 *
 * <p>该类封装了快照和变更日志的保留策略配置,包括:
 * <ul>
 *   <li>保留的最大快照数
 *   <li>保留的最小快照数
 *   <li>快照时间保留策略
 *   <li>单次删除的最大快照数
 *   <li>变更日志的对应配置
 * </ul>
 *
 * <h2>解耦模式</h2>
 * 当变更日志的保留配置大于快照的保留配置时,会启用解耦模式。
 * 这意味着变更日志可以比快照保留更长时间。
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 使用构建器创建配置
 * ExpireConfig config = ExpireConfig.builder()
 *     .snapshotRetainMax(10)
 *     .snapshotRetainMin(5)
 *     .snapshotTimeRetain(Duration.ofHours(1))
 *     .changelogRetainMax(20)  // 大于 snapshotRetainMax,启用解耦模式
 *     .build();
 *
 * // 检查是否解耦
 * boolean decoupled = config.isChangelogDecoupled();  // true
 * }</pre>
 */
public class ExpireConfig {
    /** 快照保留的最大数量 */
    private final int snapshotRetainMax;
    /** 快照保留的最小数量 */
    private final int snapshotRetainMin;
    /** 快照的时间保留策略 */
    private final Duration snapshotTimeRetain;
    /** 单次删除的最大快照数 */
    private final int snapshotMaxDeletes;
    /** 变更日志保留的最大数量 */
    private final int changelogRetainMax;
    /** 变更日志保留的最小数量 */
    private final int changelogRetainMin;
    /** 变更日志的时间保留策略 */
    private final Duration changelogTimeRetain;
    /** 单次删除的最大变更日志数 */
    private final int changelogMaxDeletes;
    /** 是否启用变更日志解耦模式 */
    private final boolean changelogDecoupled;

    /**
     * 构造过期配置对象。
     *
     * @param snapshotRetainMax 快照保留的最大数量
     * @param snapshotRetainMin 快照保留的最小数量
     * @param snapshotTimeRetain 快照的时间保留策略
     * @param snapshotMaxDeletes 单次删除的最大快照数
     * @param changelogRetainMax 变更日志保留的最大数量
     * @param changelogRetainMin 变更日志保留的最小数量
     * @param changelogTimeRetain 变更日志的时间保留策略
     * @param changelogMaxDeletes 单次删除的最大变更日志数
     */
    public ExpireConfig(
            int snapshotRetainMax,
            int snapshotRetainMin,
            Duration snapshotTimeRetain,
            int snapshotMaxDeletes,
            int changelogRetainMax,
            int changelogRetainMin,
            Duration changelogTimeRetain,
            int changelogMaxDeletes) {
        this.snapshotRetainMax = snapshotRetainMax;
        this.snapshotRetainMin = snapshotRetainMin;
        this.snapshotTimeRetain = snapshotTimeRetain;
        this.snapshotMaxDeletes = snapshotMaxDeletes;
        this.changelogRetainMax = changelogRetainMax;
        this.changelogRetainMin = changelogRetainMin;
        this.changelogTimeRetain = changelogTimeRetain;
        this.changelogMaxDeletes = changelogMaxDeletes;
        // 如果变更日志的任一保留配置大于快照的对应配置,则启用解耦模式
        this.changelogDecoupled =
                changelogRetainMax > snapshotRetainMax
                        || changelogRetainMin > snapshotRetainMin
                        || changelogTimeRetain.compareTo(snapshotTimeRetain) > 0;
    }

    /** 获取快照保留的最大数量。 */
    public int getSnapshotRetainMax() {
        return snapshotRetainMax;
    }

    /** 获取快照保留的最小数量。 */
    public int getSnapshotRetainMin() {
        return snapshotRetainMin;
    }

    /** 获取快照的时间保留策略。 */
    public Duration getSnapshotTimeRetain() {
        return snapshotTimeRetain;
    }

    /** 获取变更日志保留的最大数量。 */
    public int getChangelogRetainMax() {
        return changelogRetainMax;
    }

    /** 获取变更日志保留的最小数量。 */
    public int getChangelogRetainMin() {
        return changelogRetainMin;
    }

    /** 获取变更日志的时间保留策略。 */
    public Duration getChangelogTimeRetain() {
        return changelogTimeRetain;
    }

    /** 获取单次删除的最大快照数。 */
    public int getSnapshotMaxDeletes() {
        return snapshotMaxDeletes;
    }

    /** 获取单次删除的最大变更日志数。 */
    public int getChangelogMaxDeletes() {
        return changelogMaxDeletes;
    }

    /**
     * 判断是否启用变更日志解耦模式。
     *
     * @return 如果启用解耦模式返回 true,否则返回 false
     */
    public boolean isChangelogDecoupled() {
        return changelogDecoupled;
    }

    /**
     * 创建构建器实例。
     *
     * @return ExpireConfig 构建器
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * {@link ExpireConfig} 的构建器类。
     *
     * <p>提供流式 API 来构建过期配置对象。
     */
    public static final class Builder {
        /** 快照保留的最大数量,默认为 Integer.MAX_VALUE */
        private int snapshotRetainMax = Integer.MAX_VALUE;
        /** 快照保留的最小数量,默认为 1 */
        private int snapshotRetainMin = 1;
        /** 快照的时间保留策略,默认为最大值 */
        private Duration snapshotTimeRetain = Duration.ofMillis(Long.MAX_VALUE);
        /** 单次删除的最大快照数,默认为 Integer.MAX_VALUE */
        private int snapshotMaxDeletes = Integer.MAX_VALUE;
        // 如果未设置变更日志配置,则默认使用快照的配置
        private Integer changelogRetainMax = null;
        private Integer changelogRetainMin = null;
        private Duration changelogTimeRetain = null;
        private Integer changelogMaxDeletes = null;

        /**
         * 创建构建器实例。
         *
         * @return Builder 实例
         */
        public static Builder builder() {
            return new Builder();
        }

        /**
         * 设置快照保留的最大数量。
         *
         * @param snapshotRetainMax 最大数量
         * @return Builder 实例,用于链式调用
         */
        public Builder snapshotRetainMax(int snapshotRetainMax) {
            this.snapshotRetainMax = snapshotRetainMax;
            return this;
        }

        /**
         * 设置快照保留的最小数量。
         *
         * @param snapshotRetainMin 最小数量
         * @return Builder 实例,用于链式调用
         */
        public Builder snapshotRetainMin(int snapshotRetainMin) {
            this.snapshotRetainMin = snapshotRetainMin;
            return this;
        }

        /**
         * 设置快照的时间保留策略。
         *
         * @param snapshotTimeRetain 时间保留策略
         * @return Builder 实例,用于链式调用
         */
        public Builder snapshotTimeRetain(Duration snapshotTimeRetain) {
            this.snapshotTimeRetain = snapshotTimeRetain;
            return this;
        }

        /**
         * 设置单次删除的最大快照数。
         *
         * @param snapshotMaxDeletes 最大删除数
         * @return Builder 实例,用于链式调用
         */
        public Builder snapshotMaxDeletes(int snapshotMaxDeletes) {
            this.snapshotMaxDeletes = snapshotMaxDeletes;
            return this;
        }

        /**
         * 设置变更日志保留的最大数量。
         *
         * @param changelogRetainMax 最大数量
         * @return Builder 实例,用于链式调用
         */
        public Builder changelogRetainMax(Integer changelogRetainMax) {
            this.changelogRetainMax = changelogRetainMax;
            return this;
        }

        /**
         * 设置变更日志保留的最小数量。
         *
         * @param changelogRetainMin 最小数量
         * @return Builder 实例,用于链式调用
         */
        public Builder changelogRetainMin(Integer changelogRetainMin) {
            this.changelogRetainMin = changelogRetainMin;
            return this;
        }

        /**
         * 设置变更日志的时间保留策略。
         *
         * @param changelogTimeRetain 时间保留策略
         * @return Builder 实例,用于链式调用
         */
        public Builder changelogTimeRetain(Duration changelogTimeRetain) {
            this.changelogTimeRetain = changelogTimeRetain;
            return this;
        }

        /**
         * 设置单次删除的最大变更日志数。
         *
         * @param changelogMaxDeletes 最大删除数
         * @return Builder 实例,用于链式调用
         */
        public Builder changelogMaxDeletes(Integer changelogMaxDeletes) {
            this.changelogMaxDeletes = changelogMaxDeletes;
            return this;
        }

        /**
         * 构建 ExpireConfig 实例。
         *
         * <p>如果变更日志配置未设置,则使用快照的对应配置。
         *
         * @return ExpireConfig 实例
         */
        public ExpireConfig build() {
            return new ExpireConfig(
                    snapshotRetainMax,
                    snapshotRetainMin,
                    snapshotTimeRetain,
                    snapshotMaxDeletes,
                    changelogRetainMax == null ? snapshotRetainMax : changelogRetainMax,
                    changelogRetainMin == null ? snapshotRetainMin : changelogRetainMin,
                    changelogTimeRetain == null ? snapshotTimeRetain : changelogTimeRetain,
                    changelogMaxDeletes == null ? snapshotMaxDeletes : changelogMaxDeletes);
        }
    }
}
