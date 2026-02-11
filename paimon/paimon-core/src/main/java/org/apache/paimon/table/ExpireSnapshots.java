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

package org.apache.paimon.table;

import org.apache.paimon.options.ExpireConfig;

/**
 * 快照过期接口。
 *
 * <p>ExpireSnapshots 负责清理旧快照和相关的数据文件，防止存储空间无限增长。
 *
 * <h3>过期策略</h3>
 * <p>快照过期由 {@link ExpireConfig} 控制，包括：
 * <ul>
 *   <li>时间保留策略：保留指定时间内的快照
 *   <li>数量保留策略：保留最近的 N 个快照
 *   <li>单次过期限制：单次过期的最大快照数
 * </ul>
 *
 * <h3>配置选项</h3>
 * <ul>
 *   <li>snapshot.time-retained：快照保留时间（默认 1 小时）
 *   <li>snapshot.num-retained.min：最少保留的快照数（默认 10）
 *   <li>snapshot.num-retained.max：最多保留的快照数（默认 Integer.MAX_VALUE）
 *   <li>snapshot.expire.limit：单次过期的最大快照数（默认 10）
 * </ul>
 *
 * <h3>过期流程</h3>
 * <ol>
 *   <li>确定要过期的快照列表（基于时间和数量策略）
 *   <li>收集快照引用的数据文件
 *   <li>删除快照文件
 *   <li>删除不再被引用的数据文件
 *   <li>更新 EARLIEST 指针
 * </ol>
 *
 * <h3>实现类</h3>
 * <ul>
 *   <li>ExpireSnapshotsImpl：标准快照过期实现
 *   <li>ExpireChangelogImpl：Changelog 过期实现
 * </ul>
 *
 * @see ExpireConfig 过期配置
 * @see ExpireSnapshotsImpl 快照过期实现
 * @see ExpireChangelogImpl Changelog 过期实现
 */
public interface ExpireSnapshots {

    /**
     * 配置过期策略。
     *
     * <p>设置快照过期的配置，包括时间保留、数量保留、过期限制等。
     *
     * @param expireConfig 过期配置
     * @return ExpireSnapshots 实例（支持链式调用）
     */
    ExpireSnapshots config(ExpireConfig expireConfig);

    /**
     * 执行快照过期操作。
     *
     * <p>此方法会：
     * <ol>
     *   <li>根据配置确定要过期的快照
     *   <li>删除快照文件和不再被引用的数据文件
     *   <li>更新快照元数据
     * </ol>
     *
     * @return 过期的快照数量
     */
    int expire();
}
