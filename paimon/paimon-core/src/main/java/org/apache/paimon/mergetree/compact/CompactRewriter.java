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

import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;

import java.io.Closeable;
import java.util.List;

/**
 * 压缩重写器接口
 *
 * <p>负责将多个 section 的数据重写到新的层级，是压缩操作的核心执行组件。
 *
 * <p>主要功能：
 * <ul>
 *   <li>将多个 SortedRun 合并重写到新层级
 *   <li>升级文件到新层级（通常只更新元数据，但某些场景需要重写数据）
 * </ul>
 *
 * <p>Section 的概念：
 * <ul>
 *   <li>Section 是一组 {@link SortedRun} 的列表
 *   <li>不同 Section 之间的键范围不重叠
 *   <li>这种分层设计可以最小化同时处理的 SortedRun 数量
 * </ul>
 *
 * <p>典型实现：
 * <ul>
 *   <li>{@link MergeTreeCompactRewriter}：标准的 MergeTree 压缩重写器
 *   <li>{@link ChangelogMergeTreeRewriter}：支持 changelog 生成的重写器
 *   <li>{@link LookupMergeTreeCompactRewriter}：使用 lookup 机制生成 changelog 的重写器
 * </ul>
 *
 * @see MergeTreeCompactManager
 * @see SortedRun
 */
public interface CompactRewriter extends Closeable {

    /**
     * 将 sections 重写到新层级
     *
     * <p>工作流程：
     * <ol>
     *   <li>读取所有 section 中的数据文件
     *   <li>使用合并函数合并同一个 key 的多个版本
     *   <li>写入新的数据文件到目标层级
     *   <li>可选：生成 changelog 文件
     * </ol>
     *
     * @param outputLevel 目标层级（合并后数据写入的层级）
     * @param dropDelete 是否丢弃删除记录（参见 {@link MergeTreeCompactManager#triggerCompaction}）
     * @param sections section 列表（每个 section 包含一组 {@link SortedRun}，不同 section 的键范围不重叠）
     * @return 压缩结果（包含新生成的文件信息）
     * @throws Exception 压缩过程中的异常
     */
    CompactResult rewrite(int outputLevel, boolean dropDelete, List<List<SortedRun>> sections)
            throws Exception;

    /**
     * 升级文件到新层级
     *
     * <p>升级策略：
     * <ul>
     *   <li>通常情况：只更新文件元数据（level 字段），不重写数据
     *   <li>特殊场景：某些情况必须重写文件数据，例如 {@link ChangelogMergeTreeRewriter}
     * </ul>
     *
     * <p>使用场景：
     * <ul>
     *   <li>Level 0 的单个文件可以直接升级到 Level 1（如果没有重叠）
     *   <li>避免不必要的数据重写，提高性能
     * </ul>
     *
     * @param outputLevel 目标层级
     * @param file 待升级的文件
     * @return 压缩结果（包含升级后的文件信息）
     * @throws Exception 升级过程中的异常
     */
    CompactResult upgrade(int outputLevel, DataFileMeta file) throws Exception;
}
