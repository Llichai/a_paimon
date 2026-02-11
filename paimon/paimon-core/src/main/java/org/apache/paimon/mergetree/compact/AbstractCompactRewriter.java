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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * CompactRewriter 的抽象基类
 *
 * <p>提供 {@link CompactRewriter} 的通用实现。
 *
 * <p>实现的功能：
 * <ul>
 *   <li>upgrade：文件升级（只更新元数据，不重写数据）
 *   <li>extractFilesFromSections：从 Sections 中提取文件列表
 *   <li>close：关闭资源（默认为空实现）
 * </ul>
 *
 * <p>子类需要实现：
 * <ul>
 *   <li>rewrite：压缩重写逻辑（核心方法）
 * </ul>
 *
 * <p>典型子类：
 * <ul>
 *   <li>{@link MergeTreeCompactRewriter}：标准的 MergeTree 压缩重写器
 *   <li>{@link ChangelogMergeTreeRewriter}：支持 changelog 生成的重写器
 *   <li>{@link LookupMergeTreeCompactRewriter}：使用 lookup 机制生成 changelog 的重写器
 * </ul>
 */
public abstract class AbstractCompactRewriter implements CompactRewriter {

    /**
     * 升级文件到新层级（默认实现）
     *
     * <p>升级策略：只更新文件元数据的 level 字段，不重写数据
     *
     * <p>工作流程：
     * <ol>
     *   <li>调用 {@link DataFileMeta#upgrade} 创建新的元数据（level 更新）
     *   <li>返回 CompactResult（包含旧文件和新文件）
     * </ol>
     *
     * <p>注意：某些子类可能需要重写此方法，例如 {@link ChangelogMergeTreeRewriter}
     * 需要重写数据文件而不是只更新元数据。
     *
     * @param outputLevel 目标层级
     * @param file 待升级的文件
     * @return 压缩结果（包含升级后的文件信息）
     * @throws Exception 升级过程中的异常
     */
    @Override
    public CompactResult upgrade(int outputLevel, DataFileMeta file) throws Exception {
        return new CompactResult(file, file.upgrade(outputLevel));
    }

    /**
     * 从 Sections 中提取所有文件
     *
     * <p>数据结构：
     * <ul>
     *   <li>Section：一组 {@link SortedRun} 列表（键范围不重叠）
     *   <li>SortedRun：一组有序的 {@link DataFileMeta} 列表
     * </ul>
     *
     * <p>提取逻辑：
     * <pre>
     * sections (List<List<SortedRun>>)
     *   → flatMap 展开为 SortedRun 列表
     *   → 从每个 SortedRun 提取文件列表
     *   → flatMap 展开为文件列表
     * </pre>
     *
     * @param sections Section 列表
     * @return 所有文件的扁平列表
     */
    protected static List<DataFileMeta> extractFilesFromSections(List<List<SortedRun>> sections) {
        return sections.stream()
                .flatMap(Collection::stream) // 展开 Section 为 SortedRun
                .map(SortedRun::files) // 获取每个 SortedRun 的文件列表
                .flatMap(Collection::stream) // 展开为文件列表
                .collect(Collectors.toList());
    }

    /**
     * 关闭资源（默认为空实现）
     *
     * <p>子类可以重写此方法以释放资源（如关闭文件、清理缓存等）
     *
     * @throws IOException IO 异常
     */
    @Override
    public void close() throws IOException {}
}
