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

package org.apache.paimon.compact;

import org.apache.paimon.io.DataFileMeta;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 压缩结果
 *
 * <p>封装了一次压缩操作的结果，包括：
 * <ul>
 *   <li>before：压缩前的输入文件列表
 *   <li>after：压缩后的输出文件列表
 *   <li>changelog：压缩过程中生成的 changelog 文件列表（FULL_COMPACTION/LOOKUP 模式）
 *   <li>deletionFile：删除文件记录（用于 Deletion Vector）
 * </ul>
 *
 * <p>压缩过程：
 * <pre>
 * before 文件（多个小文件）
 *     ↓ 压缩合并
 * after 文件（少量大文件）
 *     ↓ 同时生成（FULL_COMPACTION/LOOKUP 模式）
 * changelog 文件（记录数据变化）
 * </pre>
 *
 * <p>Changelog 生成：
 * <ul>
 *   <li>INPUT 模式：不在压缩时生成，changelog 为空
 *   <li>FULL_COMPACTION 模式：全量压缩时生成 changelog
 *   <li>LOOKUP 模式：Level-0 压缩时生成 changelog
 *   <li>NONE 模式：不生成 changelog
 * </ul>
 *
 * <p>文件生命周期：
 * <ul>
 *   <li>before 文件在压缩后可以被删除（但需要考虑 snapshot 的引用）
 *   <li>after 文件是新生成的，需要添加到文件系统
 *   <li>changelog 文件需要单独管理，供下游消费
 * </ul>
 */
public class CompactResult {

    /** 压缩前的输入文件列表 */
    private final List<DataFileMeta> before;

    /** 压缩后的输出文件列表 */
    private final List<DataFileMeta> after;

    /** 压缩过程中生成的 changelog 文件列表（FULL_COMPACTION/LOOKUP 模式） */
    private final List<DataFileMeta> changelog;

    /** 删除文件记录（用于 Deletion Vector 模式） */
    @Nullable private CompactDeletionFile deletionFile;

    /**
     * 构造空的压缩结果
     */
    public CompactResult() {
        this(Collections.emptyList(), Collections.emptyList());
    }

    /**
     * 构造单文件的压缩结果
     *
     * @param before 压缩前的文件
     * @param after 压缩后的文件
     */
    public CompactResult(DataFileMeta before, DataFileMeta after) {
        this(Collections.singletonList(before), Collections.singletonList(after));
    }

    /**
     * 构造多文件的压缩结果（无 changelog）
     *
     * @param before 压缩前的文件列表
     * @param after 压缩后的文件列表
     */
    public CompactResult(List<DataFileMeta> before, List<DataFileMeta> after) {
        this(before, after, Collections.emptyList());
    }

    /**
     * 构造完整的压缩结果（包含 changelog）
     *
     * @param before 压缩前的文件列表
     * @param after 压缩后的文件列表
     * @param changelog changelog 文件列表
     */
    public CompactResult(
            List<DataFileMeta> before, List<DataFileMeta> after, List<DataFileMeta> changelog) {
        this.before = new ArrayList<>(before);
        this.after = new ArrayList<>(after);
        this.changelog = new ArrayList<>(changelog);
    }

    /**
     * 获取压缩前的文件列表
     *
     * @return 输入文件列表
     */
    public List<DataFileMeta> before() {
        return before;
    }

    /**
     * 获取压缩后的文件列表
     *
     * @return 输出文件列表
     */
    public List<DataFileMeta> after() {
        return after;
    }

    /**
     * 获取 changelog 文件列表
     *
     * @return changelog 文件列表（FULL_COMPACTION/LOOKUP 模式下非空）
     */
    public List<DataFileMeta> changelog() {
        return changelog;
    }

    /**
     * 设置删除文件记录
     *
     * @param deletionFile 删除文件记录
     */
    public void setDeletionFile(@Nullable CompactDeletionFile deletionFile) {
        this.deletionFile = deletionFile;
    }

    /**
     * 获取删除文件记录
     *
     * @return 删除文件记录
     */
    @Nullable
    public CompactDeletionFile deletionFile() {
        return deletionFile;
    }

    /**
     * 合并另一个压缩结果
     *
     * <p>将另一个压缩结果的文件列表追加到当前结果中
     *
     * @param that 另一个压缩结果
     * @throws UnsupportedOperationException 如果任一结果已设置 deletionFile
     */
    public void merge(CompactResult that) {
        before.addAll(that.before);
        after.addAll(that.after);
        changelog.addAll(that.changelog);

        if (deletionFile != null || that.deletionFile != null) {
            throw new UnsupportedOperationException(
                    "There is a bug, deletionFile can't be set before merge.");
        }
    }
}
