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

package org.apache.paimon.utils;

import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;

import javax.annotation.Nullable;

/**
 * 提交增量信息
 *
 * <p>封装了一次提交需要持久化的所有变更信息，包括：
 * <ul>
 *   <li>dataIncrement：数据文件增量（新文件、删除文件、changelog 文件）
 *   <li>compactIncrement：压缩文件增量（压缩前文件、压缩后文件、压缩 changelog）
 *   <li>compactDeletionFile：压缩删除文件记录（用于 Deletion Vector）
 * </ul>
 *
 * <p>提交流程：
 * <pre>
 * 1. MergeTreeWriter 收集增量信息
 * 2. 调用 prepareCommit() 获取 CommitIncrement
 * 3. 上层将 CommitIncrement 持久化到 manifest
 * 4. 提交完成后，新文件对读取可见，旧文件可以删除
 * </pre>
 *
 * <p>数据增量（DataIncrement）：
 * <ul>
 *   <li>newFiles：新生成的 Level-0 文件（刷新 WriteBuffer 产生）
 *   <li>deletedFiles：被删除的文件
 *   <li>changelogFiles：新生成的 changelog 文件（INPUT 模式）
 * </ul>
 *
 * <p>压缩增量（CompactIncrement）：
 * <ul>
 *   <li>compactBefore：压缩前的输入文件
 *   <li>compactAfter：压缩后的输出文件
 *   <li>changelogFiles：压缩生成的 changelog 文件（FULL_COMPACTION/LOOKUP 模式）
 * </ul>
 *
 * @see DataIncrement
 * @see CompactIncrement
 */
public class CommitIncrement {

    /** 数据文件增量（新文件、删除文件、changelog 文件） */
    private final DataIncrement dataIncrement;

    /** 压缩文件增量（压缩前文件、压缩后文件、压缩 changelog） */
    private final CompactIncrement compactIncrement;

    /** 压缩删除文件记录（用于 Deletion Vector 模式） */
    @Nullable private final CompactDeletionFile compactDeletionFile;

    /**
     * 构造提交增量信息
     *
     * @param dataIncrement 数据文件增量
     * @param compactIncrement 压缩文件增量
     * @param compactDeletionFile 压缩删除文件记录
     */
    public CommitIncrement(
            DataIncrement dataIncrement,
            CompactIncrement compactIncrement,
            @Nullable CompactDeletionFile compactDeletionFile) {
        this.dataIncrement = dataIncrement;
        this.compactIncrement = compactIncrement;
        this.compactDeletionFile = compactDeletionFile;
    }

    /**
     * 获取数据文件增量
     *
     * <p>包含新文件、删除文件、changelog 文件（INPUT 模式）
     *
     * @return 数据文件增量
     */
    public DataIncrement newFilesIncrement() {
        return dataIncrement;
    }

    /**
     * 获取压缩文件增量
     *
     * <p>包含压缩前文件、压缩后文件、压缩 changelog 文件（FULL_COMPACTION/LOOKUP 模式）
     *
     * @return 压缩文件增量
     */
    public CompactIncrement compactIncrement() {
        return compactIncrement;
    }

    /**
     * 获取压缩删除文件记录
     *
     * @return 删除文件记录（可能为 null）
     */
    @Nullable
    public CompactDeletionFile compactDeletionFile() {
        return compactDeletionFile;
    }

    @Override
    public String toString() {
        return dataIncrement.toString() + "\n" + compactIncrement + "\n" + compactDeletionFile;
    }
}
