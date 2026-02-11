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

package org.apache.paimon.io;

import org.apache.paimon.index.IndexFileMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 压缩前后的文件变化，以及压缩过程中产生的 changelog
 *
 * <p>该类封装了一次压缩操作产生的文件增量，包括：
 * <ul>
 *   <li>compactBefore：压缩前的输入文件列表（被合并的多个文件）
 *   <li>compactAfter：压缩后的输出文件列表（合并后的少量文件）
 *   <li>changelogFiles：压缩过程中生成的 changelog 文件（FULL_COMPACTION/LOOKUP 模式）
 *   <li>newIndexFiles：压缩后新生成的索引文件
 *   <li>deletedIndexFiles：压缩后删除的索引文件
 * </ul>
 *
 * <p>压缩过程示意：
 * <pre>
 * compactBefore（多个小文件，如 3 个 Level-0 文件 + 2 个 Level-1 文件）
 *     ↓ 压缩合并（Compaction）
 * compactAfter（少量大文件，如 1 个 Level-1 文件）
 *     ↓ 同时生成（FULL_COMPACTION/LOOKUP 模式）
 * changelogFiles（记录数据变化的 changelog 文件）
 * </pre>
 *
 * <p>Changelog 文件生成模式：
 * <ul>
 *   <li>INPUT 模式：changelogFiles 为空（changelog 在写入时已双写）
 *   <li>FULL_COMPACTION 模式：全量压缩到 maxLevel 时生成 changelog
 *   <li>LOOKUP 模式：Level-0 文件参与压缩时通过 lookup 生成 changelog
 *   <li>NONE 模式：changelogFiles 为空
 * </ul>
 *
 * <p>文件生命周期：
 * <ul>
 *   <li>compactBefore 文件在压缩后可以删除（需考虑 snapshot 引用）
 *   <li>compactAfter 文件是新生成的，需添加到文件系统
 *   <li>changelogFiles 需要单独管理，供下游消费 changelog
 * </ul>
 *
 * <p>与 DataIncrement 的区别：
 * <ul>
 *   <li>DataIncrement：刷新 WriteBuffer 产生的增量（Level-0 文件）
 *   <li>CompactIncrement：压缩产生的增量（合并多层文件）
 * </ul>
 *
 * @see DataIncrement
 * @see org.apache.paimon.utils.CommitIncrement
 * @see org.apache.paimon.compact.CompactResult
 */
public class CompactIncrement {

    /** 压缩前的输入文件列表（被合并的源文件） */
    private final List<DataFileMeta> compactBefore;

    /** 压缩后的输出文件列表（合并后的新文件） */
    private final List<DataFileMeta> compactAfter;

    /** 压缩过程中生成的 changelog 文件列表（FULL_COMPACTION/LOOKUP 模式） */
    private final List<DataFileMeta> changelogFiles;

    /** 压缩后新生成的索引文件列表 */
    private final List<IndexFileMeta> newIndexFiles;

    /** 压缩后删除的索引文件列表 */
    private final List<IndexFileMeta> deletedIndexFiles;

    /**
     * 构造压缩增量（不包含索引文件）
     *
     * @param compactBefore 压缩前的文件列表
     * @param compactAfter 压缩后的文件列表
     * @param changelogFiles changelog 文件列表
     */
    public CompactIncrement(
            List<DataFileMeta> compactBefore,
            List<DataFileMeta> compactAfter,
            List<DataFileMeta> changelogFiles) {
        this(compactBefore, compactAfter, changelogFiles, new ArrayList<>(), new ArrayList<>());
    }

    /**
     * 构造完整的压缩增量（包含索引文件）
     *
     * @param compactBefore 压缩前的文件列表
     * @param compactAfter 压缩后的文件列表
     * @param changelogFiles changelog 文件列表
     * @param newIndexFiles 新生成的索引文件列表
     * @param deletedIndexFiles 删除的索引文件列表
     */
    public CompactIncrement(
            List<DataFileMeta> compactBefore,
            List<DataFileMeta> compactAfter,
            List<DataFileMeta> changelogFiles,
            List<IndexFileMeta> newIndexFiles,
            List<IndexFileMeta> deletedIndexFiles) {
        this.compactBefore = compactBefore;
        this.compactAfter = compactAfter;
        this.changelogFiles = changelogFiles;
        this.newIndexFiles = newIndexFiles;
        this.deletedIndexFiles = deletedIndexFiles;
    }

    /**
     * 获取压缩前的文件列表
     *
     * <p>这些文件是压缩的输入，压缩后可以删除
     *
     * @return 压缩前的文件列表
     */
    public List<DataFileMeta> compactBefore() {
        return compactBefore;
    }

    /**
     * 获取压缩后的文件列表
     *
     * <p>这些文件是压缩的输出，需要添加到文件系统
     *
     * @return 压缩后的文件列表
     */
    public List<DataFileMeta> compactAfter() {
        return compactAfter;
    }

    /**
     * 获取 changelog 文件列表
     *
     * <p>只有在 FULL_COMPACTION 或 LOOKUP 模式下才非空：
     * <ul>
     *   <li>FULL_COMPACTION：全量压缩到 maxLevel 时生成
     *   <li>LOOKUP：Level-0 文件参与压缩时生成
     *   <li>INPUT：为空（changelog 在写入时已生成）
     * </ul>
     *
     * @return changelog 文件列表
     */
    public List<DataFileMeta> changelogFiles() {
        return changelogFiles;
    }

    /**
     * 获取新生成的索引文件列表
     *
     * @return 新索引文件列表
     */
    public List<IndexFileMeta> newIndexFiles() {
        return newIndexFiles;
    }

    /**
     * 获取删除的索引文件列表
     *
     * @return 删除的索引文件列表
     */
    public List<IndexFileMeta> deletedIndexFiles() {
        return deletedIndexFiles;
    }

    /**
     * 判断是否为空增量
     *
     * <p>当所有文件列表都为空时返回 true
     *
     * @return 是否为空增量
     */
    public boolean isEmpty() {
        return compactBefore.isEmpty()
                && compactAfter.isEmpty()
                && changelogFiles.isEmpty()
                && newIndexFiles.isEmpty()
                && deletedIndexFiles.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CompactIncrement that = (CompactIncrement) o;
        return Objects.equals(compactBefore, that.compactBefore)
                && Objects.equals(compactAfter, that.compactAfter)
                && Objects.equals(changelogFiles, that.changelogFiles)
                && Objects.equals(newIndexFiles, that.newIndexFiles)
                && Objects.equals(deletedIndexFiles, that.deletedIndexFiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(compactBefore, compactAfter, changelogFiles);
    }

    @Override
    public String toString() {
        return String.format(
                "CompactIncrement {compactBefore = %s, compactAfter = %s, changelogFiles = %s, newIndexFiles = %s, deletedIndexFiles = %s}",
                compactBefore.stream().map(DataFileMeta::fileName).collect(Collectors.toList()),
                compactAfter.stream().map(DataFileMeta::fileName).collect(Collectors.toList()),
                changelogFiles.stream().map(DataFileMeta::fileName).collect(Collectors.toList()),
                newIndexFiles.stream().map(IndexFileMeta::fileName).collect(Collectors.toList()),
                deletedIndexFiles.stream()
                        .map(IndexFileMeta::fileName)
                        .collect(Collectors.toList()));
    }

    /**
     * 创建空的压缩增量
     *
     * <p>用于没有任何压缩发生的情况
     *
     * @return 空的 CompactIncrement
     */
    public static CompactIncrement emptyIncrement() {
        return new CompactIncrement(
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }
}
