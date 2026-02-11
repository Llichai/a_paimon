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
 * 数据文件、Changelog 文件和索引文件的增量信息
 *
 * <p>该类封装了一次写入操作（刷新 WriteBuffer）产生的文件增量，包括：
 * <ul>
 *   <li>newFiles：新生成的数据文件（Level-0 文件）
 *   <li>deletedFiles：被删除的数据文件
 *   <li>changelogFiles：新生成的 changelog 文件（仅 INPUT 模式）
 *   <li>newIndexFiles：新生成的索引文件
 *   <li>deletedIndexFiles：被删除的索引文件
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>MergeTreeWriter 刷新 WriteBuffer 时创建 DataIncrement
 *   <li>将 DataIncrement 封装到 CommitIncrement 中
 *   <li>上层将 CommitIncrement 持久化到 manifest
 * </ul>
 *
 * <p>Changelog 文件生成：
 * <ul>
 *   <li>INPUT 模式：changelogFiles 非空，包含双写的 changelog 文件
 *   <li>FULL_COMPACTION/LOOKUP 模式：changelogFiles 为空，changelog 在压缩时生成
 *   <li>NONE 模式：changelogFiles 为空
 * </ul>
 *
 * <p>与 CompactIncrement 的区别：
 * <ul>
 *   <li>DataIncrement：刷新 WriteBuffer 产生的增量（Level-0 文件）
 *   <li>CompactIncrement：压缩产生的增量（合并多层文件）
 * </ul>
 *
 * @see CompactIncrement
 * @see org.apache.paimon.utils.CommitIncrement
 */
public class DataIncrement {

    /** 新生成的数据文件列表（刷新 WriteBuffer 产生的 Level-0 文件） */
    private final List<DataFileMeta> newFiles;

    /** 被删除的数据文件列表 */
    private final List<DataFileMeta> deletedFiles;

    /** 新生成的 changelog 文件列表（仅 INPUT 模式下非空） */
    private final List<DataFileMeta> changelogFiles;

    /** 新生成的索引文件列表 */
    private final List<IndexFileMeta> newIndexFiles;

    /** 被删除的索引文件列表 */
    private final List<IndexFileMeta> deletedIndexFiles;

    /**
     * 构造数据增量（不包含索引文件）
     *
     * @param newFiles 新生成的数据文件
     * @param deletedFiles 被删除的数据文件
     * @param changelogFiles changelog 文件（INPUT 模式）
     */
    public DataIncrement(
            List<DataFileMeta> newFiles,
            List<DataFileMeta> deletedFiles,
            List<DataFileMeta> changelogFiles) {
        this(newFiles, deletedFiles, changelogFiles, new ArrayList<>(), new ArrayList<>());
    }

    /**
     * 构造完整的数据增量（包含索引文件）
     *
     * @param newFiles 新生成的数据文件
     * @param deletedFiles 被删除的数据文件
     * @param changelogFiles changelog 文件
     * @param newIndexFiles 新生成的索引文件
     * @param deletedIndexFiles 被删除的索引文件
     */
    public DataIncrement(
            List<DataFileMeta> newFiles,
            List<DataFileMeta> deletedFiles,
            List<DataFileMeta> changelogFiles,
            List<IndexFileMeta> newIndexFiles,
            List<IndexFileMeta> deletedIndexFiles) {
        this.newFiles = newFiles;
        this.deletedFiles = deletedFiles;
        this.changelogFiles = changelogFiles;
        this.newIndexFiles = newIndexFiles;
        this.deletedIndexFiles = deletedIndexFiles;
    }

    /**
     * 创建空的数据增量
     *
     * <p>用于没有任何文件变化的情况
     *
     * @return 空的 DataIncrement
     */
    public static DataIncrement emptyIncrement() {
        return new DataIncrement(
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }

    /**
     * 创建只包含新索引文件的数据增量
     *
     * @param indexFiles 新索引文件列表
     * @return 包含索引文件的 DataIncrement
     */
    public static DataIncrement indexIncrement(List<IndexFileMeta> indexFiles) {
        return new DataIncrement(
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                indexFiles,
                Collections.emptyList());
    }

    /**
     * 创建只包含删除索引文件的数据增量
     *
     * @param indexFiles 删除的索引文件列表
     * @return 包含删除索引文件的 DataIncrement
     */
    public static DataIncrement deleteIndexIncrement(List<IndexFileMeta> indexFiles) {
        return new DataIncrement(
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                indexFiles);
    }

    /**
     * 获取新生成的数据文件列表
     *
     * <p>这些是刷新 WriteBuffer 产生的 Level-0 文件
     *
     * @return 新数据文件列表
     */
    public List<DataFileMeta> newFiles() {
        return newFiles;
    }

    /**
     * 获取被删除的数据文件列表
     *
     * @return 删除的数据文件列表
     */
    public List<DataFileMeta> deletedFiles() {
        return deletedFiles;
    }

    /**
     * 获取 changelog 文件列表
     *
     * <p>只有在 INPUT 模式下才非空，包含双写的 changelog 文件
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
     * 获取被删除的索引文件列表
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
        return newFiles.isEmpty()
                && deletedFiles.isEmpty()
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

        DataIncrement that = (DataIncrement) o;
        return Objects.equals(newFiles, that.newFiles)
                && Objects.equals(deletedFiles, that.deletedFiles)
                && Objects.equals(changelogFiles, that.changelogFiles)
                && Objects.equals(newIndexFiles, that.newIndexFiles)
                && Objects.equals(deletedIndexFiles, that.deletedIndexFiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                newFiles, deletedFiles, changelogFiles, newIndexFiles, deletedIndexFiles);
    }

    @Override
    public String toString() {
        return String.format(
                "DataIncrement {newFiles = %s, deletedFiles = %s, changelogFiles = %s, newIndexFiles = %s, deletedIndexFiles = %s}",
                newFiles.stream().map(DataFileMeta::fileName).collect(Collectors.toList()),
                deletedFiles.stream().map(DataFileMeta::fileName).collect(Collectors.toList()),
                changelogFiles.stream().map(DataFileMeta::fileName).collect(Collectors.toList()),
                newIndexFiles.stream().map(IndexFileMeta::fileName).collect(Collectors.toList()),
                deletedIndexFiles.stream()
                        .map(IndexFileMeta::fileName)
                        .collect(Collectors.toList()));
    }
}
