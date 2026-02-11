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

package org.apache.paimon.operation;

import org.apache.paimon.Snapshot;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;

import javax.annotation.Nullable;

import java.util.List;

/**
 * 文件恢复信息
 *
 * <p>封装从快照恢复的文件和相关元数据。
 *
 * <h2>快照回滚机制</h2>
 * <p>该类用于支持快照回滚和写入恢复功能：
 * <ul>
 *   <li><b>作业恢复</b>：作业失败后从检查点恢复，需要恢复写入器状态
 *   <li><b>快照回滚</b>：将表回滚到历史快照
 *   <li><b>分支切换</b>：切换到不同的分支时恢复文件状态
 * </ul>
 *
 * <h2>文件的恢复流程</h2>
 * <ol>
 *   <li><b>识别快照</b>：确定要恢复到的快照ID
 *   <li><b>扫描文件</b>：从快照中扫描所有数据文件
 *   <li><b>恢复索引</b>：恢复动态桶索引和删除向量索引
 *   <li><b>重建状态</b>：根据恢复的文件重建写入器状态
 * </ol>
 *
 * <h2>包含的信息</h2>
 * <ul>
 *   <li><b>快照</b>：恢复所基于的快照对象
 *   <li><b>总桶数</b>：恢复时的桶总数
 *   <li><b>数据文件</b>：需要恢复的数据文件列表
 *   <li><b>动态桶索引</b>：动态桶模式的索引文件
 *   <li><b>删除向量索引</b>：删除向量的索引文件列表
 * </ul>
 *
 * <h2>空恢复</h2>
 * <p>当没有快照可恢复时，使用 {@link #empty()} 返回空恢复对象。
 *
 * @see WriteRestore 写入恢复接口
 * @see Snapshot 快照对象
 */
public class RestoreFiles {

    /** 恢复所基于的快照（可选） */
    private final @Nullable Snapshot snapshot;

    /** 桶的总数（可选） */
    private final @Nullable Integer totalBuckets;

    /** 要恢复的数据文件列表（可选） */
    private final @Nullable List<DataFileMeta> dataFiles;

    /** 动态桶索引文件（可选） */
    private final @Nullable IndexFileMeta dynamicBucketIndex;

    /** 删除向量索引文件列表（可选） */
    private final @Nullable List<IndexFileMeta> deleteVectorsIndex;

    /**
     * 构造文件恢复信息
     *
     * @param snapshot 恢复所基于的快照
     * @param totalBuckets 桶的总数
     * @param dataFiles 要恢复的数据文件列表
     * @param dynamicBucketIndex 动态桶索引文件
     * @param deleteVectorsIndex 删除向量索引文件列表
     */
    public RestoreFiles(
            @Nullable Snapshot snapshot,
            @Nullable Integer totalBuckets,
            @Nullable List<DataFileMeta> dataFiles,
            @Nullable IndexFileMeta dynamicBucketIndex,
            @Nullable List<IndexFileMeta> deleteVectorsIndex) {
        this.snapshot = snapshot;
        this.totalBuckets = totalBuckets;
        this.dataFiles = dataFiles;
        this.dynamicBucketIndex = dynamicBucketIndex;
        this.deleteVectorsIndex = deleteVectorsIndex;
    }

    /**
     * 获取快照
     *
     * @return 快照对象，如果没有快照则返回 null
     */
    @Nullable
    public Snapshot snapshot() {
        return snapshot;
    }

    /**
     * 获取总桶数
     *
     * @return 桶的总数，如果未知则返回 null
     */
    @Nullable
    public Integer totalBuckets() {
        return totalBuckets;
    }

    /**
     * 获取数据文件列表
     *
     * @return 数据文件列表，如果没有文件则返回 null
     */
    @Nullable
    public List<DataFileMeta> dataFiles() {
        return dataFiles;
    }

    /**
     * 获取动态桶索引
     *
     * @return 动态桶索引文件，如果不存在则返回 null
     */
    @Nullable
    public IndexFileMeta dynamicBucketIndex() {
        return dynamicBucketIndex;
    }

    /**
     * 获取删除向量索引列表
     *
     * @return 删除向量索引文件列表，如果不存在则返回 null
     */
    @Nullable
    public List<IndexFileMeta> deleteVectorsIndex() {
        return deleteVectorsIndex;
    }

    /**
     * 创建空的恢复信息
     *
     * <p>用于没有快照可恢复的场景。
     *
     * @return 所有字段都为 null 的空恢复对象
     */
    public static RestoreFiles empty() {
        return new RestoreFiles(null, null, null, null, null);
    }
}
