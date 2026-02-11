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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.utils.SnapshotManager;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;

/**
 * 文件系统写入恢复实现
 *
 * <p>{@link WriteRestore} 的实现，直接从文件系统恢复文件。
 *
 * <h2>故障恢复机制</h2>
 * <ul>
 *   <li><b>快照恢复</b>：从文件系统中的最新快照恢复数据文件
 *   <li><b>索引恢复</b>：恢复动态桶索引和删除向量索引
 *   <li><b>增量恢复</b>：支持按分区-桶增量恢复
 *   <li><b>状态重建</b>：重建写入器的内部状态
 * </ul>
 *
 * <h2>临时文件的清理</h2>
 * <p>该实现不负责清理临时文件，临时文件的清理由其他组件完成：
 * <ul>
 *   <li>未提交的临时文件在作业失败时会被清理
 *   <li>孤儿文件由 {@link OrphanFilesClean} 定期清理
 * </ul>
 *
 * <h2>与标准恢复的区别</h2>
 * <ul>
 *   <li>直接从文件系统读取，不经过Catalog
 *   <li>避免了高并发情况下对Catalog的压力
 *   <li>适用于大规模并行恢复场景
 * </ul>
 *
 * @see WriteRestore 写入恢复接口
 * @see SnapshotManager 快照管理器
 * @see IndexFileHandler 索引文件处理器
 */
public class FileSystemWriteRestore implements WriteRestore {

    private final SnapshotManager snapshotManager;
    private final FileStoreScan scan;
    private final IndexFileHandler indexFileHandler;

    /**
     * 构造文件系统写入恢复器
     *
     * @param options 核心配置选项
     * @param snapshotManager 快照管理器，用于访问快照信息
     * @param scan 文件存储扫描器，用于扫描数据文件
     * @param indexFileHandler 索引文件处理器，用于恢复索引
     */
    public FileSystemWriteRestore(
            CoreOptions options,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            IndexFileHandler indexFileHandler) {
        this.snapshotManager = snapshotManager;
        this.scan = scan;
        this.indexFileHandler = indexFileHandler;
        // 如果配置了删除文件时丢弃统计信息，则在扫描器中也丢弃
        if (options.manifestDeleteFileDropStats()) {
            if (this.scan != null) {
                this.scan.dropStats();
            }
        }
    }

    /**
     * 获取指定用户的最新提交标识符
     *
     * <p>直接从文件系统读取，避免Catalog访问。
     *
     * @param user 用户名
     * @return 最新提交标识符，如果不存在则返回 Long.MIN_VALUE
     */
    @Override
    public long latestCommittedIdentifier(String user) {
        return snapshotManager
                .latestSnapshotOfUserFromFilesystem(user)
                .map(Snapshot::commitIdentifier)
                .orElse(Long.MIN_VALUE);
    }

    /**
     * 恢复指定分区-桶的文件
     *
     * <p>从文件系统的最新快照中恢复数据文件和索引文件。
     *
     * <h3>恢复流程</h3>
     * <ol>
     *   <li>从文件系统获取最新快照（不经过Catalog）
     *   <li>扫描指定分区-桶的所有数据文件
     *   <li>如果需要，恢复动态桶索引
     *   <li>如果需要，恢复删除向量索引
     *   <li>返回完整的恢复结果
     * </ol>
     *
     * @param partition 要恢复的分区
     * @param bucket 要恢复的桶ID
     * @param scanDynamicBucketIndex 是否扫描动态桶索引
     * @param scanDeleteVectorsIndex 是否扫描删除向量索引
     * @return 恢复的文件信息，如果快照不存在则返回空结果
     */
    @Override
    public RestoreFiles restoreFiles(
            BinaryRow partition,
            int bucket,
            boolean scanDynamicBucketIndex,
            boolean scanDeleteVectorsIndex) {
        // 注意：这里不使用 snapshotManager.latestSnapshot()，
        // 因为我们不想在高并发情况下淹没 Catalog
        Snapshot snapshot = snapshotManager.latestSnapshotFromFileSystem();
        if (snapshot == null) {
            return RestoreFiles.empty();
        }

        // 扫描并恢复数据文件
        List<DataFileMeta> restoreFiles = new ArrayList<>();
        List<ManifestEntry> entries =
                scan.withSnapshot(snapshot).withPartitionBucket(partition, bucket).plan().files();
        Integer totalBuckets = WriteRestore.extractDataFiles(entries, restoreFiles);

        // 恢复动态桶索引（如果需要）
        IndexFileMeta dynamicBucketIndex = null;
        if (scanDynamicBucketIndex) {
            dynamicBucketIndex =
                    indexFileHandler.scanHashIndex(snapshot, partition, bucket).orElse(null);
        }

        // 恢复删除向量索引（如果需要）
        List<IndexFileMeta> deleteVectorsIndex = null;
        if (scanDeleteVectorsIndex) {
            deleteVectorsIndex =
                    indexFileHandler.scan(snapshot, DELETION_VECTORS_INDEX, partition, bucket);
        }

        return new RestoreFiles(
                snapshot, totalBuckets, restoreFiles, dynamicBucketIndex, deleteVectorsIndex);
    }
}
