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

package org.apache.paimon.metastore;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.table.ChainGroupReadTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.utils.ChainTableUtils;
import org.apache.paimon.utils.InternalRowPartitionComputer;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 用于维护链表表快照分支的覆盖提交回调实现。
 *
 * <p><b>链表表（Chain Table）：</b>
 * 链表表是一种特殊的表结构,由多个表链接而成，通常包含：
 * <ul>
 *   <li>Delta 分支：增量数据分支，存储最新的变更
 *   <li>Snapshot 分支：快照数据分支，存储完整的历史快照
 * </ul>
 *
 * <p><b>触发条件：</b>
 * 当满足以下所有条件时，此回调会截断快照分支的对应分区：
 * <ul>
 *   <li>提交的快照类型为 {@link CommitKind#OVERWRITE}（覆盖写入）
 *   <li>表是链表表（Chain Table）
 *   <li>当前分支是 Delta 分支
 * </ul>
 *
 * <p><b>工作原理：</b>
 * <ol>
 *   <li>检测到覆盖写入提交
 *   <li>从提交中提取被覆盖的分区列表
 *   <li>切换到快照分支表
 *   <li>在快照分支中截断（truncate）这些分区
 *   <li>保持 Delta 和 Snapshot 分支的数据一致性
 * </ol>
 *
 * <p><b>幂等性保证：</b>
 * 此回调设计为幂等的。对于同一个逻辑提交，可能会被多次调用，
 * 但重复截断相同的分区是安全的操作。
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li>数据重写：当 Delta 分支覆盖写入时，同步清理 Snapshot 分支
 *   <li>分区覆盖：保持两个分支的分区数据一致
 *   <li>增量合并：定期将 Delta 合并到 Snapshot 时的一致性维护
 * </ul>
 *
 * <p><b>注意事项：</b>
 * <ul>
 *   <li>只在成功提交后执行，失败不会触发
 *   <li>retry 方法为空实现，依赖成功回调的幂等性
 *   <li>不会影响 Delta 分支的数据
 * </ul>
 *
 * @see CommitCallback
 * @see ChainGroupReadTable
 * @see FallbackReadFileStoreTable
 */
public class ChainTableOverwriteCommitCallback implements CommitCallback {

    private transient FileStoreTable table;
    private transient CoreOptions coreOptions;

    public ChainTableOverwriteCommitCallback(FileStoreTable table) {
        this.table = table;
        this.coreOptions = table.coreOptions();
    }

    @Override
    public void call(
            List<SimpleFileEntry> baseFiles,
            List<ManifestEntry> deltaFiles,
            List<IndexManifestEntry> indexFiles,
            Snapshot snapshot) {

        if (!ChainTableUtils.isScanFallbackDeltaBranch(coreOptions)) {
            return;
        }

        if (snapshot.commitKind() != CommitKind.OVERWRITE) {
            return;
        }

        // Find the underlying table for writing snapshot branch.
        FileStoreTable candidateTable = table;
        if (table instanceof FallbackReadFileStoreTable) {
            candidateTable =
                    ((ChainGroupReadTable) ((FallbackReadFileStoreTable) table).fallback())
                            .wrapped();
        }

        FileStoreTable snapshotTable =
                candidateTable.switchToBranch(coreOptions.scanFallbackSnapshotBranch());

        InternalRowPartitionComputer partitionComputer =
                new InternalRowPartitionComputer(
                        coreOptions.partitionDefaultName(),
                        table.schema().logicalPartitionType(),
                        table.schema().partitionKeys().toArray(new String[0]),
                        coreOptions.legacyPartitionName());

        List<BinaryRow> overwritePartitions =
                deltaFiles.stream()
                        .map(ManifestEntry::partition)
                        .distinct()
                        .collect(Collectors.toList());

        if (overwritePartitions.isEmpty()) {
            return;
        }

        List<Map<String, String>> candidatePartitions =
                overwritePartitions.stream()
                        .map(partitionComputer::generatePartValues)
                        .collect(Collectors.toList());

        try (BatchTableCommit commit = snapshotTable.newBatchWriteBuilder().newCommit()) {
            commit.truncatePartitions(candidatePartitions);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to truncate partitions in snapshot table: %s.",
                            candidatePartitions),
                    e);
        }
    }

    @Override
    public void retry(ManifestCommittable committable) {
        // No-op. Truncating the same partitions again is safe, but we prefer to only rely on the
        // successful commit callback.
    }

    @Override
    public void close() throws Exception {
        // no resources to close
    }
}
