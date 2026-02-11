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

package org.apache.paimon.append;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.append.AppendDeleteFileMaintainer;
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.table.BucketMode.UNAWARE_BUCKET;

/**
 * Append-Only 表的压缩任务
 *
 * <p>AppendCompactTask 由 {@link AppendCompactCoordinator} 生成,负责将多个小文件合并为一个大文件。
 *
 * <p>压缩流程:
 * <pre>
 * 1. 输入准备:
 *    - compactBefore: 需要压缩的文件列表
 *    - partition: 分区标识
 *
 * 2. 执行压缩(doCompact):
 *    - 读取 compactBefore 中的所有文件
 *    - 通过 BaseAppendFileStoreWrite.compactRewrite 重写
 *    - 生成 compactAfter 文件列表
 *
 * 3. 处理删除向量(Deletion Vector):
 *    - 如果启用 DV,为每个文件获取删除向量
 *    - 压缩后通知删除向量维护器移除旧文件的 DV
 *    - 生成新的索引文件(IndexFileMeta)
 *
 * 4. 生成提交消息:
 *    - 返回 CommitMessageImpl,包含:
 *      - DataIncrement: 空(压缩不产生新数据)
 *      - CompactIncrement: compactBefore 和 compactAfter
 *      - IndexFiles: 新增和删除的索引文件
 * </pre>
 *
 * <p>删除向量压缩:
 * 当表启用删除向量时,压缩任务会:
 * <ul>
 *   <li>从 {@link AppendDeleteFileMaintainer} 获取每个文件的删除向量
 *   <li>在压缩过程中应用删除向量,过滤被删除的行
 *   <li>压缩后,旧文件的删除向量不再需要,会被标记为删除
 *   <li>生成索引文件变更:{@code newIndexFiles} 和 {@code deletedIndexFiles}
 * </ul>
 *
 * <p>单文件压缩:
 * 即使只有一个文件,如果启用了删除向量也可能需要压缩:
 * <ul>
 *   <li>当文件有删除向量时,需要重写文件以应用删除
 *   <li>校验:{@code dvEnabled || compactBefore.size() > 1}
 * </ul>
 *
 * <p>Bucket 模式:
 * 使用 {@link BucketMode#UNAWARE_BUCKET},bucket 固定为 0:
 * <ul>
 *   <li>Append-Only 表不支持分桶(bucket)
 *   <li>bucket=0 是为了兼容旧设计
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建压缩任务
 * AppendCompactTask task = new AppendCompactTask(
 *     partition,
 *     filesToCompact
 * );
 *
 * // 执行压缩
 * CommitMessage message = task.doCompact(table, write);
 *
 * // 提交结果
 * table.newCommit().commit(Collections.singletonList(message));
 * }</pre>
 *
 * @see AppendCompactCoordinator 压缩协调器
 * @see AppendDeleteFileMaintainer 删除文件维护器
 * @see BaseAppendFileStoreWrite#compactRewrite 压缩重写方法
 */
public class AppendCompactTask {

    private final BinaryRow partition;
    private final List<DataFileMeta> compactBefore;
    private final List<DataFileMeta> compactAfter;

    public AppendCompactTask(BinaryRow partition, List<DataFileMeta> files) {
        Preconditions.checkArgument(files != null);
        this.partition = partition;
        compactBefore = new ArrayList<>(files);
        compactAfter = new ArrayList<>();
    }

    public BinaryRow partition() {
        return partition;
    }

    public List<DataFileMeta> compactBefore() {
        return compactBefore;
    }

    public List<DataFileMeta> compactAfter() {
        return compactAfter;
    }

    public CommitMessage doCompact(FileStoreTable table, BaseAppendFileStoreWrite write)
            throws Exception {
        boolean dvEnabled = table.coreOptions().deletionVectorsEnabled();
        Preconditions.checkArgument(
                dvEnabled || compactBefore.size() > 1,
                "AppendOnlyCompactionTask need more than one file input.");
        // If compact task didn't compact all files, the remain deletion files will be written into
        // new deletion files.
        List<IndexFileMeta> newIndexFiles = new ArrayList<>();
        List<IndexFileMeta> deletedIndexFiles = new ArrayList<>();
        if (dvEnabled) {
            AppendDeleteFileMaintainer dvIndexFileMaintainer =
                    BaseAppendDeleteFileMaintainer.forUnawareAppend(
                            table.store().newIndexFileHandler(),
                            table.snapshotManager().latestSnapshot(),
                            partition);
            compactAfter.addAll(
                    write.compactRewrite(
                            partition,
                            UNAWARE_BUCKET,
                            dvIndexFileMaintainer::getDeletionVector,
                            compactBefore));

            compactBefore.forEach(
                    f -> dvIndexFileMaintainer.notifyRemovedDeletionVector(f.fileName()));
            List<IndexManifestEntry> indexEntries = dvIndexFileMaintainer.persist();
            for (IndexManifestEntry entry : indexEntries) {
                if (entry.kind() == FileKind.ADD) {
                    newIndexFiles.add(entry.indexFile());
                } else {
                    deletedIndexFiles.add(entry.indexFile());
                }
            }
        } else {
            compactAfter.addAll(
                    write.compactRewrite(partition, UNAWARE_BUCKET, null, compactBefore));
        }

        CompactIncrement compactIncrement =
                new CompactIncrement(
                        compactBefore,
                        compactAfter,
                        Collections.emptyList(),
                        newIndexFiles,
                        deletedIndexFiles);
        return new CommitMessageImpl(
                partition,
                // bucket 0 is bucket for unaware-bucket table
                // for compatibility with the old design
                0,
                table.coreOptions().bucket(),
                DataIncrement.emptyIncrement(),
                compactIncrement);
    }

    public int hashCode() {
        return Objects.hash(partition, compactBefore, compactAfter);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AppendCompactTask that = (AppendCompactTask) o;
        return Objects.equals(partition, that.partition)
                && Objects.equals(compactBefore, that.compactBefore)
                && Objects.equals(compactAfter, that.compactAfter);
    }

    @Override
    public String toString() {
        return String.format(
                "CompactionTask {"
                        + "partition = %s, "
                        + "compactBefore = %s, "
                        + "compactAfter = %s}",
                partition, compactBefore, compactAfter);
    }
}
