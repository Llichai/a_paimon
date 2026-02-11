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
import org.apache.paimon.append.BucketedAppendCompactManager;
import org.apache.paimon.append.cluster.BucketedAppendClusterManager;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.compact.NoopCompactManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/**
 * 分桶追加写入实现 - 用于固定分桶模式（HASH_FIXED）的 Append-Only 表
 *
 * <p><b>固定分桶模式（HASH_FIXED）特点</b>：
 * <ul>
 *   <li>分区数量在表创建时固定（通过 {@code bucket} 配置）
 *   <li>写入时根据 bucket key 的 hash 值分配到固定的 bucket
 *   <li>优点：分桶稳定，适合批处理和流处理
 *   <li>缺点：无法动态扩容，数据倾斜时某些 bucket 可能很大
 * </ul>
 *
 * <p><b>有序写入 vs 无序写入</b>：
 * <ul>
 *   <li><b>有序写入</b>（ordered）：
 *       <ul>
 *         <li>配置：{@code bucket-key} 非空且 {@code append.ordered = true}
 *         <li>数据按照 bucket key 排序后写入
 *         <li>需要分配递增的 sequence number（用于去重和合并）
 *         <li>需要读取历史文件获取最大 sequence number
 *         <li>适合流式写入、需要精确去重的场景
 *       </ul>
 *   </li>
 *   <li><b>无序写入</b>（unordered）：
 *       <ul>
 *         <li>配置：{@code append.ordered = false}（默认）
 *         <li>数据直接追加写入，不需要排序
 *         <li>不分配 sequence number（性能更高）
 *         <li>可以启用 {@code write-only} 模式忽略历史文件（极致性能）
 *         <li>适合批量导入、日志收集等场景
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p><b>Write-Only 模式</b>：
 * <ul>
 *   <li>配置：{@code write-only = true}
 *   <li>功能：创建 Writer 时不读取历史文件（ignorePreviousFiles = true）
 *   <li>优势：
 *       <ul>
 *         <li>避免扫描大量历史文件（特别是分区很多时）
 *         <li>大幅提升写入性能（无需等待扫描完成）
 *       </ul>
 *   </li>
 *   <li>限制：
 *       <ul>
 *         <li>无法获取之前的 sequence number（仅适用于无序写入）
 *         <li>不支持增量压缩（因为不知道历史文件）
 *       </ul>
 *   </li>
 *   <li>注意：
 *       <ul>
 *         <li>只在无序模式下生效
 *         <li>有序模式下会被忽略（因为必须读取历史 sequence number）
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p><b>压缩策略</b>：
 * <ul>
 *   <li><b>NoopCompactManager</b>（write-only 模式）：
 *       <ul>
 *         <li>不执行任何压缩
 *         <li>适合一次性导入后不再修改的场景
 *       </ul>
 *   </li>
 *   <li><b>BucketedAppendClusterManager</b>（启用聚簇）：
 *       <ul>
 *         <li>配置：{@code bucket-cluster.enabled = true}
 *         <li>按照聚簇键（clustering-columns）排序数据
 *         <li>提高查询性能（范围查询、过滤等）
 *         <li>使用 Universal Compaction 策略
 *       </ul>
 *   </li>
 *   <li><b>BucketedAppendCompactManager</b>（普通压缩）：
 *       <ul>
 *         <li>合并小文件，减少文件数量
 *         <li>可选：使用 Deletion Vector 逻辑删除行
 *         <li>配置：
 *             <ul>
 *               <li>{@code compaction.min.file-num}：触发压缩的最小文件数
 *               <li>{@code file.target-file-size}：目标文件大小
 *             </ul>
 *         </li>
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p><b>Writer 清理策略</b>：
 * <ul>
 *   <li>使用 {@link #createConflictAwareWriterCleanChecker} 创建清理检查器
 *   <li>清理条件：
 *       <ul>
 *         <li>Writer 最后修改的 commit 已提交（latestCommittedIdentifier）
 *         <li>Writer 没有待提交的数据
 *         <li>Writer 没有进行中的压缩任务
 *       </ul>
 *   </li>
 *   <li>目的：及时释放不再使用的 Writer，避免内存浪费
 * </ul>
 *
 * <p><b>使用示例</b>：
 * <pre>
 * // 创建写入器（无序模式，启用 write-only）
 * BucketedAppendFileStoreWrite write = new BucketedAppendFileStoreWrite(...);
 * write.withIgnorePreviousFiles(true); // write-only 模式
 *
 * // 写入数据
 * write.write(partition, bucket, row);
 *
 * // 提交
 * List&lt;CommitMessage&gt; messages = write.prepareCommit(false, commitId);
 * </pre>
 *
 * @see BaseAppendFileStoreWrite 基础追加写入
 * @see org.apache.paimon.table.BucketMode#HASH_FIXED 固定分桶模式
 */
public class BucketedAppendFileStoreWrite extends BaseAppendFileStoreWrite {

    private final String commitUser;
    private final SchemaManager schemaManager;

    public BucketedAppendFileStoreWrite(
            FileIO fileIO,
            RawFileSplitRead read,
            long schemaId,
            String commitUser,
            RowType rowType,
            RowType partitionType,
            FileStorePathFactory pathFactory,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            CoreOptions options,
            @Nullable BucketedDvMaintainer.Factory dvMaintainerFactory,
            String tableName,
            SchemaManager schemaManager) {
        super(
                fileIO,
                read,
                schemaId,
                rowType,
                partitionType,
                pathFactory,
                snapshotManager,
                scan,
                options,
                dvMaintainerFactory,
                tableName);
        if (!options.bucketAppendOrdered()) {
            super.withIgnorePreviousFiles(options.writeOnly());
        }
        this.commitUser = commitUser;
        this.schemaManager = schemaManager;
    }

    @Override
    public void withIgnorePreviousFiles(boolean ignorePrevious) {
        if (options.bucketAppendOrdered()) {
            super.withIgnorePreviousFiles(ignorePrevious);
        } else {
            // for unordered, don't need sequence number
            // all writers to be empty if write only
            super.withIgnorePreviousFiles(options.writeOnly());
        }
    }

    @Override
    protected CompactManager getCompactManager(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> restoredFiles,
            ExecutorService compactExecutor,
            @Nullable BucketedDvMaintainer dvMaintainer) {
        if (options.writeOnly()) {
            return new NoopCompactManager();
        } else if (options.bucketClusterEnabled()) {
            return new BucketedAppendClusterManager(
                    compactExecutor,
                    restoredFiles,
                    schemaManager,
                    options.clusteringColumns(),
                    options.maxSizeAmplificationPercent(),
                    options.sortedRunSizeRatio(),
                    options.numSortedRunCompactionTrigger(),
                    options.numLevels(),
                    files -> clusterRewrite(partition, bucket, files));
        } else {
            Function<String, DeletionVector> dvFactory =
                    dvMaintainer != null
                            ? f -> dvMaintainer.deletionVectorOf(f).orElse(null)
                            : null;
            return new BucketedAppendCompactManager(
                    compactExecutor,
                    restoredFiles,
                    dvMaintainer,
                    options.compactionMinFileNum(),
                    options.targetFileSize(false),
                    options.forceRewriteAllFiles(),
                    files -> compactRewrite(partition, bucket, dvFactory, files),
                    compactionMetrics == null
                            ? null
                            : compactionMetrics.createReporter(partition, bucket));
        }
    }

    @Override
    protected Function<WriterContainer<InternalRow>, Boolean> createWriterCleanChecker() {
        return createConflictAwareWriterCleanChecker(commitUser, restore);
    }
}
