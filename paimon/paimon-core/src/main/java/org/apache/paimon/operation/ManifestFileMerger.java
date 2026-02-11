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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Manifest文件合并工具类
 *
 * <p>该类提供Manifest文件的合并功能，用于优化Manifest文件的存储和读取效率。
 *
 * <h2>小文件合并策略</h2>
 * <p>Paimon会产生大量小的Manifest文件，需要定期合并以：
 * <ul>
 *   <li><b>减少文件数量</b>：降低文件系统的元数据压力
 *   <li><b>提高读取效率</b>：减少打开文件的次数
 *   <li><b>节省存储空间</b>：通过去重和压缩减少存储
 * </ul>
 *
 * <h2>合并触发条件</h2>
 * <p>支持两种合并模式：
 * <ul>
 *   <li><b>次要压缩（Minor Compaction）</b>：
 *       <ul>
 *         <li>触发条件：累积文件大小达到 suggestedMetaSize
 *         <li>或文件数量达到 suggestedMinMetaCount
 *         <li>策略：简单地将小文件合并成大文件
 *       </ul>
 *   <li><b>完全压缩（Full Compaction）</b>：
 *       <ul>
 *         <li>触发条件：删除文件或小文件的总大小达到 manifestFullCompactionSize
 *         <li>策略：读取所有Manifest，去除DELETE条目，重新组织
 *         <li>优点：彻底清理无用数据，最大化压缩率
 *       </ul>
 * </ul>
 *
 * <h2>条目去重机制</h2>
 * <p>合并时会执行以下去重操作：
 * <ul>
 *   <li>ADD + DELETE 相同文件 → 抵消，不写入
 *   <li>多个ADD相同文件 → 保留最新的一个
 *   <li>使用 {@link FileEntry#mergeEntries} 进行智能合并
 * </ul>
 *
 * <h2>分区过滤优化</h2>
 * <p>完全压缩时使用分区过滤：
 * <ul>
 *   <li>只读取包含DELETE条目的分区的Manifest文件
 *   <li>跳过不受影响的Manifest文件
 *   <li>大幅减少I/O操作
 * </ul>
 *
 * <h2>原子性保证</h2>
 * <p>注意：该方法是原子的：
 * <ul>
 *   <li>如果合并过程中出现异常，会清理所有新生成的临时文件
 *   <li>不会影响现有的Manifest文件
 *   <li>保证元数据的一致性
 * </ul>
 *
 * @see ManifestFile Manifest文件操作
 * @see ManifestFileMeta Manifest文件元数据
 * @see FileEntry 文件条目
 */
public class ManifestFileMerger {

    private static final Logger LOG = LoggerFactory.getLogger(ManifestFileMerger.class);

    /**
     * 合并多个 {@link ManifestFileMeta} 文件
     *
     * <p>该方法会合并多个Manifest文件，其中相同数据文件的先添加后删除的 {@link ManifestEntry}
     * 会相互抵消。
     *
     * <p>注意：该方法是原子的，如果发生异常会自动清理临时文件。
     *
     * <h3>合并策略选择</h3>
     * <ol>
     *   <li>首先尝试完全压缩（如果满足触发条件）
     *   <li>如果不满足完全压缩条件，执行次要压缩
     * </ol>
     *
     * @param input 输入的Manifest文件列表
     * @param manifestFile Manifest文件操作接口
     * @param suggestedMetaSize 建议的单个Manifest文件大小
     * @param suggestedMinMetaCount 建议的最小Manifest文件数量
     * @param manifestFullCompactionSize 完全压缩的触发大小
     * @param partitionType 分区类型
     * @param manifestReadParallelism Manifest读取并行度（可选）
     * @return 合并后的Manifest文件列表
     * @throws RuntimeException 如果合并失败，会清理临时文件并抛出异常
     */
    public static List<ManifestFileMeta> merge(
            List<ManifestFileMeta> input,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            int suggestedMinMetaCount,
            long manifestFullCompactionSize,
            RowType partitionType,
            @Nullable Integer manifestReadParallelism) {
        // 用于存储新创建的Manifest文件，异常时清理
        List<ManifestFileMeta> newFilesForAbort = new ArrayList<>();

        try {
            // 尝试完全压缩
            Optional<List<ManifestFileMeta>> fullCompacted =
                    tryFullCompaction(
                            input,
                            newFilesForAbort,
                            manifestFile,
                            suggestedMetaSize,
                            manifestFullCompactionSize,
                            partitionType,
                            manifestReadParallelism);
            // 如果完全压缩成功，返回结果；否则执行次要压缩
            return fullCompacted.orElseGet(
                    () ->
                            tryMinorCompaction(
                                    input,
                                    newFilesForAbort,
                                    manifestFile,
                                    suggestedMetaSize,
                                    suggestedMinMetaCount,
                                    manifestReadParallelism));
        } catch (Throwable e) {
            // 异常发生，清理新创建的Manifest文件
            for (ManifestFileMeta manifest : newFilesForAbort) {
                manifestFile.delete(manifest.fileName());
            }
            throw new RuntimeException(e);
        }
    }

    private static List<ManifestFileMeta> tryMinorCompaction(
            List<ManifestFileMeta> input,
            List<ManifestFileMeta> newFilesForAbort,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            int suggestedMinMetaCount,
            @Nullable Integer manifestReadParallelism) {
        List<ManifestFileMeta> result = new ArrayList<>();
        List<ManifestFileMeta> candidates = new ArrayList<>();
        long totalSize = 0;
        // merge existing small manifest files
        for (ManifestFileMeta manifest : input) {
            totalSize += manifest.fileSize();
            candidates.add(manifest);
            if (totalSize >= suggestedMetaSize) {
                // reach suggested file size, perform merging and produce new file
                mergeCandidates(
                        candidates,
                        manifestFile,
                        result,
                        newFilesForAbort,
                        manifestReadParallelism);
                candidates.clear();
                totalSize = 0;
            }
        }

        // merge the last bit of manifests if there are too many
        if (candidates.size() >= suggestedMinMetaCount) {
            mergeCandidates(
                    candidates, manifestFile, result, newFilesForAbort, manifestReadParallelism);
        } else {
            result.addAll(candidates);
        }
        return result;
    }

    private static void mergeCandidates(
            List<ManifestFileMeta> candidates,
            ManifestFile manifestFile,
            List<ManifestFileMeta> result,
            List<ManifestFileMeta> newMetas,
            @Nullable Integer manifestReadParallelism) {
        if (candidates.size() == 1) {
            result.add(candidates.get(0));
            return;
        }

        Map<FileEntry.Identifier, ManifestEntry> map = new LinkedHashMap<>();
        FileEntry.mergeEntries(manifestFile, candidates, map, manifestReadParallelism);
        if (!map.isEmpty()) {
            List<ManifestFileMeta> merged = manifestFile.write(new ArrayList<>(map.values()));
            result.addAll(merged);
            newMetas.addAll(merged);
        }
    }

    public static Optional<List<ManifestFileMeta>> tryFullCompaction(
            List<ManifestFileMeta> inputs,
            List<ManifestFileMeta> newFilesForAbort,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            long sizeTrigger,
            RowType partitionType,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        checkArgument(sizeTrigger > 0, "Manifest full compaction size trigger cannot be zero.");

        // 1. should trigger full compaction

        Filter<ManifestFileMeta> mustChange =
                file -> file.numDeletedFiles() > 0 || file.fileSize() < suggestedMetaSize;
        long totalManifestSize = 0;
        long deltaDeleteFileNum = 0;
        long totalDeltaFileSize = 0;
        for (ManifestFileMeta file : inputs) {
            totalManifestSize += file.fileSize();
            if (mustChange.test(file)) {
                totalDeltaFileSize += file.fileSize();
                deltaDeleteFileNum += file.numDeletedFiles();
            }
        }

        if (totalDeltaFileSize < sizeTrigger) {
            return Optional.empty();
        }

        // 2. do full compaction

        LOG.info(
                "Start Manifest File Full Compaction: totalManifestSize: {}, deltaDeleteFileNum {}, totalDeltaFileSize {}",
                totalManifestSize,
                deltaDeleteFileNum,
                totalDeltaFileSize);

        // 2.1. read all delete entries

        Set<FileEntry.Identifier> deleteEntries =
                FileEntry.readDeletedEntries(manifestFile, inputs, manifestReadParallelism);

        // 2.2. try to skip base files by partition filter

        PartitionPredicate predicate;
        if (deleteEntries.isEmpty()) {
            predicate = PartitionPredicate.ALWAYS_FALSE;
        } else {
            if (partitionType.getFieldCount() > 0) {
                Set<BinaryRow> deletePartitions = computeDeletePartitions(deleteEntries);
                predicate = PartitionPredicate.fromMultiple(partitionType, deletePartitions);
            } else {
                predicate = PartitionPredicate.ALWAYS_TRUE;
            }
        }

        List<ManifestFileMeta> result = new ArrayList<>();
        List<ManifestFileMeta> toBeMerged = new LinkedList<>(inputs);

        if (predicate != null) {
            Iterator<ManifestFileMeta> iterator = toBeMerged.iterator();
            while (iterator.hasNext()) {
                ManifestFileMeta file = iterator.next();
                if (mustChange.test(file)) {
                    continue;
                }
                if (!predicate.test(
                        file.numAddedFiles() + file.numDeletedFiles(),
                        file.partitionStats().minValues(),
                        file.partitionStats().maxValues(),
                        file.partitionStats().nullCounts())) {
                    iterator.remove();
                    result.add(file);
                }
            }
        }

        // 2.2. merge

        if (toBeMerged.size() <= 1) {
            return Optional.empty();
        }

        RollingFileWriter<ManifestEntry, ManifestFileMeta> writer =
                manifestFile.createRollingWriter();
        Exception exception = null;
        try {
            for (ManifestFileMeta file : toBeMerged) {
                List<ManifestEntry> entries = new ArrayList<>();
                boolean requireChange = mustChange.test(file);
                for (ManifestEntry entry : manifestFile.read(file.fileName(), file.fileSize())) {
                    if (entry.kind() == FileKind.DELETE) {
                        continue;
                    }

                    if (deleteEntries.contains(entry.identifier())) {
                        requireChange = true;
                    } else {
                        entries.add(entry);
                    }
                }

                if (requireChange) {
                    writer.write(entries);
                } else {
                    result.add(file);
                }
            }
        } catch (Exception e) {
            exception = e;
        } finally {
            if (exception != null) {
                writer.abort();
                throw exception;
            }
            writer.close();
        }

        List<ManifestFileMeta> merged = writer.result();
        result.addAll(merged);
        newFilesForAbort.addAll(merged);
        return Optional.of(result);
    }

    private static Set<BinaryRow> computeDeletePartitions(Set<FileEntry.Identifier> deleteEntries) {
        Set<BinaryRow> partitions = new HashSet<>();
        for (FileEntry.Identifier identifier : deleteEntries) {
            partitions.add(identifier.partition);
        }
        return partitions;
    }
}
