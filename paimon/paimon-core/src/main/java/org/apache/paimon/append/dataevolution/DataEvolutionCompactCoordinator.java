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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.utils.RangeHelper;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 数据演化压缩协调器
 *
 * <p>DataEvolutionCompactCoordinator 负责处理跨 Schema 版本的文件压缩,
 * 将不同 Schema 版本的文件合并为统一的 Schema 版本。
 *
 * <p>数据演化场景:
 * <pre>
 * 表 Schema 演化:
 * V1: id INT, name STRING
 * V2: id INT, name STRING, age INT (添加字段)
 * V3: id INT, name STRING, age INT, city STRING (再添加字段)
 *
 * 文件分布:
 * - file1.parquet (Schema V1, 100 rows)
 * - file2.parquet (Schema V2, 200 rows)
 * - file3.parquet (Schema V3, 150 rows)
 *
 * 压缩结果:
 * - compacted.parquet (Schema V3, 450 rows)
 *   - file1 的行补充 age=null, city=null
 *   - file2 的行补充 city=null
 *   - file3 的行保持不变
 * </pre>
 *
 * <p>核心组件:
 * <ul>
 *   <li>{@link CompactScanner}:扫描 Manifest 文件,按 rowId 范围排序
 *   <li>{@link CompactPlanner}:规划压缩任务,按 rowId 范围分组
 * </ul>
 *
 * <p>扫描策略:
 * 使用 rowId 范围优化扫描:
 * <ul>
 *   <li>如果所有 Manifest 包含 rowId 信息,按 rowId 范围分组
 *   <li>合并重叠的 rowId 范围,减少重复扫描
 *   <li>批量读取 ManifestEntry,限制内存占用(FILES_BATCH = 100,000)
 * </ul>
 *
 * <p>规划策略:
 * <pre>
 * 1. 按分区分组文件
 * 2. 按 rowId 范围合并相邻文件
 * 3. 分离普通文件和 BLOB 文件
 * 4. 按大小打包文件:
 *    - 累计大小 >= targetFileSize 时触发压缩
 *    - 文件数量 >= compaction.min-file-num 时触发压缩
 * 5. 为普通文件和 BLOB 文件分别生成任务
 * </pre>
 *
 * <p>BLOB 文件处理:
 * 如果 {@code compactBlob = true}:
 * <ul>
 *   <li>根据 rowId 关联 BLOB 文件到普通文件
 *   <li>为 BLOB 文件生成独立的压缩任务
 *   <li>确保 BLOB 文件的 rowId 在普通文件的范围内
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建协调器
 * DataEvolutionCompactCoordinator coordinator =
 *     new DataEvolutionCompactCoordinator(table, true); // compactBlob
 *
 * // 生成压缩任务
 * List<DataEvolutionCompactTask> tasks = coordinator.plan();
 *
 * // 执行压缩任务
 * for (DataEvolutionCompactTask task : tasks) {
 *     CommitMessage message = task.doCompact(table, commitUser);
 *     // 提交 message
 * }
 * }</pre>
 *
 * @see DataEvolutionCompactTask 数据演化压缩任务
 * @see CompactScanner Manifest 扫描器
 * @see CompactPlanner 压缩规划器
 */
public class DataEvolutionCompactCoordinator {

    private static final int FILES_BATCH = 100_000;

    private final CompactScanner scanner;
    private final CompactPlanner planner;

    public DataEvolutionCompactCoordinator(FileStoreTable table, boolean compactBlob) {
        this(table, null, compactBlob);
    }

    public DataEvolutionCompactCoordinator(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionPredicate,
            boolean compactBlob) {
        CoreOptions options = table.coreOptions();
        long targetFileSize = options.targetFileSize(false);
        long openFileCost = options.splitOpenFileCost();
        long compactMinFileNum = options.compactionMinFileNum();

        this.scanner =
                new CompactScanner(
                        table.newSnapshotReader().withPartitionFilter(partitionPredicate),
                        table.store().newScan());
        this.planner =
                new CompactPlanner(compactBlob, targetFileSize, openFileCost, compactMinFileNum);
    }

    public List<DataEvolutionCompactTask> plan() {
        // scan files in snapshot
        List<ManifestEntry> entries = scanner.scan();
        if (!entries.isEmpty()) {
            // do plan compact tasks
            return planner.compactPlan(entries);
        }

        return Collections.emptyList();
    }

    /** Scanner to generate sorted ManifestEntries. */
    static class CompactScanner {

        private final FileStoreScan scan;
        private final Queue<List<ManifestFileMeta>> metas;

        private CompactScanner(SnapshotReader snapshotReader, FileStoreScan scan) {
            this.scan = scan;
            Snapshot snapshot = snapshotReader.snapshotManager().latestSnapshot();

            List<ManifestFileMeta> manifestFileMetas =
                    snapshotReader.manifestsReader().read(snapshot, ScanMode.ALL).filteredManifests;

            boolean allManifestMetaContainsRowId =
                    manifestFileMetas.stream()
                            .allMatch(meta -> meta.minRowId() != null && meta.maxRowId() != null);
            if (allManifestMetaContainsRowId) {
                RangeHelper<ManifestFileMeta> rangeHelper =
                        new RangeHelper<>(ManifestFileMeta::minRowId, ManifestFileMeta::maxRowId);
                this.metas =
                        new ArrayDeque<>(rangeHelper.mergeOverlappingRanges(manifestFileMetas));
            } else {
                this.metas = new ArrayDeque<>(Collections.singletonList(manifestFileMetas));
            }
        }

        List<ManifestEntry> scan() {
            List<ManifestEntry> result = new ArrayList<>();
            while (metas.peek() != null && result.size() < FILES_BATCH) {
                List<ManifestFileMeta> currentMetas = metas.poll();
                scan.readFileIterator(currentMetas)
                        .forEachRemaining(entry -> result.add(entry.copyWithoutStats()));
            }
            if (result.isEmpty()) {
                throw new EndOfScanException();
            }
            return result;
        }
    }

    /** Generate compaction tasks. */
    static class CompactPlanner {

        private final boolean compactBlob;
        private final long targetFileSize;
        private final long openFileCost;
        private final long compactMinFileNum;

        CompactPlanner(
                boolean compactBlob,
                long targetFileSize,
                long openFileCost,
                long compactMinFileNum) {
            this.compactBlob = compactBlob;
            this.targetFileSize = targetFileSize;
            this.openFileCost = openFileCost;
            this.compactMinFileNum = compactMinFileNum;
        }

        List<DataEvolutionCompactTask> compactPlan(List<ManifestEntry> input) {
            List<DataEvolutionCompactTask> tasks = new ArrayList<>();
            Map<BinaryRow, List<DataFileMeta>> partitionedFiles = new LinkedHashMap<>();
            for (ManifestEntry entry : input) {
                partitionedFiles
                        .computeIfAbsent(entry.partition(), k -> new ArrayList<>())
                        .add(entry.file());
            }

            for (Map.Entry<BinaryRow, List<DataFileMeta>> partitionFiles :
                    partitionedFiles.entrySet()) {
                BinaryRow partition = partitionFiles.getKey();
                List<DataFileMeta> files = partitionFiles.getValue();
                RangeHelper<DataFileMeta> rangeHelper =
                        new RangeHelper<>(
                                DataFileMeta::nonNullFirstRowId,
                                // merge adjacent files
                                f -> f.nonNullFirstRowId() + f.rowCount());

                List<List<DataFileMeta>> ranges = rangeHelper.mergeOverlappingRanges(files);

                for (List<DataFileMeta> group : ranges) {
                    List<DataFileMeta> dataFiles = new ArrayList<>();
                    List<DataFileMeta> blobFiles = new ArrayList<>();
                    TreeMap<Long, DataFileMeta> treeMap = new TreeMap<>();
                    Map<DataFileMeta, List<DataFileMeta>> dataFileToBlobFiles = new HashMap<>();
                    for (DataFileMeta f : group) {
                        if (!isBlobFile(f.fileName())) {
                            treeMap.put(f.nonNullFirstRowId(), f);
                            dataFiles.add(f);
                        } else {
                            blobFiles.add(f);
                        }
                    }

                    if (compactBlob) {
                        // associate blob files to data files
                        for (DataFileMeta blobFile : blobFiles) {
                            Long key = treeMap.floorKey(blobFile.nonNullFirstRowId());
                            if (key != null) {
                                DataFileMeta dataFile = treeMap.get(key);
                                if (blobFile.nonNullFirstRowId() >= dataFile.nonNullFirstRowId()
                                        && blobFile.nonNullFirstRowId()
                                                <= dataFile.nonNullFirstRowId()
                                                        + dataFile.rowCount()
                                                        - 1) {
                                    dataFileToBlobFiles
                                            .computeIfAbsent(dataFile, k -> new ArrayList<>())
                                            .add(blobFile);
                                }
                            }
                        }
                    }

                    RangeHelper<DataFileMeta> rangeHelper2 =
                            new RangeHelper<>(
                                    DataFileMeta::nonNullFirstRowId,
                                    // files group
                                    f -> f.nonNullFirstRowId() + f.rowCount() - 1);
                    List<List<DataFileMeta>> groupedFiles =
                            rangeHelper2.mergeOverlappingRanges(dataFiles);
                    List<DataFileMeta> waitCompactFiles = new ArrayList<>();

                    long weightSum = 0L;
                    for (List<DataFileMeta> fileGroup : groupedFiles) {
                        checkArgument(
                                rangeHelper.areAllRangesSame(fileGroup),
                                "Data files %s should be all row id ranges same.",
                                dataFiles);
                        long currentGroupWeight =
                                fileGroup.stream()
                                        .mapToLong(d -> Math.max(d.fileSize(), openFileCost))
                                        .sum();
                        if (currentGroupWeight > targetFileSize) {
                            // compact current file group to merge field files
                            tasks.addAll(triggerTask(fileGroup, partition, dataFileToBlobFiles));
                            // compact wait compact files
                            tasks.addAll(
                                    triggerTask(waitCompactFiles, partition, dataFileToBlobFiles));
                            waitCompactFiles = new ArrayList<>();
                            weightSum = 0;
                        } else {
                            weightSum += currentGroupWeight;
                            waitCompactFiles.addAll(fileGroup);
                            if (weightSum > targetFileSize) {
                                tasks.addAll(
                                        triggerTask(
                                                waitCompactFiles, partition, dataFileToBlobFiles));
                                waitCompactFiles = new ArrayList<>();
                                weightSum = 0L;
                            }
                        }
                    }
                    tasks.addAll(triggerTask(waitCompactFiles, partition, dataFileToBlobFiles));
                }
            }
            return tasks;
        }

        private List<DataEvolutionCompactTask> triggerTask(
                List<DataFileMeta> dataFiles,
                BinaryRow partition,
                Map<DataFileMeta, List<DataFileMeta>> dataFileToBlobFiles) {
            List<DataEvolutionCompactTask> tasks = new ArrayList<>();
            if (dataFiles.size() >= compactMinFileNum) {
                tasks.add(new DataEvolutionCompactTask(partition, dataFiles, false));
            }

            if (compactBlob) {
                List<DataFileMeta> blobFiles = new ArrayList<>();
                for (DataFileMeta dataFile : dataFiles) {
                    blobFiles.addAll(
                            dataFileToBlobFiles.getOrDefault(dataFile, Collections.emptyList()));
                }
                if (blobFiles.size() >= compactMinFileNum) {
                    tasks.add(new DataEvolutionCompactTask(partition, blobFiles, true));
                }
            }
            return tasks;
        }
    }
}
