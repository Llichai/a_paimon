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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.ChainSplit;
import org.apache.paimon.table.source.DataFilePlan;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ChainTableUtils;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 链式分组读取表 - 主分支快照 + Delta 分支增量数据的链式读取
 *
 * <p>ChainGroupReadTable 是 {@link FallbackReadFileStoreTable} 的增强版，专门用于<b>主分支快照 + Delta 增量数据</b>的场景。
 * 它不仅支持分区级别的回退，还支持<b>同一分区内的文件链式合并</b>。
 *
 * <p><b>核心概念：</b>
 * <ul>
 *   <li><b>Snapshot Branch</b>：主分支，存储周期性的全量快照数据
 *   <li><b>Delta Branch</b>：增量分支，存储快照之间的增量变更数据
 *   <li><b>Chain Read</b>：将快照数据 + 增量数据链式合并，得到最新完整数据
 * </ul>
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li><b>数据湖增量架构</b>：全量快照 + 增量更新的存储模式
 *   <li><b>流批一体</b>：批处理产生快照，流处理产生增量，查询时合并
 *   <li><b>成本优化</b>：避免频繁重写全量数据，只存储增量变化
 * </ul>
 *
 * <p><b>读取策略：</b>
 * <pre>
 * 分区 A:
 *   - Snapshot Branch: [2024-01-01 00:00 快照]
 *   - Delta Branch: [00:05 增量, 00:10 增量, 00:15 增量]
 *   → 读取时：快照 + 所有增量 (Chain Read)
 *
 * 分区 B:
 *   - Snapshot Branch: 无数据
 *   - Delta Branch: [00:00~00:20 所有增量]
 *   → 读取时：只读 Delta (Fallback Read)
 * </pre>
 *
 * <p><b>分区匹配算法：</b>
 * <ol>
 *   <li>找到 Delta 分支的最大分区 (maxDeltaPartition)
 *   <li>在 Snapshot 分支中找到 ≤ maxDeltaPartition 的分区
 *   <li>对每个 Delta 分区，找到最接近的 Snapshot 分区
 *   <li>合并 Snapshot 分区的文件 + Delta 分区的文件
 * </ol>
 *
 * <p><b>与 FallbackReadFileStoreTable 的区别：</b>
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>FallbackReadFileStoreTable</th>
 *     <th>ChainGroupReadTable</th>
 *   </tr>
 *   <tr>
 *     <td>分区粒度</td>
 *     <td>完整分区切换</td>
 *     <td>分区内文件合并</td>
 *   </tr>
 *   <tr>
 *     <td>数据模式</td>
 *     <td>主备切换</td>
 *     <td>快照+增量</td>
 *   </tr>
 *   <tr>
 *     <td>读取方式</td>
 *     <td>选择一个分支</td>
 *     <td>链式合并两个分支</td>
 *   </tr>
 *   <tr>
 *     <td>适用表类型</td>
 *     <td>任意表</td>
 *     <td>仅主键表</td>
 *   </tr>
 * </table>
 *
 * <p><b>技术要点：</b>
 * <ul>
 *   <li><b>ChainSplit</b>：特殊的分片类型，包含多个文件和文件来源映射
 *   <li><b>Partition Comparator</b>：按分区键排序，用于三角形匹配算法
 *   <li><b>File Branch Mapping</b>：记录每个文件来自哪个分支
 *   <li><b>Bucket Path Mapping</b>：记录每个文件的 Bucket 路径
 * </ul>
 *
 * @see FallbackReadFileStoreTable
 * @see org.apache.paimon.table.source.ChainSplit
 */
public class ChainGroupReadTable extends FallbackReadFileStoreTable {

    public ChainGroupReadTable(FileStoreTable snapshotStoreTable, FileStoreTable deltaStoreTable) {
        super(snapshotStoreTable, deltaStoreTable);
        checkArgument(snapshotStoreTable instanceof PrimaryKeyFileStoreTable);
        checkArgument(deltaStoreTable instanceof PrimaryKeyFileStoreTable);
    }

    @Override
    public DataTableScan newScan() {
        super.validateSchema();
        return new ChainTableBatchScan(
                wrapped.newScan(),
                fallback().newScan(),
                ((AbstractFileStoreTable) wrapped).tableSchema,
                this);
    }

    private DataTableScan newSnapshotScan() {
        return wrapped.newScan();
    }

    private DataTableScan newDeltaScan() {
        return fallback().newScan();
    }

    @Override
    public FileStoreTable copy(Map<String, String> dynamicOptions) {
        return new ChainGroupReadTable(
                wrapped.copy(dynamicOptions),
                fallback().copy(rewriteFallbackOptions(dynamicOptions)));
    }

    @Override
    public FileStoreTable copy(TableSchema newTableSchema) {
        return new ChainGroupReadTable(
                wrapped.copy(newTableSchema),
                fallback()
                        .copy(
                                newTableSchema.copy(
                                        rewriteFallbackOptions(newTableSchema.options()))));
    }

    @Override
    public FileStoreTable copyWithoutTimeTravel(Map<String, String> dynamicOptions) {
        return new ChainGroupReadTable(
                wrapped.copyWithoutTimeTravel(dynamicOptions),
                fallback().copyWithoutTimeTravel(rewriteFallbackOptions(dynamicOptions)));
    }

    @Override
    public FileStoreTable copyWithLatestSchema() {
        return new ChainGroupReadTable(
                wrapped.copyWithLatestSchema(), fallback().copyWithLatestSchema());
    }

    @Override
    public FileStoreTable switchToBranch(String branchName) {
        return new ChainGroupReadTable(switchWrappedToBranch(branchName), fallback());
    }

    /**
     * 链式读取的批量扫描实现
     *
     * <p>该扫描器实现了复杂的分区匹配和文件合并逻辑。
     *
     * <p><b>核心算法：</b>
     * <ol>
     *   <li>扫描 Snapshot 分支，获取完整分区（completePartitions）
     *   <li>扫描 Delta 分支，过滤出缺失分区（remainingPartitions）
     *   <li>对每个缺失分区：
     *       <ul>
     *         <li>找到 Snapshot 中最接近的分区（三角形匹配）
     *         <li>扫描 Delta 分区的所有文件
     *         <li>扫描 Snapshot 分区的所有文件
     *         <li>按 bucket 分组，合并文件列表
     *         <li>创建 ChainSplit（包含文件来源映射）
     *       </ul>
     * </ol>
     *
     * <p><b>三角形匹配算法：</b>
     * <pre>
     * 假设分区键为 [dt, hour]：
     * Delta 分区：(2024-01-01, 10)
     * Snapshot 候选分区：
     *   - (2024-01-01, 08) ← 选择这个（最接近且 ≤）
     *   - (2024-01-01, 05)
     *   - (2023-12-31, 23)
     * </pre>
     */
    public static class ChainTableBatchScan extends FallbackReadScan {

        private final RowDataToObjectArrayConverter partitionConverter;
        private final InternalRowPartitionComputer partitionComputer;
        private final TableSchema tableSchema;
        private final CoreOptions options;
        private final RecordComparator partitionComparator;
        private final ChainGroupReadTable chainGroupReadTable;

        public ChainTableBatchScan(
                DataTableScan mainScan,
                DataTableScan fallbackScan,
                TableSchema tableSchema,
                ChainGroupReadTable chainGroupReadTable) {
            super(mainScan, fallbackScan);
            this.tableSchema = tableSchema;
            this.options = CoreOptions.fromMap(tableSchema.options());
            this.chainGroupReadTable = chainGroupReadTable;
            this.partitionConverter =
                    new RowDataToObjectArrayConverter(tableSchema.logicalPartitionType());
            this.partitionComputer =
                    new InternalRowPartitionComputer(
                            options.partitionDefaultName(),
                            tableSchema.logicalPartitionType(),
                            tableSchema.partitionKeys().toArray(new String[0]),
                            options.legacyPartitionName());
            this.partitionComparator =
                    CodeGenUtils.newRecordComparator(
                            tableSchema.logicalPartitionType().getFieldTypes());
        }

        @Override
        public Plan plan() {
            List<Split> splits = new ArrayList<>();
            Set<BinaryRow> completePartitions = new HashSet<>();
            PredicateBuilder builder = new PredicateBuilder(tableSchema.logicalPartitionType());
            for (Split split : mainScan.plan().splits()) {
                DataSplit dataSplit = (DataSplit) split;
                HashMap<String, String> fileBucketPathMapping = new HashMap<>();
                HashMap<String, String> fileBranchMapping = new HashMap<>();
                for (DataFileMeta file : dataSplit.dataFiles()) {
                    fileBucketPathMapping.put(file.fileName(), ((DataSplit) split).bucketPath());
                    fileBranchMapping.put(file.fileName(), options.scanFallbackSnapshotBranch());
                }
                splits.add(
                        new ChainSplit(
                                dataSplit.partition(),
                                dataSplit.dataFiles(),
                                fileBucketPathMapping,
                                fileBranchMapping));
                completePartitions.add(dataSplit.partition());
            }
            List<BinaryRow> remainingPartitions =
                    fallbackScan.listPartitions().stream()
                            .filter(p -> !completePartitions.contains(p))
                            .collect(Collectors.toList());
            if (!remainingPartitions.isEmpty()) {
                fallbackScan.withPartitionFilter(remainingPartitions);
                List<BinaryRow> deltaPartitions = fallbackScan.listPartitions();
                deltaPartitions =
                        deltaPartitions.stream()
                                .sorted(partitionComparator)
                                .collect(Collectors.toList());
                BinaryRow maxPartition = deltaPartitions.get(deltaPartitions.size() - 1);
                Predicate snapshotPredicate =
                        ChainTableUtils.createTriangularPredicate(
                                maxPartition,
                                partitionConverter,
                                builder::equal,
                                builder::lessThan);
                mainScan.withPartitionFilter(snapshotPredicate);
                List<BinaryRow> candidateSnapshotPartitions = mainScan.listPartitions();
                candidateSnapshotPartitions =
                        candidateSnapshotPartitions.stream()
                                .sorted(partitionComparator)
                                .collect(Collectors.toList());
                Map<BinaryRow, BinaryRow> partitionMapping =
                        ChainTableUtils.findFirstLatestPartitions(
                                deltaPartitions, candidateSnapshotPartitions, partitionComparator);
                for (Map.Entry<BinaryRow, BinaryRow> partitionParis : partitionMapping.entrySet()) {
                    DataTableScan snapshotScan = chainGroupReadTable.newSnapshotScan();
                    DataTableScan deltaScan = chainGroupReadTable.newDeltaScan();
                    if (partitionParis.getValue() == null) {
                        List<Predicate> predicates = new ArrayList<>();
                        predicates.add(
                                ChainTableUtils.createTriangularPredicate(
                                        partitionParis.getKey(),
                                        partitionConverter,
                                        builder::equal,
                                        builder::lessThan));
                        predicates.add(
                                ChainTableUtils.createLinearPredicate(
                                        partitionParis.getKey(),
                                        partitionConverter,
                                        builder::equal));
                        deltaScan.withPartitionFilter(PredicateBuilder.or(predicates));
                    } else {
                        List<BinaryRow> selectedDeltaPartitions =
                                ChainTableUtils.getDeltaPartitions(
                                        partitionParis.getValue(),
                                        partitionParis.getKey(),
                                        tableSchema.partitionKeys(),
                                        tableSchema.logicalPartitionType(),
                                        options,
                                        partitionComparator,
                                        partitionComputer);
                        deltaScan.withPartitionFilter(selectedDeltaPartitions);
                    }
                    List<Split> subSplits = deltaScan.plan().splits();
                    Set<String> snapshotFileNames = new HashSet<>();
                    if (partitionParis.getValue() != null) {
                        snapshotScan.withPartitionFilter(
                                Collections.singletonList(partitionParis.getValue()));
                        List<Split> mainSubSplits = snapshotScan.plan().splits();
                        snapshotFileNames =
                                mainSubSplits.stream()
                                        .flatMap(
                                                s ->
                                                        ((DataSplit) s)
                                                                .dataFiles().stream()
                                                                        .map(
                                                                                DataFileMeta
                                                                                        ::fileName))
                                        .collect(Collectors.toSet());
                        subSplits.addAll(mainSubSplits);
                    }
                    Map<Integer, List<DataSplit>> bucketSplits = new LinkedHashMap<>();
                    for (Split split : subSplits) {
                        DataSplit dataSplit = (DataSplit) split;
                        Integer totalBuckets = dataSplit.totalBuckets();
                        checkNotNull(totalBuckets);
                        checkArgument(
                                totalBuckets == options.bucket(),
                                "Inconsistent bucket num " + dataSplit.bucket());
                        bucketSplits
                                .computeIfAbsent(dataSplit.bucket(), k -> new ArrayList<>())
                                .add(dataSplit);
                    }
                    for (Map.Entry<Integer, List<DataSplit>> entry : bucketSplits.entrySet()) {
                        HashMap<String, String> fileBucketPathMapping = new HashMap<>();
                        HashMap<String, String> fileBranchMapping = new HashMap<>();
                        List<DataSplit> splitList = entry.getValue();
                        for (DataSplit dataSplit : splitList) {
                            for (DataFileMeta file : dataSplit.dataFiles()) {
                                fileBucketPathMapping.put(file.fileName(), dataSplit.bucketPath());
                                String branch =
                                        snapshotFileNames.contains(file.fileName())
                                                ? options.scanFallbackSnapshotBranch()
                                                : options.scanFallbackDeltaBranch();
                                fileBranchMapping.put(file.fileName(), branch);
                            }
                        }
                        ChainSplit split =
                                new ChainSplit(
                                        partitionParis.getKey(),
                                        entry.getValue().stream()
                                                .flatMap(
                                                        datsSplit -> datsSplit.dataFiles().stream())
                                                .collect(Collectors.toList()),
                                        fileBucketPathMapping,
                                        fileBranchMapping);
                        splits.add(split);
                    }
                }
            }
            return new DataFilePlan<>(splits);
        }

        @Override
        public List<PartitionEntry> listPartitionEntries() {
            return super.listPartitionEntries();
        }
    }

    @Override
    public InnerTableRead newRead() {
        return new Read();
    }

    private class Read implements InnerTableRead {

        private final InnerTableRead mainRead;
        private final InnerTableRead fallbackRead;

        private Read() {
            this.mainRead = wrapped.newRead();
            this.fallbackRead = fallback().newRead();
        }

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            mainRead.withFilter(predicate);
            fallbackRead.withFilter(predicate);
            return this;
        }

        @Override
        public InnerTableRead withReadType(RowType readType) {
            mainRead.withReadType(readType);
            fallbackRead.withReadType(readType);
            return this;
        }

        @Override
        public InnerTableRead forceKeepDelete() {
            mainRead.forceKeepDelete();
            fallbackRead.forceKeepDelete();
            return this;
        }

        @Override
        public TableRead executeFilter() {
            mainRead.executeFilter();
            fallbackRead.executeFilter();
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            mainRead.withIOManager(ioManager);
            fallbackRead.withIOManager(ioManager);
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            checkArgument(split instanceof ChainSplit);
            return fallbackRead.createReader(split);
        }
    }
}
