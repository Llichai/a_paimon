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
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataFilePlan;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SegmentsCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 回退读取表 - 主分支缺失分区时自动回退到备用分支读取
 *
 * <p>FallbackReadFileStoreTable 是一种容错表，主要用于<b>分支合并场景</b>：当主分支（current branch）
 * 中某些分区不存在时，自动从回退分支（fallback branch）中读取这些分区的数据。
 *
 * <p><b>核心功能：</b>
 * <ul>
 *   <li><b>分区级别的回退</b>：以分区为粒度，主分支有的分区读主分支，没有的读回退分支
 *   <li><b>透明切换</b>：对用户透明，自动合并两个分支的扫描结果
 *   <li><b>配置自动转换</b>：自动将主分支的配置转换为回退分支的配置（如快照 ID、分桶数）
 * </ul>
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li><b>分支合并</b>：主分支只有部分分区，其余从备份分支读取
 *   <li><b>增量迁移</b>：主分支逐步迁移数据，未迁移的分区从旧分支读取
 *   <li><b>容灾恢复</b>：主分支数据丢失时，从备份分支恢复
 * </ul>
 *
 * <p><b>回退逻辑示例：</b>
 * <pre>
 * 主分支（main）分区：[2024-01-01, 2024-01-03]
 * 回退分支（backup）分区：[2024-01-01, 2024-01-02, 2024-01-03, 2024-01-04]
 *
 * 扫描结果：
 * - 2024-01-01：读取主分支（主分支优先）
 * - 2024-01-02：读取回退分支（主分支没有）
 * - 2024-01-03：读取主分支
 * - 2024-01-04：读取回退分支
 * </pre>
 *
 * <p><b>核心组件：</b>
 * <ul>
 *   <li><b>FallbackReadScan</b>：合并主分支和回退分支的扫描结果
 *   <li><b>FallbackSplit</b>：标记分片来源（主分支或回退分支）
 *   <li><b>Read</b>：根据分片来源选择对应的读取器
 * </ul>
 *
 * <p><b>配置转换规则：</b>
 * <ul>
 *   <li><b>branch</b>：回退分支的 branch 配置永远不变
 *   <li><b>scan.snapshot-id</b>：主分支快照 ID 转换为回退分支对应时间的快照 ID
 *   <li><b>bucket</b>：移除 bucket 配置，使用回退分支的分桶数
 * </ul>
 *
 * <p><b>Schema 校验：</b>
 * <ul>
 *   <li>主分支和回退分支的 RowType 必须相同（忽略 nullable）
 *   <li>如果都有主键，则主键必须相同
 *   <li>如果主分支有主键，回退分支必须也有主键
 * </ul>
 *
 * <p><b>继承关系：</b>
 * <pre>
 * DelegatedFileStoreTable (抽象类，委托模式)
 *   ↑
 * FallbackReadFileStoreTable (回退读取)
 *   ↑
 * ChainGroupReadTable (链式分组读取，更复杂的回退逻辑)
 * </pre>
 *
 * <p><b>示例用法：</b>
 * <pre>{@code
 * // 创建回退表
 * FileStoreTable mainTable = ...; // 主分支表
 * FileStoreTable fallbackTable = ...; // 回退分支表
 * FileStoreTable fallbackReadTable = new FallbackReadFileStoreTable(mainTable, fallbackTable);
 *
 * // 扫描（自动合并两个分支）
 * DataTableScan scan = fallbackReadTable.newScan();
 * Plan plan = scan.plan();
 *
 * // 读取（自动根据分片来源选择读取器）
 * InnerTableRead read = fallbackReadTable.newRead();
 * for (Split split : plan.splits()) {
 *     RecordReader<InternalRow> reader = read.createReader(split);
 *     // 如果 split 来自主分支，使用主分支读取器
 *     // 如果 split 来自回退分支，使用回退分支读取器
 * }
 * }</pre>
 *
 * @see DelegatedFileStoreTable
 * @see ChainGroupReadTable
 * @see FallbackSplit
 */
public class FallbackReadFileStoreTable extends DelegatedFileStoreTable {

    private static final Logger LOG = LoggerFactory.getLogger(FallbackReadFileStoreTable.class);

    /** 回退分支表（当主分支缺失分区时，从此表读取） */
    private final FileStoreTable fallback;

    /**
     * 构造回退读取表
     *
     * @param wrapped 主分支表（当前分支）
     * @param fallback 回退分支表（备用分支）
     * @throws IllegalArgumentException 如果 wrapped 本身已经是 FallbackReadFileStoreTable
     */
    public FallbackReadFileStoreTable(FileStoreTable wrapped, FileStoreTable fallback) {
        super(wrapped);
        this.fallback = fallback;

        Preconditions.checkArgument(!(wrapped instanceof FallbackReadFileStoreTable));
        if (fallback instanceof FallbackReadFileStoreTable) {
            // ChainGroupReadTable need to be wrapped again
            if (!(fallback instanceof ChainGroupReadTable)) {
                throw new IllegalArgumentException(
                        "This is a bug, perhaps there is a recursive call.");
            }
        }
    }

    public FileStoreTable fallback() {
        return fallback;
    }

    @Override
    public FileStoreTable copy(Map<String, String> dynamicOptions) {
        return new FallbackReadFileStoreTable(
                wrapped.copy(dynamicOptions),
                fallback.copy(rewriteFallbackOptions(dynamicOptions)));
    }

    @Override
    public FileStoreTable copy(TableSchema newTableSchema) {
        return new FallbackReadFileStoreTable(
                wrapped.copy(newTableSchema),
                fallback.copy(
                        newTableSchema.copy(rewriteFallbackOptions(newTableSchema.options()))));
    }

    @Override
    public FileStoreTable copyWithoutTimeTravel(Map<String, String> dynamicOptions) {
        return new FallbackReadFileStoreTable(
                wrapped.copyWithoutTimeTravel(dynamicOptions),
                fallback.copyWithoutTimeTravel(rewriteFallbackOptions(dynamicOptions)));
    }

    @Override
    public FileStoreTable copyWithLatestSchema() {
        return new FallbackReadFileStoreTable(
                wrapped.copyWithLatestSchema(), fallback.copyWithLatestSchema());
    }

    @Override
    public FileStoreTable switchToBranch(String branchName) {
        return new FallbackReadFileStoreTable(switchWrappedToBranch(branchName), fallback);
    }

    @Override
    public void setManifestCache(SegmentsCache<Path> manifestCache) {
        super.setManifestCache(manifestCache);
        fallback.setManifestCache(manifestCache);
    }

    protected FileStoreTable switchWrappedToBranch(String branchName) {
        Optional<TableSchema> optionalSchema =
                wrapped.schemaManager().copyWithBranch(branchName).latest();
        Preconditions.checkArgument(
                optionalSchema.isPresent(), "Branch " + branchName + " does not exist");

        TableSchema branchSchema = optionalSchema.get();
        Options branchOptions = new Options(branchSchema.options());
        branchOptions.set(CoreOptions.BRANCH, branchName);
        branchSchema = branchSchema.copy(branchOptions.toMap());
        return FileStoreTableFactory.createWithoutFallbackBranch(
                wrapped.fileIO(),
                wrapped.location(),
                branchSchema,
                new Options(),
                wrapped.catalogEnvironment());
    }

    /**
     * 重写配置选项以适配回退分支
     *
     * <p>该方法将主分支的动态选项转换为回退分支的选项，确保正确性：
     * <ol>
     *   <li><b>branch</b>：保持回退分支的 branch 不变（不使用主分支的 branch）
     *   <li><b>scan.snapshot-id</b>：主分支的快照 ID 转换为回退分支对应时间的快照 ID
     *   <li><b>bucket</b>：移除 bucket 配置，使用回退分支的分桶数
     * </ol>
     *
     * <p><b>为什么需要转换快照 ID？</b>
     * <ul>
     *   <li>主分支和回退分支的快照 ID 可能不同（各自独立递增）
     *   <li>需要根据时间戳找到回退分支中对应的快照
     *   <li>如果回退分支没有对应时间的快照，则使用第一个快照
     * </ul>
     *
     * <p><b>为什么移除 bucket？</b>
     * <ul>
     *   <li>主分支和回退分支的分桶数可能不同
     *   <li>必须使用回退分支的分桶数，否则读取会出错
     * </ul>
     *
     * @param options 主分支的动态选项
     * @return 适配回退分支的选项
     */
    protected Map<String, String> rewriteFallbackOptions(Map<String, String> options) {
        Map<String, String> result = new HashMap<>(options);

        // branch of fallback table should never change
        String branchKey = CoreOptions.BRANCH.key();
        if (options.containsKey(branchKey)) {
            result.put(branchKey, fallback.options().get(branchKey));
        }

        // snapshot ids may be different between the main branch and the fallback branch,
        // so we need to convert main branch snapshot id to millisecond,
        // then convert millisecond to fallback branch snapshot id
        String scanSnapshotIdOptionKey = CoreOptions.SCAN_SNAPSHOT_ID.key();
        String scanSnapshotId = options.get(scanSnapshotIdOptionKey);
        if (scanSnapshotId != null) {
            long id = Long.parseLong(scanSnapshotId);
            long millis = wrapped.snapshotManager().snapshot(id).timeMillis();
            Snapshot fallbackSnapshot = fallback.snapshotManager().earlierOrEqualTimeMills(millis);
            long fallbackId;
            if (fallbackSnapshot == null) {
                fallbackId = Snapshot.FIRST_SNAPSHOT_ID;
            } else {
                fallbackId = fallbackSnapshot.id();
            }
            result.put(scanSnapshotIdOptionKey, String.valueOf(fallbackId));
        }

        // bucket number of main branch and fallback branch are very likely different,
        // so we remove bucket in options to use fallback branch's bucket number
        result.remove(CoreOptions.BUCKET.key());

        return result;
    }

    @Override
    public DataTableScan newScan() {
        validateSchema();
        return new FallbackReadScan(wrapped.newScan(), fallback.newScan());
    }

    /**
     * 校验主分支和回退分支的 Schema 兼容性
     *
     * <p>校验规则：
     * <ol>
     *   <li><b>RowType 必须相同</b>（忽略 nullable 差异）：
     *       <ul>
     *         <li>字段数量相同
     *         <li>字段顺序相同
     *         <li>字段类型相同（ignoreNullable）
     *       </ul>
     *   <li><b>主键必须一致</b>：
     *       <ul>
     *         <li>如果主分支有主键，回退分支也必须有主键
     *         <li>主键列表必须完全相同
     *       </ul>
     * </ol>
     *
     * @throws IllegalArgumentException 如果 Schema 不兼容
     */
    protected void validateSchema() {
        String mainBranch = wrapped.coreOptions().branch();
        String fallbackBranch = fallback.coreOptions().branch();
        RowType mainRowType = wrapped.schema().logicalRowType();
        RowType fallbackRowType = fallback.schema().logicalRowType();
        Preconditions.checkArgument(
                sameRowTypeIgnoreNullable(mainRowType, fallbackRowType),
                "Branch %s and %s does not have the same row type.\n"
                        + "Row type of branch %s is %s.\n"
                        + "Row type of branch %s is %s.",
                mainBranch,
                fallbackBranch,
                mainBranch,
                mainRowType,
                fallbackBranch,
                fallbackRowType);

        List<String> mainPrimaryKeys = wrapped.schema().primaryKeys();
        List<String> fallbackPrimaryKeys = fallback.schema().primaryKeys();
        if (!mainPrimaryKeys.isEmpty()) {
            if (fallbackPrimaryKeys.isEmpty()) {
                throw new IllegalArgumentException(
                        "Branch "
                                + mainBranch
                                + " has primary keys while fallback branch "
                                + fallbackBranch
                                + " does not. This is not allowed.");
            }
            Preconditions.checkArgument(
                    mainPrimaryKeys.equals(fallbackPrimaryKeys),
                    "Branch %s and %s both have primary keys but are not the same.\n"
                            + "Primary keys of %s are %s.\n"
                            + "Primary keys of %s are %s.",
                    mainBranch,
                    fallbackBranch,
                    mainBranch,
                    mainPrimaryKeys,
                    fallbackBranch,
                    fallbackPrimaryKeys);
        }
    }

    private boolean sameRowTypeIgnoreNullable(RowType mainRowType, RowType fallbackRowType) {
        if (mainRowType.getFieldCount() != fallbackRowType.getFieldCount()) {
            return false;
        }
        for (int i = 0; i < mainRowType.getFieldCount(); i++) {
            DataType mainType = mainRowType.getFields().get(i).type();
            DataType fallbackType = fallbackRowType.getFields().get(i).type();
            if (!mainType.equalsIgnoreNullable(fallbackType)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 回退分片接口 - 标记分片来源（主分支或回退分支）
     *
     * <p>FallbackSplit 是对普通 Split 的包装，添加了一个布尔标记 {@link #isFallback()}，
     * 用于在读取时判断应该使用主分支读取器还是回退分支读取器。
     *
     * <p><b>设计目的：</b>
     * <ul>
     *   <li>在扫描阶段，将主分支和回退分支的分片合并到一起
     *   <li>在读取阶段，根据标记选择正确的读取器
     * </ul>
     *
     * <p><b>两种实现：</b>
     * <ul>
     *   <li>{@link FallbackDataSplit}：用于 DataSplit
     *   <li>{@link FallbackSplitImpl}：用于其他类型的 Split
     * </ul>
     */
    public interface FallbackSplit extends Split {

        /**
         * 判断该分片是否来自回退分支
         *
         * @return true = 来自回退分支，false = 来自主分支
         */
        boolean isFallback();

        /**
         * 获取被包装的原始分片
         *
         * @return 原始 Split 对象
         */
        Split wrapped();
    }

    /**
     * 通用 Split 的回退实现（非 DataSplit）
     *
     * <p>该类用于包装所有非 DataSplit 类型的分片。
     */
    public static class FallbackSplitImpl implements FallbackSplit {

        private static final long serialVersionUID = 1L;

        private final Split split;
        private final boolean isFallback;

        public FallbackSplitImpl(Split split, boolean isFallback) {
            this.split = split;
            this.isFallback = isFallback;
        }

        @Override
        public boolean isFallback() {
            return isFallback;
        }

        @Override
        public Split wrapped() {
            return split;
        }

        @Override
        public long rowCount() {
            return split.rowCount();
        }

        @Override
        public OptionalLong mergedRowCount() {
            return split.mergedRowCount();
        }
    }

    /**
     * DataSplit 的回退实现
     *
     * <p>该类继承自 {@link DataSplit}，添加了 {@link #isFallback} 标记。
     *
     * <p><b>序列化支持：</b>
     * <ul>
     *   <li>重写了 {@link #serialize(DataOutputView)} 和 {@link #deserialize(DataInputView)}
     *   <li>支持 Java 标准序列化（writeObject/readObject）
     * </ul>
     *
     * <p><b>注意：</b>
     * <ul>
     *   <li>equals 和 hashCode 同时考虑 DataSplit 内容和 isFallback 标记
     * </ul>
     */
    public static class FallbackDataSplit extends DataSplit implements FallbackSplit {

        private static final long serialVersionUID = 1L;

        private boolean isFallback;

        private FallbackDataSplit(DataSplit dataSplit, boolean isFallback) {
            assign(dataSplit);
            this.isFallback = isFallback;
        }

        @Override
        public boolean equals(Object o) {
            return super.equals(o) && isFallback == ((FallbackDataSplit) o).isFallback;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), isFallback);
        }

        private void writeObject(ObjectOutputStream out) throws IOException {
            serialize(new DataOutputViewStreamWrapper(out));
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            FallbackDataSplit split = deserialize(new DataInputViewStreamWrapper(in));
            assign(split);
            this.isFallback = split.isFallback;
        }

        @Override
        public void serialize(DataOutputView out) throws IOException {
            super.serialize(out);
            out.writeBoolean(isFallback);
        }

        public static FallbackDataSplit deserialize(DataInputView in) throws IOException {
            DataSplit dataSplit = DataSplit.deserialize(in);
            return new FallbackDataSplit(dataSplit, in.readBoolean());
        }

        @Override
        public boolean isFallback() {
            return isFallback;
        }

        @Override
        public Split wrapped() {
            return this;
        }
    }

    /**
     * 将普通 Split 包装为 FallbackSplit
     *
     * @param split 原始分片
     * @param fallback 是否来自回退分支
     * @return 包装后的 FallbackSplit
     */
    public static FallbackSplit toFallbackSplit(Split split, boolean fallback) {
        if (split instanceof DataSplit) {
            return new FallbackDataSplit((DataSplit) split, fallback);
        }
        return new FallbackSplitImpl(split, fallback);
    }

    /**
     * 回退读取的扫描实现
     *
     * <p>该类合并主分支和回退分支的扫描结果，核心逻辑：
     * <ol>
     *   <li>扫描主分支，获取所有分区列表（completePartitions）
     *   <li>扫描回退分支，过滤掉主分支已有的分区（remainingPartitions）
     *   <li>合并两个分支的分片，标记来源
     * </ol>
     *
     * <p><b>扫描流程：</b>
     * <pre>
     * 1. mainScan.plan() → 主分支的分片（标记 isFallback=false）
     * 2. 收集主分支的所有分区 → completePartitions
     * 3. fallbackScan.listPartitions() → 回退分支的分区列表
     * 4. 过滤出 remainingPartitions = fallbackPartitions - completePartitions
     * 5. fallbackScan.withPartitionFilter(remainingPartitions).plan()
     *    → 回退分支的分片（标记 isFallback=true）
     * 6. 合并所有分片返回
     * </pre>
     *
     * <p><b>所有方法都会同时调用主分支和回退分支的对应方法</b>：
     * <ul>
     *   <li>withShard → 两边都设置分片
     *   <li>withFilter → 两边都设置过滤器
     *   <li>withPartitionFilter → 两边都设置分区过滤
     * </ul>
     */
    public static class FallbackReadScan implements DataTableScan {

        /** 主分支扫描器 */
        protected final DataTableScan mainScan;

        /** 回退分支扫描器 */
        protected final DataTableScan fallbackScan;

        /**
         * 构造回退读取扫描器
         *
         * @param mainScan 主分支扫描器
         * @param fallbackScan 回退分支扫描器
         */
        public FallbackReadScan(DataTableScan mainScan, DataTableScan fallbackScan) {
            this.mainScan = mainScan;
            this.fallbackScan = fallbackScan;
        }

        @Override
        public FallbackReadScan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
            mainScan.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
            fallbackScan.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
            return this;
        }

        @Override
        public FallbackReadScan withFilter(Predicate predicate) {
            mainScan.withFilter(predicate);
            fallbackScan.withFilter(predicate);
            return this;
        }

        @Override
        public FallbackReadScan withLimit(int limit) {
            mainScan.withLimit(limit);
            fallbackScan.withLimit(limit);
            return this;
        }

        @Override
        public FallbackReadScan withPartitionFilter(Map<String, String> partitionSpec) {
            mainScan.withPartitionFilter(partitionSpec);
            fallbackScan.withPartitionFilter(partitionSpec);
            return this;
        }

        @Override
        public FallbackReadScan withPartitionFilter(List<BinaryRow> partitions) {
            mainScan.withPartitionFilter(partitions);
            fallbackScan.withPartitionFilter(partitions);
            return this;
        }

        @Override
        public InnerTableScan withPartitionsFilter(List<Map<String, String>> partitions) {
            mainScan.withPartitionsFilter(partitions);
            fallbackScan.withPartitionsFilter(partitions);
            return this;
        }

        @Override
        public InnerTableScan withPartitionFilter(PartitionPredicate partitionPredicate) {
            mainScan.withPartitionFilter(partitionPredicate);
            fallbackScan.withPartitionFilter(partitionPredicate);
            return this;
        }

        @Override
        public FallbackReadScan withBucketFilter(Filter<Integer> bucketFilter) {
            mainScan.withBucketFilter(bucketFilter);
            fallbackScan.withBucketFilter(bucketFilter);
            return this;
        }

        @Override
        public FallbackReadScan withLevelFilter(Filter<Integer> levelFilter) {
            mainScan.withLevelFilter(levelFilter);
            fallbackScan.withLevelFilter(levelFilter);
            return this;
        }

        @Override
        public FallbackReadScan withMetricRegistry(MetricRegistry metricRegistry) {
            mainScan.withMetricRegistry(metricRegistry);
            fallbackScan.withMetricRegistry(metricRegistry);
            return this;
        }

        @Override
        public InnerTableScan withTopN(TopN topN) {
            mainScan.withTopN(topN);
            fallbackScan.withTopN(topN);
            return this;
        }

        @Override
        public InnerTableScan dropStats() {
            mainScan.dropStats();
            fallbackScan.dropStats();
            return this;
        }

        @Override
        public TableScan.Plan plan() {
            List<Split> splits = new ArrayList<>();
            Set<BinaryRow> completePartitions = new HashSet<>();
            for (Split split : mainScan.plan().splits()) {
                DataSplit dataSplit = (DataSplit) split;
                splits.add(toFallbackSplit(dataSplit, false));
                completePartitions.add(dataSplit.partition());
            }

            List<BinaryRow> remainingPartitions =
                    fallbackScan.listPartitions().stream()
                            .filter(p -> !completePartitions.contains(p))
                            .collect(Collectors.toList());
            if (!remainingPartitions.isEmpty()) {
                fallbackScan.withPartitionFilter(remainingPartitions);
                for (Split split : fallbackScan.plan().splits()) {
                    splits.add(toFallbackSplit(split, true));
                }
            }
            return new DataFilePlan<>(splits);
        }

        @Override
        public List<PartitionEntry> listPartitionEntries() {
            List<PartitionEntry> partitionEntries =
                    new ArrayList<>(mainScan.listPartitionEntries());
            Set<BinaryRow> partitions =
                    partitionEntries.stream()
                            .map(PartitionEntry::partition)
                            .collect(Collectors.toSet());
            List<PartitionEntry> fallBackPartitionEntries = fallbackScan.listPartitionEntries();
            fallBackPartitionEntries.stream()
                    .filter(e -> !partitions.contains(e.partition()))
                    .forEach(partitionEntries::add);
            return partitionEntries;
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
            this.fallbackRead = fallback.newRead();
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
            if (split instanceof FallbackSplit) {
                FallbackSplit fallbackSplit = (FallbackSplit) split;
                if (fallbackSplit.isFallback()) {
                    try {
                        return fallbackRead.createReader(fallbackSplit.wrapped());
                    } catch (Exception ignored) {
                        LOG.error(
                                "Reading from fallback branch has problems: {}",
                                fallbackSplit.wrapped());
                    }
                }
            }
            return mainRead.createReader(split);
        }
    }
}
