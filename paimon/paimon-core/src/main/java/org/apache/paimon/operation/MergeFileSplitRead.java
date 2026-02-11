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
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.ChainKeyValueFileReaderFactory;
import org.apache.paimon.io.ChainReadContext;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.mergetree.DropDeleteReader;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.MergeTreeReaders;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.mergetree.compact.IntervalPartition;
import org.apache.paimon.mergetree.compact.LookupMergeFunction;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.mergetree.compact.MergeFunctionWrapper;
import org.apache.paimon.mergetree.compact.ReducerMergeFunctionWrapper;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.ChainSplit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.predicate.PredicateBuilder.containsFields;
import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;

/**
 * 合并文件分片读取 - 用于主键表（KeyValue表）的 LSM 合并读取
 *
 * <p>该类处理主键表的复杂读取逻辑，包括：
 * <ul>
 *   <li><b>LSM 合并</b>：将多个层级的文件合并为一个有序流
 *   <li><b>RowKind 处理</b>：处理 +I/-U/-D 等变更语义
 *   <li><b>Sequence Number</b>：根据序列号确定记录的新旧关系
 * </ul>
 *
 * <p><b>为什么需要合并？</b>
 * <pre>
 * LSM Tree 存储结构：
 * Level 0: [file1(key1:v3, key2:v2), file2(key1:v2, key3:v1)]
 * Level 1: [file3(key1:v1, key2:v1)]
 *
 * 读取 key1 时：
 * - file1 有 key1:v3 (最新)
 * - file2 有 key1:v2
 * - file3 有 key1:v1 (最旧)
 * -> 需要合并，返回 key1:v3
 * </pre>
 *
 * <p><b>合并读取流程</b>：
 * <ol>
 *   <li><b>区间分区（Interval Partition）</b>：
 *       <ul>
 *         <li>将文件按照 key range 分组，重叠的文件分到一组
 *         <li>每组称为一个 Section
 *       </ul>
 *   </li>
 *   <li><b>Section 内合并</b>：
 *       <ul>
 *         <li>使用 SortMergeReader 按照 key 排序合并
 *         <li>使用 MergeFunction 处理同一个 key 的多条记录
 *       </ul>
 *   </li>
 *   <li><b>RowKind 处理</b>：
 *       <ul>
 *         <li>应用 MergeFunction（如 Deduplicate、PartialUpdate 等）
 *         <li>最终返回每个 key 的最新值
 *       </ul>
 *   </li>
 *   <li><b>删除过滤</b>：
 *       <ul>
 *         <li>使用 DropDeleteReader 过滤 DELETE 记录
 *         <li>除非 forceKeepDelete = true（CDC 场景）
 *       </ul>
 *   </li>
 * </ol>
 *
 * <p><b>过滤器下推策略</b>：
 * <ul>
 *   <li><b>重叠 Section</b>：只下推主键过滤器（filtersForKeys）
 *       <ul>
 *         <li>原因：非主键列的值可能在不同文件中不同，下推会导致错误
 *         <li>示例：
 *             <pre>
 *             File1: (key=1, value=100, seq=2)
 *             File2: (key=1, value=10, seq=3)
 *             查询：value >= 100
 *             - 如果下推到 File2，会漏掉 key=1（最新值是 10）
 *             - 正确做法：读取所有记录，合并后再过滤
 *             </pre>
 *       </ul>
 *   </li>
 *   <li><b>非重叠 Section</b>：下推所有过滤器（filtersForAll）
 *       <ul>
 *         <li>原因：每个 key 只出现一次，不会有歧义
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p><b>Sequence Number 处理</b>：
 * <ul>
 *   <li>确保读取类型包含 sequence 字段（用于判断记录新旧）
 *   <li>如果用户投影不包含 sequence，自动添加并在输出时去掉
 *   <li>支持自定义 sequence 字段（通过 {@code sequence.field} 配置）
 * </ul>
 *
 * <p><b>流式读取 vs 批量读取</b>：
 * <ul>
 *   <li><b>流式读取</b>（split.isStreaming()）：
 *       <ul>
 *         <li>不合并，直接按文件顺序读取
 *         <li>原因：流式增量读取，文件之间不重叠
 *         <li>使用 {@link #createNoMergeReader}
 *       </ul>
 *   </li>
 *   <li><b>批量读取</b>：
 *       <ul>
 *         <li>需要合并，处理历史数据
 *         <li>使用 {@link #createMergeReader}
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p><b>Chain Split 支持</b>：
 * <ul>
 *   <li>用于跨分支读取（Branch 功能）
 *   <li>支持文件路径映射（不同分支的文件可能在不同路径）
 *   <li>使用 ChainKeyValueFileReaderFactory 创建 Reader
 * </ul>
 *
 * @see RawFileSplitRead 如果是批处理且读取原始文件，推荐使用 RawFileSplitRead
 * @see org.apache.paimon.mergetree.MergeTreeReaders LSM 合并读取器
 * @see org.apache.paimon.mergetree.compact.MergeFunction 合并函数
 */
public class MergeFileSplitRead implements SplitRead<KeyValue> {

    private final TableSchema tableSchema;
    private final FileIO fileIO;
    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;
    private final Comparator<InternalRow> keyComparator;
    private final MergeFunctionFactory<KeyValue> mfFactory;
    private final MergeSorter mergeSorter;
    private final List<String> sequenceFields;
    private final boolean sequenceOrder;

    @Nullable private RowType readKeyType;
    @Nullable private RowType outerReadType;

    @Nullable private List<Predicate> filtersForKeys;
    @Nullable private List<Predicate> filtersForAll;

    private boolean forceKeepDelete = false;

    public MergeFileSplitRead(
            CoreOptions options,
            TableSchema schema,
            RowType keyType,
            RowType valueType,
            Comparator<InternalRow> keyComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            KeyValueFileReaderFactory.Builder readerFactoryBuilder) {
        this.tableSchema = schema;
        this.readerFactoryBuilder = readerFactoryBuilder;
        this.fileIO = readerFactoryBuilder.fileIO();
        this.keyComparator = keyComparator;
        this.mfFactory = mfFactory;
        this.mergeSorter =
                new MergeSorter(
                        CoreOptions.fromMap(tableSchema.options()), keyType, valueType, null);
        this.sequenceFields = options.sequenceField();
        this.sequenceOrder = options.sequenceFieldSortOrderIsAscending();
    }

    public Comparator<InternalRow> keyComparator() {
        return keyComparator;
    }

    public MergeSorter mergeSorter() {
        return mergeSorter;
    }

    public TableSchema tableSchema() {
        return tableSchema;
    }

    public MergeFileSplitRead withReadKeyType(RowType readKeyType) {
        readerFactoryBuilder.withReadKeyType(readKeyType);
        this.readKeyType = readKeyType;
        return this;
    }

    @Override
    public MergeFileSplitRead withReadType(RowType readType) {
        RowType tableRowType = tableSchema.logicalRowType();
        RowType adjustedReadType = readType;

        if (!sequenceFields.isEmpty()) {
            // make sure actual readType contains sequence fields
            List<String> readFieldNames = readType.getFieldNames();
            List<DataField> extraFields = new ArrayList<>();
            for (String seqField : sequenceFields) {
                if (!readFieldNames.contains(seqField)) {
                    extraFields.add(tableRowType.getField(seqField));
                }
            }
            if (!extraFields.isEmpty()) {
                List<DataField> allFields = new ArrayList<>(readType.getFields());
                allFields.addAll(extraFields);
                adjustedReadType = new RowType(allFields);
            }
        }
        adjustedReadType = mfFactory.adjustReadType(adjustedReadType);

        readerFactoryBuilder.withReadValueType(adjustedReadType);
        mergeSorter.setProjectedValueType(adjustedReadType);

        // When finalReadType != readType, need to project the outer read type
        if (adjustedReadType != readType) {
            outerReadType = readType;
        }

        return this;
    }

    @Override
    public MergeFileSplitRead withIOManager(IOManager ioManager) {
        this.mergeSorter.setIOManager(ioManager);
        if (mfFactory instanceof LookupMergeFunction.Factory) {
            ((LookupMergeFunction.Factory) mfFactory).withIOManager(ioManager);
        }
        return this;
    }

    @Override
    public MergeFileSplitRead forceKeepDelete() {
        this.forceKeepDelete = true;
        return this;
    }

    @Override
    public MergeFileSplitRead withFilter(Predicate predicate) {
        if (predicate == null) {
            return this;
        }

        List<Predicate> allFilters = new ArrayList<>();
        List<Predicate> pkFilters = null;
        List<String> primaryKeys = tableSchema.trimmedPrimaryKeys();
        Set<String> nonPrimaryKeys =
                tableSchema.fieldNames().stream()
                        .filter(name -> !primaryKeys.contains(name))
                        .collect(Collectors.toSet());
        for (Predicate sub : splitAnd(predicate)) {
            allFilters.add(sub);
            if (!containsFields(sub, nonPrimaryKeys)) {
                if (pkFilters == null) {
                    pkFilters = new ArrayList<>();
                }
                // TODO Actually, the index is wrong, but it is OK.
                //  The orc filter just use name instead of index.
                pkFilters.add(sub);
            }
        }
        // Consider this case:
        // Denote (seqNumber, key, value) as a record. We have two overlapping runs in a section:
        //   * First run: (1, k1, 100), (2, k2, 200)
        //   * Second run: (3, k1, 10), (4, k2, 20)
        // If we push down filter "value >= 100" for this section, only the first run will be read,
        // and the second run is lost. This will produce incorrect results.
        //
        // So for sections with overlapping runs, we only push down key filters.
        // For sections with only one run, as each key only appears once, it is OK to push down
        // value filters.
        filtersForAll = allFilters;
        filtersForKeys = pkFilters;
        return this;
    }

    @Override
    public RecordReader<KeyValue> createReader(Split split) throws IOException {
        if (split instanceof DataSplit) {
            return createReader((DataSplit) split);
        } else if (split instanceof ChainSplit) {
            return createChainReader((ChainSplit) split);
        } else {
            throw new IllegalArgumentException(
                    "Un-supported split type: " + split.getClass().getName());
        }
    }

    public RecordReader<KeyValue> createReader(DataSplit split) throws IOException {
        if (split.isStreaming() || split.bucket() == BucketMode.POSTPONE_BUCKET) {
            return createNoMergeReader(
                    split.partition(),
                    split.bucket(),
                    split.dataFiles(),
                    split.deletionFiles().orElse(null),
                    split.isStreaming());
        } else {
            return createMergeReader(
                    split.partition(),
                    split.bucket(),
                    split.dataFiles(),
                    split.deletionFiles().orElse(null),
                    forceKeepDelete);
        }
    }

    public RecordReader<KeyValue> createChainReader(ChainSplit chainSplit) throws IOException {
        List<DataFileMeta> files = chainSplit.dataFiles();
        ChainReadContext chainReadContext =
                new ChainReadContext.Builder()
                        .withLogicalPartition(chainSplit.logicalPartition())
                        .withFileBranchPathMapping(chainSplit.fileBranchMapping())
                        .withFileBucketPathMapping(chainSplit.fileBucketPathMapping())
                        .build();
        DeletionVector.Factory dvFactory =
                DeletionVector.factory(fileIO, files, chainSplit.deletionFiles().orElse(null));
        ChainKeyValueFileReaderFactory.Builder builder =
                ChainKeyValueFileReaderFactory.newBuilder(readerFactoryBuilder);
        ChainKeyValueFileReaderFactory overlappedSectionFactory =
                builder.build(null, dvFactory, false, filtersForKeys, chainReadContext);
        ChainKeyValueFileReaderFactory nonOverlappedSectionFactory =
                builder.build(null, dvFactory, false, filtersForAll, chainReadContext);
        return createMergeReader(
                files, overlappedSectionFactory, nonOverlappedSectionFactory, forceKeepDelete);
    }

    public RecordReader<KeyValue> createMergeReader(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            @Nullable List<DeletionFile> deletionFiles,
            boolean keepDelete)
            throws IOException {
        // Sections are read by SortMergeReader, which sorts and merges records by keys.
        // So we cannot project keys or else the sorting will be incorrect.
        DeletionVector.Factory dvFactory = DeletionVector.factory(fileIO, files, deletionFiles);
        KeyValueFileReaderFactory overlappedSectionFactory =
                readerFactoryBuilder.build(partition, bucket, dvFactory, false, filtersForKeys);
        KeyValueFileReaderFactory nonOverlappedSectionFactory =
                readerFactoryBuilder.build(partition, bucket, dvFactory, false, filtersForAll);
        return createMergeReader(
                files, overlappedSectionFactory, nonOverlappedSectionFactory, keepDelete);
    }

    public RecordReader<KeyValue> createMergeReader(
            List<DataFileMeta> files,
            KeyValueFileReaderFactory overlappedSectionFactory,
            KeyValueFileReaderFactory nonOverlappedSectionFactory,
            boolean keepDelete)
            throws IOException {
        List<ReaderSupplier<KeyValue>> sectionReaders = new ArrayList<>();
        MergeFunctionWrapper<KeyValue> mergeFuncWrapper =
                new ReducerMergeFunctionWrapper(mfFactory.create(actualReadType()));
        for (List<SortedRun> section : new IntervalPartition(files, keyComparator).partition()) {
            sectionReaders.add(
                    () ->
                            MergeTreeReaders.readerForSection(
                                    section,
                                    section.size() > 1
                                            ? overlappedSectionFactory
                                            : nonOverlappedSectionFactory,
                                    keyComparator,
                                    createUdsComparator(),
                                    mergeFuncWrapper,
                                    mergeSorter));
        }
        RecordReader<KeyValue> reader = ConcatRecordReader.create(sectionReaders);

        if (!keepDelete) {
            reader = new DropDeleteReader(reader);
        }

        return projectOuter(projectKey(reader));
    }

    public RecordReader<KeyValue> createNoMergeReader(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            @Nullable List<DeletionFile> deletionFiles,
            boolean onlyFilterKey)
            throws IOException {
        KeyValueFileReaderFactory readerFactory =
                readerFactoryBuilder.build(
                        partition,
                        bucket,
                        DeletionVector.factory(fileIO, files, deletionFiles),
                        true,
                        onlyFilterKey ? filtersForKeys : filtersForAll);
        List<ReaderSupplier<KeyValue>> suppliers = new ArrayList<>();
        for (DataFileMeta file : files) {
            suppliers.add(() -> readerFactory.createRecordReader(file));
        }

        return projectOuter(ConcatRecordReader.create(suppliers));
    }

    /**
     * Returns the pushed read type if {@link #withReadType(RowType)} was called, else the default
     * read type.
     */
    private RowType actualReadType() {
        return readerFactoryBuilder.readValueType();
    }

    private RecordReader<KeyValue> projectKey(RecordReader<KeyValue> reader) {
        if (readKeyType == null) {
            return reader;
        }

        ProjectedRow projectedRow = ProjectedRow.from(readKeyType, tableSchema.logicalRowType());
        return reader.transform(kv -> kv.replaceKey(projectedRow.replaceRow(kv.key())));
    }

    private RecordReader<KeyValue> projectOuter(RecordReader<KeyValue> reader) {
        if (outerReadType != null) {
            ProjectedRow projectedRow = ProjectedRow.from(outerReadType, actualReadType());
            reader = reader.transform(kv -> kv.replaceValue(projectedRow.replaceRow(kv.value())));
        }
        return reader;
    }

    @Nullable
    public UserDefinedSeqComparator createUdsComparator() {
        return UserDefinedSeqComparator.create(actualReadType(), sequenceFields, sequenceOrder);
    }
}
