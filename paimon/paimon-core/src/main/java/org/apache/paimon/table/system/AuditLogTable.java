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

package org.apache.paimon.table.system;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.BucketEntry;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.ManifestsReader;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.PredicateReplaceVisitor;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.StartingContext;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BiFilter;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SimpleFileReader;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.TABLE_READ_SEQUENCE_NUMBER_ENABLED;
import static org.apache.paimon.catalog.Identifier.SYSTEM_TABLE_SPLITTER;

/**
 * 审计日志表。
 *
 * <p>用于读取表的审计日志(Audit Log),提供完整的变更历史记录。审计日志包含所有的数据变更操作,
 * 包括插入(+I)、更新(+U/-U)和删除(-D)操作。
 *
 * <h2>特性</h2>
 * <ul>
 *   <li><b>完整变更历史</b>: 记录表的所有数据变更操作</li>
 *   <li><b>行级变更</b>: 每条记录都包含 {@code _ROW_KIND} 字段标识操作类型</li>
 *   <li><b>序列号支持</b>: 可选包含 {@code _SEQUENCE_NUMBER} 字段用于排序</li>
 *   <li><b>时间旅行</b>: 支持查询特定快照或时间点的审计日志</li>
 *   <li><b>流式读取</b>: 支持流式消费增量变更</li>
 * </ul>
 *
 * <h2>表结构 (Schema)</h2>
 * <p>审计日志表的 Schema 由以下部分组成:
 * <ul>
 *   <li><b>_ROW_KIND</b> (STRING): 行操作类型
 *     <ul>
 *       <li>+I (INSERT): 插入操作</li>
 *       <li>-U (UPDATE_BEFORE): 更新前的旧值</li>
 *       <li>+U (UPDATE_AFTER): 更新后的新值</li>
 *       <li>-D (DELETE): 删除操作</li>
 *     </ul>
 *   </li>
 *   <li><b>_SEQUENCE_NUMBER</b> (BIGINT, 可选): 序列号,用于确定操作顺序<br>
 *       需要设置 {@code table-read.sequence-number.enabled = true}
 *   </li>
 *   <li><b>数据字段</b>: 原表的所有字段</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * -- 查询所有审计日志
 * SELECT * FROM my_table$audit_log;
 *
 * -- 查询特定类型的操作
 * SELECT * FROM my_table$audit_log WHERE _ROW_KIND = '+I';
 *
 * -- 统计各类操作数量
 * SELECT _ROW_KIND, COUNT(*) FROM my_table$audit_log GROUP BY _ROW_KIND;
 *
 * -- 流式消费增量变更(Flink SQL)
 * SELECT * FROM my_table$audit_log /*+ OPTIONS('scan.mode'='latest') */;
 * }</pre>
 *
 * <h2>配置选项</h2>
 * <table border="1">
 *   <tr><th>选项</th><th>默认值</th><th>描述</th></tr>
 *   <tr>
 *     <td>table-read.sequence-number.enabled</td>
 *     <td>false</td>
 *     <td>是否在输出中包含序列号字段</td>
 *   </tr>
 * </table>
 *
 * <h2>与 Binlog 表的区别</h2>
 * <ul>
 *   <li><b>审计日志表</b>: 每条变更记录一行,UPDATE 操作分为两行(-U 和 +U)</li>
 *   <li><b>Binlog 表</b>: UPDATE 操作合并为一行,使用数组表示前后值</li>
 * </ul>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li>审计日志表保留所有删除操作的记录(不会被 compaction 清除)</li>
 *   <li>输出的 {@code _ROW_KIND} 始终为 INSERT,实际的操作类型在字段值中体现</li>
 *   <li>不支持通过 hint 动态设置 {@code table-read.sequence-number.enabled}</li>
 * </ul>
 *
 * @see DataTable
 * @see ReadonlyTable
 * @see BinlogTable
 */
public class AuditLogTable implements DataTable, ReadonlyTable {

    /** 系统表名称常量。 */
    public static final String AUDIT_LOG = "audit_log";

    protected final FileStoreTable wrapped;

    protected final List<DataField> specialFields;

    public AuditLogTable(FileStoreTable wrapped) {
        this.wrapped = wrapped;
        this.specialFields = new ArrayList<>();
        specialFields.add(SpecialFields.ROW_KIND);

        boolean includeSequenceNumber =
                CoreOptions.fromMap(wrapped.options()).tableReadSequenceNumberEnabled();

        if (includeSequenceNumber) {
            this.wrapped.options().put(CoreOptions.KEY_VALUE_SEQUENCE_NUMBER_ENABLED.key(), "true");
            specialFields.add(SpecialFields.SEQUENCE_NUMBER);
        }
    }

    /** Creates a PredicateReplaceVisitor that adjusts field indices by systemFieldCount. */
    private PredicateReplaceVisitor createPredicateConverter() {
        return p -> {
            Optional<FieldRef> fieldRefOptional = p.fieldRefOptional();
            if (!fieldRefOptional.isPresent()) {
                return Optional.empty();
            }
            FieldRef fieldRef = fieldRefOptional.get();
            if (fieldRef.index() < specialFields.size()) {
                return Optional.empty();
            }
            return Optional.of(
                    new LeafPredicate(
                            p.function(),
                            fieldRef.type(),
                            fieldRef.index() - specialFields.size(),
                            fieldRef.name(),
                            p.literals()));
        };
    }

    @Override
    public Optional<Snapshot> latestSnapshot() {
        return wrapped.latestSnapshot();
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        return wrapped.snapshot(snapshotId);
    }

    @Override
    public SimpleFileReader<ManifestFileMeta> manifestListReader() {
        return wrapped.manifestListReader();
    }

    @Override
    public SimpleFileReader<ManifestEntry> manifestFileReader() {
        return wrapped.manifestFileReader();
    }

    @Override
    public SimpleFileReader<IndexManifestEntry> indexManifestFileReader() {
        return wrapped.indexManifestFileReader();
    }

    @Override
    public String name() {
        return wrapped.name() + SYSTEM_TABLE_SPLITTER + AUDIT_LOG;
    }

    @Override
    public RowType rowType() {
        List<DataField> fields = new ArrayList<>(specialFields);
        fields.addAll(wrapped.rowType().getFields());
        return new RowType(fields);
    }

    @Override
    public List<String> partitionKeys() {
        return wrapped.partitionKeys();
    }

    @Override
    public Map<String, String> options() {
        return wrapped.options();
    }

    @Override
    public List<String> primaryKeys() {
        return Collections.emptyList();
    }

    @Override
    public SnapshotReader newSnapshotReader() {
        return new AuditLogDataReader(wrapped.newSnapshotReader());
    }

    @Override
    public DataTableScan newScan() {
        return new AuditLogBatchScan(wrapped.newScan());
    }

    @Override
    public StreamDataTableScan newStreamScan() {
        return new AuditLogStreamScan(wrapped.newStreamScan());
    }

    @Override
    public CoreOptions coreOptions() {
        return wrapped.coreOptions();
    }

    @Override
    public Path location() {
        return wrapped.location();
    }

    @Override
    public SnapshotManager snapshotManager() {
        return wrapped.snapshotManager();
    }

    @Override
    public ChangelogManager changelogManager() {
        return wrapped.changelogManager();
    }

    @Override
    public ConsumerManager consumerManager() {
        return wrapped.consumerManager();
    }

    @Override
    public SchemaManager schemaManager() {
        return wrapped.schemaManager();
    }

    @Override
    public TagManager tagManager() {
        return wrapped.tagManager();
    }

    @Override
    public BranchManager branchManager() {
        return wrapped.branchManager();
    }

    @Override
    public DataTable switchToBranch(String branchName) {
        return new AuditLogTable(wrapped.switchToBranch(branchName));
    }

    @Override
    public InnerTableRead newRead() {
        return new AuditLogRead(wrapped.newRead());
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        if (Boolean.parseBoolean(
                dynamicOptions.getOrDefault(TABLE_READ_SEQUENCE_NUMBER_ENABLED.key(), "false"))) {
            throw new UnsupportedOperationException(
                    "table-read.sequence-number.enabled is not supported by hint.");
        }
        return new AuditLogTable(wrapped.copy(dynamicOptions));
    }

    @Override
    public FileIO fileIO() {
        return wrapped.fileIO();
    }

    /** Push down predicate to dataScan and dataRead. */
    private Optional<Predicate> convert(Predicate predicate) {
        PredicateReplaceVisitor converter = createPredicateConverter();
        List<Predicate> result =
                PredicateBuilder.splitAnd(predicate).stream()
                        .map(p -> p.visit(converter))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toList());
        if (result.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(PredicateBuilder.and(result));
    }

    private class AuditLogDataReader implements SnapshotReader {

        private final SnapshotReader wrapped;

        private AuditLogDataReader(SnapshotReader wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public Integer parallelism() {
            return wrapped.parallelism();
        }

        @Override
        public SnapshotManager snapshotManager() {
            return wrapped.snapshotManager();
        }

        @Override
        public ChangelogManager changelogManager() {
            return wrapped.changelogManager();
        }

        @Override
        public ManifestsReader manifestsReader() {
            return wrapped.manifestsReader();
        }

        @Override
        public List<ManifestEntry> readManifest(ManifestFileMeta manifest) {
            return wrapped.readManifest(manifest);
        }

        @Override
        public ConsumerManager consumerManager() {
            return wrapped.consumerManager();
        }

        @Override
        public SplitGenerator splitGenerator() {
            return wrapped.splitGenerator();
        }

        @Override
        public FileStorePathFactory pathFactory() {
            return wrapped.pathFactory();
        }

        public SnapshotReader withSnapshot(long snapshotId) {
            wrapped.withSnapshot(snapshotId);
            return this;
        }

        public SnapshotReader withSnapshot(Snapshot snapshot) {
            wrapped.withSnapshot(snapshot);
            return this;
        }

        public SnapshotReader withFilter(Predicate predicate) {
            convert(predicate).ifPresent(wrapped::withFilter);
            return this;
        }

        @Override
        public SnapshotReader withPartitionFilter(Map<String, String> partitionSpec) {
            wrapped.withPartitionFilter(partitionSpec);
            return this;
        }

        @Override
        public SnapshotReader withPartitionFilter(Predicate predicate) {
            wrapped.withPartitionFilter(predicate);
            return this;
        }

        @Override
        public SnapshotReader withPartitionFilter(List<BinaryRow> partitions) {
            wrapped.withPartitionFilter(partitions);
            return this;
        }

        @Override
        public SnapshotReader withPartitionFilter(PartitionPredicate partitionPredicate) {
            wrapped.withPartitionFilter(partitionPredicate);
            return this;
        }

        @Override
        public SnapshotReader withPartitionsFilter(List<Map<String, String>> partitions) {
            wrapped.withPartitionsFilter(partitions);
            return this;
        }

        @Override
        public SnapshotReader withMode(ScanMode scanMode) {
            wrapped.withMode(scanMode);
            return this;
        }

        @Override
        public SnapshotReader withLevel(int level) {
            wrapped.withLevel(level);
            return this;
        }

        @Override
        public SnapshotReader withLevelFilter(Filter<Integer> levelFilter) {
            wrapped.withLevelFilter(levelFilter);
            return this;
        }

        @Override
        public SnapshotReader withLevelMinMaxFilter(BiFilter<Integer, Integer> minMaxFilter) {
            wrapped.withLevelMinMaxFilter(minMaxFilter);
            return this;
        }

        @Override
        public SnapshotReader enableValueFilter() {
            wrapped.enableValueFilter();
            return this;
        }

        @Override
        public SnapshotReader withManifestEntryFilter(Filter<ManifestEntry> filter) {
            wrapped.withManifestEntryFilter(filter);
            return this;
        }

        public SnapshotReader withBucket(int bucket) {
            wrapped.withBucket(bucket);
            return this;
        }

        @Override
        public SnapshotReader onlyReadRealBuckets() {
            wrapped.onlyReadRealBuckets();
            return this;
        }

        @Override
        public SnapshotReader withBucketFilter(Filter<Integer> bucketFilter) {
            wrapped.withBucketFilter(bucketFilter);
            return this;
        }

        @Override
        public SnapshotReader withDataFileNameFilter(Filter<String> fileNameFilter) {
            wrapped.withDataFileNameFilter(fileNameFilter);
            return this;
        }

        @Override
        public SnapshotReader dropStats() {
            wrapped.dropStats();
            return this;
        }

        @Override
        public SnapshotReader keepStats() {
            wrapped.keepStats();
            return this;
        }

        @Override
        public SnapshotReader withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
            wrapped.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
            return this;
        }

        @Override
        public SnapshotReader withMetricRegistry(MetricRegistry registry) {
            wrapped.withMetricRegistry(registry);
            return this;
        }

        @Override
        public SnapshotReader withRowRanges(List<Range> rowRanges) {
            wrapped.withRowRanges(rowRanges);
            return this;
        }

        @Override
        public SnapshotReader withReadType(RowType readType) {
            wrapped.withReadType(readType);
            return this;
        }

        @Override
        public SnapshotReader withLimit(int limit) {
            wrapped.withLimit(limit);
            return this;
        }

        @Override
        public Plan read() {
            return wrapped.read();
        }

        @Override
        public Plan readChanges() {
            return wrapped.readChanges();
        }

        @Override
        public Plan readIncrementalDiff(Snapshot before) {
            return wrapped.readIncrementalDiff(before);
        }

        @Override
        public List<BinaryRow> partitions() {
            return wrapped.partitions();
        }

        @Override
        public List<PartitionEntry> partitionEntries() {
            return wrapped.partitionEntries();
        }

        @Override
        public List<BucketEntry> bucketEntries() {
            return wrapped.bucketEntries();
        }

        @Override
        public Iterator<ManifestEntry> readFileIterator() {
            return wrapped.readFileIterator();
        }
    }

    private class AuditLogBatchScan implements DataTableScan {

        private final DataTableScan batchScan;

        private AuditLogBatchScan(DataTableScan batchScan) {
            this.batchScan = batchScan;
        }

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            convert(predicate).ifPresent(batchScan::withFilter);
            return this;
        }

        @Override
        public InnerTableScan withMetricRegistry(MetricRegistry metricsRegistry) {
            batchScan.withMetricRegistry(metricsRegistry);
            return this;
        }

        @Override
        public InnerTableScan withLimit(int limit) {
            batchScan.withLimit(limit);
            return this;
        }

        @Override
        public InnerTableScan withPartitionFilter(Map<String, String> partitionSpec) {
            batchScan.withPartitionFilter(partitionSpec);
            return this;
        }

        @Override
        public InnerTableScan withPartitionFilter(List<BinaryRow> partitions) {
            batchScan.withPartitionFilter(partitions);
            return this;
        }

        @Override
        public InnerTableScan withPartitionsFilter(List<Map<String, String>> partitions) {
            batchScan.withPartitionsFilter(partitions);
            return this;
        }

        @Override
        public InnerTableScan withPartitionFilter(PartitionPredicate partitionPredicate) {
            batchScan.withPartitionFilter(partitionPredicate);
            return this;
        }

        @Override
        public InnerTableScan withBucketFilter(Filter<Integer> bucketFilter) {
            batchScan.withBucketFilter(bucketFilter);
            return this;
        }

        @Override
        public InnerTableScan withLevelFilter(Filter<Integer> levelFilter) {
            batchScan.withLevelFilter(levelFilter);
            return this;
        }

        @Override
        public Plan plan() {
            return batchScan.plan();
        }

        @Override
        public List<PartitionEntry> listPartitionEntries() {
            return batchScan.listPartitionEntries();
        }

        @Override
        public DataTableScan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
            batchScan.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
            return this;
        }
    }

    private class AuditLogStreamScan implements StreamDataTableScan {

        private final StreamDataTableScan streamScan;

        private AuditLogStreamScan(StreamDataTableScan streamScan) {
            this.streamScan = streamScan;
        }

        @Override
        public StreamDataTableScan withFilter(Predicate predicate) {
            convert(predicate).ifPresent(streamScan::withFilter);
            return this;
        }

        @Override
        public StartingContext startingContext() {
            return streamScan.startingContext();
        }

        @Override
        public Plan plan() {
            return streamScan.plan();
        }

        @Override
        public List<PartitionEntry> listPartitionEntries() {
            return streamScan.listPartitionEntries();
        }

        @Nullable
        @Override
        public Long checkpoint() {
            return streamScan.checkpoint();
        }

        @Nullable
        @Override
        public Long watermark() {
            return streamScan.watermark();
        }

        @Override
        public void restore(@Nullable Long nextSnapshotId) {
            streamScan.restore(nextSnapshotId);
        }

        @Override
        public void restore(@Nullable Long nextSnapshotId, boolean scanAllSnapshot) {
            streamScan.restore(nextSnapshotId, scanAllSnapshot);
        }

        @Override
        public void notifyCheckpointComplete(@Nullable Long nextSnapshot) {
            streamScan.notifyCheckpointComplete(nextSnapshot);
        }

        @Override
        public StreamDataTableScan withMetricRegistry(MetricRegistry metricsRegistry) {
            streamScan.withMetricRegistry(metricsRegistry);
            return this;
        }

        @Override
        public DataTableScan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
            streamScan.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
            return this;
        }
    }

    class AuditLogRead implements InnerTableRead {

        // Special index for rowkind field
        protected static final int ROW_KIND_INDEX = -1;
        // _SEQUENCE_NUMBER is at index 0 by setting: KEY_VALUE_SEQUENCE_NUMBER_ENABLED
        protected static final int SEQUENCE_NUMBER_INDEX = 0;

        protected final InnerTableRead dataRead;

        protected int[] readProjection;

        protected AuditLogRead(InnerTableRead dataRead) {
            this.dataRead = dataRead.forceKeepDelete();
            this.readProjection = defaultProjection();
        }

        /** Default projection, add system fields (rowkind, and optionally _SEQUENCE_NUMBER). */
        private int[] defaultProjection() {
            int dataFieldCount = wrapped.rowType().getFieldCount();
            int[] projection = new int[dataFieldCount + specialFields.size()];
            projection[0] = ROW_KIND_INDEX;
            if (specialFields.contains(SpecialFields.SEQUENCE_NUMBER)) {
                projection[1] = SEQUENCE_NUMBER_INDEX;
            }
            for (int i = 0; i < dataFieldCount; i++) {
                projection[specialFields.size() + i] = i + specialFields.size() - 1;
            }
            return projection;
        }

        /** Build projection array from readType. */
        private int[] buildProjection(RowType readType) {
            List<DataField> fields = readType.getFields();
            int[] projection = new int[fields.size()];
            int dataFieldIndex = 0;

            for (int i = 0; i < fields.size(); i++) {
                String fieldName = fields.get(i).name();
                if (fieldName.equals(SpecialFields.ROW_KIND.name())) {
                    projection[i] = ROW_KIND_INDEX;
                } else if (fieldName.equals(SpecialFields.SEQUENCE_NUMBER.name())) {
                    projection[i] = SEQUENCE_NUMBER_INDEX;
                } else {
                    projection[i] = dataFieldIndex + specialFields.size() - 1;
                    dataFieldIndex++;
                }
            }
            return projection;
        }

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            convert(predicate).ifPresent(dataRead::withFilter);
            return this;
        }

        @Override
        public InnerTableRead withReadType(RowType readType) {
            this.readProjection = buildProjection(readType);
            List<DataField> dataFields = extractDataFields(readType);
            dataRead.withReadType(new RowType(readType.isNullable(), dataFields));
            return this;
        }

        /** Extract data fields (non-system fields) from readType. */
        private List<DataField> extractDataFields(RowType readType) {
            return readType.getFields().stream()
                    .filter(f -> !SpecialFields.isSystemField(f.name()))
                    .collect(Collectors.toList());
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            this.dataRead.withIOManager(ioManager);
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            return dataRead.createReader(split).transform(this::convertRow);
        }

        private InternalRow convertRow(InternalRow data) {
            return new AuditLogRow(readProjection, data);
        }
    }

    /**
     * A {@link ProjectedRow} which returns row kind and sequence number when mapping index is
     * negative.
     */
    static class AuditLogRow extends ProjectedRow {

        AuditLogRow(int[] indexMapping, InternalRow row) {
            super(indexMapping);
            replaceRow(row);
        }

        @Override
        public RowKind getRowKind() {
            return RowKind.INSERT;
        }

        @Override
        public void setRowKind(RowKind kind) {
            throw new UnsupportedOperationException(
                    "Set row kind is not supported in AuditLogRowData.");
        }

        @Override
        public boolean isNullAt(int pos) {
            if (indexMapping[pos] < 0) {
                // row kind and sequence num are always not null
                return false;
            }
            return super.isNullAt(pos);
        }

        @Override
        public BinaryString getString(int pos) {
            int index = indexMapping[pos];
            if (index == AuditLogRead.ROW_KIND_INDEX) {
                return BinaryString.fromString(row.getRowKind().shortString());
            }
            return super.getString(pos);
        }
    }
}
