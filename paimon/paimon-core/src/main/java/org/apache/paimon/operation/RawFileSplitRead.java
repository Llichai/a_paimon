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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.ApplyDeletionVectorReader;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.bitmap.ApplyBitmapIndexRecordReader;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatKey;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataFileRecordReader;
import org.apache.paimon.io.FileIndexEvaluator;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.partition.PartitionUtils;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.reader.EmptyFileRecordReader;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.FormatReaderMapping;
import org.apache.paimon.utils.FormatReaderMapping.Builder;
import org.apache.paimon.utils.IOExceptionSupplier;
import org.apache.paimon.utils.RoaringBitmap32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;
import static org.apache.paimon.table.SpecialFields.rowTypeWithRowTracking;

/**
 * 原始文件分片读取 - 直接从 DataSplit 读取原始文件，无需合并
 *
 * <p><b>适用场景</b>：
 * <ul>
 *   <li>Append-Only 表：数据只追加，不需要合并
 *   <li>批量读取：一次性读取所有历史数据，不考虑实时性
 *   <li>无删除操作：不需要处理 DELETE 记录
 * </ul>
 *
 * <p><b>与 MergeFileSplitRead 的区别</b>：
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>RawFileSplitRead</th>
 *     <th>MergeFileSplitRead</th>
 *   </tr>
 *   <tr>
 *     <td>是否合并</td>
 *     <td>否</td>
 *     <td>是（LSM 合并）</td>
 *   </tr>
 *   <tr>
 *     <td>性能</td>
 *     <td>高（无合并开销）</td>
 *     <td>中（需要排序合并）</td>
 *   </tr>
 *   <tr>
 *     <td>适用表类型</td>
 *     <td>Append-Only 表</td>
 *     <td>主键表（KeyValue表）</td>
 *   </tr>
 *   <tr>
 *     <td>DELETE 处理</td>
 *     <td>不处理（Append-Only 无删除）</td>
 *     <td>处理 DELETE 记录</td>
 *   </tr>
 *   <tr>
 *     <td>过滤器下推</td>
 *     <td>全部下推</td>
 *     <td>部分下推（重叠文件只下推主键过滤器）</td>
 *   </tr>
 * </table>
 *
 * <p><b>读取流程</b>：
 * <ol>
 *   <li>获取 Split 中的文件列表（DataFileMeta）
 *   <li>为每个文件创建 FileRecordReader
 *   <li>使用 ConcatRecordReader 串联所有文件的 Reader
 *   <li>应用过滤器、投影、TopN/Limit 优化
 * </ol>
 *
 * <p><b>Deletion Vector 支持</b>：
 * <ul>
 *   <li>Deletion Vector 用于逻辑删除（标记某些行已删除，但不物理删除）
 *   <li>读取时应用 DV 过滤已删除的行
 *   <li>实现：ApplyDeletionVectorReader 包装 FileRecordReader
 * </ul>
 *
 * <p><b>文件索引（File Index）优化</b>：
 * <ul>
 *   <li>配置：{@code file-index.read.enabled = true}
 *   <li>支持的索引类型：
 *       <ul>
 *         <li>Bloom Filter：快速判断 key 是否存在
 *         <li>Bitmap Index：按位图过滤数据
 *         <li>Min-Max Index：范围过滤
 *       </ul>
 *   </li>
 *   <li>工作流程：
 *       <pre>
 *       FileIndexEvaluator.evaluate(file, filters):
 *       1. 读取文件的索引（bloom/bitmap/min-max）
 *       2. 应用过滤条件
 *       3. 返回结果：
 *          - SKIP: 文件不包含任何匹配数据 -> EmptyFileRecordReader
 *          - BITMAP: 返回匹配的行号位图 -> ApplyBitmapIndexRecordReader
 *          - ALL: 文件可能包含数据 -> DataFileRecordReader（正常读取）
 *       </pre>
 *   </li>
 * </ul>
 *
 * <p><b>Schema Evolution 支持</b>：
 * <ul>
 *   <li>问题：文件写入时的 schema 可能与当前读取的 schema 不同
 *   <li>解决：
 *       <ul>
 *         <li>FormatReaderMapping：建立文件 schema 和读取 schema 的映射
 *         <li>自动类型转换（如 INT -> LONG）
 *         <li>缺失列填充默认值（NULL）
 *       </ul>
 *   </li>
 *   <li>实现：
 *       <pre>
 *       formatReaderMappings.computeIfAbsent(
 *           new FormatKey(schemaId, formatIdentifier),
 *           key -> buildMapping(fileSchema, readSchema)
 *       )
 *       </pre>
 *   </li>
 * </ul>
 *
 * <p><b>Row Tracking 支持</b>：
 * <ul>
 *   <li>为每行数据分配唯一的 Row ID（用于 UPDATE/DELETE 操作）
 *   <li>配置：{@code row-tracking.enabled = true}
 *   <li>实现：
 *       <ul>
 *         <li>文件中记录 firstRowId 和 maxSequenceNumber
 *         <li>读取时计算每行的 Row ID：firstRowId + rowIndex
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p><b>过滤器和投影下推</b>：
 * <ul>
 *   <li>过滤器：通过 FormatReaderMapping 传递给文件格式（ORC/Parquet）
 *   <li>投影：只读取需要的列，减少 IO
 *   <li>TopN/Limit：提前停止读取，减少数据量
 * </ul>
 *
 * <p><b>IncrementalSplit 支持</b>：
 * <ul>
 *   <li>用于增量读取（CDC 场景）
 *   <li>包含 beforeFiles 和 afterFiles
 *   <li>只读取 afterFiles（beforeFiles 被忽略）
 * </ul>
 *
 * <p><b>使用示例</b>：
 * <pre>
 * RawFileSplitRead read = new RawFileSplitRead(...);
 * read.withReadType(projectedType)     // 投影
 *     .withFilter(predicate)           // 过滤器
 *     .withLimit(1000);                // Limit
 * RecordReader&lt;InternalRow&gt; reader = read.createReader(split);
 * while (reader.readNext() != null) {
 *     // 处理记录
 * }
 * </pre>
 *
 * @see MergeFileSplitRead 主键表的合并读取
 * @see org.apache.paimon.io.DataFileRecordReader 数据文件读取器
 */
public class RawFileSplitRead implements SplitRead<InternalRow> {

    private static final Logger LOG = LoggerFactory.getLogger(RawFileSplitRead.class);

    private final FileIO fileIO;
    private final SchemaManager schemaManager;
    private final TableSchema schema;
    private final FileFormatDiscover formatDiscover;
    private final FileStorePathFactory pathFactory;
    private final Map<FormatKey, FormatReaderMapping> formatReaderMappings;
    private final boolean fileIndexReadEnabled;
    private final boolean rowTrackingEnabled;

    private RowType readRowType;
    @Nullable private List<Predicate> filters;
    @Nullable private TopN topN;
    @Nullable private Integer limit;

    public RawFileSplitRead(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            RowType rowType,
            FileFormatDiscover formatDiscover,
            FileStorePathFactory pathFactory,
            boolean fileIndexReadEnabled,
            boolean rowTrackingEnabled) {
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.schema = schema;
        this.formatDiscover = formatDiscover;
        this.pathFactory = pathFactory;
        this.formatReaderMappings = new HashMap<>();
        this.fileIndexReadEnabled = fileIndexReadEnabled;
        this.rowTrackingEnabled = rowTrackingEnabled;
        this.readRowType = rowType;
    }

    @Override
    public SplitRead<InternalRow> forceKeepDelete() {
        return this;
    }

    @Override
    public SplitRead<InternalRow> withIOManager(@Nullable IOManager ioManager) {
        return this;
    }

    @Override
    public SplitRead<InternalRow> withReadType(RowType readRowType) {
        this.readRowType = readRowType;
        return this;
    }

    @Override
    public RawFileSplitRead withFilter(Predicate predicate) {
        if (predicate != null) {
            this.filters = splitAnd(predicate);
        }
        return this;
    }

    @Override
    public SplitRead<InternalRow> withTopN(@Nullable TopN topN) {
        this.topN = topN;
        return this;
    }

    @Override
    public SplitRead<InternalRow> withLimit(@Nullable Integer limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public RecordReader<InternalRow> createReader(Split s) throws IOException {
        if (s instanceof DataSplit) {
            DataSplit split = (DataSplit) s;
            return createReader(
                    split.partition(),
                    split.bucket(),
                    split.dataFiles(),
                    split.deletionFiles().orElse(null));
        } else {
            IncrementalSplit split = (IncrementalSplit) s;
            if (!split.beforeFiles().isEmpty()) {
                LOG.info("Ignore split before files: {}", split.beforeFiles());
            }
            return createReader(
                    split.partition(),
                    split.bucket(),
                    split.afterFiles(),
                    split.afterDeletionFiles());
        }
    }

    public RecordReader<InternalRow> createReader(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            List<DeletionFile> deletionFiles)
            throws IOException {
        DeletionVector.Factory dvFactory = DeletionVector.factory(fileIO, files, deletionFiles);
        Map<String, IOExceptionSupplier<DeletionVector>> dvFactories = new HashMap<>();
        for (DataFileMeta file : files) {
            dvFactories.put(file.fileName(), () -> dvFactory.create(file.fileName()).orElse(null));
        }
        return createReader(partition, bucket, files, dvFactories);
    }

    public RecordReader<InternalRow> createReader(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            @Nullable Map<String, IOExceptionSupplier<DeletionVector>> dvFactories)
            throws IOException {
        DataFilePathFactory dataFilePathFactory =
                pathFactory.createDataFilePathFactory(partition, bucket);
        List<ReaderSupplier<InternalRow>> suppliers = new ArrayList<>();

        Builder formatReaderMappingBuilder =
                new Builder(
                        formatDiscover,
                        readRowType.getFields(),
                        schema -> {
                            if (rowTrackingEnabled) {
                                // maybe file has no row id and sequence number, but in manifest
                                // entry
                                return rowTypeWithRowTracking(schema.logicalRowType(), true, true)
                                        .getFields();
                            }
                            return schema.fields();
                        },
                        filters,
                        topN,
                        limit);

        for (DataFileMeta file : files) {
            suppliers.add(
                    createFileReader(
                            partition,
                            dataFilePathFactory,
                            file,
                            formatReaderMappingBuilder,
                            dvFactories));
        }

        return ConcatRecordReader.create(suppliers);
    }

    private ReaderSupplier<InternalRow> createFileReader(
            BinaryRow partition,
            DataFilePathFactory dataFilePathFactory,
            DataFileMeta file,
            Builder formatBuilder,
            @Nullable Map<String, IOExceptionSupplier<DeletionVector>> dvFactories) {
        String formatIdentifier = DataFilePathFactory.formatIdentifier(file.fileName());
        long schemaId = file.schemaId();

        FormatReaderMapping formatReaderMapping =
                formatReaderMappings.computeIfAbsent(
                        new FormatKey(file.schemaId(), formatIdentifier),
                        key ->
                                formatBuilder.build(
                                        formatIdentifier,
                                        schema,
                                        schemaId == schema.id()
                                                ? schema
                                                : schemaManager.schema(schemaId)));

        IOExceptionSupplier<DeletionVector> dvFactory =
                dvFactories == null ? null : dvFactories.get(file.fileName());
        return () ->
                createFileReader(
                        partition, file, dataFilePathFactory, formatReaderMapping, dvFactory);
    }

    private FileRecordReader<InternalRow> createFileReader(
            BinaryRow partition,
            DataFileMeta file,
            DataFilePathFactory dataFilePathFactory,
            FormatReaderMapping formatReaderMapping,
            IOExceptionSupplier<DeletionVector> dvFactory)
            throws IOException {
        FileIndexResult fileIndexResult = null;
        DeletionVector deletionVector = dvFactory == null ? null : dvFactory.get();
        if (fileIndexReadEnabled) {
            fileIndexResult =
                    FileIndexEvaluator.evaluate(
                            fileIO,
                            formatReaderMapping.getDataSchema(),
                            formatReaderMapping.getDataFilters(),
                            formatReaderMapping.getTopN(),
                            formatReaderMapping.getLimit(),
                            dataFilePathFactory,
                            file,
                            deletionVector);
            if (!fileIndexResult.remain()) {
                return new EmptyFileRecordReader<>();
            }
        }

        RoaringBitmap32 selection = null;
        if (fileIndexResult instanceof BitmapIndexResult) {
            selection = ((BitmapIndexResult) fileIndexResult).get();
        }

        FormatReaderContext formatReaderContext =
                new FormatReaderContext(
                        fileIO, dataFilePathFactory.toPath(file), file.fileSize(), selection);
        FileRecordReader<InternalRow> fileRecordReader =
                new DataFileRecordReader(
                        schema.logicalRowType(),
                        formatReaderMapping.getReaderFactory(),
                        formatReaderContext,
                        formatReaderMapping.getIndexMapping(),
                        formatReaderMapping.getCastMapping(),
                        PartitionUtils.create(formatReaderMapping.getPartitionPair(), partition),
                        rowTrackingEnabled,
                        file.firstRowId(),
                        file.maxSequenceNumber(),
                        formatReaderMapping.getSystemFields());

        if (fileIndexResult instanceof BitmapIndexResult) {
            fileRecordReader =
                    new ApplyBitmapIndexRecordReader(
                            fileRecordReader, (BitmapIndexResult) fileIndexResult);
        }

        if (deletionVector != null && !deletionVector.isEmpty()) {
            return new ApplyDeletionVectorReader(fileRecordReader, deletionVector);
        }
        return fileRecordReader;
    }
}
