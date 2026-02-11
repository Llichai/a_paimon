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

package org.apache.paimon.table.format;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileRecordReader;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.partition.PartitionUtils;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileUtils;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.partition.PartitionPredicate.fromPredicate;
import static org.apache.paimon.partition.PartitionPredicate.splitPartitionPredicatesAndDataPredicates;
import static org.apache.paimon.predicate.PredicateBuilder.excludePredicateWithFields;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * FormatTable 的读取构建器。
 *
 * <p>FormatReadBuilder 为 {@link FormatTable} 提供读取构建功能，用于直接读取外部格式文件（ORC、Parquet、CSV、JSON、TEXT）。
 *
 * <h3>与普通表读取的区别：</h3>
 * <ul>
 *   <li><b>无快照管理</b>：直接读取外部文件，不依赖 Paimon 的 Snapshot 和 Manifest
 *   <li><b>无 Bucket</b>：不支持分桶，数据按文件划分
 *   <li><b>无流读取</b>：仅支持批量读取，不支持流式扫描
 *   <li><b>简单过滤</b>：支持分区过滤和数据过滤，但不支持 TopN、向量搜索等高级功能
 * </ul>
 *
 * <h3>主要功能：</h3>
 * <ul>
 *   <li>构建扫描器 ({@link FormatTableScan})：扫描外部格式文件
 *   <li>构建读取器 ({@link FormatTableRead})：读取外部格式文件
 *   <li>支持谓词下推：将过滤条件推到文件格式层执行
 *   <li>支持列裁剪：只读取需要的列
 *   <li>支持分区裁剪：只扫描满足条件的分区
 * </ul>
 *
 * @see FormatTable
 * @see FormatTableScan
 * @see FormatTableRead
 */
public class FormatReadBuilder implements ReadBuilder {

    private static final long serialVersionUID = 1L;

    /** FormatTable 实例 */
    private final FormatTable table;

    /** 读取的行类型（可能是投影后的类型） */
    private RowType readType;

    /** 核心配置选项 */
    private final CoreOptions options;

    /** 数据过滤谓词（可选） */
    @Nullable private Predicate filter;

    /** 分区过滤谓词（可选） */
    @Nullable private PartitionPredicate partitionFilter;

    /** 读取记录数限制（可选） */
    @Nullable private Integer limit;

    /**
     * 构造 FormatReadBuilder。
     *
     * @param table FormatTable 实例
     */
    public FormatReadBuilder(FormatTable table) {
        this.table = table;
        this.readType = this.table.rowType();
        this.options = new CoreOptions(table.options());
    }

    /**
     * 获取表名。
     *
     * @return 表名
     */
    @Override
    public String tableName() {
        return this.table.name();
    }

    /**
     * 获取读取的行类型。
     *
     * @return 读取的行类型（可能是投影后的类型）
     */
    @Override
    public RowType readType() {
        return this.readType;
    }

    /**
     * 添加数据过滤谓词。
     *
     * <p>如果已存在过滤条件，则将新谓词与现有谓词进行 AND 操作。
     * 对于 FormatTable，过滤谓词会被拆分为：
     * <ul>
     *   <li>分区过滤：用于分区裁剪
     *   <li>数据过滤：推到文件格式层执行
     * </ul>
     *
     * @param predicate 要添加的谓词
     * @return 当前构建器
     */
    @Override
    public ReadBuilder withFilter(Predicate predicate) {
        if (this.filter == null) {
            this.filter = predicate;
        } else {
            this.filter = PredicateBuilder.and(this.filter, predicate);
        }
        return this;
    }

    /**
     * 根据分区规格设置分区过滤。
     *
     * <p>将分区规格（如 {year=2023, month=01}）转换为分区谓词，用于分区裁剪。
     *
     * @param partitionSpec 分区规格（分区列名 -> 分区值）
     * @return 当前构建器
     */
    @Override
    public ReadBuilder withPartitionFilter(Map<String, String> partitionSpec) {
        if (partitionSpec != null) {
            RowType partitionType = table.rowType().project(table.partitionKeys());
            PartitionPredicate partitionPredicate =
                    fromPredicate(
                            partitionType,
                            createPartitionPredicate(
                                    partitionSpec, partitionType, table.defaultPartName()));
            withPartitionFilter(partitionPredicate);
        }
        return this;
    }

    /**
     * 设置分区过滤谓词。
     *
     * <p>分区过滤在扫描阶段生效，用于跳过不满足条件的分区目录。
     * 对于 FormatTable，可以优化分区目录的遍历，减少 I/O 操作（特别是云存储）。
     *
     * @param partitionPredicate 分区过滤谓词
     * @return 当前构建器
     */
    @Override
    public ReadBuilder withPartitionFilter(PartitionPredicate partitionPredicate) {
        this.partitionFilter = partitionPredicate;
        return this;
    }

    /**
     * 设置读取的行类型。
     *
     * <p>通常用于列裁剪，只读取需要的列，减少 I/O 和 CPU 开销。
     *
     * @param readType 读取的行类型
     * @return 当前构建器
     */
    @Override
    public ReadBuilder withReadType(RowType readType) {
        this.readType = readType;
        return this;
    }

    /**
     * 设置列投影。
     *
     * <p>根据列索引数组创建投影后的行类型。
     *
     * @param projection 列索引数组（null 表示读取所有列）
     * @return 当前构建器
     */
    @Override
    public ReadBuilder withProjection(int[] projection) {
        if (projection == null) {
            return this;
        }
        return withReadType(readType().project(projection));
    }

    /**
     * 设置读取记录数限制。
     *
     * <p>限制最多读取的记录数。对于 FormatTable：
     * <ul>
     *   <li>在扫描阶段限制分片数量
     *   <li>在读取阶段限制记录数量
     * </ul>
     *
     * @param limit 最大记录数
     * @return 当前构建器
     */
    @Override
    public ReadBuilder withLimit(int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * 创建表扫描器。
     *
     * <p>如果未显式设置分区过滤，会尝试从数据过滤谓词中提取分区过滤条件，用于优化分区裁剪。
     *
     * @return FormatTableScan 实例
     */
    @Override
    public TableScan newScan() {
        PartitionPredicate partitionFilter = this.partitionFilter;
        // 如果没有显式设置分区过滤，尝试从数据过滤中提取
        if (partitionFilter == null && this.filter != null && !table.partitionKeys().isEmpty()) {
            Optional<PartitionPredicate> partitionPredicateOpt =
                    splitPartitionPredicatesAndDataPredicates(
                                    filter, table.rowType(), table.partitionKeys())
                            .getLeft();
            if (partitionPredicateOpt.isPresent()) {
                partitionFilter = partitionPredicateOpt.get();
            }
        }
        return new FormatTableScan(table, partitionFilter, limit);
    }

    /**
     * 创建表读取器。
     *
     * @return FormatTableRead 实例
     */
    @Override
    public TableRead newRead() {
        return new FormatTableRead(readType(), table.rowType(), this, filter, limit);
    }

    /**
     * 为指定的数据分片创建记录读取器。
     *
     * <p>这个方法实现了 FormatTable 的核心读取逻辑：
     * <ol>
     *   <li>使用 FileFormatDiscover 发现文件格式（ORC、Parquet、CSV、JSON、TEXT）
     *   <li>创建底层文件格式的读取器
     *   <li>过滤掉分区列的谓词（分区列已在扫描时过滤）
     *   <li>将谓词下推到文件格式层
     *   <li>处理分区列的值填充（从分区路径提取值）
     *   <li>支持文件的范围读取（offset + length）
     * </ol>
     *
     * @param dataSplit 数据分片
     * @return 记录读取器
     * @throws IOException 如果读取失败
     */
    protected RecordReader<InternalRow> createReader(FormatDataSplit dataSplit) throws IOException {
        Path filePath = dataSplit.dataPath();
        FormatReaderContext formatReaderContext =
                new FormatReaderContext(table.fileIO(), filePath, dataSplit.fileSize(), null);

        // 跳过分区列的谓词（已在扫描时过滤），只保留数据列的谓词
        List<Predicate> readFilters =
                excludePredicateWithFields(
                        PredicateBuilder.splitAnd(filter), new HashSet<>(table.partitionKeys()));

        // 获取数据行类型（不包含分区列）
        RowType dataRowType = getRowTypeWithoutPartition(table.rowType(), table.partitionKeys());
        RowType readRowType = getRowTypeWithoutPartition(readType(), table.partitionKeys());

        // 创建文件格式读取器工厂
        FormatReaderFactory readerFactory =
                FileFormatDiscover.of(options)
                        .discover(options.formatType())
                        .createReaderFactory(dataRowType, readRowType, readFilters);

        // 计算分区列在读取类型中的位置映射
        Pair<int[], RowType> partitionMapping =
                PartitionUtils.getPartitionMapping(
                        table.partitionKeys(), readType().getFields(), table.partitionType());
        try {
            FileRecordReader<InternalRow> reader;
            Long length = dataSplit.length();
            if (length != null) {
                // 范围读取（用于大文件拆分）
                reader =
                        readerFactory.createReader(formatReaderContext, dataSplit.offset(), length);
            } else {
                // 全文件读取
                checkArgument(dataSplit.offset() == 0, "Offset must be 0.");
                reader = readerFactory.createReader(formatReaderContext);
            }

            // 包装为 DataFileRecordReader，处理分区列的值填充
            return new DataFileRecordReader(
                    readType(),
                    reader,
                    null,
                    null,
                    PartitionUtils.create(partitionMapping, dataSplit.partition()),
                    false,
                    null,
                    0,
                    Collections.emptyMap(),
                    null);
        } catch (Exception e) {
            // 检查文件是否存在，提供更友好的错误信息
            FileUtils.checkExists(formatReaderContext.fileIO(), formatReaderContext.filePath());
            throw e;
        }
    }

    /**
     * 获取不包含分区列的行类型。
     *
     * <p>对于 FormatTable，分区列的值存储在目录路径中，数据文件中不包含分区列。
     *
     * @param rowType 完整的行类型
     * @param partitionKeys 分区列名列表
     * @return 不包含分区列的行类型
     */
    private static RowType getRowTypeWithoutPartition(RowType rowType, List<String> partitionKeys) {
        return rowType.project(
                rowType.getFieldNames().stream()
                        .filter(name -> !partitionKeys.contains(name))
                        .collect(Collectors.toList()));
    }

    // ===================== 不支持的操作 ===============================

    /**
     * FormatTable 不支持 TopN（返回自身，忽略此设置）。
     */
    @Override
    public ReadBuilder withTopN(TopN topN) {
        return this;
    }

    /**
     * FormatTable 不支持删除统计信息（返回自身，忽略此设置）。
     */
    @Override
    public ReadBuilder dropStats() {
        return this;
    }

    /**
     * FormatTable 不支持 Bucket 过滤。
     *
     * @throws UnsupportedOperationException 始终抛出异常
     */
    @Override
    public ReadBuilder withBucketFilter(Filter<Integer> bucketFilter) {
        throw new UnsupportedOperationException("Format Table does not support withBucketFilter.");
    }

    /**
     * FormatTable 不支持 Bucket 选择。
     *
     * @throws UnsupportedOperationException 始终抛出异常
     */
    @Override
    public ReadBuilder withBucket(int bucket) {
        throw new UnsupportedOperationException("Format Table does not support withBucket.");
    }

    /**
     * FormatTable 不支持分片读取。
     *
     * @throws UnsupportedOperationException 始终抛出异常
     */
    @Override
    public ReadBuilder withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        throw new UnsupportedOperationException("Format Table does not support withShard.");
    }

    /**
     * FormatTable 不支持行范围过滤。
     *
     * @throws UnsupportedOperationException 始终抛出异常
     */
    @Override
    public ReadBuilder withRowRanges(List<Range> rowRanges) {
        throw new UnsupportedOperationException("Format Table does not support withRowRanges.");
    }

    /**
     * FormatTable 不支持向量搜索。
     *
     * @throws UnsupportedOperationException 始终抛出异常
     */
    @Override
    public ReadBuilder withVectorSearch(VectorSearch vectorSearch) {
        throw new UnsupportedOperationException("Format Table does not support withRowRanges.");
    }

    /**
     * FormatTable 不支持流式扫描。
     *
     * @throws UnsupportedOperationException 始终抛出异常
     */
    @Override
    public StreamTableScan newStreamScan() {
        throw new UnsupportedOperationException("Format Table does not support stream scan.");
    }
}
