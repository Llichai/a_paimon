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

package org.apache.paimon.crosspartition;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;
import org.apache.paimon.utils.TypeUtils;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.CoreOptions.SCAN_MODE;
import static org.apache.paimon.CoreOptions.StartupMode.LATEST;
import static org.apache.paimon.io.SplitsParallelReadUtil.parallelExecute;

/**
 * 索引引导器
 *
 * <p>从 Paimon 表中引导（初始化）主键索引。
 *
 * <p>核心功能：
 * <ul>
 *   <li>读取表数据：扫描表的所有数据文件
 *   <li>提取主键：提取主键和分区信息
 *   <li>构建索引：为全局索引分配器提供初始数据
 * </ul>
 *
 * <p>工作流程：
 * <pre>
 * 1. 扫描表：
 *    使用 LATEST 模式读取最新快照
 *    过滤：仅读取当前分配器负责的桶
 *
 * 2. 提取数据：
 *    投影：仅读取主键字段（减少数据量）
 *    附加：添加分区和桶信息
 *
 * 3. TTL 过滤：
 *    根据索引 TTL 过滤过期文件
 *    仅加载未过期的数据到索引
 *
 * 4. 并行读取：
 *    支持多线程并行读取文件
 *    提高 bootstrap 性能
 * </pre>
 *
 * <p>输出格式：
 * <pre>
 * Bootstrap 记录 = [主键字段...] + [分区字段...] + [桶号]
 * </pre>
 *
 * <p>性能优化：
 * <ul>
 *   <li>投影下推：仅读取必需的主键字段
 *   <li>桶过滤：每个分配器只读取自己负责的桶
 *   <li>并行读取：多线程并行读取文件
 *   <li>TTL 过滤：跳过过期文件，减少索引大小
 * </ul>
 *
 * @see GlobalIndexAssigner 全局索引分配器
 */
public class IndexBootstrap implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 桶字段名称（添加到 bootstrap 记录的最后） */
    public static final String BUCKET_FIELD = "_BUCKET";

    /** 文件存储表 */
    private final FileStoreTable table;

    /**
     * 构造索引引导器
     *
     * @param table 文件存储表
     */
    public IndexBootstrap(FileStoreTable table) {
        this.table = table;
    }

    /**
     * 引导索引并收集记录
     *
     * <p>读取表数据并将 bootstrap 记录发送给收集器。
     *
     * @param numAssigners 分配器总数
     * @param assignId 当前分配器 ID
     * @param collector 记录收集器
     * @throws IOException 如果读取失败
     */
    public void bootstrap(int numAssigners, int assignId, Consumer<InternalRow> collector)
            throws IOException {
        bootstrap(numAssigners, assignId).forEachRemaining(collector);
    }

    /**
     * 创建 Bootstrap 记录读取器
     *
     * <p>执行流程：
     * <ol>
     *   <li>创建投影：仅读取主键字段（使用 trimmedPrimaryKeys）
     *   <li>扫描表：使用 LATEST 模式读取最新快照
     *   <li>过滤桶：仅读取 bucket % numAssigners == assignId 的桶
     *   <li>过滤层级：读取所有层级（包括 deletion vectors 表）
     *   <li>TTL 过滤：根据索引 TTL 过滤过期文件
     *   <li>附加信息：为每条记录添加分区和桶信息
     *   <li>并行读取：使用多线程并行读取文件
     * </ol>
     *
     * @param numAssigners 分配器总数
     * @param assignId 当前分配器 ID
     * @return Bootstrap 记录读取器
     * @throws IOException 如果创建失败
     */
    public RecordReader<InternalRow> bootstrap(int numAssigners, int assignId) throws IOException {
        RowType rowType = table.rowType();

        List<String> fieldNames = rowType.getFieldNames();
        // 使用 trimmedPrimaryKeys 减少数据大小，因为我们会在末尾添加分区
        int[] keyProjection =
                table.schema().trimmedPrimaryKeys().stream()
                        .map(fieldNames::indexOf)
                        .mapToInt(Integer::intValue)
                        .toArray();

        // 强制使用 LATEST 扫描模式
        ReadBuilder readBuilder =
                table.copy(Collections.singletonMap(SCAN_MODE.key(), LATEST.toString()))
                        .newReadBuilder()
                        .withProjection(keyProjection);

        DataTableScan tableScan = (DataTableScan) readBuilder.newScan();
        List<Split> splits =
                tableScan
                        // 桶过滤：仅读取当前分配器负责的桶
                        .withBucketFilter(bucket -> bucket % numAssigners == assignId)
                        // 层级过滤：deletion vectors 表需要读取所有主键
                        .withLevelFilter(level -> true)
                        .plan()
                        .splits();

        // 根据索引 TTL 过滤过期文件
        CoreOptions options = CoreOptions.fromMap(table.options());
        Duration indexTtl = options.crossPartitionUpsertIndexTtl();
        if (indexTtl != null) {
            long indexTtlMillis = indexTtl.toMillis();
            long currentTime = System.currentTimeMillis();
            splits =
                    splits.stream()
                            .filter(split -> filterSplit(split, indexTtlMillis, currentTime))
                            .collect(Collectors.toList());
        }

        // 创建分区和桶的转换器
        RowDataToObjectArrayConverter partBucketConverter =
                new RowDataToObjectArrayConverter(
                        TypeUtils.concat(
                                TypeUtils.project(rowType, table.partitionKeys()),
                                RowType.of(DataTypes.INT())));

        // 并行读取文件
        return parallelExecute(
                TypeUtils.project(rowType, keyProjection),
                s -> readBuilder.newRead().createReader(s),
                splits,
                options.pageSize(),
                options.crossPartitionUpsertBootstrapParallelism(),
                // 为每个 split 提取分区和桶信息
                split -> {
                    DataSplit dataSplit = ((DataSplit) split);
                    int bucket = dataSplit.bucket();
                    return partBucketConverter.toGenericRow(
                            new JoinedRow(dataSplit.partition(), GenericRow.of(bucket)));
                },
                // 将主键和分区桶信息拼接
                (row, extra) -> new JoinedRow().replace(row, extra));
    }

    /**
     * 根据索引 TTL 过滤 Split
     *
     * <p>如果 Split 中的所有文件都已过期，则过滤掉该 Split。
     *
     * <p>过期判断：currentTime > fileTime + indexTtl
     *
     * @param split 数据 Split
     * @param indexTtl 索引 TTL（毫秒）
     * @param currentTime 当前时间（毫秒）
     * @return true 如果 Split 包含未过期的文件
     */
    @VisibleForTesting
    static boolean filterSplit(Split split, long indexTtl, long currentTime) {
        List<DataFileMeta> files = ((DataSplit) split).dataFiles();
        for (DataFileMeta file : files) {
            long fileTime = file.creationTimeEpochMillis();
            if (currentTime <= fileTime + indexTtl) {
                // 至少有一个文件未过期
                return true;
            }
        }
        // 所有文件都已过期
        return false;
    }

    /**
     * 获取 Bootstrap 记录的类型
     *
     * <p>Bootstrap 记录包含：
     * <ul>
     *   <li>主键字段（trimmed）
     *   <li>分区字段
     *   <li>桶号字段（_BUCKET，INT 类型）
     * </ul>
     *
     * @param schema 表 Schema
     * @return Bootstrap 记录的行类型
     */
    public static RowType bootstrapType(TableSchema schema) {
        List<String> primaryKeys = schema.trimmedPrimaryKeys();
        List<String> partitionKeys = schema.partitionKeys();
        List<DataField> bootstrapFields =
                new ArrayList<>(
                        schema.projectedLogicalRowType(
                                        Stream.concat(primaryKeys.stream(), partitionKeys.stream())
                                                .collect(Collectors.toList()))
                                .getFields());
        // 添加桶字段
        bootstrapFields.add(
                new DataField(
                        RowType.currentHighestFieldId(bootstrapFields) + 1,
                        BUCKET_FIELD,
                        DataTypes.INT().notNull()));
        return new RowType(bootstrapFields);
    }
}
