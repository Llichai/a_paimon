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

package org.apache.paimon.migrate;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryWriter;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.StatsCollectorFactories;
import org.apache.paimon.utils.TypeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * 文件元数据构造工具类。
 *
 * <p>用于为外部文件构造文件元数据，主要功能包括：
 * <ul>
 *   <li>从外部文件系统读取文件并构造 {@link DataFileMeta}
 *   <li>提取文件统计信息（行数、列统计等）
 *   <li>创建提交消息（{@link CommitMessage}）
 *   <li>处理分区值的序列化
 *   <li>文件重命名和路径管理
 * </ul>
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li>数据迁移：从 Hive、Iceberg 等系统迁移数据到 Paimon
 *   <li>外部表注册：将现有文件注册为 Paimon 表
 *   <li>文件元数据重建：重新构造损坏的元数据
 * </ul>
 *
 * <p><b>注意事项：</b>
 * <ul>
 *   <li>文件重命名操作会记录回滚信息，支持失败回滚
 *   <li>自动提取文件统计信息以优化查询性能
 *   <li>支持多种文件格式（通过 {@link FileFormat}）
 * </ul>
 */
public class FileMetaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FileMetaUtils.class);

    /**
     * 为指定位置的外部文件构造文件元数据列表。
     *
     * @param fileIO 文件 IO 操作接口
     * @param format 文件格式（如 "parquet"、"orc"、"avro" 等）
     * @param location 文件所在位置
     * @param paimonTable Paimon 目标表
     * @param filter 文件过滤器，用于筛选需要处理的文件
     * @param dir 目标目录
     * @param rollback 回滚映射，记录文件重命名操作以支持回滚
     * @return 构造的文件元数据列表
     * @throws IOException 如果文件读取失败
     */
    public static List<DataFileMeta> construct(
            FileIO fileIO,
            String format,
            String location,
            Table paimonTable,
            Predicate<FileStatus> filter,
            Path dir,
            Map<Path, Path> rollback)
            throws IOException {
        List<FileStatus> fileStatuses =
                Arrays.stream(fileIO.listStatus(new Path(location)))
                        .filter(s -> !s.isDir())
                        .filter(filter)
                        .collect(Collectors.toList());

        return fileStatuses.stream()
                .map(
                        status ->
                                constructFileMeta(
                                        format, status, fileIO, paimonTable, dir, rollback))
                .collect(Collectors.toList());
    }

    /**
     * 创建提交消息（用于非分桶表）。
     *
     * @param partition 分区键
     * @param totalBuckets 总桶数
     * @param dataFileMetas 数据文件元数据列表
     * @return 提交消息对象
     */
    public static CommitMessage createCommitMessage(
            BinaryRow partition, int totalBuckets, List<DataFileMeta> dataFileMetas) {
        return new CommitMessageImpl(
                partition,
                0,
                totalBuckets,
                new DataIncrement(dataFileMetas, Collections.emptyList(), Collections.emptyList()),
                new CompactIncrement(
                        Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));
    }

    /**
     * 创建提交消息（用于分桶表，包含索引文件）。
     *
     * @param partition 分区键
     * @param bucket 桶编号
     * @param totalBuckets 总桶数
     * @param dataFileMetas 数据文件元数据列表
     * @param indexFileMetas 索引文件元数据列表
     * @return 提交消息对象
     */
    public static CommitMessage createCommitMessage(
            BinaryRow partition,
            int bucket,
            int totalBuckets,
            List<DataFileMeta> dataFileMetas,
            List<IndexFileMeta> indexFileMetas) {
        return new CommitMessageImpl(
                partition,
                bucket,
                totalBuckets,
                new DataIncrement(
                        dataFileMetas,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        indexFileMetas,
                        Collections.emptyList()),
                new CompactIncrement(
                        Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));
    }

    public static DataFileMeta constructFileMeta(
            String format,
            FileStatus fileStatus,
            FileIO fileIO,
            Table table,
            Path dir,
            Map<Path, Path> rollback) {

        try {
            SimpleStatsExtractor simpleStatsExtractor = createSimpleStatsExtractor(table, format);
            Path newPath = renameFile(fileIO, fileStatus.getPath(), dir, format, rollback);
            return constructFileMeta(
                    newPath.getName(),
                    fileStatus.getLen(),
                    newPath,
                    simpleStatsExtractor,
                    fileIO,
                    table);
        } catch (IOException e) {
            throw new RuntimeException("error when construct file meta", e);
        }
    }

    public static DataFileMeta constructFileMeta(
            String format,
            FileStatus fileStatus,
            FileIO fileIO,
            Table table,
            Path dir,
            Map<Path, Path> rollback,
            long schemaId) {

        try {
            RowType rowTypeWithSchemaId =
                    ((FileStoreTable) table).schemaManager().schema(schemaId).logicalRowType();
            CoreOptions options = ((FileStoreTable) table).coreOptions();
            SimpleColStatsCollector.Factory[] factories =
                    StatsCollectorFactories.createStatsFactories(
                            options.statsMode(), options, rowTypeWithSchemaId.getFieldNames());

            SimpleStatsExtractor simpleStatsExtractor =
                    FileFormat.fromIdentifier(format, options.toConfiguration())
                            .createStatsExtractor(rowTypeWithSchemaId, factories)
                            .orElseThrow(
                                    () ->
                                            new RuntimeException(
                                                    "Can't get table stats extractor for format "
                                                            + format));
            Path newPath = renameFile(fileIO, fileStatus.getPath(), dir, format, rollback);
            return constructFileMeta(
                    newPath.getName(),
                    fileStatus.getLen(),
                    newPath,
                    simpleStatsExtractor,
                    fileIO,
                    table,
                    schemaId);
        } catch (IOException e) {
            throw new RuntimeException("error when construct file meta", e);
        }
    }

    private static Path renameFile(
            FileIO fileIO, Path originPath, Path newDir, String format, Map<Path, Path> rollback)
            throws IOException {
        String suffix = "." + format;
        String fileName = originPath.getName();
        String newFileName = fileName.endsWith(suffix) ? fileName : fileName + "." + format;
        Path newPath = new Path(newDir, newFileName);
        rollback.put(newPath, originPath);
        LOG.info("Migration: rename file from " + originPath + " to " + newPath);
        fileIO.rename(originPath, newPath);
        return newPath;
    }

    public static DataFileMeta constructFileMeta(
            String fileName,
            long fileSize,
            Path path,
            SimpleStatsExtractor simpleStatsExtractor,
            FileIO fileIO,
            Table table)
            throws IOException {
        return constructFileMeta(
                fileName,
                fileSize,
                path,
                simpleStatsExtractor,
                fileIO,
                table,
                ((FileStoreTable) table).schema().id());
    }

    private static DataFileMeta constructFileMeta(
            String fileName,
            long fileSize,
            Path path,
            SimpleStatsExtractor simpleStatsExtractor,
            FileIO fileIO,
            Table table,
            long schemaId)
            throws IOException {
        RowType rowTypeWithSchemaId =
                ((FileStoreTable) table).schemaManager().schema(schemaId).logicalRowType();

        SimpleStatsConverter statsArraySerializer = new SimpleStatsConverter(rowTypeWithSchemaId);

        Pair<SimpleColStats[], SimpleStatsExtractor.FileInfo> fileInfo =
                simpleStatsExtractor.extractWithFileInfo(fileIO, path, fileSize);
        SimpleStats stats = statsArraySerializer.toBinaryAllMode(fileInfo.getLeft());

        return DataFileMeta.forAppend(
                fileName,
                fileSize,
                fileInfo.getRight().getRowCount(),
                stats,
                0,
                0,
                schemaId,
                Collections.emptyList(),
                null,
                FileSource.APPEND,
                null,
                null,
                null,
                null);
    }

    public static BinaryRow writePartitionValue(
            RowType partitionRowType,
            List<String> partitionValues,
            List<BinaryWriter.ValueSetter> valueSetters,
            String partitionDefaultName) {

        BinaryRow binaryRow = new BinaryRow(partitionRowType.getFieldCount());
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(binaryRow);

        List<DataField> fields = partitionRowType.getFields();

        for (int i = 0; i < fields.size(); i++) {
            String partitionName = partitionValues.get(i);
            if (partitionName.equals(partitionDefaultName)) {
                binaryRowWriter.setNullAt(i);
            } else {
                Object value = TypeUtils.castFromString(partitionName, fields.get(i).type());
                valueSetters.get(i).setValue(binaryRowWriter, i, value);
            }
        }
        binaryRowWriter.complete();
        return binaryRow;
    }

    public static BinaryRow writePartitionValue(
            RowType partitionRowType,
            List<Object> partitionValues,
            List<BinaryWriter.ValueSetter> valueSetters) {
        BinaryRow binaryRow = new BinaryRow(partitionRowType.getFieldCount());
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(binaryRow);

        List<DataField> fields = partitionRowType.getFields();

        for (int i = 0; i < fields.size(); i++) {
            Object value = partitionValues.get(i);
            if (value == null) {
                binaryRowWriter.setNullAt(i);
            } else {
                valueSetters.get(i).setValue(binaryRowWriter, i, value);
            }
        }
        binaryRowWriter.complete();
        return binaryRow;
    }

    public static SimpleStatsExtractor createSimpleStatsExtractor(Table table, String format) {
        CoreOptions options = ((FileStoreTable) table).coreOptions();
        SimpleColStatsCollector.Factory[] factories =
                StatsCollectorFactories.createStatsFactories(
                        options.statsMode(), options, table.rowType().getFieldNames());

        return FileFormat.fromIdentifier(format, options.toConfiguration())
                .createStatsExtractor(table.rowType(), factories)
                .orElseThrow(
                        () ->
                                new RuntimeException(
                                        "Can't get table stats extractor for format " + format));
    }
}
