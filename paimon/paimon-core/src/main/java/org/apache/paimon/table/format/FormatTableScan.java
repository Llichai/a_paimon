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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.csv.CsvOptions;
import org.apache.paimon.format.json.JsonOptions;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.partition.PartitionPredicate.DefaultPartitionPredicate;
import org.apache.paimon.partition.PartitionPredicate.MultiplePartitionPredicate;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.LeafFunction;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.format.predicate.PredicateUtils;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PartitionPathUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.format.text.HadoopCompressionUtils.isCompressed;
import static org.apache.paimon.format.text.TextLineReader.isDefaultDelimiter;
import static org.apache.paimon.utils.InternalRowPartitionComputer.convertSpecToInternalRow;
import static org.apache.paimon.utils.PartitionPathUtils.searchPartSpecAndPaths;

/**
 * FormatTable 的扫描实现。
 *
 * <p>FormatTableScan 为 {@link FormatTable} 提供文件扫描功能，直接扫描外部格式文件目录，不依赖 Paimon 的 Manifest 和 Snapshot。
 *
 * <h3>与 DataTableScan 的区别：</h3>
 * <ul>
 *   <li><b>无 Manifest</b>：直接列出文件系统的文件，不读取 Manifest
 *   <li><b>无 Snapshot</b>：没有快照概念，每次扫描都是最新状态
 *   <li><b>分区裁剪</b>：通过分区路径过滤，提前跳过不满足条件的分区目录（特别是云存储）
 *   <li><b>文件拆分</b>：大文件可以拆分为多个 Split（CSV、JSON 支持范围读取）
 * </ul>
 *
 * <h3>主要功能：</h3>
 * <ul>
 *   <li>扫描分区：根据分区结构列出所有分区目录
 *   <li>分区裁剪：应用分区过滤谓词，跳过不满足条件的分区
 *   <li>文件列表：列出每个分区下的数据文件
 *   <li>文件拆分：将大文件拆分为多个 Split（如果文件格式支持）
 *   <li>等值优化：如果分区过滤包含前导等值条件，直接扫描特定分区路径
 * </ul>
 *
 * <h3>分区扫描优化：</h3>
 * <pre>{@code
 * // 示例：对于分区 year=2023/month=01/day=15
 * // 如果过滤条件为 year=2023 AND month=01
 * // 则直接从 table/year=2023/month=01 开始扫描，避免遍历所有年份和月份
 * }</pre>
 *
 * @see FormatTable
 * @see FormatReadBuilder
 */
public class FormatTableScan implements InnerTableScan {

    /** FormatTable 实例 */
    private final FormatTable table;

    /** 核心配置选项 */
    private final CoreOptions coreOptions;

    /** 分区过滤谓词（可选） */
    @Nullable private PartitionPredicate partitionFilter;

    /** 读取记录数限制（可选，会限制返回的 Split 数量） */
    @Nullable private final Integer limit;

    /** 目标分片大小（用于大文件拆分） */
    private final long targetSplitSize;

    /** 文件格式类型 */
    private final FormatTable.Format format;

    /**
     * 构造 FormatTableScan。
     *
     * @param table FormatTable 实例
     * @param partitionFilter 分区过滤谓词
     * @param limit 读取记录数限制
     */
    public FormatTableScan(
            FormatTable table,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Integer limit) {
        this.table = table;
        this.coreOptions = new CoreOptions(table.options());
        this.partitionFilter = partitionFilter;
        this.limit = limit;
        this.targetSplitSize = coreOptions.splitTargetSize();
        this.format = table.format();
    }

    /**
     * 设置分区过滤谓词。
     *
     * @param partitionPredicate 分区过滤谓词
     * @return 当前扫描器
     */
    @Override
    public InnerTableScan withPartitionFilter(PartitionPredicate partitionPredicate) {
        this.partitionFilter = partitionPredicate;
        return this;
    }

    /**
     * 创建扫描计划。
     *
     * @return 扫描计划
     */
    @Override
    public Plan plan() {
        return new FormatTableScanPlan();
    }

    /**
     * 列出所有分区条目。
     *
     * <p>通过扫描文件系统的分区目录，生成分区条目列表。
     * 注意：FormatTable 没有快照，所以分区条目的统计信息（记录数、文件数等）都是 -1。
     *
     * @return 分区条目列表
     */
    @Override
    public List<PartitionEntry> listPartitionEntries() {
        // 搜索所有分区目录
        List<Pair<LinkedHashMap<String, String>, Path>> partition2Paths =
                searchPartSpecAndPaths(
                        table.fileIO(),
                        new Path(table.location()),
                        table.partitionKeys().size(),
                        table.partitionKeys(),
                        coreOptions.formatTablePartitionOnlyValueInPath());

        // 转换为分区条目
        List<PartitionEntry> partitionEntries = new ArrayList<>();
        for (Pair<LinkedHashMap<String, String>, Path> partition2Path : partition2Paths) {
            BinaryRow row = toPartitionRow(partition2Path.getKey());
            // FormatTable 没有快照，统计信息都是 -1
            partitionEntries.add(new PartitionEntry(row, -1L, -1L, -1L, -1L, -1));
        }
        return partitionEntries;
    }

    /**
     * FormatTable 不支持数据过滤（应使用 ReadBuilder.withFilter）。
     *
     * @throws UnsupportedOperationException 始终抛出异常
     */
    @Override
    public InnerTableScan withFilter(Predicate predicate) {
        throw new UnsupportedOperationException("Filter is not supported for FormatTable.");
    }

    /**
     * 判断文件名是否是数据文件。
     *
     * <p>数据文件的特征：
     * <ul>
     *   <li>不以 . 开头（排除隐藏文件）
     *   <li>不以 _ 开头（排除临时文件和元数据文件，如 _SUCCESS）
     * </ul>
     *
     * @param fileName 文件名
     * @return 是否是数据文件
     */
    public static boolean isDataFileName(String fileName) {
        return fileName != null && !fileName.startsWith(".") && !fileName.startsWith("_");
    }

    /**
     * 将分区规格转换为二进制行。
     *
     * @param partitionSpec 分区规格（分区列名 -> 分区值）
     * @return 二进制行
     */
    private BinaryRow toPartitionRow(LinkedHashMap<String, String> partitionSpec) {
        RowType partitionType = table.partitionType();
        GenericRow row =
                convertSpecToInternalRow(partitionSpec, partitionType, table.defaultPartName());
        return new InternalRowSerializer(partitionType).toBinaryRow(row);
    }

    /**
     * FormatTable 的扫描计划实现。
     *
     * <p>负责生成数据分片列表，执行以下步骤：
     * <ol>
     *   <li>查找所有分区（如果有分区）
     *   <li>应用分区过滤（跳过不满足条件的分区）
     *   <li>列出每个分区下的数据文件
     *   <li>尝试拆分大文件（如果文件格式支持）
     *   <li>应用 limit（限制返回的分片数量）
     * </ol>
     */
    private class FormatTableScanPlan implements Plan {
        @Override
        public List<Split> splits() {
            List<Split> splits = new ArrayList<>();
            try {
                FileIO fileIO = table.fileIO();
                if (!table.partitionKeys().isEmpty()) {
                    // 分区表：遍历分区
                    for (Pair<LinkedHashMap<String, String>, Path> pair : findPartitions()) {
                        LinkedHashMap<String, String> partitionSpec = pair.getKey();
                        BinaryRow partitionRow = toPartitionRow(partitionSpec);
                        // 应用分区过滤
                        if (partitionFilter == null || partitionFilter.test(partitionRow)) {
                            splits.addAll(createSplits(fileIO, pair.getValue(), partitionRow));
                        }
                    }
                } else {
                    // 非分区表：直接扫描表目录
                    splits.addAll(createSplits(fileIO, new Path(table.location()), null));
                }

                // 应用 limit（限制分片数量）
                if (limit != null) {
                    if (limit <= 0) {
                        return new ArrayList<>();
                    }
                    if (splits.size() > limit) {
                        return splits.subList(0, limit);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to scan files", e);
            }
            return splits;
        }
    }

    /**
     * 查找所有分区。
     *
     * <p>根据分区过滤类型采用不同的策略：
     * <ul>
     *   <li><b>MultiplePartitionPredicate</b>：直接生成指定的分区路径（精确匹配）
     *   <li><b>DefaultPartitionPredicate</b>：使用分区谓词优化目录遍历（提前剪枝）
     *   <li><b>无过滤</b>：全量扫描所有分区
     * </ul>
     *
     * <p>对于等值分区过滤（如 year=2023 AND month=01），会计算起始扫描路径，
     * 避免遍历所有年份和月份目录，提高扫描效率（特别是云存储）。
     *
     * @return 分区规格和路径的列表
     */
    List<Pair<LinkedHashMap<String, String>, Path>> findPartitions() {
        boolean onlyValueInPath = coreOptions.formatTablePartitionOnlyValueInPath();

        if (partitionFilter instanceof MultiplePartitionPredicate) {
            // 策略1：直接生成指定的分区路径（适用于 IN 谓词）
            Set<BinaryRow> partitions = ((MultiplePartitionPredicate) partitionFilter).partitions();
            return generatePartitions(
                    table.partitionKeys(),
                    table.partitionType(),
                    table.defaultPartName(),
                    new Path(table.location()),
                    partitions,
                    onlyValueInPath);
        } else {
            // 策略2：使用分区谓词优化目录遍历
            // 这会在遍历过程中提前剪枝不满足条件的分区，
            // 对云存储（OSS/S3）特别重要，可以减少大量的 listStatus 调用
            Map<String, Predicate> partitionPredicates = new HashMap<>();
            if (partitionFilter instanceof DefaultPartitionPredicate) {
                Predicate predicate = ((DefaultPartitionPredicate) partitionFilter).predicate();
                partitionPredicates =
                        PredicateUtils.splitPartitionPredicate(table.partitionType(), predicate);
            }

            // 计算扫描起始路径和层级（等值优化）
            Pair<Path, Integer> scanPathAndLevel =
                    computeScanPathAndLevel(
                            new Path(table.location()),
                            table.partitionKeys(),
                            partitionFilter,
                            table.partitionType(),
                            onlyValueInPath);

            // 从起始路径开始搜索分区
            return searchPartSpecAndPaths(
                    table.fileIO(),
                    scanPathAndLevel.getLeft(),
                    scanPathAndLevel.getRight(),
                    table.partitionKeys(),
                    onlyValueInPath,
                    partitionPredicates,
                    table.partitionType(),
                    table.defaultPartName());
        }
    }

    /**
     * 为指定的分区集合生成分区路径。
     *
     * <p>适用于 MultiplePartitionPredicate（如 IN 谓词），直接根据分区值生成路径，
     * 不需要扫描文件系统。
     *
     * @param partitionKeys 分区列名列表
     * @param partitionType 分区类型
     * @param defaultPartName 默认分区名
     * @param tablePath 表路径
     * @param partitions 分区集合
     * @param onlyValueInPath 是否只有值在路径中（true: 2023/01, false: year=2023/month=01）
     * @return 分区规格和路径的列表
     */
    protected static List<Pair<LinkedHashMap<String, String>, Path>> generatePartitions(
            List<String> partitionKeys,
            RowType partitionType,
            String defaultPartName,
            Path tablePath,
            Set<BinaryRow> partitions,
            boolean onlyValueInPath) {
        InternalRowPartitionComputer partitionComputer =
                new InternalRowPartitionComputer(
                        defaultPartName,
                        partitionType,
                        partitionKeys.toArray(new String[0]),
                        false);
        List<Pair<LinkedHashMap<String, String>, Path>> result = new ArrayList<>();
        for (BinaryRow part : partitions) {
            LinkedHashMap<String, String> partSpec = partitionComputer.generatePartValues(part);

            String path =
                    onlyValueInPath
                            ? PartitionPathUtils.generatePartitionPathUtil(partSpec, true)
                            : PartitionPathUtils.generatePartitionPath(partSpec);
            result.add(Pair.of(partSpec, new Path(tablePath, path)));
        }
        return result;
    }

    /**
     * 计算扫描起始路径和层级。
     *
     * <p>这个方法实现了重要的优化：如果分区过滤包含前导等值条件，
     * 则直接从对应的分区子目录开始扫描，减少目录遍历。
     *
     * <h4>示例：</h4>
     * <pre>{@code
     * 分区结构：year/month/day
     * 过滤条件：year=2023 AND month=01 AND day > 15
     *
     * 优化前：从表根目录扫描，遍历所有年份、月份
     * 优化后：从 table/year=2023/month=01 开始扫描，只遍历天级目录
     *
     * 返回值：(Path("table/year=2023/month=01"), 1)
     * }</pre>
     *
     * @param tableLocation 表位置
     * @param partitionKeys 分区列名列表
     * @param partitionFilter 分区过滤谓词
     * @param partitionType 分区类型
     * @param onlyValueInPath 是否只有值在路径中
     * @return 扫描路径和剩余层级
     */
    protected static Pair<Path, Integer> computeScanPathAndLevel(
            Path tableLocation,
            List<String> partitionKeys,
            PartitionPredicate partitionFilter,
            RowType partitionType,
            boolean onlyValueInPath) {
        Path scanPath = tableLocation;
        int level = partitionKeys.size();

        if (!partitionKeys.isEmpty()) {
            // 尝试优化等值分区过滤
            if (partitionFilter instanceof DefaultPartitionPredicate) {
                Map<String, String> equalityPrefix =
                        extractLeadingEqualityPartitionSpecWhenOnlyAnd(
                                partitionKeys,
                                ((DefaultPartitionPredicate) partitionFilter).predicate());
                if (!equalityPrefix.isEmpty()) {
                    // 使用优化的扫描路径
                    String partitionPath =
                            PartitionPathUtils.generatePartitionPath(
                                    equalityPrefix, partitionType, onlyValueInPath);
                    scanPath = new Path(tableLocation, partitionPath);
                    level = partitionKeys.size() - equalityPrefix.size();
                }
            }
        }
        return Pair.of(scanPath, level);
    }

    /**
     * 为指定路径创建数据分片。
     *
     * <p>列出路径下的所有文件，为每个数据文件创建一个或多个分片：
     * <ul>
     *   <li>小文件：一个文件一个分片
     *   <li>大文件：根据文件格式和大小拆分为多个分片
     * </ul>
     *
     * @param fileIO 文件 I/O
     * @param path 扫描路径
     * @param partition 分区值（null 表示非分区表）
     * @return 数据分片列表
     * @throws IOException 如果列出文件失败
     */
    private List<Split> createSplits(FileIO fileIO, Path path, BinaryRow partition)
            throws IOException {
        List<Split> splits = new ArrayList<>();
        FileStatus[] files = fileIO.listFiles(path, true);
        for (FileStatus file : files) {
            if (isDataFileName(file.getPath().getName())) {
                List<FormatDataSplit> fileSplits = tryToSplitLargeFile(file, partition);
                splits.addAll(fileSplits);
            }
        }
        return splits;
    }

    /**
     * 尝试拆分大文件。
     *
     * <p>根据以下条件判断是否拆分：
     * <ul>
     *   <li>文件大小是否超过 targetSplitSize
     *   <li>文件格式是否支持范围读取（CSV、JSON）
     *   <li>文件是否压缩（压缩文件不可拆分）
     *   <li>行分隔符是否是默认值（自定义分隔符不可拆分）
     * </ul>
     *
     * @param file 文件状态
     * @param partition 分区值
     * @return 数据分片列表（可能包含多个范围分片）
     */
    private List<FormatDataSplit> tryToSplitLargeFile(FileStatus file, BinaryRow partition) {
        if (!preferToSplitFile(file)) {
            // 不拆分：返回单个分片
            return Collections.singletonList(
                    new FormatDataSplit(file.getPath(), file.getLen(), partition));
        }

        // 拆分：按 targetSplitSize 拆分为多个分片
        List<FormatDataSplit> splits = new ArrayList<>();
        long remainingBytes = file.getLen();
        long currentStart = 0;

        while (remainingBytes > 0) {
            long splitSize = Math.min(targetSplitSize, remainingBytes);

            FormatDataSplit split =
                    new FormatDataSplit(
                            file.getPath(), file.getLen(), currentStart, splitSize, partition);
            splits.add(split);
            currentStart += splitSize;
            remainingBytes -= splitSize;
        }
        return splits;
    }

    /**
     * 判断是否应该拆分文件。
     *
     * <p>拆分条件：
     * <ul>
     *   <li>文件大小 > targetSplitSize
     *   <li>文件格式支持范围读取（CSV、JSON）
     *   <li>文件未压缩
     *   <li>使用默认的行分隔符（\n）
     * </ul>
     *
     * @param file 文件状态
     * @return 是否应该拆分
     */
    private boolean preferToSplitFile(FileStatus file) {
        if (file.getLen() <= targetSplitSize) {
            return false;
        }

        Options options = coreOptions.toConfiguration();
        switch (format) {
            case CSV:
                return !isCompressed(file.getPath())
                        && isDefaultDelimiter(options.get(CsvOptions.LINE_DELIMITER));
            case JSON:
                return !isCompressed(file.getPath())
                        && isDefaultDelimiter(options.get(JsonOptions.LINE_DELIMITER));
            default:
                // ORC、Parquet、TEXT 不支持范围读取
                return false;
        }
    }

    /**
     * 从谓词中提取前导等值分区规格（仅限 AND 连接的谓词）。
     *
     * <p>这个方法用于优化分区扫描，提取形如 "a=1 AND b=2 AND c>3" 的谓词中的 {a=1, b=2} 部分。
     *
     * <h4>要求：</h4>
     * <ul>
     *   <li>谓词必须是 AND 连接的
     *   <li>等值条件必须是连续的前导分区列
     *   <li>不能有间隔（如 a=1 AND c=3 无法优化，因为缺少 b）
     * </ul>
     *
     * <h4>示例：</h4>
     * <pre>{@code
     * 分区列: [year, month, day]
     *
     * 谓词: year=2023 AND month=01 AND day>15
     * 返回: {year=2023, month=01}
     *
     * 谓词: year=2023 AND day=01
     * 返回: {year=2023}
     *
     * 谓词: year=2023 AND month>01
     * 返回: {year=2023}
     *
     * 谓词: month=01
     * 返回: {}
     * }</pre>
     *
     * @param partitionKeys 分区列名列表（有序）
     * @param predicate 分区谓词
     * @return 前导等值分区规格
     */
    public static Map<String, String> extractLeadingEqualityPartitionSpecWhenOnlyAnd(
            List<String> partitionKeys, Predicate predicate) {
        // 拆分为 AND 连接的子谓词
        List<Predicate> predicates = PredicateBuilder.splitAnd(predicate);
        Map<String, String> equals = new HashMap<>();

        // 收集所有等值条件
        for (Predicate sub : predicates) {
            if (sub instanceof LeafPredicate) {
                Optional<FieldRef> fieldRefOptional = ((LeafPredicate) sub).fieldRefOptional();
                if (fieldRefOptional.isPresent()) {
                    FieldRef fieldRef = fieldRefOptional.get();
                    LeafFunction function = ((LeafPredicate) sub).function();
                    String field = fieldRef.name();
                    if (function instanceof Equal && partitionKeys.contains(field)) {
                        equals.put(field, ((LeafPredicate) sub).literals().get(0).toString());
                    }
                }
            }
        }

        // 提取前导连续的等值条件
        Map<String, String> result = new HashMap<>(partitionKeys.size());
        for (String partitionKey : partitionKeys) {
            if (equals.containsKey(partitionKey)) {
                result.put(partitionKey, equals.get(partitionKey));
            } else {
                // 遇到第一个非等值条件，停止
                break;
            }
        }
        return result;
    }
}
