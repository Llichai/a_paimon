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

package org.apache.paimon.table.source;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMeta08Serializer;
import org.apache.paimon.io.DataFileMeta09Serializer;
import org.apache.paimon.io.DataFileMeta10LegacySerializer;
import org.apache.paimon.io.DataFileMeta12LegacySerializer;
import org.apache.paimon.io.DataFileMetaFirstRowIdLegacySerializer;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.predicate.CompareUtils;
import org.apache.paimon.stats.SimpleStatsEvolution;
import org.apache.paimon.stats.SimpleStatsEvolutions;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.FunctionWithIOException;
import org.apache.paimon.utils.InternalRowUtils;
import org.apache.paimon.utils.RangeHelper;
import org.apache.paimon.utils.SerializationUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.paimon.io.DataFilePathFactory.INDEX_PATH_SUFFIX;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 标准数据分片实现，包含分区、桶、数据文件列表等完整信息。
 *
 * <p>DataSplit 是最常用的 {@link Split} 实现，用于表示一个桶（bucket）的部分或全部数据文件。
 * 它包含了读取数据所需的所有信息：分区键、桶号、数据文件列表、删除向量等。
 *
 * <h3>Split 的结构</h3>
 * <ul>
 *   <li><b>分区 (partition)</b>: 该 Split 所属的分区键</li>
 *   <li><b>桶 (bucket)</b>: 该 Split 所属的桶号</li>
 *   <li><b>数据文件 (dataFiles)</b>: 要读取的数据文件列表（{@link DataFileMeta}）</li>
 *   <li><b>删除文件 (dataDeletionFiles)</b>: 对应的删除向量文件列表（可选）</li>
 *   <li><b>快照 (snapshotId)</b>: 该 Split 来自哪个快照</li>
 * </ul>
 *
 * <h3>与 ManifestEntry 的关系</h3>
 * <ul>
 *   <li><b>ManifestEntry</b>: FileStore 层的扫描输出，包含单个数据文件的信息</li>
 *   <li><b>DataSplit</b>: Table 层的扫描输出，包含多个数据文件（同一桶的文件打包成一个 Split）</li>
 * </ul>
 *
 * <h3>rawConvertible 标志</h3>
 * <p>该标志表示 Split 中的文件是否可以不经合并直接读取：
 * <ul>
 *   <li><b>true</b>: 文件可以直接读取（追加表，或主键表的单文件 Split）</li>
 *   <li><b>false</b>: 文件需要合并读取（主键表的多文件 Split）</li>
 * </ul>
 *
 * <h3>行数统计</h3>
 * <p>DataSplit 提供三种行数统计方法：
 * <ul>
 *   <li><b>{@link #rowCount()}</b>: 所有文件的原始行数总和（可能包含重复）</li>
 *   <li><b>{@link #mergedRowCount()}</b>: 合并后的实际行数（考虑删除向量和数据演化）</li>
 *   <li><b>{@link #rawMergedRowCount()}</b>: 使用删除向量计算的合并行数</li>
 *   <li><b>{@link #dataEvolutionMergedRowCount()}</b>: 使用 firstRowId 计算的合并行数</li>
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 构建 DataSplit
 * DataSplit split = DataSplit.builder()
 *     .withSnapshot(10)
 *     .withPartition(partition)
 *     .withBucket(5)
 *     .withBucketPath("bucket-5")
 *     .withDataFiles(dataFiles)
 *     .withDataDeletionFiles(deletionFiles)  // 可选
 *     .rawConvertible(true)
 *     .build();
 *
 * // 读取 Split
 * RecordReader<InternalRow> reader = tableRead.createReader(split);
 * }</pre>
 *
 * <h3>序列化</h3>
 * <p>DataSplit 实现了 Java 序列化，支持在网络间传输。序列化格式包含版本号，
 * 支持向后兼容（可以反序列化旧版本的 Split）。
 *
 * @see Split 分片接口
 * @see TableScan 生成 Split 的扫描接口
 * @see TableRead 读取 Split 的接口
 * @see DataFileMeta 数据文件元数据
 * @see DeletionFile 删除向量文件
 */
public class DataSplit implements Split {

    private static final long serialVersionUID = 7L;
    private static final long MAGIC = -2394839472490812314L;
    private static final int VERSION = 8;

    private long snapshotId = 0;
    private BinaryRow partition;
    private int bucket = -1;
    private String bucketPath;
    @Nullable private Integer totalBuckets;

    private List<DataFileMeta> dataFiles;
    @Nullable private List<DeletionFile> dataDeletionFiles;

    private boolean isStreaming = false;
    private boolean rawConvertible;

    /** 无参构造函数（用于序列化）。 */
    public DataSplit() {}

    /** 获取快照 ID（该 Split 来自哪个快照）。 */
    public long snapshotId() {
        return snapshotId;
    }

    /** 获取分区键（二进制格式）。 */
    public BinaryRow partition() {
        return partition;
    }

    /** 获取桶号。 */
    public int bucket() {
        return bucket;
    }

    /** 获取桶路径（完整的文件系统路径）。 */
    public String bucketPath() {
        return bucketPath;
    }

    /**
     * 获取总桶数（仅延迟分桶表）。
     *
     * <p>对于延迟分桶表（postpone bucket table），总桶数可能会动态变化。
     * 对于固定桶数的表，此字段为 null。
     */
    public @Nullable Integer totalBuckets() {
        return totalBuckets;
    }

    /** 获取数据文件列表。 */
    public List<DataFileMeta> dataFiles() {
        return dataFiles;
    }

    /**
     * 获取删除文件列表（删除向量）。
     *
     * <p>与数据文件列表一一对应，如果某个数据文件没有删除，对应位置为 null。
     *
     * @return 删除文件列表，如果不使用删除向量则返回 empty
     */
    @Override
    public Optional<List<DeletionFile>> deletionFiles() {
        return Optional.ofNullable(dataDeletionFiles);
    }

    /**
     * 是否是流式 Split。
     *
     * <p>流式 Split 用于持续读取场景，可能包含一些特殊的处理逻辑。
     */
    public boolean isStreaming() {
        return isStreaming;
    }

    /**
     * 是否可以转换为原始文件（不需要合并）。
     *
     * <p>如果为 true，Split 中的文件可以直接读取，不需要合并。
     */
    public boolean rawConvertible() {
        return rawConvertible;
    }

    /**
     * 获取最早的文件创建时间（毫秒时间戳）。
     *
     * <p>返回 Split 中所有数据文件的最早创建时间，用于：
     * <ul>
     *   <li>时间旅行查询</li>
     *   <li>数据保留策略</li>
     * </ul>
     *
     * @return 最早的文件创建时间，如果文件列表为空则返回 empty
     */
    public OptionalLong earliestFileCreationEpochMillis() {
        return this.dataFiles.stream().mapToLong(DataFileMeta::creationTimeEpochMillis).min();
    }

    /**
     * 返回所有数据文件的原始行数总和（可能包含重复）。
     *
     * <p>对于主键表，多个文件可能包含同一主键的不同版本，
     * 因此原始行数大于实际行数。
     *
     * @return 原始行数总和
     */
    @Override
    public long rowCount() {
        long rowCount = 0;
        for (DataFileMeta file : dataFiles) {
            rowCount += file.rowCount();
        }
        return rowCount;
    }

    /**
     * 返回合并后的实际行数（考虑删除向量和数据演化）。
     *
     * <p>该方法会尝试两种计算方式：
     * <ol>
     *   <li>如果 rawConvertible = true 且有删除向量统计，使用 {@link #rawMergedRowCount()}</li>
     *   <li>如果启用了数据演化（有 firstRowId），使用 {@link #dataEvolutionMergedRowCount()}</li>
     * </ol>
     *
     * <p>如果两种方式都不可用，返回 empty。
     *
     * @return 合并后的实际行数，如果无法计算则返回 empty
     */
    @Override
    public OptionalLong mergedRowCount() {
        if (rawMergedRowCountAvailable()) {
            return OptionalLong.of(rawMergedRowCount());
        }
        if (dataEvolutionRowCountAvailable()) {
            return OptionalLong.of(dataEvolutionMergedRowCount());
        }
        return OptionalLong.empty();
    }

    /**
     * 检查是否可以使用删除向量计算合并行数。
     *
     * <p>需要满足以下条件：
     * <ul>
     *   <li>rawConvertible = true（文件可以直接读取）</li>
     *   <li>所有删除向量都有 cardinality 统计（已删除行数）</li>
     * </ul>
     */
    private boolean rawMergedRowCountAvailable() {
        return rawConvertible
                && (dataDeletionFiles == null
                        || dataDeletionFiles.stream()
                                .allMatch(f -> f == null || f.cardinality() != null));
    }

    /**
     * 使用删除向量计算合并行数。
     *
     * <p>对于每个数据文件：
     * <ul>
     *   <li>如果没有删除向量，行数 = 文件行数</li>
     *   <li>如果有删除向量，行数 = 文件行数 - 已删除行数</li>
     * </ul>
     *
     * @return 合并后的行数
     */
    private long rawMergedRowCount() {
        long sum = 0L;
        for (int i = 0; i < dataFiles.size(); i++) {
            DataFileMeta file = dataFiles.get(i);
            DeletionFile deletionFile = dataDeletionFiles == null ? null : dataDeletionFiles.get(i);
            Long cardinality = deletionFile == null ? null : deletionFile.cardinality();
            if (deletionFile == null) {
                sum += file.rowCount();
            } else if (cardinality != null) {
                sum += file.rowCount() - cardinality;
            }
        }
        return sum;
    }

    /**
     * 检查是否可以使用数据演化计算合并行数。
     *
     * <p>需要所有数据文件都有 firstRowId（数据演化模式）。
     */
    private boolean dataEvolutionRowCountAvailable() {
        for (DataFileMeta file : dataFiles) {
            if (file.firstRowId() == null) {
                return false;
            }
        }
        return true;
    }

    /**
     * 使用数据演化（firstRowId）计算合并行数。
     *
     * <p>数据演化模式下，文件可能存在行重叠（overlap）。使用 firstRowId 和 rowCount
     * 计算行范围，将重叠的文件合并，取最大的行数。
     *
     * <h3>示例</h3>
     * <pre>
     * 文件 A: firstRowId=0, rowCount=100  -> [0, 99]
     * 文件 B: firstRowId=50, rowCount=80  -> [50, 129]
     * 文件 C: firstRowId=150, rowCount=50 -> [150, 199]
     *
     * A 和 B 重叠，取 max(100, 80) = 100
     * C 不重叠，取 50
     * 总行数 = 100 + 50 = 150
     * </pre>
     *
     * @return 合并后的行数
     */
    private long dataEvolutionMergedRowCount() {
        long sum = 0L;
        RangeHelper<DataFileMeta> rangeHelper =
                new RangeHelper<>(
                        DataFileMeta::nonNullFirstRowId,
                        f -> f.nonNullFirstRowId() + f.rowCount() - 1);
        List<List<DataFileMeta>> ranges = rangeHelper.mergeOverlappingRanges(dataFiles);
        for (List<DataFileMeta> group : ranges) {
            long maxCount = 0;
            for (DataFileMeta file : group) {
                maxCount = Math.max(maxCount, file.rowCount());
            }
            sum += maxCount;
        }
        return sum;
    }

    /**
     * 获取指定字段的最小值（跨所有数据文件）。
     *
     * <p>该方法会遍历所有数据文件的统计信息，找到最小值。
     * 用于优化查询（如范围过滤）。
     *
     * @param fieldIndex 字段索引
     * @param dataField 字段定义
     * @param evolutions 统计信息演化器（处理 schema 演化）
     * @return 最小值，如果所有文件的最小值都为 null，则返回 null
     */
    public Object minValue(int fieldIndex, DataField dataField, SimpleStatsEvolutions evolutions) {
        Object minValue = null;
        for (DataFileMeta dataFile : dataFiles) {
            SimpleStatsEvolution evolution = evolutions.getOrCreate(dataFile.schemaId());
            InternalRow minValues =
                    evolution.evolution(
                            dataFile.valueStats().minValues(), dataFile.valueStatsCols());
            Object other = InternalRowUtils.get(minValues, fieldIndex, dataField.type());
            if (minValue == null) {
                minValue = other;
            } else if (other != null) {
                if (CompareUtils.compareLiteral(dataField.type(), minValue, other) > 0) {
                    minValue = other;
                }
            }
        }
        return minValue;
    }

    /**
     * 获取指定字段的最大值（跨所有数据文件）。
     *
     * <p>该方法会遍历所有数据文件的统计信息，找到最大值。
     * 用于优化查询（如范围过滤）。
     *
     * @param fieldIndex 字段索引
     * @param dataField 字段定义
     * @param evolutions 统计信息演化器（处理 schema 演化）
     * @return 最大值，如果所有文件的最大值都为 null，则返回 null
     */
    public Object maxValue(int fieldIndex, DataField dataField, SimpleStatsEvolutions evolutions) {
        Object maxValue = null;
        for (DataFileMeta dataFile : dataFiles) {
            SimpleStatsEvolution evolution = evolutions.getOrCreate(dataFile.schemaId());
            InternalRow maxValues =
                    evolution.evolution(
                            dataFile.valueStats().maxValues(), dataFile.valueStatsCols());
            Object other = InternalRowUtils.get(maxValues, fieldIndex, dataField.type());
            if (maxValue == null) {
                maxValue = other;
            } else if (other != null) {
                if (CompareUtils.compareLiteral(dataField.type(), maxValue, other) < 0) {
                    maxValue = other;
                }
            }
        }
        return maxValue;
    }

    /**
     * 获取指定字段的 NULL 值数量（跨所有数据文件）。
     *
     * <p>该方法会累加所有数据文件中该字段的 NULL 值数量。
     * 用于优化查询（如 IS NULL / IS NOT NULL 过滤）。
     *
     * @param fieldIndex 字段索引
     * @param evolutions 统计信息演化器（处理 schema 演化）
     * @return NULL 值数量，如果统计信息不可用则返回 null
     */
    public Long nullCount(int fieldIndex, SimpleStatsEvolutions evolutions) {
        Long sum = null;
        for (DataFileMeta dataFile : dataFiles) {
            SimpleStatsEvolution evolution = evolutions.getOrCreate(dataFile.schemaId());
            InternalArray nullCounts =
                    evolution.evolution(
                            dataFile.valueStats().nullCounts(),
                            dataFile.rowCount(),
                            dataFile.valueStatsCols());
            Long nullCount =
                    (Long) InternalRowUtils.get(nullCounts, fieldIndex, DataTypes.BIGINT());
            if (sum == null) {
                sum = nullCount;
            } else if (nullCount != null) {
                sum += nullCount;
            }
        }
        return sum;
    }

    /**
     * 将 Split 转换为原始文件列表（如果可以直接读取）。
     *
     * <p>如果 rawConvertible = true，返回 {@link RawFile} 列表，
     * 可用于外部引擎直接读取文件（如 Spark、Flink）。
     *
     * @return 如果可以直接读取，返回原始文件列表；否则返回 empty
     */
    @Override
    public Optional<List<RawFile>> convertToRawFiles() {
        if (rawConvertible) {
            return Optional.of(
                    dataFiles.stream()
                            .map(f -> makeRawTableFile(bucketPath, f))
                            .collect(Collectors.toList()));
        } else {
            return Optional.empty();
        }
    }

    /** 创建原始文件对象（包含文件路径、大小、格式等信息）。 */
    private RawFile makeRawTableFile(String bucketPath, DataFileMeta file) {
        return new RawFile(
                file.externalPath().orElse(bucketPath + "/" + file.fileName()),
                file.fileSize(),
                0,
                file.fileSize(),
                file.fileFormat(),
                file.schemaId(),
                file.rowCount());
    }

    /**
     * 获取索引文件列表（与数据文件一一对应）。
     *
     * <p>从数据文件的 extraFiles 中提取索引文件（后缀为 INDEX_PATH_SUFFIX）。
     *
     * @return 如果有索引文件，返回索引文件列表；否则返回 empty
     */
    @Override
    @Nullable
    public Optional<List<IndexFile>> indexFiles() {
        List<IndexFile> indexFiles = new ArrayList<>();
        boolean hasIndexFile = false;
        for (DataFileMeta file : dataFiles) {
            List<String> exFiles =
                    file.extraFiles().stream()
                            .filter(s -> s.endsWith(INDEX_PATH_SUFFIX))
                            .collect(Collectors.toList());
            if (exFiles.isEmpty()) {
                indexFiles.add(null);
            } else if (exFiles.size() == 1) {
                hasIndexFile = true;
                indexFiles.add(new IndexFile(bucketPath + "/" + exFiles.get(0)));
            } else {
                throw new RuntimeException(
                        "Wrong number of file index for file "
                                + file.fileName()
                                + " index files: "
                                + String.join(",", exFiles));
            }
        }

        return hasIndexFile ? Optional.of(indexFiles) : Optional.empty();
    }

    /**
     * 过滤数据文件，创建新的 DataSplit。
     *
     * <p>该方法会根据指定的过滤条件过滤数据文件，如果过滤后还有文件，
     * 创建一个新的 DataSplit（保留其他字段不变）。
     *
     * <h3>使用场景</h3>
     * <ul>
     *   <li>根据文件级别的统计信息过滤文件</li>
     *   <li>根据文件的时间戳过滤文件</li>
     * </ul>
     *
     * @param filter 文件过滤条件
     * @return 如果过滤后还有文件，返回新的 DataSplit；否则返回 empty
     */
    public Optional<DataSplit> filterDataFile(Predicate<DataFileMeta> filter) {
        List<DataFileMeta> filtered = new ArrayList<>();
        List<DeletionFile> filteredDeletion = dataDeletionFiles == null ? null : new ArrayList<>();
        for (int i = 0; i < dataFiles.size(); i++) {
            DataFileMeta file = dataFiles.get(i);
            if (filter.test(file)) {
                filtered.add(file);
                if (filteredDeletion != null) {
                    filteredDeletion.add(dataDeletionFiles.get(i));
                }
            }
        }
        if (filtered.isEmpty()) {
            return Optional.empty();
        }
        DataSplit split = new DataSplit();
        split.assign(this);
        split.dataFiles = filtered;
        split.dataDeletionFiles = filteredDeletion;
        return Optional.of(split);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataSplit dataSplit = (DataSplit) o;
        return snapshotId == dataSplit.snapshotId
                && bucket == dataSplit.bucket
                && isStreaming == dataSplit.isStreaming
                && rawConvertible == dataSplit.rawConvertible
                && Objects.equals(partition, dataSplit.partition)
                && Objects.equals(bucketPath, dataSplit.bucketPath)
                && Objects.equals(totalBuckets, dataSplit.totalBuckets)
                && Objects.equals(dataFiles, dataSplit.dataFiles)
                && Objects.equals(dataDeletionFiles, dataSplit.dataDeletionFiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                snapshotId,
                partition,
                bucket,
                bucketPath,
                totalBuckets,
                dataFiles,
                dataDeletionFiles,
                isStreaming,
                rawConvertible);
    }

    @Override
    public String toString() {
        return "{"
                + "snapshotId="
                + snapshotId
                + ", partition=hash-"
                + partition.hashCode()
                + ", bucket="
                + bucket
                + ", rawConvertible="
                + rawConvertible
                + '}'
                + "@"
                + Integer.toHexString(hashCode());
    }

    /** Java 序列化写出方法（委托给自定义序列化）。 */
    private void writeObject(ObjectOutputStream out) throws IOException {
        serialize(new DataOutputViewStreamWrapper(out));
    }

    /** Java 序列化读入方法（委托给自定义反序列化）。 */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        assign(deserialize(new DataInputViewStreamWrapper(in)));
    }

    /** 从另一个 DataSplit 复制所有字段（用于反序列化和克隆）。 */
    protected void assign(DataSplit other) {
        this.snapshotId = other.snapshotId;
        this.partition = other.partition;
        this.bucket = other.bucket;
        this.bucketPath = other.bucketPath;
        this.totalBuckets = other.totalBuckets;
        this.dataFiles = other.dataFiles;
        this.dataDeletionFiles = other.dataDeletionFiles;
        this.isStreaming = other.isStreaming;
        this.rawConvertible = other.rawConvertible;
    }

    /**
     * 序列化 DataSplit 到输出流。
     *
     * <p>序列化格式包含版本号（VERSION = 8），支持向后兼容。
     * 序列化内容包括：magic 数字、版本号、快照 ID、分区、桶、数据文件列表、
     * 删除文件列表等。
     *
     * @param out 输出流
     * @throws IOException 如果序列化失败
     */
    public void serialize(DataOutputView out) throws IOException {
        out.writeLong(MAGIC);
        out.writeInt(VERSION);
        out.writeLong(snapshotId);
        SerializationUtils.serializeBinaryRow(partition, out);
        out.writeInt(bucket);
        out.writeUTF(bucketPath);
        if (totalBuckets != null) {
            out.writeBoolean(true);
            out.writeInt(totalBuckets);
        } else {
            out.writeBoolean(false);
        }

        DataFileMetaSerializer dataFileSer = new DataFileMetaSerializer();

        // compatible with old beforeFiles
        out.writeInt(0);
        DeletionFile.serializeList(out, null);

        out.writeInt(dataFiles.size());
        for (DataFileMeta file : dataFiles) {
            dataFileSer.serialize(file, out);
        }

        DeletionFile.serializeList(out, dataDeletionFiles);

        out.writeBoolean(isStreaming);

        out.writeBoolean(rawConvertible);
    }

    /**
     * 从输入流反序列化 DataSplit。
     *
     * <p>支持反序列化多个版本的 DataSplit（版本 1-8），实现向后兼容。
     * 每个版本可能使用不同的数据文件序列化器。
     *
     * @param in 输入流
     * @return 反序列化的 DataSplit
     * @throws IOException 如果反序列化失败
     */
    public static DataSplit deserialize(DataInputView in) throws IOException {
        long magic = in.readLong();
        int version = magic == MAGIC ? in.readInt() : 1;
        // version 1 does not write magic number in, so the first long is snapshot id.
        long snapshotId = version == 1 ? magic : in.readLong();
        BinaryRow partition = SerializationUtils.deserializeBinaryRow(in);
        int bucket = in.readInt();
        String bucketPath = in.readUTF();
        Integer totalBuckets = version >= 6 && in.readBoolean() ? in.readInt() : null;

        FunctionWithIOException<DataInputView, DataFileMeta> dataFileSer =
                getFileMetaSerde(version);
        FunctionWithIOException<DataInputView, DeletionFile> deletionFileSerde =
                getDeletionFileSerde(version);
        int beforeNumber = in.readInt();
        if (beforeNumber > 0) {
            throw new RuntimeException("Cannot deserialize data split with before files.");
        }

        List<DeletionFile> beforeDeletionFiles =
                DeletionFile.deserializeList(in, deletionFileSerde);
        if (beforeDeletionFiles != null) {
            throw new RuntimeException("Cannot deserialize data split with before deletion files.");
        }

        int fileNumber = in.readInt();
        List<DataFileMeta> dataFiles = new ArrayList<>(fileNumber);
        for (int i = 0; i < fileNumber; i++) {
            dataFiles.add(dataFileSer.apply(in));
        }

        List<DeletionFile> dataDeletionFiles = DeletionFile.deserializeList(in, deletionFileSerde);

        boolean isStreaming = in.readBoolean();
        boolean rawConvertible = in.readBoolean();

        DataSplit.Builder builder =
                builder()
                        .withSnapshot(snapshotId)
                        .withPartition(partition)
                        .withBucket(bucket)
                        .withBucketPath(bucketPath)
                        .withTotalBuckets(totalBuckets)
                        .withDataFiles(dataFiles)
                        .isStreaming(isStreaming)
                        .rawConvertible(rawConvertible);

        if (dataDeletionFiles != null) {
            builder.withDataDeletionFiles(dataDeletionFiles);
        }
        return builder.build();
    }

    /** 根据版本号获取对应的数据文件序列化器（支持版本 1-8）。 */
    private static FunctionWithIOException<DataInputView, DataFileMeta> getFileMetaSerde(
            int version) {
        if (version == 1) {
            DataFileMeta08Serializer serializer = new DataFileMeta08Serializer();
            return serializer::deserialize;
        } else if (version == 2) {
            DataFileMeta09Serializer serializer = new DataFileMeta09Serializer();
            return serializer::deserialize;
        } else if (version == 3 || version == 4) {
            DataFileMeta10LegacySerializer serializer = new DataFileMeta10LegacySerializer();
            return serializer::deserialize;
        } else if (version == 5 || version == 6) {
            DataFileMeta12LegacySerializer serializer = new DataFileMeta12LegacySerializer();
            return serializer::deserialize;
        } else if (version == 7) {
            DataFileMetaFirstRowIdLegacySerializer serializer =
                    new DataFileMetaFirstRowIdLegacySerializer();
            return serializer::deserialize;
        } else if (version == 8) {
            DataFileMetaSerializer serializer = new DataFileMetaSerializer();
            return serializer::deserialize;
        } else {
            throw new UnsupportedOperationException("Unsupported version: " + version);
        }
    }

    /** 根据版本号获取对应的删除文件序列化器（支持版本 1-8）。 */
    private static FunctionWithIOException<DataInputView, DeletionFile> getDeletionFileSerde(
            int version) {
        if (version >= 1 && version <= 3) {
            return DeletionFile::deserializeV3;
        } else if (version >= 4) {
            return DeletionFile::deserialize;
        } else {
            throw new UnsupportedOperationException("Unsupported version: " + version);
        }
    }

    /** 创建 Builder 实例（用于构建 DataSplit）。 */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * DataSplit 的构建器，用于逐步设置各个字段并构建 DataSplit。
     *
     * <h3>使用示例</h3>
     * <pre>{@code
     * DataSplit split = DataSplit.builder()
     *     .withSnapshot(10)
     *     .withPartition(partition)
     *     .withBucket(5)
     *     .withBucketPath("path/to/bucket-5")
     *     .withDataFiles(dataFiles)
     *     .withDataDeletionFiles(deletionFiles)  // 可选
     *     .rawConvertible(true)
     *     .isStreaming(false)
     *     .build();
     * }</pre>
     */
    public static class Builder {

        private final DataSplit split = new DataSplit();

        /** 设置快照 ID。 */
        public Builder withSnapshot(long snapshot) {
            this.split.snapshotId = snapshot;
            return this;
        }

        /** 设置分区键。 */
        public Builder withPartition(BinaryRow partition) {
            this.split.partition = partition;
            return this;
        }

        /** 设置桶号。 */
        public Builder withBucket(int bucket) {
            this.split.bucket = bucket;
            return this;
        }

        /** 设置桶路径。 */
        public Builder withBucketPath(String bucketPath) {
            this.split.bucketPath = bucketPath;
            return this;
        }

        /** 设置总桶数（仅延迟分桶表）。 */
        public Builder withTotalBuckets(Integer totalBuckets) {
            this.split.totalBuckets = totalBuckets;
            return this;
        }

        /** 设置数据文件列表。 */
        public Builder withDataFiles(List<DataFileMeta> dataFiles) {
            this.split.dataFiles = new ArrayList<>(dataFiles);
            return this;
        }

        /** 设置删除文件列表（可选）。 */
        public Builder withDataDeletionFiles(List<DeletionFile> dataDeletionFiles) {
            this.split.dataDeletionFiles = new ArrayList<>(dataDeletionFiles);
            return this;
        }

        /** 设置是否是流式 Split。 */
        public Builder isStreaming(boolean isStreaming) {
            this.split.isStreaming = isStreaming;
            return this;
        }

        /** 设置是否可以转换为原始文件（不需要合并）。 */
        public Builder rawConvertible(boolean rawConvertible) {
            this.split.rawConvertible = rawConvertible;
            return this;
        }

        /**
         * 构建 DataSplit 实例。
         *
         * <p>会检查必需字段是否已设置（partition、bucket、bucketPath、dataFiles）。
         *
         * @return 构建的 DataSplit
         * @throws IllegalArgumentException 如果必需字段未设置
         */
        public DataSplit build() {
            checkArgument(split.partition != null);
            checkArgument(split.bucket != -1);
            checkArgument(split.bucketPath != null);
            checkArgument(split.dataFiles != null);

            DataSplit split = new DataSplit();
            split.assign(this.split);
            return split;
        }
    }
}
