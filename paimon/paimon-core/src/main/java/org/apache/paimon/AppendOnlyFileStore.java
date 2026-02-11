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

package org.apache.paimon;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.operation.AppendFileStoreWrite;
import org.apache.paimon.operation.AppendOnlyFileStoreScan;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.operation.BucketSelectConverter;
import org.apache.paimon.operation.BucketedAppendFileStoreWrite;
import org.apache.paimon.operation.DataEvolutionFileStoreScan;
import org.apache.paimon.operation.DataEvolutionSplitRead;
import org.apache.paimon.operation.RawFileSplitRead;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.predicate.PredicateBuilder.and;
import static org.apache.paimon.predicate.PredicateBuilder.pickTransformFieldMapping;
import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;

/**
 * Append-Only 文件存储实现
 *
 * <p>AppendOnlyFileStore 用于 Append-Only 表，只支持追加写入，不支持更新和删除。
 *
 * <p>与 KeyValueFileStore 的核心区别：
 * <ul>
 *   <li>数据模型：直接存储 InternalRow，无需 KeyValue 封装
 *   <li>主键：不需要主键，无法更新和删除记录
 *   <li>MergeTree：不使用 MergeTree，写入更快
 *   <li>Compaction：只做文件合并（Concatenation），不做记录合并
 * </ul>
 *
 * <p>两种分桶模式：
 * <ul>
 *   <li>BUCKET_UNAWARE：无分桶（bucket = -1）
 *     <ul>
 *       <li>每个写入任务独立写入，不按桶组织
 *       <li>文件数量随写入任务数增加
 *       <li>适用场景：日志表、事件流表
 *       <li>写入器：{@link AppendFileStoreWrite}
 *     </ul>
 *   </li>
 *   <li>HASH_FIXED：固定哈希分桶（bucket >= 0）
 *     <ul>
 *       <li>根据 Bucket Key 计算固定的桶号
 *       <li>每个桶独立 Compaction
 *       <li>适用场景：需要控制文件数量的场景
 *       <li>写入器：{@link BucketedAppendFileStoreWrite}
 *     </ul>
 *   </li>
 * </ul>
 *
 * <p>数据演化支持：
 * <ul>
 *   <li>启用 data-evolution.enabled 后，支持 Schema 演化
 *   <li>使用 {@link DataEvolutionSplitRead} 读取不同 Schema 版本的数据
 *   <li>使用 {@link DataEvolutionFileStoreScan} 扫描支持 Schema 演化
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建 AppendOnlyFileStore
 * AppendOnlyFileStore store = ...;
 *
 * // 写入数据
 * FileStoreWrite<InternalRow> write = store.newWrite("user");
 * write.write(GenericRow.of(1, "Alice"));
 *
 * // 扫描数据
 * FileStoreScan scan = store.newScan();
 * List<DataSplit> splits = scan.plan().splits();
 *
 * // 读取数据
 * SplitRead<InternalRow> read = store.newRead();
 * RecordReader<InternalRow> reader = read.createReader(split);
 * }</pre>
 */
public class AppendOnlyFileStore extends AbstractFileStore<InternalRow> {

    /** 分桶键类型 */
    private final RowType bucketKeyType;
    /** 行类型（表的 Schema） */
    private final RowType rowType;

    public AppendOnlyFileStore(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            CoreOptions options,
            RowType partitionType,
            RowType bucketKeyType,
            RowType rowType,
            String tableName,
            CatalogEnvironment catalogEnvironment) {
        super(fileIO, schemaManager, schema, tableName, options, partitionType, catalogEnvironment);
        this.bucketKeyType = bucketKeyType;
        this.rowType = rowType;
    }

    /**
     * 获取分桶模式
     *
     * <p>根据 bucket 配置值决定分桶模式：
     * <ul>
     *   <li>bucket = -1：BUCKET_UNAWARE（无分桶）
     *   <li>bucket >= 0：HASH_FIXED（固定哈希分桶）
     * </ul>
     *
     * @return 分桶模式
     */
    @Override
    public BucketMode bucketMode() {
        return options.bucket() == -1 ? BucketMode.BUCKET_UNAWARE : BucketMode.HASH_FIXED;
    }

    /**
     * 创建数据读取器
     *
     * <p>RawFileSplitRead 直接读取数据文件，不执行任何合并逻辑。
     *
     * @return 数据读取器实例
     */
    @Override
    public RawFileSplitRead newRead() {
        return new RawFileSplitRead(
                fileIO,
                schemaManager,
                schema,
                rowType,
                FileFormatDiscover.of(options),
                pathFactory(),
                options.fileIndexReadEnabled(),
                options.rowTrackingEnabled());
    }

    /**
     * 创建数据演化读取器
     *
     * <p>DataEvolutionSplitRead 支持读取不同 Schema 版本的数据文件，并自动进行 Schema 对齐。
     *
     * <p>只有在启用 data-evolution.enabled 时才能使用此读取器。
     *
     * @return 数据演化读取器实例
     * @throws IllegalStateException 如果未启用数据演化
     */
    public DataEvolutionSplitRead newDataEvolutionRead() {
        if (!options.dataEvolutionEnabled()) {
            throw new IllegalStateException(
                    "Field merge read is only supported when data-evolution.enabled is true.");
        }
        return new DataEvolutionSplitRead(
                fileIO,
                schemaManager,
                schema,
                rowType,
                FileFormatDiscover.of(options),
                pathFactory());
    }

    /**
     * 创建数据写入器（使用默认 writeId）
     *
     * @param commitUser 提交用户标识
     * @return 数据写入器实例
     */
    @Override
    public BaseAppendFileStoreWrite newWrite(String commitUser) {
        return newWrite(commitUser, null);
    }

    /**
     * 创建数据写入器（指定 writeId）
     *
     * <p>根据分桶模式选择不同的写入器实现：
     * <ul>
     *   <li>BUCKET_UNAWARE：使用 {@link AppendFileStoreWrite}
     *     <ul>
     *       <li>每个写入任务独立写入
     *       <li>不需要协调分桶
     *     </ul>
     *   </li>
     *   <li>HASH_FIXED：使用 {@link BucketedAppendFileStoreWrite}
     *     <ul>
     *       <li>根据 Bucket Key 写入到固定桶
     *       <li>支持删除向量（Deletion Vector）
     *     </ul>
     *   </li>
     * </ul>
     *
     * @param commitUser 提交用户标识
     * @param writeId 写入器 ID（可选）
     * @return 数据写入器实例
     */
    @Override
    public BaseAppendFileStoreWrite newWrite(String commitUser, @Nullable Integer writeId) {
        if (bucketMode() == BucketMode.BUCKET_UNAWARE) {
            // BUCKET_UNAWARE 模式：无分桶写入
            RawFileSplitRead readForCompact = newRead();
            // 如果启用行追踪，读取时需要包含 Row Tracking 字段
            if (options.rowTrackingEnabled()) {
                readForCompact.withReadType(SpecialFields.rowTypeWithRowTracking(rowType));
            }
            return new AppendFileStoreWrite(
                    fileIO,
                    readForCompact,
                    schema.id(),
                    rowType,
                    partitionType,
                    pathFactory(),
                    snapshotManager(),
                    newScan(),
                    options,
                    tableName);
        } else {
            // HASH_FIXED 模式：固定哈希分桶写入
            BucketedDvMaintainer.Factory dvMaintainerFactory =
                    options.deletionVectorsEnabled()
                            ? BucketedDvMaintainer.factory(newIndexFileHandler())
                            : null;
            return new BucketedAppendFileStoreWrite(
                    fileIO,
                    newRead(),
                    schema.id(),
                    commitUser,
                    rowType,
                    partitionType,
                    pathFactory(),
                    snapshotManager(),
                    newScan(),
                    options,
                    dvMaintainerFactory,
                    tableName,
                    schemaManager);
        }
    }

    /**
     * 创建文件扫描器
     *
     * <p>根据配置选择不同的扫描器实现：
     * <ul>
     *   <li>启用数据演化：使用 {@link DataEvolutionFileStoreScan}
     *   <li>普通模式：使用 {@link AppendOnlyFileStoreScan}
     * </ul>
     *
     * <p>桶选择转换器（BucketSelectConverter）：
     * <ul>
     *   <li>HASH_FIXED：根据 Bucket Key 过滤桶
     *   <li>BUCKET_UNAWARE：不支持桶过滤
     * </ul>
     *
     * @return 文件扫描器实例
     */
    @Override
    public AppendOnlyFileStoreScan newScan() {
        // 桶选择转换器：根据谓词条件推导桶过滤条件
        BucketSelectConverter bucketSelectConverter =
                predicate -> {
                    // 只有 HASH_FIXED 模式支持桶过滤
                    if (bucketMode() != BucketMode.HASH_FIXED) {
                        return Optional.empty();
                    }

                    // 如果没有分桶键，无法进行桶过滤
                    if (bucketKeyType.getFieldCount() == 0) {
                        return Optional.empty();
                    }

                    // 从谓词条件中提取分桶键过滤条件
                    List<Predicate> bucketFilters =
                            pickTransformFieldMapping(
                                    splitAnd(predicate),
                                    rowType.getFieldNames(),
                                    bucketKeyType.getFieldNames());
                    if (!bucketFilters.isEmpty()) {
                        return BucketSelectConverter.create(
                                and(bucketFilters), bucketKeyType, options.bucketFunctionType());
                    }
                    return Optional.empty();
                };

        // 如果启用数据演化，使用 DataEvolutionFileStoreScan
        if (options().dataEvolutionEnabled()) {
            return new DataEvolutionFileStoreScan(
                    newManifestsReader(),
                    bucketSelectConverter,
                    snapshotManager(),
                    schemaManager,
                    schema,
                    manifestFileFactory(),
                    options.scanManifestParallelism(),
                    options.deletionVectorsEnabled());
        }

        // 普通模式，使用 AppendOnlyFileStoreScan
        return new AppendOnlyFileStoreScan(
                newManifestsReader(),
                bucketSelectConverter,
                snapshotManager(),
                schemaManager,
                schema,
                manifestFileFactory(),
                options.scanManifestParallelism(),
                options.fileIndexReadEnabled(),
                options.deletionVectorsEnabled());
    }

    /**
     * 创建主键比较器
     *
     * <p>Append-Only 表没有主键，返回 null。
     *
     * @return null
     */
    @Override
    public Comparator<InternalRow> newKeyComparator() {
        return null;
    }
}
