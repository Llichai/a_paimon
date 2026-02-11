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

import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.index.DynamicBucketIndexMaintainer;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.operation.AbstractFileStoreWrite;
import org.apache.paimon.operation.BucketSelectConverter;
import org.apache.paimon.operation.KeyValueFileStoreScan;
import org.apache.paimon.operation.KeyValueFileStoreWrite;
import org.apache.paimon.operation.MergeFileSplitRead;
import org.apache.paimon.operation.RawFileSplitRead;
import org.apache.paimon.postpone.PostponeBucketFileStoreWrite;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.apache.paimon.utils.UserDefinedSeqComparator;
import org.apache.paimon.utils.ValueEqualiserSupplier;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.paimon.predicate.PredicateBuilder.and;
import static org.apache.paimon.predicate.PredicateBuilder.pickTransformFieldMapping;
import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;

/**
 * KeyValue 文件存储实现
 *
 * <p>KeyValueFileStore 是 Primary Key 表的存储实现，核心特性是基于 MergeTree 的数据合并。
 *
 * <p>核心概念：
 * <ul>
 *   <li>Primary Key：主键字段，用于唯一标识记录
 *   <li>MergeTree：LSM Tree 数据结构，支持高效的更新和删除
 *   <li>MergeFunction：合并函数，定义相同主键记录的合并逻辑（Deduplicate、Aggregate、PartialUpdate 等）
 * </ul>
 *
 * <p>四种分桶模式（BucketMode）：
 * <ul>
 *   <li>HASH_FIXED：固定哈希分桶（bucket >= 0）
 *     <ul>
 *       <li>根据 Bucket Key 计算固定的桶号
 *       <li>桶数在表创建时确定，不可更改
 *       <li>适用场景：数据量可预估的场景
 *     </ul>
 *   </li>
 *   <li>HASH_DYNAMIC：动态哈希分桶（bucket = -1，无跨分区更新）
 *     <ul>
 *       <li>根据主键动态分配桶号
 *       <li>使用 DynamicBucketIndex 维护 Key -> Bucket 映射
 *       <li>适用场景：数据量不可预估的场景
 *     </ul>
 *   </li>
 *   <li>KEY_DYNAMIC：主键动态分桶（bucket = -1，支持跨分区更新）
 *     <ul>
 *       <li>支持跨分区更新同一主键的记录
 *       <li>需要扫描所有分区查找记录
 *       <li>适用场景：需要跨分区更新的场景
 *     </ul>
 *   </li>
 *   <li>POSTPONE_MODE：延迟分桶模式（bucket = -2）
 *     <ul>
 *       <li>写入时不确定桶号，在 Compaction 时再分配
 *       <li>由 PostponeBucketFileStoreWrite 处理
 *       <li>适用场景：特殊的分桶策略
 *     </ul>
 *   </li>
 * </ul>
 *
 * <p>与 Batch 6 的关系：
 * <ul>
 *   <li>{@link KeyValueFileStoreWrite}：负责写入 KeyValue 数据，创建 MergeTreeWriter
 *   <li>{@link KeyValueFileStoreScan}：负责扫描 KeyValue 数据，读取 Manifest 文件
 * </ul>
 *
 * <p>MergeTree 机制：
 * <ul>
 *   <li>写入：数据先写入 WriteBuffer（内存），达到阈值后 Flush 到 Level 0
 *   <li>Compaction：定期合并多个文件，将 Level 0 文件合并到更高 Level
 *   <li>读取：从所有 Level 读取数据，通过 MergeFunction 合并相同主键的记录
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建 KeyValueFileStore
 * KeyValueFileStore store = ...;
 *
 * // 写入数据
 * FileStoreWrite<KeyValue> write = store.newWrite("user");
 * write.write(new KeyValue().replace(key, RowKind.INSERT, value));
 *
 * // 扫描数据
 * FileStoreScan scan = store.newScan();
 * List<DataSplit> splits = scan.plan().splits();
 *
 * // 读取数据
 * SplitRead<KeyValue> read = store.newRead();
 * RecordReader<KeyValue> reader = read.createReader(split);
 * }</pre>
 */
public class KeyValueFileStore extends AbstractFileStore<KeyValue> {

    /** 是否支持跨分区更新（用于判断 KEY_DYNAMIC 模式） */
    private final boolean crossPartitionUpdate;
    /** 分桶键类型 */
    private final RowType bucketKeyType;
    /** 主键类型 */
    private final RowType keyType;
    /** 值类型 */
    private final RowType valueType;
    /** KeyValue 字段提取器 */
    private final KeyValueFieldsExtractor keyValueFieldsExtractor;
    /** 主键比较器供应器 */
    private final Supplier<Comparator<InternalRow>> keyComparatorSupplier;
    /** 日志去重相等性检查供应器 */
    private final Supplier<RecordEqualiser> logDedupEqualSupplier;
    /** MergeFunction 工厂 */
    private final MergeFunctionFactory<KeyValue> mfFactory;

    public KeyValueFileStore(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            boolean crossPartitionUpdate,
            CoreOptions options,
            RowType partitionType,
            RowType bucketKeyType,
            RowType keyType,
            RowType valueType,
            KeyValueFieldsExtractor keyValueFieldsExtractor,
            MergeFunctionFactory<KeyValue> mfFactory,
            String tableName,
            CatalogEnvironment catalogEnvironment) {
        super(fileIO, schemaManager, schema, tableName, options, partitionType, catalogEnvironment);
        this.crossPartitionUpdate = crossPartitionUpdate;
        this.bucketKeyType = bucketKeyType;
        this.keyType = keyType;
        this.valueType = valueType;
        this.keyValueFieldsExtractor = keyValueFieldsExtractor;
        this.mfFactory = mfFactory;
        this.keyComparatorSupplier = new KeyComparatorSupplier(keyType);
        List<String> logDedupIgnoreFields = options.changelogRowDeduplicateIgnoreFields();
        this.logDedupEqualSupplier =
                options.changelogRowDeduplicate()
                        ? ValueEqualiserSupplier.fromIgnoreFields(valueType, logDedupIgnoreFields)
                        : () -> null;
    }

    /**
     * 获取分桶模式
     *
     * <p>根据 bucket 配置值决定分桶模式：
     * <ul>
     *   <li>bucket >= 0：HASH_FIXED（固定哈希分桶）
     *   <li>bucket = -1 且无跨分区更新：HASH_DYNAMIC（动态哈希分桶）
     *   <li>bucket = -1 且支持跨分区更新：KEY_DYNAMIC（主键动态分桶）
     *   <li>bucket = -2：POSTPONE_MODE（延迟分桶模式）
     * </ul>
     *
     * @return 分桶模式
     */
    @Override
    public BucketMode bucketMode() {
        int bucket = options.bucket();
        switch (bucket) {
            case -2:
                return BucketMode.POSTPONE_MODE;
            case -1:
                return crossPartitionUpdate ? BucketMode.KEY_DYNAMIC : BucketMode.HASH_DYNAMIC;
            default:
                return BucketMode.HASH_FIXED;
        }
    }

    /**
     * 创建数据读取器
     *
     * <p>MergeFileSplitRead 负责读取 KeyValue 数据并执行 MergeTree 合并：
     * <ul>
     *   <li>从多个 Level 的文件读取数据
     *   <li>通过 MergeFunction 合并相同主键的记录
     *   <li>应用投影和过滤
     * </ul>
     *
     * @return 数据读取器实例
     */
    @Override
    public MergeFileSplitRead newRead() {
        return new MergeFileSplitRead(
                options,
                schema,
                keyType,
                valueType,
                newKeyComparator(),
                mfFactory,
                newReaderFactoryBuilder());
    }

    /**
     * 创建批处理原始文件读取器（不执行 MergeTree 合并）
     *
     * <p>用于直接读取数据文件，跳过 MergeTree 合并逻辑。
     *
     * @return 原始文件读取器实例
     */
    public RawFileSplitRead newBatchRawFileRead() {
        return new RawFileSplitRead(
                fileIO,
                schemaManager,
                schema,
                valueType,
                FileFormatDiscover.of(options),
                pathFactory(),
                options.fileIndexReadEnabled(),
                false);
    }

    /**
     * 创建 KeyValue 文件读取器工厂构建器
     *
     * <p>文件读取器工厂负责创建文件读取器，用于读取数据文件。
     *
     * @return 文件读取器工厂构建器实例
     */
    public KeyValueFileReaderFactory.Builder newReaderFactoryBuilder() {
        return KeyValueFileReaderFactory.builder(
                fileIO,
                schemaManager,
                schema,
                keyType,
                valueType,
                FileFormatDiscover.of(options),
                pathFactory(),
                keyValueFieldsExtractor,
                options);
    }

    /**
     * 创建数据写入器（使用默认 writeId）
     *
     * @param commitUser 提交用户标识
     * @return 数据写入器实例
     */
    @Override
    public AbstractFileStoreWrite<KeyValue> newWrite(String commitUser) {
        return newWrite(commitUser, null);
    }

    /**
     * 创建数据写入器（指定 writeId）
     *
     * <p>根据分桶模式选择不同的写入器实现：
     * <ul>
     *   <li>POSTPONE_MODE：使用 {@link PostponeBucketFileStoreWrite}
     *   <li>其他模式：使用 {@link KeyValueFileStoreWrite}
     * </ul>
     *
     * <p>KeyValueFileStoreWrite 负责：
     * <ul>
     *   <li>创建 MergeTreeWriter，写入 KeyValue 数据
     *   <li>管理动态分桶索引（HASH_DYNAMIC 模式）
     *   <li>管理删除向量（Deletion Vector）
     * </ul>
     *
     * @param commitUser 提交用户标识
     * @param writeId 写入器 ID（可选）
     * @return 数据写入器实例
     */
    @Override
    public AbstractFileStoreWrite<KeyValue> newWrite(String commitUser, @Nullable Integer writeId) {
        // POSTPONE_MODE：延迟分桶模式
        if (options.bucket() == BucketMode.POSTPONE_BUCKET) {
            return new PostponeBucketFileStoreWrite(
                    fileIO,
                    pathFactory(),
                    schema,
                    commitUser,
                    partitionType,
                    keyType,
                    valueType,
                    mfFactory,
                    this::pathFactory,
                    newReaderFactoryBuilder(),
                    snapshotManager(),
                    newScan(),
                    options,
                    tableName,
                    writeId);
        }
        // 动态分桶索引工厂（HASH_DYNAMIC 模式）
        DynamicBucketIndexMaintainer.Factory indexFactory = null;
        if (bucketMode() == BucketMode.HASH_DYNAMIC) {
            indexFactory = new DynamicBucketIndexMaintainer.Factory(newIndexFileHandler());
        }
        // 删除向量维护工厂（启用删除向量时）
        BucketedDvMaintainer.Factory dvMaintainerFactory = null;
        if (options.deletionVectorsEnabled()) {
            dvMaintainerFactory = BucketedDvMaintainer.factory(newIndexFileHandler());
        }
        return new KeyValueFileStoreWrite(
                fileIO,
                schemaManager,
                schema,
                commitUser,
                partitionType,
                keyType,
                valueType,
                keyComparatorSupplier,
                () -> UserDefinedSeqComparator.create(valueType, options),
                logDedupEqualSupplier,
                mfFactory,
                pathFactory(),
                this::pathFactory,
                snapshotManager(),
                newScan(),
                indexFactory,
                dvMaintainerFactory,
                options,
                keyValueFieldsExtractor,
                tableName);
    }

    /**
     * 创建文件扫描器
     *
     * <p>KeyValueFileStoreScan 负责：
     * <ul>
     *   <li>读取 Manifest 文件，获取数据文件列表
     *   <li>应用分区过滤和桶过滤
     *   <li>生成数据分片（DataSplit）
     * </ul>
     *
     * <p>桶选择转换器（BucketSelectConverter）：
     * <ul>
     *   <li>HASH_FIXED 和 POSTPONE_MODE：根据 Bucket Key 过滤桶
     *   <li>其他模式：不支持桶过滤
     * </ul>
     *
     * @return 文件扫描器实例
     */
    @Override
    public KeyValueFileStoreScan newScan() {
        BucketMode bucketMode = bucketMode();
        // 桶选择转换器：根据主键过滤条件推导桶过滤条件
        BucketSelectConverter bucketSelectConverter =
                keyFilter -> {
                    // 只有 HASH_FIXED 和 POSTPONE_MODE 支持桶过滤
                    if (bucketMode != BucketMode.HASH_FIXED
                            && bucketMode != BucketMode.POSTPONE_MODE) {
                        return Optional.empty();
                    }

                    // 从主键过滤条件中提取分桶键过滤条件
                    List<Predicate> bucketFilters =
                            pickTransformFieldMapping(
                                    splitAnd(keyFilter),
                                    keyType.getFieldNames(),
                                    bucketKeyType.getFieldNames());
                    if (!bucketFilters.isEmpty()) {
                        return BucketSelectConverter.create(
                                and(bucketFilters), bucketKeyType, options.bucketFunctionType());
                    }
                    return Optional.empty();
                };

        return new KeyValueFileStoreScan(
                newManifestsReader(),
                bucketSelectConverter,
                snapshotManager(),
                schemaManager,
                schema,
                keyValueFieldsExtractor,
                manifestFileFactory(),
                options.scanManifestParallelism(),
                options.deletionVectorsEnabled(),
                options.mergeEngine(),
                options.changelogProducer(),
                options.fileIndexReadEnabled() && options.deletionVectorsEnabled());
    }

    /**
     * 创建主键比较器
     *
     * <p>主键比较器用于 MergeTree 合并，比较两个主键的大小。
     *
     * @return 主键比较器实例
     */
    @Override
    public Comparator<InternalRow> newKeyComparator() {
        return keyComparatorSupplier.get();
    }
}
