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
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.compact.NoopCompactManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.index.DynamicBucketIndexMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RecordLevelExpire;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.lookup.LookupStrategy;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.LookupFile;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.MergeTreeWriter;
import org.apache.paimon.mergetree.compact.CompactRewriter;
import org.apache.paimon.mergetree.compact.CompactStrategy;
import org.apache.paimon.mergetree.compact.EarlyFullCompaction;
import org.apache.paimon.mergetree.compact.ForceUpLevel0Compaction;
import org.apache.paimon.mergetree.compact.FullChangelogMergeTreeCompactRewriter;
import org.apache.paimon.mergetree.compact.LookupMergeFunction;
import org.apache.paimon.mergetree.compact.LookupMergeTreeCompactRewriter;
import org.apache.paimon.mergetree.compact.LookupMergeTreeCompactRewriter.FirstRowMergeFunctionWrapperFactory;
import org.apache.paimon.mergetree.compact.LookupMergeTreeCompactRewriter.LookupMergeFunctionWrapperFactory;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.mergetree.compact.MergeTreeCompactManager;
import org.apache.paimon.mergetree.compact.MergeTreeCompactRewriter;
import org.apache.paimon.mergetree.compact.OffPeakHours;
import org.apache.paimon.mergetree.compact.UniversalCompaction;
import org.apache.paimon.mergetree.lookup.LookupSerializerFactory;
import org.apache.paimon.mergetree.lookup.PersistEmptyProcessor;
import org.apache.paimon.mergetree.lookup.PersistPositionProcessor;
import org.apache.paimon.mergetree.lookup.PersistProcessor;
import org.apache.paimon.mergetree.lookup.PersistValueAndPosProcessor;
import org.apache.paimon.mergetree.lookup.PersistValueProcessor;
import org.apache.paimon.mergetree.lookup.RemoteLookupFileManager;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.paimon.CoreOptions.ChangelogProducer.FULL_COMPACTION;
import static org.apache.paimon.CoreOptions.MergeEngine.DEDUPLICATE;
import static org.apache.paimon.format.FileFormat.fileFormat;
import static org.apache.paimon.lookup.LookupStoreFactory.bfGenerator;
import static org.apache.paimon.mergetree.LookupFile.localFilePrefix;
import static org.apache.paimon.utils.FileStorePathFactory.createFormatPathFactories;

/**
 * {@link FileStoreWrite} for {@link KeyValueFileStore}.
 */
public class KeyValueFileStoreWrite extends MemoryFileStoreWrite<KeyValue> {

    private static final Logger LOG = LoggerFactory.getLogger(KeyValueFileStoreWrite.class);

    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;
    private final KeyValueFileWriterFactory.Builder writerFactoryBuilder;
    private final Supplier<Comparator<InternalRow>> keyComparatorSupplier;
    private final Supplier<FieldsComparator> udsComparatorSupplier;
    private final Supplier<RecordEqualiser> logDedupEqualSupplier;
    private final MergeFunctionFactory<KeyValue> mfFactory;
    private final CoreOptions options;
    private final RowType keyType;
    private final RowType valueType;
    private final FileIO fileIO;
    private final SchemaManager schemaManager;
    private final TableSchema schema;
    private final RowType partitionType;
    private final String commitUser;
    @Nullable
    private final RecordLevelExpire recordLevelExpire;
    @Nullable
    private Cache<String, LookupFile> lookupFileCache;

    public KeyValueFileStoreWrite(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            String commitUser,
            RowType partitionType,
            RowType keyType,
            RowType valueType,
            Supplier<Comparator<InternalRow>> keyComparatorSupplier,
            Supplier<FieldsComparator> udsComparatorSupplier,
            Supplier<RecordEqualiser> logDedupEqualSupplier,
            MergeFunctionFactory<KeyValue> mfFactory,
            FileStorePathFactory pathFactory,
            BiFunction<CoreOptions, String, FileStorePathFactory> formatPathFactory,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            @Nullable DynamicBucketIndexMaintainer.Factory dbMaintainerFactory,
            @Nullable BucketedDvMaintainer.Factory dvMaintainerFactory,
            CoreOptions options,
            KeyValueFieldsExtractor extractor,
            String tableName) {
        super(
                snapshotManager,
                scan,
                options,
                partitionType,
                dbMaintainerFactory,
                dvMaintainerFactory,
                tableName);
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.schema = schema;
        this.partitionType = partitionType;
        this.keyType = keyType;
        this.valueType = valueType;
        this.commitUser = commitUser;

        this.udsComparatorSupplier = udsComparatorSupplier;
        this.readerFactoryBuilder =
                KeyValueFileReaderFactory.builder(
                        fileIO,
                        schemaManager,
                        schema,
                        keyType,
                        valueType,
                        FileFormatDiscover.of(options),
                        pathFactory,
                        extractor,
                        options);
        this.recordLevelExpire = RecordLevelExpire.create(options, schema, schemaManager);
        this.writerFactoryBuilder =
                KeyValueFileWriterFactory.builder(
                        fileIO,
                        schema.id(),
                        keyType,
                        valueType,
                        fileFormat(options),
                        createFormatPathFactories(options, formatPathFactory),
                        options.targetFileSize(true));
        this.keyComparatorSupplier = keyComparatorSupplier;
        this.logDedupEqualSupplier = logDedupEqualSupplier;
        this.mfFactory = mfFactory;
        this.options = options;
    }

    @Override
    public KeyValueFileStoreWrite withIOManager(IOManager ioManager) {
        super.withIOManager(ioManager);
        if (mfFactory instanceof LookupMergeFunction.Factory) {
            ((LookupMergeFunction.Factory) mfFactory).withIOManager(ioManager);
        }
        return this;
    }

    @Override
    protected MergeTreeWriter createWriter(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> restoreFiles,
            long restoredMaxSeqNumber,
            @Nullable CommitIncrement restoreIncrement,
            ExecutorService compactExecutor,
            @Nullable BucketedDvMaintainer dvMaintainer) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Creating merge tree writer for partition {} bucket {} from restored files {}",
                    partition,
                    bucket,
                    restoreFiles);
        }

        KeyValueFileWriterFactory writerFactory =
                writerFactoryBuilder.build(partition, bucket, options);
        Comparator<InternalRow> keyComparator = keyComparatorSupplier.get();
        Levels levels = new Levels(keyComparator, restoreFiles, options.numLevels());
        CompactStrategy compactStrategy = createCompactStrategy(options);
        CompactManager compactManager =
                createCompactManager(
                        partition, bucket, compactStrategy, compactExecutor, levels, dvMaintainer);

        return new MergeTreeWriter(
                options.writeBufferSpillable(),
                options.writeBufferSpillDiskSize(),
                options.localSortMaxNumFileHandles(),
                options.spillCompressOptions(),
                ioManager,
                compactManager,
                restoredMaxSeqNumber,
                keyComparator,
                mfFactory.create(),
                writerFactory,
                options.commitForceCompact(),
                options.changelogProducer(),
                restoreIncrement,
                UserDefinedSeqComparator.create(valueType, options));
    }

    private CompactStrategy createCompactStrategy(CoreOptions options) {
        if (options.needLookup()) {
            Integer compactMaxInterval = null;
            switch (options.lookupCompact()) {
                case GENTLE:
                    compactMaxInterval = options.lookupCompactMaxInterval();
                    break;
                case RADICAL:
                    break;
            }
            return new ForceUpLevel0Compaction(
                    new UniversalCompaction(
                            options.maxSizeAmplificationPercent(),
                            options.sortedRunSizeRatio(),
                            options.numSortedRunCompactionTrigger(),
                            EarlyFullCompaction.create(options),
                            OffPeakHours.create(options)),
                    compactMaxInterval);
        }

        UniversalCompaction universal =
                new UniversalCompaction(
                        options.maxSizeAmplificationPercent(),
                        options.sortedRunSizeRatio(),
                        options.numSortedRunCompactionTrigger(),
                        EarlyFullCompaction.create(options),
                        OffPeakHours.create(options));
        if (options.compactionForceUpLevel0()) {
            return new ForceUpLevel0Compaction(universal, null);
        } else {
            return universal;
        }
    }

    private CompactManager createCompactManager(
            BinaryRow partition,
            int bucket,
            CompactStrategy compactStrategy,
            ExecutorService compactExecutor,
            Levels levels,
            @Nullable BucketedDvMaintainer dvMaintainer) {
        if (options.writeOnly()) {
            return new NoopCompactManager();
        } else {
            Comparator<InternalRow> keyComparator = keyComparatorSupplier.get();
            @Nullable FieldsComparator userDefinedSeqComparator = udsComparatorSupplier.get();
            CompactRewriter rewriter =
                    createRewriter(
                            partition,
                            bucket,
                            keyComparator,
                            userDefinedSeqComparator,
                            levels,
                            dvMaintainer);
            return new MergeTreeCompactManager(
                    compactExecutor,
                    levels,
                    compactStrategy,
                    keyComparator,
                    options.compactionFileSize(true),
                    options.numSortedRunStopTrigger(),
                    rewriter,
                    compactionMetrics == null
                            ? null
                            : compactionMetrics.createReporter(partition, bucket),
                    dvMaintainer,
                    options.prepareCommitWaitCompaction(),
                    options.needLookup(),
                    recordLevelExpire,
                    options.forceRewriteAllFiles(),
                    options.isChainTable());
        }
    }

    /**
     * 创建压缩重写器（Compaction Rewriter）
     *
     * <p>这是选择不同 Changelog 生成策略的核心方法：
     * <ul>
     *   <li>FULL_COMPACTION 模式：创建 FullChangelogMergeTreeCompactRewriter
     *   <li>LOOKUP 模式：创建 LookupMergeTreeCompactRewriter
     *   <li>INPUT 模式或无 changelog：创建普通的 MergeTreeCompactRewriter
     * </ul>
     *
     * @param partition                分区信息
     * @param bucket                   桶号
     * @param keyComparator            键比较器
     * @param userDefinedSeqComparator 用户自定义序列号比较器
     * @param levels                   LSM 树层级信息
     * @param dvMaintainer             删除向量维护器
     * @return 对应模式的压缩重写器
     */
    private MergeTreeCompactRewriter createRewriter(
            BinaryRow partition,
            int bucket,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            Levels levels,
            @Nullable BucketedDvMaintainer dvMaintainer) {
        // 准备公共组件
        DeletionVector.Factory dvFactory = DeletionVector.factory(dvMaintainer);
        KeyValueFileReaderFactory keyReaderFactory =
                readerFactoryBuilder.build(partition, bucket, dvFactory);
        FileReaderFactory<KeyValue> readerFactory = keyReaderFactory;
        if (recordLevelExpire != null) {
            readerFactory = recordLevelExpire.wrap(readerFactory);
        }
        KeyValueFileWriterFactory writerFactory =
                writerFactoryBuilder.build(partition, bucket, options);
        MergeSorter mergeSorter = new MergeSorter(options, keyType, valueType, ioManager);
        int maxLevel = options.numLevels() - 1;
        MergeEngine mergeEngine = options.mergeEngine();
        ChangelogProducer changelogProducer = options.changelogProducer();
        LookupStrategy lookupStrategy = options.lookupStrategy();

        // ========== FULL_COMPACTION 模式：全量压缩生成 Changelog ==========
        // 在每次全量压缩时生成 changelog
        // 通过比较压缩前后的值来产生 changelog 记录
        if (changelogProducer.equals(FULL_COMPACTION)) {
            return new FullChangelogMergeTreeCompactRewriter(
                    maxLevel,
                    mergeEngine,
                    readerFactory,
                    writerFactory,
                    keyComparator,
                    userDefinedSeqComparator,
                    mfFactory,
                    mergeSorter,
                    logDedupEqualSupplier.get());
        }
        // ========== LOOKUP 模式：通过查找生成 Changelog ==========
        // 在 Level-0 文件参与压缩时，通过 lookup 上层数据生成 changelog
        else if (lookupStrategy.needLookup) {
            // 配置持久化处理器和合并函数包装器工厂
            PersistProcessor.Factory<?> processorFactory;
            LookupMergeTreeCompactRewriter.MergeFunctionWrapperFactory<?> wrapperFactory;
            FileReaderFactory<KeyValue> lookupReaderFactory = readerFactory;

            // First Row 引擎的特殊处理
            if (lookupStrategy.isFirstRow) {
                if (options.deletionVectorsEnabled()) {
                    throw new UnsupportedOperationException(
                            "First row merge engine does not need deletion vectors because there is no deletion of old data in this merge engine.");
                }
                // First Row 只需要存储位置信息，不需要存储完整的值
                lookupReaderFactory =
                        readerFactoryBuilder
                                .copyWithoutProjection()
                                .withReadValueType(RowType.of())
                                .build(partition, bucket, dvFactory);
                processorFactory = PersistEmptyProcessor.factory();
                wrapperFactory = new FirstRowMergeFunctionWrapperFactory();
            } else {
                // LOOKUP 模式：根据是否使用 Deletion Vector 选择不同的持久化策略
                if (lookupStrategy.deletionVector) {
                    // Deletion Vector 模式：需要存储值和位置信息用于标记删除
                    if (lookupStrategy.produceChangelog
                            || mergeEngine != DEDUPLICATE
                            || !options.sequenceField().isEmpty()) {
                        // 需要生成 changelog 或使用聚合引擎：存储完整的值和位置
                        processorFactory = PersistValueAndPosProcessor.factory(valueType);
                    } else {
                        // 简单去重：只存储位置信息即可
                        processorFactory = PersistPositionProcessor.factory();
                    }
                } else {
                    // 非 Deletion Vector 模式：存储完整的值用于比较
                    processorFactory = PersistValueProcessor.factory(valueType);
                }
                // 创建 LOOKUP 模式的合并函数包装器工厂
                wrapperFactory =
                        new LookupMergeFunctionWrapperFactory<>(
                                logDedupEqualSupplier.get(),
                                lookupStrategy,
                                UserDefinedSeqComparator.create(valueType, options));
            }

            // 创建 LookupLevels 用于查找历史数据
            LookupLevels<?> lookupLevels =
                    createLookupLevels(
                            partition, bucket, levels, processorFactory, lookupReaderFactory);

            // 可选：创建远程 lookup 文件管理器
            RemoteLookupFileManager<?> remoteLookupFileManager = null;
            if (options.lookupRemoteFileEnabled()) {
                remoteLookupFileManager =
                        new RemoteLookupFileManager<>(
                                fileIO,
                                keyReaderFactory.pathFactory(),
                                lookupLevels,
                                options.lookupRemoteLevelThreshold());
            }

            // 创建 LOOKUP 模式的压缩重写器
            return new LookupMergeTreeCompactRewriter(
                    maxLevel,
                    mergeEngine,
                    lookupLevels,
                    readerFactory,
                    writerFactory,
                    keyComparator,
                    userDefinedSeqComparator,
                    mfFactory,
                    mergeSorter,
                    wrapperFactory,
                    lookupStrategy.produceChangelog,  // 是否生成 changelog
                    dvMaintainer,
                    options,
                    remoteLookupFileManager);
        }
        // ========== INPUT/NONE 模式：不在压缩时生成 Changelog ==========
        // INPUT 模式在写入时已生成 changelog，NONE 模式不生成 changelog
        else {
            return new MergeTreeCompactRewriter(
                    readerFactory,
                    writerFactory,
                    keyComparator,
                    userDefinedSeqComparator,
                    mfFactory,
                    mergeSorter);
        }
    }

    private <T> LookupLevels<T> createLookupLevels(
            BinaryRow partition,
            int bucket,
            Levels levels,
            PersistProcessor.Factory<T> processorFactory,
            FileReaderFactory<KeyValue> readerFactory) {
        if (ioManager == null) {
            throw new RuntimeException(
                    "Can not use lookup, there is no temp disk directory to use.");
        }
        LookupStoreFactory lookupStoreFactory =
                LookupStoreFactory.create(
                        options,
                        cacheManager,
                        new RowCompactedSerializer(keyType).createSliceComparator());
        Options options = this.options.toConfiguration();
        if (lookupFileCache == null) {
            lookupFileCache =
                    LookupFile.createCache(
                            options.get(CoreOptions.LOOKUP_CACHE_FILE_RETENTION),
                            options.get(CoreOptions.LOOKUP_CACHE_MAX_DISK_SIZE));
        }
        return new LookupLevels<>(
                schemaId -> schemaManager.schema(schemaId).logicalRowType(),
                schema.id(),
                levels,
                keyComparatorSupplier.get(),
                keyType,
                processorFactory,
                LookupSerializerFactory.INSTANCE.get(),
                readerFactory::createRecordReader,
                file ->
                        ioManager
                                .createChannel(
                                        localFilePrefix(partitionType, partition, bucket, file))
                                .getPathFile(),
                lookupStoreFactory,
                bfGenerator(options),
                lookupFileCache);
    }

    @Override
    protected Function<WriterContainer<KeyValue>, Boolean> createWriterCleanChecker() {
        return createConflictAwareWriterCleanChecker(commitUser, restore);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (lookupFileCache != null) {
            lookupFileCache.invalidateAll();
        }
    }
}
