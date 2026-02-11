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

import org.apache.paimon.CoreOptions.ExternalPathStrategy;
import org.apache.paimon.catalog.RenamingSnapshotCommit;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.catalog.TableRollback;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.GlobalIndexScanBuilder;
import org.apache.paimon.globalindex.GlobalIndexScanBuilderImpl;
import org.apache.paimon.iceberg.IcebergCommitCallback;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.metastore.AddPartitionCommitCallback;
import org.apache.paimon.metastore.AddPartitionTagCallback;
import org.apache.paimon.metastore.ChainTableOverwriteCommitCallback;
import org.apache.paimon.metastore.TagPreviewCommitCallback;
import org.apache.paimon.operation.ChangelogDeletion;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.operation.ManifestsReader;
import org.apache.paimon.operation.PartitionExpire;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.operation.TagDeletion;
import org.apache.paimon.operation.commit.CommitRollback;
import org.apache.paimon.operation.commit.ConflictDetection;
import org.apache.paimon.operation.commit.StrictModeChecker;
import org.apache.paimon.partition.PartitionExpireStrategy;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.service.ServiceManager;
import org.apache.paimon.stats.StatsFile;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PartitionHandler;
import org.apache.paimon.table.sink.CallbackUtils;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.tag.SuccessFileTagCallback;
import org.apache.paimon.tag.TagAutoManager;
import org.apache.paimon.tag.TagPreview;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.IndexFilePathFactories;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.partition.PartitionExpireStrategy.createPartitionExpireStrategy;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 抽象文件存储基类
 *
 * <p>AbstractFileStore 提供了 FileStore 接口的通用实现，包含所有表类型共享的逻辑。
 *
 * <p>主要职责：
 * <ul>
 *   <li>管理文件路径：通过 {@link FileStorePathFactory} 生成各类文件路径
 *   <li>创建工厂对象：Manifest、Snapshot、Index 等文件的工厂
 *   <li>创建操作对象：Commit、Scan、Read、Write 等核心操作
 *   <li>管理回调：CommitCallback、TagCallback 等
 *   <li>支持外部存储路径（多存储介质）
 * </ul>
 *
 * <p>子类实现：
 * <ul>
 *   <li>{@link KeyValueFileStore}：Primary Key 表，支持 MergeTree
 *   <li>{@link AppendOnlyFileStore}：Append-Only 表
 * </ul>
 *
 * <p>缓存机制：
 * <ul>
 *   <li>readManifestCache：Manifest 文件缓存（Segments 级别）
 *   <li>snapshotCache：Snapshot 文件缓存
 * </ul>
 *
 * @param <T> 读写记录的类型（KeyValue 或 InternalRow）
 */
abstract class AbstractFileStore<T> implements FileStore<T> {

    /** 文件 I/O 操作接口 */
    protected final FileIO fileIO;
    /** Schema 管理器 */
    protected final SchemaManager schemaManager;
    /** 表的 Schema */
    protected final TableSchema schema;
    /** 表名 */
    protected final String tableName;
    /** 表的配置选项 */
    protected final CoreOptions options;
    /** 分区类型 */
    protected final RowType partitionType;
    /** Catalog 环境（提供 Snapshot 加载器、分区处理器等） */
    protected final CatalogEnvironment catalogEnvironment;

    /** Manifest 文件缓存（Segments 级别） */
    @Nullable private SegmentsCache<Path> readManifestCache;
    /** Snapshot 文件缓存 */
    @Nullable private Cache<Path, Snapshot> snapshotCache;

    protected AbstractFileStore(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            String tableName,
            CoreOptions options,
            RowType partitionType,
            CatalogEnvironment catalogEnvironment) {
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.schema = schema;
        this.tableName = tableName;
        this.options = options;
        this.partitionType = partitionType;
        this.catalogEnvironment = catalogEnvironment;
    }

    /**
     * 获取路径工厂（使用默认文件格式）
     *
     * @return 路径工厂实例
     */
    @Override
    public FileStorePathFactory pathFactory() {
        return pathFactory(options, options.fileFormatString());
    }

    /**
     * 创建路径工厂（指定文件格式）
     *
     * <p>路径工厂负责生成所有文件的路径，包括：
     * <ul>
     *   <li>数据文件路径
     *   <li>Manifest 文件路径
     *   <li>索引文件路径
     *   <li>外部存储路径（多存储介质支持）
     * </ul>
     *
     * @param options 配置选项
     * @param format 文件格式
     * @return 路径工厂实例
     */
    protected FileStorePathFactory pathFactory(CoreOptions options, String format) {
        return new FileStorePathFactory(
                options.path(),
                partitionType,
                options.partitionDefaultName(),
                format,
                options.dataFilePrefix(),
                options.changelogFilePrefix(),
                options.legacyPartitionName(),
                options.fileSuffixIncludeCompression(),
                options.fileCompression(),
                options.dataFilePathDirectory(),
                createExternalPaths(),
                options.externalPathStrategy(),
                options.indexFileInDataFileDir(),
                options.globalIndexExternalPath());
    }

    /**
     * 创建外部存储路径列表
     *
     * <p>外部存储路径用于将数据文件存储到多个存储介质，例如：
     * <ul>
     *   <li>HDFS + OSS（对象存储）
     *   <li>本地文件系统 + S3
     * </ul>
     *
     * <p>根据 {@link ExternalPathStrategy} 决定使用哪些外部路径：
     * <ul>
     *   <li>NONE：不使用外部路径
     *   <li>SPECIFIC_FS：只使用指定文件系统的路径
     *   <li>ALL：使用所有配置的外部路径
     * </ul>
     *
     * @return 外部路径列表
     */
    private List<Path> createExternalPaths() {
        String externalPaths = options.dataFileExternalPaths();
        ExternalPathStrategy strategy = options.externalPathStrategy();
        if (externalPaths == null
                || externalPaths.isEmpty()
                || strategy == ExternalPathStrategy.NONE) {
            return Collections.emptyList();
        }

        String specificFS = options.externalSpecificFS();

        List<Path> paths = new ArrayList<>();
        for (String pathString : externalPaths.split(",")) {
            Path path = new Path(pathString.trim());
            String scheme = path.toUri().getScheme();
            if (scheme == null) {
                throw new IllegalArgumentException("scheme should not be null: " + path);
            }

            // SPECIFIC_FS 策略：只使用指定文件系统的路径
            if (strategy == ExternalPathStrategy.SPECIFIC_FS) {
                checkArgument(
                        specificFS != null,
                        "External path specificFS should not be null when strategy is specificFS.");
                if (scheme.equalsIgnoreCase(specificFS)) {
                    paths.add(path);
                }
            } else {
                paths.add(path);
            }
        }
        checkArgument(!paths.isEmpty(), "External paths should not be empty");
        return paths;
    }

    /**
     * 创建快照管理器
     *
     * <p>快照管理器负责：
     * <ul>
     *   <li>读取和写入 Snapshot 文件
     *   <li>查找最新快照
     *   <li>管理快照的生命周期
     * </ul>
     *
     * @return 快照管理器实例
     */
    @Override
    public SnapshotManager snapshotManager() {
        return new SnapshotManager(
                fileIO,
                options.path(),
                options.branch(),
                catalogEnvironment.snapshotLoader(),
                snapshotCache);
    }

    /**
     * 创建 Changelog 管理器
     *
     * <p>Changelog 管理器负责读取和写入独立的 Changelog 文件。
     *
     * @return Changelog 管理器实例
     */
    @Override
    public ChangelogManager changelogManager() {
        return new ChangelogManager(fileIO, options.path(), options.branch());
    }

    /**
     * 创建 Manifest File 工厂
     *
     * <p>Manifest File 记录数据文件的元信息，包括文件路径、数据范围、统计信息等。
     *
     * @return Manifest File 工厂实例
     */
    @Override
    public ManifestFile.Factory manifestFileFactory() {
        return new ManifestFile.Factory(
                fileIO,
                schemaManager,
                partitionType,
                FileFormat.manifestFormat(options),
                options.manifestCompression(),
                pathFactory(),
                options.manifestTargetSize().getBytes(),
                readManifestCache);
    }

    /**
     * 创建 Manifest List 工厂
     *
     * <p>Manifest List 记录 Manifest File 的元信息，是 Snapshot 的核心组成部分。
     *
     * @return Manifest List 工厂实例
     */
    @Override
    public ManifestList.Factory manifestListFactory() {
        return new ManifestList.Factory(
                fileIO,
                FileFormat.manifestFormat(options),
                options.manifestCompression(),
                pathFactory(),
                readManifestCache);
    }

    /**
     * 创建索引 Manifest 工厂
     *
     * <p>索引 Manifest 记录索引文件的元信息（如删除向量）。
     *
     * @return 索引 Manifest 工厂实例
     */
    @Override
    public IndexManifestFile.Factory indexManifestFileFactory() {
        return new IndexManifestFile.Factory(
                fileIO,
                FileFormat.manifestFormat(options),
                options.manifestCompression(),
                pathFactory(),
                readManifestCache);
    }

    /**
     * 创建索引文件处理器
     *
     * <p>索引文件处理器负责管理索引文件，包括：
     * <ul>
     *   <li>删除向量（Deletion Vector）
     *   <li>动态分桶索引
     * </ul>
     *
     * @return 索引文件处理器实例
     */
    @Override
    public IndexFileHandler newIndexFileHandler() {
        return new IndexFileHandler(
                fileIO,
                snapshotManager(),
                indexManifestFileFactory().create(),
                new IndexFilePathFactories(pathFactory()),
                options.dvIndexFileTargetSize(),
                options.deletionVectorBitmap64());
    }

    /**
     * 创建统计文件处理器
     *
     * <p>统计文件处理器负责读取和写入统计信息文件。
     *
     * @return 统计文件处理器实例
     */
    @Override
    public StatsFileHandler newStatsFileHandler() {
        return new StatsFileHandler(
                snapshotManager(),
                schemaManager,
                new StatsFile(fileIO, pathFactory().statsFileFactory()));
    }

    /**
     * 创建 Manifest 读取器
     *
     * <p>Manifest 读取器用于从 Snapshot 中读取 Manifest 文件。
     *
     * @return Manifest 读取器实例
     */
    protected ManifestsReader newManifestsReader() {
        return new ManifestsReader(
                partitionType,
                options.partitionDefaultName(),
                snapshotManager(),
                manifestListFactory());
    }

    @Override
    public RowType partitionType() {
        return partitionType;
    }

    @Override
    public CoreOptions options() {
        return options;
    }

    @Override
    public boolean mergeSchema(RowType rowType, boolean allowExplicitCast) {
        return schemaManager.mergeSchema(rowType, allowExplicitCast);
    }

    /**
     * 创建提交器
     *
     * <p>提交器是 Paimon 写入流程的核心组件，负责：
     * <ul>
     *   <li>生成新的 Snapshot 文件
     *   <li>更新 Manifest 文件（合并、压缩）
     *   <li>执行冲突检测（并发写入场景）
     *   <li>触发回调（如同步到 Metastore、Iceberg）
     *   <li>执行 Strict Mode 检查
     * </ul>
     *
     * <p>回调类型：
     * <ul>
     *   <li>AddPartitionCommitCallback：同步分区到 Metastore
     *   <li>IcebergCommitCallback：同步元数据到 Iceberg
     *   <li>ChainTableOverwriteCommitCallback：处理链表覆盖
     *   <li>自定义 CommitCallback
     * </ul>
     *
     * @param commitUser 提交用户标识
     * @param table 表实例
     * @return 提交器实例
     */
    @Override
    public FileStoreCommitImpl newCommit(String commitUser, FileStoreTable table) {
        SnapshotManager snapshotManager = snapshotManager();
        // 获取快照提交实现（由 Catalog 提供）
        SnapshotCommit snapshotCommit = catalogEnvironment.snapshotCommit(snapshotManager);
        if (snapshotCommit == null) {
            snapshotCommit = new RenamingSnapshotCommit(snapshotManager, Lock.empty());
        }
        // 创建冲突检测工厂
        ConflictDetection.Factory conflictDetectFactory =
                scanner ->
                        new ConflictDetection(
                                tableName,
                                commitUser,
                                partitionType,
                                pathFactory(),
                                newKeyComparator(),
                                bucketMode(),
                                options.deletionVectorsEnabled(),
                                options.dataEvolutionEnabled(),
                                newIndexFileHandler(),
                                snapshotManager,
                                scanner);
        // 创建严格模式检查器
        StrictModeChecker strictModeChecker =
                StrictModeChecker.create(
                        snapshotManager,
                        commitUser,
                        this::newScan,
                        options.commitStrictModeLastSafeSnapshot().orElse(null));
        // 创建回滚处理器
        CommitRollback rollback = null;
        TableRollback tableRollback = catalogEnvironment.catalogTableRollback();
        if (tableRollback != null) {
            rollback = new CommitRollback(tableRollback);
        }
        return new FileStoreCommitImpl(
                snapshotCommit,
                fileIO,
                schemaManager,
                tableName,
                commitUser,
                partitionType,
                options,
                options.partitionDefaultName(),
                pathFactory(),
                snapshotManager,
                manifestFileFactory(),
                manifestListFactory(),
                indexManifestFileFactory(),
                newScan(),
                options.bucket(),
                options.manifestTargetSize(),
                options.manifestFullCompactionThresholdSize(),
                options.manifestMergeMinCount(),
                partitionType.getFieldCount() > 0 && options.dynamicPartitionOverwrite(),
                options.branch(),
                newStatsFileHandler(),
                bucketMode(),
                options.scanManifestParallelism(),
                createCommitCallbacks(commitUser, table),
                options.commitMaxRetries(),
                options.commitTimeout(),
                options.commitMinRetryWait(),
                options.commitMaxRetryWait(),
                options.rowTrackingEnabled(),
                options.commitDiscardDuplicateFiles(),
                conflictDetectFactory,
                strictModeChecker,
                rollback);
    }

    /**
     * 创建快照删除器
     *
     * <p>快照删除器负责：
     * <ul>
     *   <li>删除过期的 Snapshot 文件
     *   <li>删除不再被引用的数据文件
     *   <li>删除不再被引用的 Manifest 文件
     *   <li>删除不再被引用的索引文件
     * </ul>
     *
     * @return 快照删除器实例
     */
    @Override
    public SnapshotDeletion newSnapshotDeletion() {
        return new SnapshotDeletion(
                fileIO,
                pathFactory(),
                manifestFileFactory().create(),
                manifestListFactory().create(),
                newIndexFileHandler(),
                newStatsFileHandler(),
                options.changelogProducer() != CoreOptions.ChangelogProducer.NONE,
                options.cleanEmptyDirectories(),
                options.fileOperationThreadNum());
    }

    /**
     * 创建 Changelog 删除器
     *
     * <p>Changelog 删除器负责删除独立的 Changelog 文件及其关联的数据文件。
     *
     * @return Changelog 删除器实例
     */
    @Override
    public ChangelogDeletion newChangelogDeletion() {
        return new ChangelogDeletion(
                fileIO,
                pathFactory(),
                manifestFileFactory().create(),
                manifestListFactory().create(),
                newIndexFileHandler(),
                newStatsFileHandler(),
                options.cleanEmptyDirectories(),
                options.fileOperationThreadNum());
    }

    /**
     * 创建标签管理器
     *
     * @return 标签管理器实例
     */
    @Override
    public TagManager newTagManager() {
        return new TagManager(fileIO, options.path(), DEFAULT_MAIN_BRANCH, options);
    }

    /**
     * 创建标签删除器
     *
     * <p>标签删除器负责删除标签及其关联的数据文件。
     *
     * @return 标签删除器实例
     */
    @Override
    public TagDeletion newTagDeletion() {
        return new TagDeletion(
                fileIO,
                pathFactory(),
                manifestFileFactory().create(),
                manifestListFactory().create(),
                newIndexFileHandler(),
                newStatsFileHandler(),
                options.cleanEmptyDirectories(),
                options.fileOperationThreadNum());
    }

    /**
     * 创建主键比较器（由子类实现）
     *
     * <p>用于：
     * <ul>
     *   <li>KeyValueFileStore：比较主键
     *   <li>AppendOnlyFileStore：返回 null
     * </ul>
     *
     * @return 主键比较器（可能为 null）
     */
    public abstract Comparator<InternalRow> newKeyComparator();

    /**
     * 创建分区计算器
     *
     * <p>分区计算器负责根据记录计算分区路径。
     *
     * @return 分区计算器实例
     */
    @Override
    public InternalRowPartitionComputer partitionComputer() {
        return new InternalRowPartitionComputer(
                options.partitionDefaultName(),
                schema.logicalPartitionType(),
                schema.partitionKeys().toArray(new String[0]),
                options.legacyPartitionName());
    }

    /**
     * 创建提交回调列表
     *
     * <p>提交回调在快照提交成功后被触发，用于：
     * <ul>
     *   <li>同步分区到 Metastore（AddPartitionCommitCallback）
     *   <li>同步元数据到 Iceberg（IcebergCommitCallback）
     *   <li>处理链表覆盖（ChainTableOverwriteCommitCallback）
     *   <li>执行自定义逻辑
     * </ul>
     *
     * @param commitUser 提交用户标识
     * @param table 表实例
     * @return 提交回调列表
     */
    private List<CommitCallback> createCommitCallbacks(String commitUser, FileStoreTable table) {
        List<CommitCallback> callbacks = new ArrayList<>();

        // 1. 分区同步回调（需要 Metastore 支持）
        if (options.partitionedTableInMetastore() && !schema.partitionKeys().isEmpty()) {
            PartitionHandler partitionHandler = catalogEnvironment.partitionHandler();
            if (partitionHandler != null) {
                callbacks.add(
                        new AddPartitionCommitCallback(partitionHandler, partitionComputer()));
            }
        }

        // 2. 标签预览回调（Tag-to-Partition 功能）
        TagPreview tagPreview = TagPreview.create(options);
        if (options.tagToPartitionField() != null
                && tagPreview != null
                && schema.partitionKeys().isEmpty()) {
            PartitionHandler partitionHandler = catalogEnvironment.partitionHandler();
            if (partitionHandler != null) {
                TagPreviewCommitCallback callback =
                        new TagPreviewCommitCallback(
                                new AddPartitionTagCallback(
                                        partitionHandler, options.tagToPartitionField()),
                                tagPreview);
                callbacks.add(callback);
            }
        }

        // 3. Iceberg 元数据同步回调
        if (options.toConfiguration().get(IcebergOptions.METADATA_ICEBERG_STORAGE)
                != IcebergOptions.StorageType.DISABLED) {
            callbacks.add(new IcebergCommitCallback(table, commitUser));
        }

        // 4. 链表覆盖回调
        if (options.isChainTable()) {
            callbacks.add(new ChainTableOverwriteCommitCallback(table));
        }

        // 5. 自定义回调
        callbacks.addAll(CallbackUtils.loadCommitCallbacks(options, table));
        return callbacks;
    }

    /**
     * 创建分区过期器（使用默认配置）
     *
     * <p>如果满足以下条件，返回 null：
     * <ul>
     *   <li>未配置分区过期时间
     *   <li>表没有分区字段
     * </ul>
     *
     * @param commitUser 提交用户标识
     * @param table 表实例
     * @return 分区过期器实例（可能为 null）
     */
    @Override
    @Nullable
    public PartitionExpire newPartitionExpire(String commitUser, FileStoreTable table) {
        Duration partitionExpireTime = options.partitionExpireTime();
        if (partitionExpireTime == null || partitionType().getFieldCount() == 0) {
            return null;
        }

        return newPartitionExpire(
                commitUser,
                table,
                partitionExpireTime,
                options.partitionExpireCheckInterval(),
                createPartitionExpireStrategy(
                        options,
                        partitionType(),
                        catalogEnvironment.catalogLoader(),
                        catalogEnvironment.identifier()));
    }

    /**
     * 创建分区过期器（指定配置）
     *
     * <p>分区过期器负责：
     * <ul>
     *   <li>定期检查过期分区
     *   <li>删除过期分区的数据
     *   <li>同步删除 Metastore 中的分区
     * </ul>
     *
     * @param commitUser 提交用户标识
     * @param table 表实例
     * @param expirationTime 分区过期时间
     * @param checkInterval 检查间隔
     * @param expireStrategy 过期策略
     * @return 分区过期器实例
     */
    @Override
    public PartitionExpire newPartitionExpire(
            String commitUser,
            FileStoreTable table,
            Duration expirationTime,
            Duration checkInterval,
            PartitionExpireStrategy expireStrategy) {
        PartitionHandler partitionHandler = null;
        if (options.partitionedTableInMetastore()) {
            partitionHandler = catalogEnvironment.partitionHandler();
        }

        return new PartitionExpire(
                expirationTime,
                checkInterval,
                expireStrategy,
                newScan(),
                newCommit(commitUser, table),
                partitionHandler,
                options.endInputCheckPartitionExpire(),
                options.partitionExpireMaxNum(),
                options.partitionExpireBatchSize());
    }

    /**
     * 创建标签自动管理器
     *
     * <p>标签自动管理器负责：
     * <ul>
     *   <li>根据策略自动创建标签
     *   <li>自动删除过期标签
     * </ul>
     *
     * @param table 表实例
     * @return 标签自动管理器实例
     */
    @Override
    public TagAutoManager newTagAutoManager(FileStoreTable table) {
        return TagAutoManager.create(
                options,
                snapshotManager(),
                newTagManager(),
                newTagDeletion(),
                createTagCallbacks(table));
    }

    /**
     * 创建标签回调列表
     *
     * <p>标签回调在标签创建时被触发，用于：
     * <ul>
     *   <li>同步分区到 Metastore（Tag-to-Partition）
     *   <li>创建成功文件
     *   <li>同步元数据到 Iceberg
     * </ul>
     *
     * @param table 表实例
     * @return 标签回调列表
     */
    @Override
    public List<TagCallback> createTagCallbacks(FileStoreTable table) {
        List<TagCallback> callbacks = new ArrayList<>(CallbackUtils.loadTagCallbacks(options));
        String partitionField = options.tagToPartitionField();

        // 1. Tag-to-Partition 回调
        if (partitionField != null) {
            PartitionHandler partitionHandler = catalogEnvironment.partitionHandler();
            if (partitionHandler != null) {
                callbacks.add(new AddPartitionTagCallback(partitionHandler, partitionField));
            }
        }
        // 2. 成功文件回调
        if (options.tagCreateSuccessFile()) {
            callbacks.add(new SuccessFileTagCallback(fileIO, newTagManager().tagDirectory()));
        }
        // 3. Iceberg 元数据同步回调
        if (options.toConfiguration().get(IcebergOptions.METADATA_ICEBERG_STORAGE)
                != IcebergOptions.StorageType.DISABLED) {
            callbacks.add(new IcebergCommitCallback(table, ""));
        }
        return callbacks;
    }

    /**
     * 创建服务管理器
     *
     * @return 服务管理器实例
     */
    @Override
    public ServiceManager newServiceManager() {
        return new ServiceManager(fileIO, options.path());
    }

    /**
     * 设置 Manifest 缓存
     *
     * @param manifestCache Manifest 缓存实例
     */
    @Override
    public void setManifestCache(SegmentsCache<Path> manifestCache) {
        this.readManifestCache = manifestCache;
    }

    /**
     * 设置快照缓存
     *
     * @param cache 快照缓存实例
     */
    @Override
    public void setSnapshotCache(Cache<Path, Snapshot> cache) {
        this.snapshotCache = cache;
    }

    /**
     * 创建全局索引扫描构建器
     *
     * @return 全局索引扫描构建器实例
     */
    @Override
    public GlobalIndexScanBuilder newGlobalIndexScanBuilder() {
        return new GlobalIndexScanBuilderImpl(
                options.toConfiguration(),
                schema.logicalRowType(),
                fileIO,
                pathFactory().globalIndexFileFactory(),
                snapshotManager(),
                newIndexFileHandler());
    }
}
