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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.DataEvolutionBatchScan;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaValidation;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.sink.AppendTableRowKeyExtractor;
import org.apache.paimon.table.sink.DynamicBucketRowKeyExtractor;
import org.apache.paimon.table.sink.FixedBucketRowKeyExtractor;
import org.apache.paimon.table.sink.FixedBucketWriteSelector;
import org.apache.paimon.table.sink.PostponeBucketRowKeyExtractor;
import org.apache.paimon.table.sink.RowKeyExtractor;
import org.apache.paimon.table.sink.RowKindGenerator;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.WriteSelector;
import org.apache.paimon.table.source.DataTableBatchScan;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.DataTableStreamScan;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.SnapshotReaderImpl;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
import org.apache.paimon.tag.TagAutoManager;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.CatalogBranchManager;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.DVMetaCache;
import org.apache.paimon.utils.FileSystemBranchManager;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.SimpleFileReader;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.SnapshotNotExistException;
import org.apache.paimon.utils.StringUtils;
import org.apache.paimon.utils.TagManager;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.function.BiConsumer;

import static org.apache.paimon.CoreOptions.PATH;

/**
 * 抽象 {@link FileStoreTable} 实现类。
 *
 * <p>AbstractFileStoreTable 是 FileStoreTable 的抽象基类，提供了大部分公共实现：
 * <ul>
 *   <li>缓存管理：Manifest、Snapshot、Statistics、DV 元数据缓存
 *   <li>快照操作：获取最新快照、根据 ID 获取快照
 *   <li>表复制：支持动态选项、时间旅行、Schema 演化
 *   <li>版本管理：标签和分支的创建、删除、回滚
 *   <li>快照过期：创建快照过期和 Changelog 过期管理器
 *   <li>表提交：创建 TableCommit 并配置过期策略
 * </ul>
 *
 * <h3>继承层次</h3>
 * <pre>
 * AbstractFileStoreTable (抽象基类)
 *   ├── PrimaryKeyFileStoreTable (主键表实现)
 *   └── AppendOnlyFileStoreTable (追加表实现)
 * </pre>
 *
 * <h3>子类需要实现的方法</h3>
 * <ul>
 *   <li>store()：返回底层的 FileStore（KeyValueFileStore 或 AppendOnlyFileStore）
 *   <li>newRead()：创建表读取器
 *   <li>newWrite()：创建表写入器
 *   <li>splitGenerator()：创建分片生成器
 *   <li>nonPartitionFilterConsumer()：处理非分区过滤器
 *   <li>newLocalTableQuery()：创建本地表查询器（主键表支持）
 *   <li>supportStreamingReadOverwrite()：是否支持流式读取 Overwrite
 * </ul>
 *
 * <h3>设计要点</h3>
 * <ul>
 *   <li>延迟初始化：FileStore 使用 transient 修饰，按需创建
 *   <li>缓存支持：支持多种缓存，提升读取性能
 *   <li>不可变性检查：动态选项修改时检查配置项的可变性
 *   <li>时间旅行：支持通过动态选项切换到历史 Schema
 *   <li>分支管理：支持文件系统和 Catalog 两种分支管理方式
 * </ul>
 */
abstract class AbstractFileStoreTable implements FileStoreTable {

    private static final long serialVersionUID = 1L;

    /** 文件 IO 接口。 */
    protected final FileIO fileIO;

    /** 表的存储路径。 */
    protected final Path path;

    /** 表的 Schema。 */
    protected final TableSchema tableSchema;

    /** Catalog 环境，包含表的标识符、锁工厂等。 */
    protected final CatalogEnvironment catalogEnvironment;

    /** Manifest 缓存，缓存已读取的 Manifest 文件内容（transient 不序列化）。 */
    @Nullable protected transient SegmentsCache<Path> manifestCache;

    /** 快照缓存，缓存已读取的快照对象（transient 不序列化）。 */
    @Nullable protected transient Cache<Path, Snapshot> snapshotCache;

    /** 统计信息缓存，缓存表的统计信息（transient 不序列化）。 */
    @Nullable protected transient Cache<String, Statistics> statsCache;

    /** Deletion Vector 元数据缓存（transient 不序列化）。 */
    @Nullable protected transient DVMetaCache dvmetaCache;

    /**
     * 构造 AbstractFileStoreTable。
     *
     * <p>构造过程中会确保 tableSchema 包含 path 配置项，如果没有则自动添加。
     * 这保证了表总是可以通过 options 获取自己的路径。
     *
     * @param fileIO 文件 IO 接口
     * @param path 表的存储路径
     * @param tableSchema 表的 Schema
     * @param catalogEnvironment Catalog 环境
     */
    protected AbstractFileStoreTable(
            FileIO fileIO,
            Path path,
            TableSchema tableSchema,
            CatalogEnvironment catalogEnvironment) {
        this.fileIO = fileIO;
        this.path = path;
        // 确保 tableSchema 总是包含 path 配置
        if (!tableSchema.options().containsKey(PATH.key())) {
            // make sure table is always available
            Map<String, String> newOptions = new HashMap<>(tableSchema.options());
            newOptions.put(PATH.key(), path.toString());
            tableSchema = tableSchema.copy(newOptions);
        }
        this.tableSchema = tableSchema;
        this.catalogEnvironment = catalogEnvironment;
    }

    /**
     * 返回当前分支名称。
     *
     * <p>分支名称从配置项 {@link CoreOptions#BRANCH} 获取，默认为主分支。
     *
     * @return 当前分支名称
     */
    public String currentBranch() {
        return CoreOptions.branch(options());
    }

    /**
     * 设置 Manifest 缓存并同步到底层 FileStore。
     *
     * @param manifestCache Manifest 缓存实例
     */
    @Override
    public void setManifestCache(SegmentsCache<Path> manifestCache) {
        this.manifestCache = manifestCache;
        store().setManifestCache(manifestCache);
    }

    /**
     * 获取 Manifest 缓存。
     *
     * @return Manifest 缓存实例，如果未设置则返回 null
     */
    @Nullable
    @Override
    public SegmentsCache<Path> getManifestCache() {
        return manifestCache;
    }

    /**
     * 设置快照缓存并同步到底层 FileStore。
     *
     * @param cache 快照缓存实例
     */
    @Override
    public void setSnapshotCache(Cache<Path, Snapshot> cache) {
        this.snapshotCache = cache;
        store().setSnapshotCache(cache);
    }

    /**
     * 设置统计信息缓存。
     *
     * @param cache 统计信息缓存实例
     */
    @Override
    public void setStatsCache(Cache<String, Statistics> cache) {
        this.statsCache = cache;
    }

    /**
     * 设置 Deletion Vector 元数据缓存。
     *
     * @param cache DV 元数据缓存实例
     */
    @Override
    public void setDVMetaCache(DVMetaCache cache) {
        this.dvmetaCache = cache;
    }

    /**
     * 获取最新快照。
     *
     * <p>从 SnapshotManager 获取最新快照，可能返回 null（表为空）。
     *
     * @return 包含最新快照的 Optional
     */
    @Override
    public Optional<Snapshot> latestSnapshot() {
        Snapshot snapshot = store().snapshotManager().latestSnapshot();
        return Optional.ofNullable(snapshot);
    }

    /**
     * 根据快照 ID 获取快照。
     *
     * @param snapshotId 快照 ID
     * @return Snapshot 对象
     * @throws SnapshotNotExistException 如果快照不存在
     */
    @Override
    public Snapshot snapshot(long snapshotId) {
        return store().snapshotManager().snapshot(snapshotId);
    }

    /**
     * 创建 Manifest List 文件读取器。
     *
     * @return ManifestFileMeta 读取器
     */
    @Override
    public SimpleFileReader<ManifestFileMeta> manifestListReader() {
        return store().manifestListFactory().create();
    }

    /**
     * 创建 Manifest 文件读取器。
     *
     * @return ManifestEntry 读取器
     */
    @Override
    public SimpleFileReader<ManifestEntry> manifestFileReader() {
        return store().manifestFileFactory().create();
    }

    /**
     * 创建索引 Manifest 文件读取器。
     *
     * @return IndexManifestEntry 读取器
     */
    @Override
    public SimpleFileReader<IndexManifestEntry> indexManifestFileReader() {
        return store().indexManifestFileFactory().create();
    }

    /**
     * 返回表名（不含数据库名）。
     *
     * @return 表名
     */
    @Override
    public String name() {
        return identifier().getObjectName();
    }

    /**
     * 返回完整表名（数据库名.表名）。
     *
     * @return 完整表名
     */
    @Override
    public String fullName() {
        return identifier().getFullName();
    }

    /**
     * 返回表的标识符（Identifier）。
     *
     * <p>Identifier 包含数据库名、表名、分支名和系统表名。
     * 如果 CatalogEnvironment 中没有 identifier，则从表路径解析。
     *
     * @return Identifier 对象
     */
    public Identifier identifier() {
        Identifier identifier = catalogEnvironment.identifier();
        return identifier == null
                ? SchemaManager.identifierFromPath(location().toString(), true, currentBranch())
                : identifier;
    }

    /**
     * 返回表的 UUID。
     *
     * <p>UUID 格式：fullName.earliestCreationTime
     *
     * @return 表的 UUID
     */
    @Override
    public String uuid() {
        if (catalogEnvironment.uuid() != null) {
            return catalogEnvironment.uuid();
        }
        long earliestCreationTime = schemaManager().earliestCreationTime();
        return fullName() + "." + earliestCreationTime;
    }

    /**
     * 返回表的统计信息。
     *
     * <p>统计信息从最新快照（或时间旅行快照）的统计文件中读取，并缓存在 statsCache 中。
     *
     * @return 包含统计信息的 Optional
     */
    @Override
    public Optional<Statistics> statistics() {
        Snapshot snapshot = TimeTravelUtil.tryTravelOrLatest(this);
        if (snapshot != null) {
            String file = snapshot.statistics();
            if (file == null) {
                return Optional.empty();
            }
            // 优先从缓存获取
            if (statsCache != null) {
                Statistics stats = statsCache.getIfPresent(file);
                if (stats != null) {
                    return Optional.of(stats);
                }
            }
            // 从文件读取并缓存
            Statistics stats = store().newStatsFileHandler().readStats(file);
            if (statsCache != null) {
                statsCache.put(file, stats);
            }
            return Optional.of(stats);
        }
        return Optional.empty();
    }

    /**
     * 创建可选的写入选择器。
     *
     * <p>目前只有 HASH_FIXED 模式需要写入选择器。
     *
     * @return 包含 WriteSelector 的 Optional
     */
    @Override
    public Optional<WriteSelector> newWriteSelector() {
        switch (bucketMode()) {
            case HASH_FIXED:
                return Optional.of(new FixedBucketWriteSelector(schema()));
            case BUCKET_UNAWARE:
            case POSTPONE_MODE:
                return Optional.empty();
            default:
                throw new UnsupportedOperationException(
                        "Currently, write selector does not support table mode: " + bucketMode());
        }
    }

    /**
     * 返回 Catalog 环境。
     *
     * @return CatalogEnvironment 对象
     */
    @Override
    public CatalogEnvironment catalogEnvironment() {
        return catalogEnvironment;
    }

    /**
     * 创建新的 Catalog 环境，用于指定分支。
     *
     * <p>复制当前的 CatalogEnvironment，但修改 Identifier 中的分支名。
     *
     * @param branch 目标分支名称
     * @return 新的 CatalogEnvironment
     */
    protected CatalogEnvironment newCatalogEnvironment(String branch) {
        Identifier identifier = identifier();
        return catalogEnvironment.copy(
                new Identifier(
                        identifier.getDatabaseName(),
                        identifier.getTableName(),
                        branch,
                        identifier.getSystemTableName()));
    }

    /**
     * 创建行键提取器。
     *
     * <p>根据不同的桶模式创建不同的行键提取器：
     * <ul>
     *   <li>HASH_FIXED：FixedBucketRowKeyExtractor
     *   <li>HASH_DYNAMIC/KEY_DYNAMIC：DynamicBucketRowKeyExtractor
     *   <li>BUCKET_UNAWARE：AppendTableRowKeyExtractor
     *   <li>POSTPONE_MODE：PostponeBucketRowKeyExtractor
     * </ul>
     *
     * @return RowKeyExtractor 实例
     */
    public RowKeyExtractor createRowKeyExtractor() {
        switch (bucketMode()) {
            case HASH_FIXED:
                return new FixedBucketRowKeyExtractor(schema());
            case HASH_DYNAMIC:
            case KEY_DYNAMIC:
                return new DynamicBucketRowKeyExtractor(schema());
            case BUCKET_UNAWARE:
                return new AppendTableRowKeyExtractor(schema());
            case POSTPONE_MODE:
                return new PostponeBucketRowKeyExtractor(schema());
            default:
                throw new UnsupportedOperationException("Unsupported mode: " + bucketMode());
        }
    }

    /**
     * 创建新的快照读取器。
     *
     * <p>SnapshotReader 是读取表数据的核心组件，支持：
     * <ul>
     *   <li>批量扫描和流式扫描
     *   <li>快照隔离读取
     *   <li>增量读取
     *   <li>Changelog 读取
     * </ul>
     *
     * @return SnapshotReader 实例
     */
    @Override
    public SnapshotReader newSnapshotReader() {
        return new SnapshotReaderImpl(
                store().newScan(),
                tableSchema,
                coreOptions(),
                snapshotManager(),
                changelogManager(),
                splitGenerator(),
                nonPartitionFilterConsumer(),
                store().pathFactory(),
                name(),
                store().newIndexFileHandler(),
                dvmetaCache);
    }

    /**
     * 创建新的数据表扫描器。
     *
     * <p>如果启用了数据演化（data evolution），会用 DataEvolutionBatchScan 包装
     * 标准的 DataTableBatchScan，以支持 Schema 演化场景。
     *
     * @return DataTableScan 实例
     */
    @Override
    public DataTableScan newScan() {
        DataTableBatchScan scan =
                new DataTableBatchScan(
                        tableSchema,
                        schemaManager(),
                        coreOptions(),
                        newSnapshotReader(),
                        catalogEnvironment.tableQueryAuth(coreOptions()));
        // 如果启用数据演化，包装为 DataEvolutionBatchScan
        if (coreOptions().dataEvolutionEnabled()) {
            return new DataEvolutionBatchScan(this, scan);
        }
        return scan;
    }

    /**
     * 创建新的流式数据表扫描器。
     *
     * <p>流式扫描器用于持续监控表的新增数据。
     *
     * @return StreamDataTableScan 实例
     */
    @Override
    public StreamDataTableScan newStreamScan() {
        return new DataTableStreamScan(
                tableSchema,
                coreOptions(),
                newSnapshotReader(),
                snapshotManager(),
                changelogManager(),
                supportStreamingReadOverwrite(),
                catalogEnvironment.tableQueryAuth(coreOptions()),
                !tableSchema.primaryKeys().isEmpty());
    }

    /**
     * 创建分片生成器（由子类实现）。
     *
     * <p>分片生成器负责将扫描结果划分为多个分片（Split），以支持并行读取。
     *
     * @return SplitGenerator 实例
     */
    protected abstract SplitGenerator splitGenerator();

    /**
     * 返回非分区过滤器的消费者（由子类实现）。
     *
     * <p>不同表类型对非分区列的过滤器处理不同：
     * <ul>
     *   <li>主键表：只能在键列上下推过滤器
     *   <li>追加表：可以在所有列上下推过滤器
     * </ul>
     *
     * @return BiConsumer<FileStoreScan, Predicate>
     */
    protected abstract BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer();

    /**
     * 复制表并应用动态选项。
     *
     * <p>此方法会：
     * <ol>
     *   <li>检查动态选项的不可变性
     *   <li>尝试触发时间旅行（如果有相关选项）
     *   <li>创建新的 TableSchema
     *   <li>返回新的 Table 实例
     * </ol>
     *
     * @param dynamicOptions 动态选项 Map
     * @return 新的 FileStoreTable 实例
     */
    @Override
    public FileStoreTable copy(Map<String, String> dynamicOptions) {
        checkImmutability(dynamicOptions);
        return copyInternal(dynamicOptions, true);
    }

    /**
     * 复制表并应用动态选项，但不触发时间旅行。
     *
     * @param dynamicOptions 动态选项 Map
     * @return 新的 FileStoreTable 实例
     */
    @Override
    public FileStoreTable copyWithoutTimeTravel(Map<String, String> dynamicOptions) {
        checkImmutability(dynamicOptions);
        return copyInternal(dynamicOptions, false);
    }

    /**
     * 检查动态选项的不可变性。
     *
     * <p>某些配置项标记为 Immutable，不允许通过动态选项修改。
     * 如果尝试修改这些配置项，会抛出 UnsupportedOperationException。
     *
     * @param dynamicOptions 动态选项 Map
     * @throws UnsupportedOperationException 如果尝试修改不可变配置项
     */
    private void checkImmutability(Map<String, String> dynamicOptions) {
        Map<String, String> oldOptions = tableSchema.options();
        // check option is not immutable
        dynamicOptions.forEach(
                (k, newValue) -> {
                    String oldValue = oldOptions.get(k);
                    if (!Objects.equals(oldValue, newValue)) {
                        SchemaManager.checkAlterTableOption(oldOptions, k, oldValue, newValue);
                    }
                });
    }

    /**
     * 内部复制方法，实现表的复制逻辑。
     *
     * <p>复制过程：
     * <ol>
     *   <li>合并原 Schema 的 options 和动态 options
     *   <li>设置 path 和默认值
     *   <li>如果 tryTimeTravel=true，尝试时间旅行到指定快照的 Schema
     *   <li>验证新 Schema 的合法性
     *   <li>创建新的 FileStoreTable 实例
     * </ol>
     *
     * @param dynamicOptions 动态选项
     * @param tryTimeTravel 是否尝试时间旅行
     * @return 新的 FileStoreTable 实例
     */
    protected FileStoreTable copyInternal(
            Map<String, String> dynamicOptions, boolean tryTimeTravel) {
        Map<String, String> options = new HashMap<>(tableSchema.options());

        // merge non-null dynamic options into schema.options
        dynamicOptions.forEach(
                (k, v) -> {
                    if (v == null) {
                        options.remove(k);
                    } else {
                        options.put(k, v);
                    }
                });

        Options newOptions = Options.fromMap(options);

        // set path always
        newOptions.set(PATH, path.toString());

        // set dynamic options with default values
        CoreOptions.setDefaultValues(newOptions);

        // copy a new table schema to contain dynamic options
        TableSchema newTableSchema = tableSchema.copy(newOptions.toMap());

        if (tryTimeTravel) {
            // see if merged options contain time travel option
            newTableSchema = tryTimeTravel(newOptions).orElse(newTableSchema);
        }

        // validate schema with new options
        SchemaValidation.validateTableSchema(newTableSchema);

        return copy(newTableSchema);
    }

    /**
     * 复制表并使用最新的 Schema。
     *
     * @return 使用最新 Schema 的 FileStoreTable 实例
     */
    @Override
    public FileStoreTable copyWithLatestSchema() {
        Optional<TableSchema> optionalLatestSchema = schemaManager().latest();
        if (optionalLatestSchema.isPresent()) {
            Map<String, String> options = tableSchema.options();
            TableSchema newTableSchema = optionalLatestSchema.get();
            // 保留当前的配置选项
            newTableSchema = newTableSchema.copy(options);
            SchemaValidation.validateTableSchema(newTableSchema);
            return copy(newTableSchema);
        } else {
            return this;
        }
    }

    /**
     * 根据新的 TableSchema 复制表。
     *
     * <p>根据 primaryKeys 判断表类型：
     * <ul>
     *   <li>primaryKeys 为空：创建 AppendOnlyFileStoreTable
     *   <li>primaryKeys 不为空：创建 PrimaryKeyFileStoreTable
     * </ul>
     *
     * <p>复制后的表会继承所有缓存设置。
     *
     * @param newTableSchema 新的 TableSchema
     * @return 新的 FileStoreTable 实例
     */
    @Override
    public FileStoreTable copy(TableSchema newTableSchema) {
        AbstractFileStoreTable copied =
                newTableSchema.primaryKeys().isEmpty()
                        ? new AppendOnlyFileStoreTable(
                                fileIO, path, newTableSchema, catalogEnvironment)
                        : new PrimaryKeyFileStoreTable(
                                fileIO, path, newTableSchema, catalogEnvironment);
        // 继承缓存设置
        if (snapshotCache != null) {
            copied.setSnapshotCache(snapshotCache);
        }
        if (manifestCache != null) {
            copied.setManifestCache(manifestCache);
        }
        if (statsCache != null) {
            copied.setStatsCache(statsCache);
        }
        return copied;
    }

    /**
     * 创建 Schema 管理器。
     *
     * @return SchemaManager 实例
     */
    @Override
    public SchemaManager schemaManager() {
        return new SchemaManager(fileIO(), path, currentBranch());
    }

    /**
     * 返回核心配置选项。
     *
     * @return CoreOptions 实例
     */
    @Override
    public CoreOptions coreOptions() {
        return store().options();
    }

    /**
     * 返回文件 IO 接口。
     *
     * @return FileIO 实例
     */
    @Override
    public FileIO fileIO() {
        return fileIO;
    }

    /**
     * 返回表的存储路径。
     *
     * @return 表的路径
     */
    @Override
    public Path location() {
        return path;
    }

    /**
     * 返回表的 Schema。
     *
     * @return TableSchema 实例
     */
    @Override
    public TableSchema schema() {
        return tableSchema;
    }

    /**
     * 返回快照管理器。
     *
     * @return SnapshotManager 实例
     */
    @Override
    public SnapshotManager snapshotManager() {
        return store().snapshotManager();
    }

    /**
     * 返回 Changelog 管理器。
     *
     * @return ChangelogManager 实例
     */
    @Override
    public ChangelogManager changelogManager() {
        return store().changelogManager();
    }

    /**
     * 创建新的快照过期管理器。
     *
     * <p>快照过期负责清理旧快照和相关的数据文件。
     *
     * @return ExpireSnapshots 实例
     */
    @Override
    public ExpireSnapshots newExpireSnapshots() {
        return new ExpireSnapshotsImpl(
                snapshotManager(),
                changelogManager(),
                store().newSnapshotDeletion(),
                store().newTagManager());
    }

    /**
     * 创建新的 Changelog 过期管理器。
     *
     * <p>Changelog 过期独立于快照过期，用于清理 Changelog 文件。
     *
     * @return ExpireSnapshots 实例
     */
    @Override
    public ExpireSnapshots newExpireChangelog() {
        return new ExpireChangelogImpl(
                snapshotManager(),
                changelogManager(),
                tagManager(),
                store().newChangelogDeletion());
    }

    /**
     * 创建新的表提交器。
     *
     * <p>TableCommit 包装了底层的 FileStoreCommit，并添加了以下功能：
     * <ul>
     *   <li>快照过期：提交后自动清理旧快照
     *   <li>分区过期：提交后自动删除过期分区
     *   <li>标签自动创建：根据配置自动创建标签
     *   <li>消费者过期：清理过期的消费者进度
     * </ul>
     *
     * @param commitUser 提交用户标识
     * @return TableCommitImpl 实例
     */
    @Override
    public TableCommitImpl newCommit(String commitUser) {
        CoreOptions options = coreOptions();
        return new TableCommitImpl(
                store().newCommit(commitUser, this),
                newExpireRunnable(),
                options.writeOnly() ? null : store().newPartitionExpire(commitUser, this),
                options.writeOnly() ? null : store().newTagAutoManager(this),
                options.writeOnly() ? null : CoreOptions.fromMap(options()).consumerExpireTime(),
                new ConsumerManager(fileIO, path, snapshotManager().branch()),
                options.snapshotExpireExecutionMode(),
                name(),
                options.forceCreatingSnapshot(),
                options.fileOperationThreadNum());
    }

    /**
     * 创建消费者管理器。
     *
     * @return ConsumerManager 实例
     */
    @Override
    public ConsumerManager consumerManager() {
        return new ConsumerManager(fileIO, path, snapshotManager().branch());
    }

    /**
     * 创建快照过期的 Runnable。
     *
     * <p>如果表配置为 write-only 模式，返回 null（不执行过期）。
     * 否则创建一个 Runnable，依次执行快照过期和 Changelog 过期。
     *
     * @return 快照过期 Runnable，如果不需要则返回 null
     */
    @Nullable
    protected Runnable newExpireRunnable() {
        CoreOptions options = coreOptions();
        Runnable snapshotExpire = null;

        if (!options.writeOnly()) {
            boolean changelogDecoupled = options.changelogLifecycleDecoupled();
            ExpireConfig expireConfig = options.expireConfig();
            ExpireSnapshots expireChangelog = newExpireChangelog().config(expireConfig);
            ExpireSnapshots expireSnapshots = newExpireSnapshots().config(expireConfig);
            snapshotExpire =
                    () -> {
                        expireSnapshots.expire();
                        if (changelogDecoupled) {
                            expireChangelog.expire();
                        }
                    };
        }

        return snapshotExpire;
    }

    /**
     * 尝试时间旅行到指定快照的 Schema。
     *
     * <p>从 options 中检查时间旅行相关配置（如 scan.snapshot-id、scan.timestamp-millis），
     * 如果存在则尝试获取对应快照的 Schema。
     *
     * @param options 配置选项
     * @return 包含快照 Schema 的 Optional，如果不需要时间旅行则返回 empty
     */
    private Optional<TableSchema> tryTimeTravel(Options options) {
        Snapshot snapshot;
        try {
            snapshot =
                    TimeTravelUtil.tryTravelToSnapshot(options, snapshotManager(), tagManager())
                            .orElse(null);
        } catch (Exception e) {
            return Optional.empty();
        }
        if (snapshot == null) {
            return Optional.empty();
        }
        return Optional.of(schemaManager().schema(snapshot.schemaId()).copy(options.toMap()));
    }

    /**
     * 将表回滚到指定的快照 ID。
     *
     * <p>回滚策略：
     * <ol>
     *   <li>优先使用 SnapshotManager 的 rollback 方法（支持快照保留）
     *   <li>如果不支持，使用 RollbackHelper 清理大于指定快照的所有文件
     *   <li>如果快照不存在，尝试从标签中查找
     * </ol>
     *
     * @param snapshotId 目标快照 ID
     * @throws IllegalArgumentException 如果快照不存在
     */
    @Override
    public void rollbackTo(long snapshotId) {
        SnapshotManager snapshotManager = snapshotManager();
        try {
            // 尝试使用 SnapshotManager 的 rollback（新版本支持）
            snapshotManager.rollback(Instant.snapshot(snapshotId));
        } catch (UnsupportedOperationException e) {
            try {
                // 使用 RollbackHelper 进行回滚
                Snapshot snapshot = snapshotManager.tryGetSnapshot(snapshotId);
                rollbackHelper().cleanLargerThan(snapshot);
            } catch (FileNotFoundException ex) {
                // try to get snapshot from tag
                TagManager tagManager = tagManager();
                SortedMap<Snapshot, List<String>> tags = tagManager.tags();
                for (Map.Entry<Snapshot, List<String>> entry : tags.entrySet()) {
                    if (entry.getKey().id() == snapshotId) {
                        rollbackTo(entry.getValue().get(0));
                        return;
                    }
                }
                throw new IllegalArgumentException(
                        String.format("Rollback snapshot '%s' doesn't exist.", snapshotId), ex);
            }
        }
    }

    /**
     * 将表回滚到指定的标签。
     *
     * <p>回滚到标签等价于回滚到标签指向的快照。
     *
     * @param tagName 标签名称
     * @throws TagNotExistException 如果标签不存在
     */
    @Override
    public void rollbackTo(String tagName) {
        SnapshotManager snapshotManager = snapshotManager();
        try {
            // 尝试使用 SnapshotManager 的 rollback
            snapshotManager.rollback(Instant.tag(tagName));
        } catch (UnsupportedOperationException e) {
            // 使用 RollbackHelper 进行回滚
            Snapshot taggedSnapshot = tagManager().getOrThrow(tagName).trimToSnapshot();
            RollbackHelper rollbackHelper = rollbackHelper();
            rollbackHelper.cleanLargerThan(taggedSnapshot);
            rollbackHelper.createSnapshotFileIfNeeded(taggedSnapshot);
        }
    }

    /**
     * 查找指定 ID 的快照。
     *
     * <p>查找策略：
     * <ol>
     *   <li>首先在快照目录中查找
     *   <li>如果不存在，在标签中查找
     * </ol>
     *
     * @param fromSnapshotId 快照 ID
     * @return Snapshot 对象
     * @throws SnapshotNotExistException 如果快照不存在
     */
    public Snapshot findSnapshot(long fromSnapshotId) throws SnapshotNotExistException {
        SnapshotManager snapshotManager = snapshotManager();
        Snapshot snapshot = null;
        if (snapshotManager.snapshotExists(fromSnapshotId)) {
            snapshot = snapshotManager.snapshot(fromSnapshotId);
        } else {
            // 在标签中查找
            SortedMap<Snapshot, List<String>> tags = tagManager().tags();
            for (Snapshot snap : tags.keySet()) {
                if (snap.id() == fromSnapshotId) {
                    snapshot = snap;
                    break;
                } else if (snap.id() > fromSnapshotId) {
                    break;
                }
            }
        }

        SnapshotNotExistException.checkNotNull(
                snapshot,
                String.format(
                        "Cannot create tag because given snapshot #%s doesn't exist.",
                        fromSnapshotId));

        return snapshot;
    }

    /**
     * 创建标签自动管理器。
     *
     * @return TagAutoManager 实例
     */
    @Override
    public TagAutoManager newTagAutoManager() {
        return store().newTagAutoManager(this);
    }

    /**
     * 从指定快照 ID 创建标签。
     *
     * @param tagName 标签名称
     * @param fromSnapshotId 源快照 ID
     */
    @Override
    public void createTag(String tagName, long fromSnapshotId) {
        createTag(tagName, findSnapshot(fromSnapshotId), coreOptions().tagDefaultTimeRetained());
    }

    /**
     * 从指定快照 ID 创建标签，并指定保留时间。
     *
     * @param tagName 标签名称
     * @param fromSnapshotId 源快照 ID
     * @param timeRetained 保留时间
     */
    @Override
    public void createTag(String tagName, long fromSnapshotId, Duration timeRetained) {
        createTag(tagName, findSnapshot(fromSnapshotId), timeRetained);
    }

    /**
     * 从最新快照创建标签。
     *
     * @param tagName 标签名称
     * @throws SnapshotNotExistException 如果最新快照不存在
     */
    @Override
    public void createTag(String tagName) {
        Snapshot latestSnapshot = snapshotManager().latestSnapshot();
        SnapshotNotExistException.checkNotNull(
                latestSnapshot, "Cannot create tag because latest snapshot doesn't exist.");
        createTag(tagName, latestSnapshot, coreOptions().tagDefaultTimeRetained());
    }

    /**
     * 从最新快照创建标签，并指定保留时间。
     *
     * @param tagName 标签名称
     * @param timeRetained 保留时间
     * @throws SnapshotNotExistException 如果最新快照不存在
     */
    @Override
    public void createTag(String tagName, Duration timeRetained) {
        Snapshot latestSnapshot = snapshotManager().latestSnapshot();
        SnapshotNotExistException.checkNotNull(
                latestSnapshot, "Cannot create tag because latest snapshot doesn't exist.");
        createTag(tagName, latestSnapshot, timeRetained);
    }

    /**
     * 内部创建标签方法。
     *
     * @param tagName 标签名称
     * @param fromSnapshot 源快照
     * @param timeRetained 保留时间
     */
    private void createTag(String tagName, Snapshot fromSnapshot, @Nullable Duration timeRetained) {
        tagManager()
                .createTag(
                        fromSnapshot,
                        tagName,
                        timeRetained,
                        store().createTagCallbacks(this),
                        false);
    }

    /**
     * 重命名标签。
     *
     * @param tagName 原标签名称
     * @param targetTagName 新标签名称
     */
    @Override
    public void renameTag(String tagName, String targetTagName) {
        tagManager().renameTag(tagName, targetTagName);
    }

    /**
     * 替换标签。
     *
     * @param tagName 标签名称
     * @param fromSnapshotId 新的源快照 ID，null 表示使用最新快照
     * @param timeRetained 新的保留时间
     */
    @Override
    public void replaceTag(
            String tagName, @Nullable Long fromSnapshotId, @Nullable Duration timeRetained) {
        if (fromSnapshotId == null) {
            Snapshot latestSnapshot = snapshotManager().latestSnapshot();
            SnapshotNotExistException.checkNotNull(
                    latestSnapshot, "Cannot replace tag because latest snapshot doesn't exist.");
            tagManager()
                    .replaceTag(
                            latestSnapshot,
                            tagName,
                            timeRetained,
                            store().createTagCallbacks(this));
        } else {
            tagManager()
                    .replaceTag(
                            findSnapshot(fromSnapshotId),
                            tagName,
                            timeRetained,
                            store().createTagCallbacks(this));
        }
    }

    /**
     * 删除标签。
     *
     * @param tagName 标签名称
     */
    @Override
    public void deleteTag(String tagName) {
        tagManager()
                .deleteTag(
                        tagName,
                        store().newTagDeletion(),
                        snapshotManager(),
                        store().createTagCallbacks(this));
    }

    /**
     * 创建空分支。
     *
     * @param branchName 分支名称
     */
    @Override
    public void createBranch(String branchName) {
        branchManager().createBranch(branchName);
    }

    /**
     * 从标签创建分支。
     *
     * @param branchName 分支名称
     * @param tagName 源标签名称
     */
    @Override
    public void createBranch(String branchName, String tagName) {
        branchManager().createBranch(branchName, tagName);
    }

    /**
     * 删除分支。
     *
     * <p>如果分支是 scan.fallback-branch 配置的回退分支，会抛出异常。
     *
     * @param branchName 分支名称
     * @throws IllegalArgumentException 如果尝试删除回退分支
     */
    @Override
    public void deleteBranch(String branchName) {
        String fallbackBranch =
                coreOptions().toConfiguration().get(CoreOptions.SCAN_FALLBACK_BRANCH);
        if (!StringUtils.isNullOrWhitespaceOnly(fallbackBranch)
                && branchName.equals(fallbackBranch)) {
            throw new IllegalArgumentException(
                    String.format(
                            "can not delete the fallback branch. "
                                    + "branchName to be deleted is %s. you have set 'scan.fallback-branch' = '%s'. "
                                    + "you should reset 'scan.fallback-branch' before deleting this branch.",
                            branchName, fallbackBranch));
        }

        branchManager().dropBranch(branchName);
    }

    /**
     * 快进合并分支到主分支。
     *
     * @param branchName 要合并的分支名称
     */
    @Override
    public void fastForward(String branchName) {
        branchManager().fastForward(branchName);
    }

    /**
     * 创建标签管理器。
     *
     * @return TagManager 实例
     */
    @Override
    public TagManager tagManager() {
        return new TagManager(fileIO, path, currentBranch(), coreOptions());
    }

    /**
     * 创建分支管理器。
     *
     * <p>分支管理器有两种实现：
     * <ul>
     *   <li>CatalogBranchManager：如果 Catalog 支持版本管理
     *   <li>FileSystemBranchManager：默认实现，基于文件系统
     * </ul>
     *
     * @return BranchManager 实例
     */
    @Override
    public BranchManager branchManager() {
        if (catalogEnvironment.catalogLoader() != null
                && catalogEnvironment.supportsVersionManagement()) {
            return new CatalogBranchManager(catalogEnvironment.catalogLoader(), identifier());
        }
        return new FileSystemBranchManager(
                fileIO, path, snapshotManager(), tagManager(), schemaManager());
    }

    /**
     * 切换到指定分支。
     *
     * <p>如果目标分支与当前分支相同，返回当前实例。
     * 否则加载目标分支的 Schema，创建新的 FileStoreTable 实例。
     *
     * @param branchName 目标分支名称
     * @return 指向目标分支的 FileStoreTable 实例
     * @throws IllegalArgumentException 如果分支不存在
     */
    @Override
    public FileStoreTable switchToBranch(String branchName) {
        String currentBranch = BranchManager.normalizeBranch(currentBranch());
        String targetBranch = BranchManager.normalizeBranch(branchName);
        if (currentBranch.equals(targetBranch)) {
            return this;
        }

        Optional<TableSchema> optionalSchema =
                new SchemaManager(fileIO(), location(), targetBranch).latest();
        Preconditions.checkArgument(
                optionalSchema.isPresent(), "Branch " + targetBranch + " does not exist");

        TableSchema branchSchema = optionalSchema.get();
        Options branchOptions = new Options(branchSchema.options());
        branchOptions.set(CoreOptions.BRANCH, targetBranch);
        branchSchema = branchSchema.copy(branchOptions.toMap());
        return FileStoreTableFactory.create(
                fileIO(),
                location(),
                branchSchema,
                new Options(),
                newCatalogEnvironment(targetBranch));
    }

    /**
     * 创建回滚辅助类。
     *
     * @return RollbackHelper 实例
     */
    private RollbackHelper rollbackHelper() {
        return new RollbackHelper(snapshotManager(), changelogManager(), tagManager(), fileIO);
    }

    /**
     * 创建行类型生成器。
     *
     * @return RowKindGenerator 实例
     */
    protected RowKindGenerator rowKindGenerator() {
        return RowKindGenerator.create(schema(), store().options());
    }

    /**
     * 判断两个 AbstractFileStoreTable 是否相等。
     *
     * <p>相等条件：路径相同且 TableSchema 相同。
     *
     * @param o 要比较的对象
     * @return 如果相等返回 true
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractFileStoreTable that = (AbstractFileStoreTable) o;
        return Objects.equals(path, that.path) && Objects.equals(tableSchema, that.tableSchema);
    }
}
