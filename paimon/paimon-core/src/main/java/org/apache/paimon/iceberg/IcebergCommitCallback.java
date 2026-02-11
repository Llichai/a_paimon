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

package org.apache.paimon.iceberg;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.factories.FactoryException;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.manifest.IcebergConversions;
import org.apache.paimon.iceberg.manifest.IcebergDataFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestEntry;
import org.apache.paimon.iceberg.manifest.IcebergManifestFile;
import org.apache.paimon.iceberg.manifest.IcebergManifestFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestList;
import org.apache.paimon.iceberg.manifest.IcebergPartitionSummary;
import org.apache.paimon.iceberg.metadata.IcebergDataField;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.iceberg.metadata.IcebergPartitionField;
import org.apache.paimon.iceberg.metadata.IcebergPartitionSpec;
import org.apache.paimon.iceberg.metadata.IcebergRef;
import org.apache.paimon.iceberg.metadata.IcebergSchema;
import org.apache.paimon.iceberg.metadata.IcebergSnapshot;
import org.apache.paimon.iceberg.metadata.IcebergSnapshotSummary;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DataFilePathFactories;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.ManifestReadThreadPool;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;

/**
 * Iceberg 提交回调类。
 *
 * <p>这是一个 {@link CommitCallback} 实现，用于创建 Iceberg 兼容的元数据，使得 Iceberg 读取器能够读取
 * Paimon 的 {@link RawFile}。
 *
 * <h3>功能职责</h3>
 * <ul>
 *   <li>在 Paimon 提交后生成 Iceberg 格式的元数据文件
 *   <li>维护 Iceberg Manifest File 和 Manifest List
 *   <li>支持增量更新和完整重建两种模式
 *   <li>处理 Deletion Vector 到 Iceberg 删除文件的转换
 *   <li>管理 Iceberg 元数据的过期和清理
 *   <li>支持 Iceberg 标签（Tag）的创建和删除
 * </ul>
 *
 * <h3>工作流程</h3>
 * <ul>
 *   <li><b>提交回调</b>：在 Paimon 提交完成后被调用
 *   <li><b>元数据生成</b>：根据基础元数据增量生成或完整重建
 *   <li><b>Manifest 写入</b>：将数据文件信息写入 Manifest File
 *   <li><b>List 生成</b>：生成 Manifest List 汇总所有 Manifest File
 *   <li><b>版本更新</b>：更新 version-hint.text 文件
 *   <li><b>元数据提交</b>：可选地提交到外部 Catalog（Hive/REST）
 * </ul>
 *
 * <h3>增量更新策略</h3>
 * <ul>
 *   <li>如果存在基础元数据：基于旧元数据增量更新
 *   <li>如果不存在基础元数据：完整扫描表生成元数据
 *   <li>快速路径：仅新增文件时，直接追加新 Manifest
 *   <li>慢速路径：有删除时，需要重写受影响的 Manifest
 * </ul>
 *
 * <h3>Deletion Vector 处理</h3>
 * <p>当启用删除向量且格式为 V3 时：
 * <ul>
 *   <li>将 Paimon DV 转换为 Iceberg Position Deletes
 *   <li>使用 Puffin 格式存储删除文件引用
 *   <li>生成独立的删除文件 Manifest
 *   <li>在 Manifest List 中同时包含数据和删除文件
 * </ul>
 *
 * <h3>元数据过期</h3>
 * <ul>
 *   <li>根据配置保留最近的 N 个快照
 *   <li>删除过期的 Manifest File 和 Manifest List
 *   <li>清理旧版本的 metadata.json 文件
 *   <li>避免删除仍在使用的共享文件
 * </ul>
 *
 * <h3>标签支持</h3>
 * <ul>
 *   <li>标签仅在指向存在的 Iceberg 快照时才添加
 *   <li>通过重写最新元数据文件来添加/删除标签
 *   <li>保持 Paimon 快照 ID 和 Iceberg 版本号一致
 * </ul>
 *
 * <h3>与其他组件的关系</h3>
 * <ul>
 *   <li>依赖 {@link IcebergManifestFile} 处理 Manifest 文件
 *   <li>依赖 {@link IcebergManifestList} 处理 Manifest List
 *   <li>依赖 {@link IcebergMetadata} 表示元数据结构
 *   <li>可选依赖 {@link IcebergMetadataCommitter} 提交到外部系统
 * </ul>
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li>Paimon 表需要被 Iceberg 引擎（Spark/Trino）读取
 *   <li>多引擎查询同一份数据
 *   <li>利用 Iceberg 生态工具
 * </ul>
 */
public class IcebergCommitCallback implements CommitCallback, TagCallback {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergCommitCallback.class);

    // see org.apache.iceberg.hadoop.Util
    private static final String VERSION_HINT_FILENAME = "version-hint.text";

    private static final String PUFFIN_FORMAT = "puffin";

    private final FileStoreTable table;
    private final String commitUser;

    private final IcebergPathFactory pathFactory;
    private final @Nullable IcebergMetadataCommitter metadataCommitter;

    private final FileStorePathFactory fileStorePathFactory;
    private final IcebergManifestFile manifestFile;
    private final IcebergManifestList manifestList;
    private final int formatVersion;

    private final IndexFileHandler indexFileHandler;
    private final boolean needAddDvToIceberg;

    // -------------------------------------------------------------------------------------
    // Public interface
    // -------------------------------------------------------------------------------------

    /**
     * 构造 Iceberg 提交回调。
     *
     * @param table Paimon 表
     * @param commitUser 提交用户标识
     */
    public IcebergCommitCallback(FileStoreTable table, String commitUser) {
        this.table = table;
        this.commitUser = commitUser;

        IcebergOptions.StorageType storageType =
                table.coreOptions().toConfiguration().get(IcebergOptions.METADATA_ICEBERG_STORAGE);
        this.pathFactory = new IcebergPathFactory(catalogTableMetadataPath(table));

        IcebergMetadataCommitterFactory metadataCommitterFactory;
        try {
            metadataCommitterFactory =
                    FactoryUtil.discoverFactory(
                            IcebergCommitCallback.class.getClassLoader(),
                            IcebergMetadataCommitterFactory.class,
                            storageType.toString());
        } catch (FactoryException ignore) {
            metadataCommitterFactory = null;
        }
        this.metadataCommitter =
                metadataCommitterFactory == null ? null : metadataCommitterFactory.create(table);

        this.fileStorePathFactory = table.store().pathFactory();
        this.manifestFile = IcebergManifestFile.create(table, pathFactory);
        this.manifestList = IcebergManifestList.create(table, pathFactory);

        this.formatVersion =
                table.coreOptions().toConfiguration().get(IcebergOptions.FORMAT_VERSION);
        Preconditions.checkArgument(
                formatVersion == IcebergMetadata.FORMAT_VERSION_V2
                        || formatVersion == IcebergMetadata.FORMAT_VERSION_V3,
                "Unsupported iceberg format version! Only version 2 or version 3 is valid, but current version is ",
                formatVersion);

        this.indexFileHandler = table.store().newIndexFileHandler();
        this.needAddDvToIceberg = needAddDvToIceberg();
    }

    /**
     * 获取 Iceberg 元数据目录路径。
     *
     * @param table Paimon 表
     * @return Iceberg metadata 目录路径
     */
    public static Path catalogTableMetadataPath(FileStoreTable table) {
        Path icebergDBPath = catalogDatabasePath(table);
        return new Path(icebergDBPath, String.format("%s/metadata", table.location().getName()));
    }

    /**
     * 获取 Iceberg 数据库目录路径。
     *
     * <p>根据存储类型和位置配置确定路径：
     * <ul>
     *   <li>TABLE_LOCATION：直接使用表所在数据库路径
     *   <li>CATALOG_STORAGE：使用 catalog 的 iceberg 子目录
     * </ul>
     *
     * @param table Paimon 表
     * @return Iceberg 数据库路径
     */
    public static Path catalogDatabasePath(FileStoreTable table) {
        Path dbPath = table.location().getParent();
        final String dbSuffix = ".db";

        IcebergOptions.StorageType storageType =
                table.coreOptions().toConfiguration().get(IcebergOptions.METADATA_ICEBERG_STORAGE);

        if (!dbPath.getName().endsWith(dbSuffix)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Storage type %s can only be used on Paimon tables in a Paimon warehouse.",
                            storageType.name()));
        }

        IcebergOptions.StorageLocation storageLocation =
                table.coreOptions()
                        .toConfiguration()
                        .getOptional(IcebergOptions.METADATA_ICEBERG_STORAGE_LOCATION)
                        .orElse(inferDefaultMetadataLocation(storageType));

        switch (storageLocation) {
            case TABLE_LOCATION:
                return dbPath;
            case CATALOG_STORAGE:
                String dbName =
                        dbPath.getName()
                                .substring(0, dbPath.getName().length() - dbSuffix.length());
                return new Path(dbPath.getParent(), String.format("iceberg/%s/", dbName));
            default:
                throw new UnsupportedOperationException(
                        "Unknown storage location " + storageLocation.name());
        }
    }

    /**
     * 根据存储类型推断默认的元数据存储位置。
     *
     * @param storageType 存储类型
     * @return 元数据存储位置
     */
    private static IcebergOptions.StorageLocation inferDefaultMetadataLocation(
            IcebergOptions.StorageType storageType) {
        switch (storageType) {
            case TABLE_LOCATION:
                return IcebergOptions.StorageLocation.TABLE_LOCATION;
            case HIVE_CATALOG:
            case HADOOP_CATALOG:
            case REST_CATALOG:
                return IcebergOptions.StorageLocation.CATALOG_STORAGE;
            default:
                throw new UnsupportedOperationException(
                        "Unknown storage type: " + storageType.name());
        }
    }

    @Override
    public void close() throws Exception {}

    /**
     * 提交回调入口，在 Paimon 提交完成后调用。
     *
     * @param baseFiles 基础文件列表（未使用）
     * @param deltaFiles 增量变更文件
     * @param indexFiles 索引文件
     * @param snapshot 当前快照
     */
    @Override
    public void call(
            List<SimpleFileEntry> baseFiles,
            List<ManifestEntry> deltaFiles,
            List<IndexManifestEntry> indexFiles,
            Snapshot snapshot) {
        createMetadata(
                snapshot,
                (removedFiles, addedFiles) ->
                        collectFileChanges(deltaFiles, removedFiles, addedFiles),
                indexFiles);
    }

    /**
     * 重试提交回调，用于失败后恢复。
     *
     * <p>通过快照扫描重新收集文件变更并生成元数据。
     *
     * @param committable 提交内容
     */
    @Override
    public void retry(ManifestCommittable committable) {
        SnapshotManager snapshotManager = table.snapshotManager();
        Snapshot snapshot =
                snapshotManager
                        .findSnapshotsForIdentifiers(
                                commitUser, Collections.singletonList(committable.identifier()))
                        .stream()
                        .max(Comparator.comparingLong(Snapshot::id))
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "There is no snapshot for commit user "
                                                        + commitUser
                                                        + " and identifier "
                                                        + committable.identifier()
                                                        + ". This is unexpected."));
        long snapshotId = snapshot.id();
        createMetadata(
                snapshot,
                (removedFiles, addedFiles) ->
                        collectFileChanges(snapshotId, removedFiles, addedFiles),
                indexFileHandler.scan(snapshot, DELETION_VECTORS_INDEX));
    }

    /**
     * 创建或更新 Iceberg 元数据。
     *
     * <p>核心逻辑：
     * <ul>
     *   <li>第一个快照：删除旧元数据并完整重建
     *   <li>已存在元数据：跳过避免重复
     *   <li>有基础元数据：增量更新
     *   <li>无基础元数据：完整重建
     * </ul>
     *
     * @param snapshot 当前快照
     * @param fileChangesCollector 文件变更收集器
     * @param indexFiles 索引文件列表
     */
    private void createMetadata(
            Snapshot snapshot,
            FileChangesCollector fileChangesCollector,
            List<IndexManifestEntry> indexFiles) {
        long snapshotId = snapshot.id();
        try {
            if (snapshotId == Snapshot.FIRST_SNAPSHOT_ID) {
                // If Iceberg metadata is stored separately in another directory, dropping the table
                // will not delete old Iceberg metadata. So we delete them here, when the table is
                // created again and the first snapshot is committed.
                table.fileIO().delete(pathFactory.metadataDirectory(), true);
            }

            if (table.fileIO().exists(pathFactory.toMetadataPath(snapshotId))) {
                return;
            }

            Path baseMetadataPath = pathFactory.toMetadataPath(snapshotId - 1);

            if (table.fileIO().exists(baseMetadataPath)) {
                createMetadataWithBase(
                        fileChangesCollector,
                        indexFiles.stream()
                                .filter(
                                        index ->
                                                index.indexFile()
                                                        .indexType()
                                                        .equals(DELETION_VECTORS_INDEX))
                                .collect(Collectors.toList()),
                        snapshot,
                        baseMetadataPath);
            } else {
                createMetadataWithoutBase(snapshotId);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // -------------------------------------------------------------------------------------
    // Create metadata afresh
    // -------------------------------------------------------------------------------------

    /**
     * 从零开始创建 Iceberg 元数据（无基础元数据）。
     *
     * <p>流程：
     * <ol>
     *   <li>完整扫描表读取所有数据文件
     *   <li>将数据文件转换为 IcebergManifestEntry
     *   <li>生成 Deletion Vector 的 ManifestEntry
     *   <li>写入 Manifest File
     *   <li>写入 Manifest List
     *   <li>生成初始 IcebergMetadata
     *   <li>写入 metadata.json 和 version-hint.text
     *   <li>可选提交到外部 Catalog
     * </ol>
     *
     * @param snapshotId 快照 ID
     * @throws IOException 如果 I/O 操作失败
     */
    private void createMetadataWithoutBase(long snapshotId) throws IOException {
        SnapshotReader snapshotReader = table.newSnapshotReader().withSnapshot(snapshotId);
        SchemaCache schemaCache = new SchemaCache();
        List<IcebergManifestEntry> dataFileEntries = new ArrayList<>();
        List<IcebergManifestEntry> dvFileEntries = new ArrayList<>();

        List<DataSplit> filteredDataSplits =
                snapshotReader.read().dataSplits().stream()
                        .filter(DataSplit::rawConvertible)
                        .collect(Collectors.toList());
        for (DataSplit dataSplit : filteredDataSplits) {
            dataSplitToManifestEntries(
                    dataSplit, snapshotId, schemaCache, dataFileEntries, dvFileEntries);
        }

        List<IcebergManifestFileMeta> manifestFileMetas = new ArrayList<>();
        if (!dataFileEntries.isEmpty()) {
            manifestFileMetas.addAll(
                    manifestFile.rollingWrite(dataFileEntries.iterator(), snapshotId));
        }
        if (!dvFileEntries.isEmpty()) {
            manifestFileMetas.addAll(
                    manifestFile.rollingWrite(
                            dvFileEntries.iterator(),
                            snapshotId,
                            IcebergManifestFileMeta.Content.DELETES));
        }

        String manifestListFileName = manifestList.writeWithoutRolling(manifestFileMetas);

        int schemaId = (int) schemaCache.getLatestSchemaId();
        IcebergSchema icebergSchema = schemaCache.get(schemaId);
        List<IcebergPartitionField> partitionFields =
                getPartitionFields(table.schema().partitionKeys(), icebergSchema);
        IcebergSnapshot snapshot =
                new IcebergSnapshot(
                        snapshotId,
                        snapshotId,
                        null,
                        System.currentTimeMillis(),
                        IcebergSnapshotSummary.APPEND,
                        pathFactory.toManifestListPath(manifestListFileName).toString(),
                        schemaId,
                        null,
                        null);

        // Tags can only be included in Iceberg if they point to an Iceberg snapshot that
        // exists. Otherwise, an Iceberg client fails to parse the metadata and all reads fail.
        // Only the latest snapshot ID is added to Iceberg in this code path. Since this snapshot
        // has just been committed to Paimon, it is not possible for any Paimon tag to reference it
        // yet.
        // After https://github.com/apache/paimon/issues/6107 we can add tags here.
        Map<String, IcebergRef> refs = new HashMap<>();

        String tableUuid = UUID.randomUUID().toString();

        List<IcebergSchema> allSchemas =
                IntStream.rangeClosed(0, schemaId)
                        .mapToObj(schemaCache::get)
                        .collect(Collectors.toList());
        IcebergMetadata metadata =
                new IcebergMetadata(
                        formatVersion,
                        tableUuid,
                        table.location().toString(),
                        snapshotId,
                        icebergSchema.highestFieldId(),
                        allSchemas,
                        schemaId,
                        Collections.singletonList(new IcebergPartitionSpec(partitionFields)),
                        partitionFields.stream()
                                .mapToInt(IcebergPartitionField::fieldId)
                                .max()
                                .orElse(
                                        // not sure why, this is a result tested by hand
                                        IcebergPartitionField.FIRST_FIELD_ID - 1),
                        Collections.singletonList(snapshot),
                        (int) snapshotId,
                        refs);

        Path metadataPath = pathFactory.toMetadataPath(snapshotId);
        table.fileIO().tryToWriteAtomic(metadataPath, metadata.toJson());
        table.fileIO()
                .overwriteFileUtf8(
                        new Path(pathFactory.metadataDirectory(), VERSION_HINT_FILENAME),
                        String.valueOf(snapshotId));

        expireAllBefore(snapshotId);

        if (metadataCommitter != null) {
            switch (metadataCommitter.identifier()) {
                case "hive":
                    metadataCommitter.commitMetadata(metadataPath, null);
                    break;
                case "rest":
                    metadataCommitter.commitMetadata(metadata, null);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported metadata committer: " + metadataCommitter.identifier());
            }
        }
    }

    /**
     * 将 DataSplit 转换为 Iceberg Manifest Entry。
     *
     * <p>包括：
     * <ul>
     *   <li>数据文件的 ManifestEntry（状态为 ADDED）
     *   <li>删除向量的 ManifestEntry（如果启用 DV）
     * </ul>
     *
     * @param dataSplit 数据分片
     * @param snapshotId 快照 ID
     * @param schemaCache Schema 缓存
     * @param dataFileEntries 数据文件条目集合（输出参数）
     * @param dvFileEntries 删除文件条目集合（输出参数）
     */
    private void dataSplitToManifestEntries(
            DataSplit dataSplit,
            long snapshotId,
            SchemaCache schemaCache,
            List<IcebergManifestEntry> dataFileEntries,
            List<IcebergManifestEntry> dvFileEntries) {
        List<RawFile> rawFiles = dataSplit.convertToRawFiles().get();

        for (int i = 0; i < dataSplit.dataFiles().size(); i++) {
            DataFileMeta paimonFileMeta = dataSplit.dataFiles().get(i);
            RawFile rawFile = rawFiles.get(i);
            IcebergDataFileMeta fileMeta =
                    IcebergDataFileMeta.create(
                            IcebergDataFileMeta.Content.DATA,
                            rawFile.path(),
                            rawFile.format(),
                            dataSplit.partition(),
                            rawFile.rowCount(),
                            rawFile.fileSize(),
                            schemaCache.get(paimonFileMeta.schemaId()),
                            paimonFileMeta.valueStats(),
                            paimonFileMeta.valueStatsCols());
            dataFileEntries.add(
                    new IcebergManifestEntry(
                            IcebergManifestEntry.Status.ADDED,
                            snapshotId,
                            snapshotId,
                            snapshotId,
                            fileMeta));

            if (needAddDvToIceberg
                    && dataSplit.deletionFiles().isPresent()
                    && dataSplit.deletionFiles().get().get(i) != null) {
                DeletionFile deletionFile = dataSplit.deletionFiles().get().get(i);

                // Iceberg will check the cardinality between deserialized dv and iceberg deletion
                // file, so if deletionFile.cardinality() is null, we should stop synchronizing all
                // dvs.
                Preconditions.checkState(
                        deletionFile.cardinality() != null,
                        "cardinality in DeletionFile is null, stop generating dv for iceberg. "
                                + "dataFile path is {}, deletionFile is {}",
                        rawFile.path(),
                        deletionFile);

                // We can not get the file size of the complete DV index file from the DeletionFile,
                // so we set 'fileSizeInBytes' to -1(default in iceberg)
                IcebergDataFileMeta deleteFileMeta =
                        IcebergDataFileMeta.createForDeleteFile(
                                IcebergDataFileMeta.Content.POSITION_DELETES,
                                deletionFile.path(),
                                PUFFIN_FORMAT,
                                dataSplit.partition(),
                                deletionFile.cardinality(),
                                -1,
                                rawFile.path(),
                                deletionFile.offset(),
                                deletionFile.length());

                dvFileEntries.add(
                        new IcebergManifestEntry(
                                IcebergManifestEntry.Status.ADDED,
                                snapshotId,
                                snapshotId,
                                snapshotId,
                                deleteFileMeta));
            }
        }
    }

    /**
     * 获取分区字段定义。
     *
     * @param partitionKeys 分区键名称列表
     * @param icebergSchema Iceberg Schema
     * @return 分区字段列表
     */
    private List<IcebergPartitionField> getPartitionFields(
            List<String> partitionKeys, IcebergSchema icebergSchema) {
        Map<String, IcebergDataField> fields = new HashMap<>();
        for (IcebergDataField field : icebergSchema.fields()) {
            fields.put(field.name(), field);
        }

        List<IcebergPartitionField> result = new ArrayList<>();
        int fieldId = IcebergPartitionField.FIRST_FIELD_ID;
        for (String partitionKey : partitionKeys) {
            result.add(new IcebergPartitionField(fields.get(partitionKey), fieldId));
            fieldId++;
        }
        return result;
    }

    // -------------------------------------------------------------------------------------
    // Create metadata based on old ones
    // -------------------------------------------------------------------------------------

    /**
     * 基于旧元数据增量创建新元数据。
     *
     * <p>优化策略：
     * <ul>
     *   <li>格式版本变更：重建元数据
     *   <li>仅新增文件：快速追加新 Manifest
     *   <li>有删除文件：重写受影响的 Manifest
     *   <li>重用未变更的 Manifest
     *   <li>合并小 Manifest 文件
     *   <li>更新 Schema（如果需要）
     *   <li>清理过期的 Snapshot 和 Manifest
     * </ul>
     *
     * @param fileChangesCollector 文件变更收集器
     * @param indexFiles 索引文件列表
     * @param snapshot 当前快照
     * @param baseMetadataPath 基础元数据文件路径
     * @throws IOException 如果 I/O 操作失败
     */
    private void createMetadataWithBase(
            FileChangesCollector fileChangesCollector,
            List<IndexManifestEntry> indexFiles,
            Snapshot snapshot,
            Path baseMetadataPath)
            throws IOException {
        long snapshotId = snapshot.id();
        IcebergMetadata baseMetadata = IcebergMetadata.fromPath(table.fileIO(), baseMetadataPath);

        if (!isSameFormatVersion(baseMetadata.formatVersion())) {
            // we need to recreate iceberg metadata if format version changed
            createMetadataWithoutBase(snapshot.id());
            return;
        }

        List<IcebergManifestFileMeta> baseManifestFileMetas =
                manifestList.read(baseMetadata.currentSnapshot().manifestList());

        // base manifest file for data files
        List<IcebergManifestFileMeta> baseDataManifestFileMetas =
                baseManifestFileMetas.stream()
                        .filter(meta -> meta.content() == IcebergManifestFileMeta.Content.DATA)
                        .collect(Collectors.toList());

        // base manifest file for deletion vector index files
        List<IcebergManifestFileMeta> baseDVManifestFileMetas =
                baseManifestFileMetas.stream()
                        .filter(meta -> meta.content() == IcebergManifestFileMeta.Content.DELETES)
                        .collect(Collectors.toList());

        Map<String, BinaryRow> removedFiles = new LinkedHashMap<>();
        Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles = new LinkedHashMap<>();
        boolean isAddOnly = fileChangesCollector.collect(removedFiles, addedFiles);
        Set<BinaryRow> modifiedPartitionsSet = new LinkedHashSet<>(removedFiles.values());
        modifiedPartitionsSet.addAll(
                addedFiles.values().stream().map(Pair::getLeft).collect(Collectors.toList()));
        List<BinaryRow> modifiedPartitions = new ArrayList<>(modifiedPartitionsSet);

        // Note that this check may be different from `removedFiles.isEmpty()`,
        // because if a file's level is changed, it will first be removed and then added.
        // In this case, if `baseMetadata` already contains this file, we should not add a
        // duplicate.
        List<IcebergManifestFileMeta> newDataManifestFileMetas;
        IcebergSnapshotSummary snapshotSummary;
        if (isAddOnly) {
            // Fast case. We don't need to remove files from `baseMetadata`. We only need to append
            // new metadata files.
            newDataManifestFileMetas = new ArrayList<>(baseDataManifestFileMetas);
            newDataManifestFileMetas.addAll(
                    createNewlyAddedManifestFileMetas(addedFiles, snapshotId));
            snapshotSummary = IcebergSnapshotSummary.APPEND;
        } else {
            Pair<List<IcebergManifestFileMeta>, IcebergSnapshotSummary> result =
                    createWithDeleteManifestFileMetas(
                            removedFiles,
                            addedFiles,
                            modifiedPartitions,
                            baseDataManifestFileMetas,
                            snapshotId);
            newDataManifestFileMetas = result.getLeft();
            snapshotSummary = result.getRight();
        }

        List<IcebergManifestFileMeta> newDVManifestFileMetas = new ArrayList<>();
        if (needAddDvToIceberg) {
            if (!indexFiles.isEmpty()) {
                // reconstruct the dv index
                newDVManifestFileMetas.addAll(createDvManifestFileMetas(snapshot));
            } else {
                // no new dv index, reuse the old one
                newDVManifestFileMetas.addAll(baseDVManifestFileMetas);
            }
        }

        // compact data manifest file if needed
        newDataManifestFileMetas = compactMetadataIfNeeded(newDataManifestFileMetas, snapshotId);

        String manifestListFileName =
                manifestList.writeWithoutRolling(
                        Stream.concat(
                                        newDataManifestFileMetas.stream(),
                                        newDVManifestFileMetas.stream())
                                .collect(Collectors.toList()));

        // add new schemas if needed
        SchemaCache schemaCache = new SchemaCache();
        int schemaId = (int) schemaCache.getLatestSchemaId();
        IcebergSchema icebergSchema = schemaCache.get(schemaId);
        List<IcebergSchema> schemas = baseMetadata.schemas();
        if (baseMetadata.currentSchemaId() != schemaId) {
            Preconditions.checkArgument(
                    schemaId > baseMetadata.currentSchemaId(),
                    "currentSchemaId{%s} in paimon should be greater than currentSchemaId{%s} in base metadata.",
                    schemaId,
                    baseMetadata.currentSchemaId());
            schemas = new ArrayList<>(schemas);
            schemas.addAll(
                    IntStream.rangeClosed(baseMetadata.currentSchemaId() + 1, schemaId)
                            .mapToObj(schemaCache::get)
                            .collect(Collectors.toList()));
        }

        List<IcebergSnapshot> snapshots = new ArrayList<>(baseMetadata.snapshots());
        snapshots.add(
                new IcebergSnapshot(
                        snapshotId,
                        snapshotId,
                        snapshotId - 1,
                        System.currentTimeMillis(),
                        snapshotSummary,
                        pathFactory.toManifestListPath(manifestListFileName).toString(),
                        schemaId,
                        null,
                        null));

        // all snapshots in this list, except the last one, need to expire
        List<IcebergSnapshot> toExpireExceptLast = new ArrayList<>();
        for (int i = 0; i + 1 < snapshots.size(); i++) {
            toExpireExceptLast.add(snapshots.get(i));
            // commit callback is called before expire, so we cannot use current earliest snapshot
            // and have to check expire condition by ourselves
            if (!shouldExpire(snapshots.get(i), snapshotId)) {
                snapshots = snapshots.subList(i, snapshots.size());
                break;
            }
        }

        // Tags can only be included in Iceberg if they point to an Iceberg snapshot that
        // exists. Otherwise an Iceberg client fails to parse the metadata and all reads fail.
        Set<Long> snapshotIds =
                snapshots.stream().map(IcebergSnapshot::snapshotId).collect(Collectors.toSet());
        Map<String, IcebergRef> refs =
                table.tagManager().tags().entrySet().stream()
                        .filter(entry -> snapshotIds.contains(entry.getKey().id()))
                        .collect(
                                Collectors.toMap(
                                        entry -> entry.getValue().get(0),
                                        entry -> new IcebergRef(entry.getKey().id())));

        IcebergMetadata metadata =
                new IcebergMetadata(
                        baseMetadata.formatVersion(),
                        baseMetadata.tableUuid(),
                        baseMetadata.location(),
                        snapshotId,
                        icebergSchema.highestFieldId(),
                        schemas,
                        schemaId,
                        baseMetadata.partitionSpecs(),
                        baseMetadata.lastPartitionId(),
                        snapshots,
                        (int) snapshotId,
                        refs);

        Path metadataPath = pathFactory.toMetadataPath(snapshotId);
        table.fileIO().tryToWriteAtomic(metadataPath, metadata.toJson());
        table.fileIO()
                .overwriteFileUtf8(
                        new Path(pathFactory.metadataDirectory(), VERSION_HINT_FILENAME),
                        String.valueOf(snapshotId));

        deleteApplicableMetadataFiles(snapshotId);
        for (int i = 0; i + 1 < toExpireExceptLast.size(); i++) {
            expireManifestList(
                    new Path(toExpireExceptLast.get(i).manifestList()).getName(),
                    new Path(toExpireExceptLast.get(i + 1).manifestList()).getName());
        }

        if (metadataCommitter != null) {
            switch (metadataCommitter.identifier()) {
                case "hive":
                    metadataCommitter.commitMetadata(metadataPath, baseMetadataPath);
                    break;
                case "rest":
                    metadataCommitter.commitMetadata(metadata, baseMetadata);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported metadata committer: " + metadataCommitter.identifier());
            }
        }
    }

    /**
     * 文件变更收集器接口。
     *
     * <p>用于收集新增和删除的文件。
     */
    private interface FileChangesCollector {
        /**
         * 收集文件变更。
         *
         * @param removedFiles 删除的文件 Map（路径 -> 分区）
         * @param addedFiles 新增的文件 Map（路径 -> (分区, 文件元数据)）
         * @return 是否仅有新增（无删除）
         * @throws IOException 如果 I/O 操作失败
         */
        boolean collect(
                Map<String, BinaryRow> removedFiles,
                Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles)
                throws IOException;
    }

    /**
     * 从 ManifestEntry 列表收集文件变更。
     *
     * @param manifestEntries Manifest 条目列表
     * @param removedFiles 删除的文件集合（输出）
     * @param addedFiles 新增的文件集合（输出）
     * @return 是否仅有新增文件
     */
    private boolean collectFileChanges(
            List<ManifestEntry> manifestEntries,
            Map<String, BinaryRow> removedFiles,
            Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles) {
        boolean isAddOnly = true;
        DataFilePathFactories factories = new DataFilePathFactories(fileStorePathFactory);
        for (ManifestEntry entry : manifestEntries) {
            DataFilePathFactory dataFilePathFactory =
                    factories.get(entry.partition(), entry.bucket());
            String path = dataFilePathFactory.toPath(entry).toString();
            switch (entry.kind()) {
                case ADD:
                    if (shouldAddFileToIceberg(entry.file())) {
                        removedFiles.remove(path);
                        addedFiles.put(path, Pair.of(entry.partition(), entry.file()));
                    }
                    break;
                case DELETE:
                    isAddOnly = false;
                    addedFiles.remove(path);
                    removedFiles.put(path, entry.partition());
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unknown ManifestEntry FileKind " + entry.kind());
            }
        }
        return isAddOnly;
    }

    /**
     * 通过快照扫描收集文件变更。
     *
     * @param snapshotId 快照 ID
     * @param removedFiles 删除的文件集合（输出）
     * @param addedFiles 新增的文件集合（输出）
     * @return 是否仅有新增文件
     */
    private boolean collectFileChanges(
            long snapshotId,
            Map<String, BinaryRow> removedFiles,
            Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles) {
        return collectFileChanges(
                table.store()
                        .newScan()
                        .withKind(ScanMode.DELTA)
                        .withSnapshot(snapshotId)
                        .plan()
                        .files(),
                removedFiles,
                addedFiles);
    }

    /**
     * 判断文件是否应该添加到 Iceberg。
     *
     * <p>规则：
     * <ul>
     *   <li>Append-only 表：所有文件都添加
     *   <li>主键表且启用 DV：Level > 0 的文件添加
     *   <li>主键表且未启用 DV：只添加最高层级的文件
     * </ul>
     *
     * @param meta 文件元数据
     * @return 是否应添加
     */
    private boolean shouldAddFileToIceberg(DataFileMeta meta) {
        if (table.primaryKeys().isEmpty()) {
            return true;
        } else {
            if (needAddDvToIceberg) {
                return meta.level() > 0;
            }
            int maxLevel = table.coreOptions().numLevels() - 1;
            return meta.level() == maxLevel;
        }
    }

    /**
     * 为新增文件创建 Manifest File Meta。
     *
     * <p>将新增文件写入新的 Manifest File。
     *
     * @param addedFiles 新增的文件 Map
     * @param currentSnapshotId 当前快照 ID
     * @return Manifest File Meta 列表
     * @throws IOException 如果 I/O 操作失败
     */
    private List<IcebergManifestFileMeta> createNewlyAddedManifestFileMetas(
            Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles, long currentSnapshotId)
            throws IOException {
        if (addedFiles.isEmpty()) {
            return Collections.emptyList();
        }

        SchemaCache schemaCache = new SchemaCache();
        return manifestFile.rollingWrite(
                addedFiles.entrySet().stream()
                        .map(
                                e -> {
                                    DataFileMeta paimonFileMeta = e.getValue().getRight();
                                    IcebergDataFileMeta icebergFileMeta =
                                            IcebergDataFileMeta.create(
                                                    IcebergDataFileMeta.Content.DATA,
                                                    e.getKey(),
                                                    paimonFileMeta.fileFormat(),
                                                    e.getValue().getLeft(),
                                                    paimonFileMeta.rowCount(),
                                                    paimonFileMeta.fileSize(),
                                                    schemaCache.get(paimonFileMeta.schemaId()),
                                                    paimonFileMeta.valueStats(),
                                                    paimonFileMeta.valueStatsCols());
                                    return new IcebergManifestEntry(
                                            IcebergManifestEntry.Status.ADDED,
                                            currentSnapshotId,
                                            currentSnapshotId,
                                            currentSnapshotId,
                                            icebergFileMeta);
                                })
                        .iterator(),
                currentSnapshotId);
    }

    /**
     * 处理删除的文件并创建新的 Manifest File Meta。
     *
     * <p>策略：
     * <ul>
     *   <li>检查基础 Manifest 是否包含被修改的分区
     *   <li>可重用：直接保留原 Manifest
     *   <li>需重写：标记删除的文件，重新生成 Manifest
     *   <li>去重：移除已存在的新增文件
     * </ul>
     *
     * @param removedFiles 删除的文件
     * @param addedFiles 新增的文件
     * @param modifiedPartitions 修改的分区列表
     * @param baseManifestFileMetas 基础 Manifest File Meta 列表
     * @param currentSnapshotId 当前快照 ID
     * @return (新的 Manifest Meta 列表, 快照摘要)
     * @throws IOException 如果 I/O 操作失败
     */
    private Pair<List<IcebergManifestFileMeta>, IcebergSnapshotSummary>
            createWithDeleteManifestFileMetas(
                    Map<String, BinaryRow> removedFiles,
                    Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles,
                    List<BinaryRow> modifiedPartitions,
                    List<IcebergManifestFileMeta> baseManifestFileMetas,
                    long currentSnapshotId)
                    throws IOException {
        IcebergSnapshotSummary snapshotSummary = IcebergSnapshotSummary.APPEND;
        List<IcebergManifestFileMeta> newManifestFileMetas = new ArrayList<>();

        RowType partitionType = table.schema().logicalPartitionType();
        PartitionPredicate predicate =
                PartitionPredicate.fromMultiple(partitionType, modifiedPartitions);

        for (IcebergManifestFileMeta fileMeta : baseManifestFileMetas) {
            // use partition predicate to only check modified partitions
            int numFields = partitionType.getFieldCount();
            GenericRow minValues = new GenericRow(numFields);
            GenericRow maxValues = new GenericRow(numFields);
            long[] nullCounts = new long[numFields];
            for (int i = 0; i < numFields; i++) {
                IcebergPartitionSummary summary = fileMeta.partitions().get(i);
                DataType fieldType = partitionType.getTypeAt(i);
                minValues.setField(
                        i, IcebergConversions.toPaimonObject(fieldType, summary.lowerBound()));
                maxValues.setField(
                        i, IcebergConversions.toPaimonObject(fieldType, summary.upperBound()));
                // IcebergPartitionSummary only has `containsNull` field and does not have the
                // exact number of nulls.
                nullCounts[i] = summary.containsNull() ? 1 : 0;
            }

            if (predicate == null
                    || predicate.test(
                            fileMeta.liveRowsCount(),
                            minValues,
                            maxValues,
                            new GenericArray(nullCounts))) {
                // check if any IcebergManifestEntry in this manifest file meta is removed
                List<IcebergManifestEntry> entries =
                        manifestFile.read(new Path(fileMeta.manifestPath()).getName());
                boolean canReuseFile = true;
                for (IcebergManifestEntry entry : entries) {
                    if (entry.isLive()) {
                        String path = entry.file().filePath();
                        if (addedFiles.containsKey(path)) {
                            // added file already exists (most probably due to level changes),
                            // remove it to not add a duplicate.
                            addedFiles.remove(path);
                        } else if (removedFiles.containsKey(path)) {
                            canReuseFile = false;
                        }
                    }
                }

                if (canReuseFile) {
                    // nothing is removed, use this file meta again
                    newManifestFileMetas.add(fileMeta);
                } else {
                    // some file is removed, rewrite this file meta
                    snapshotSummary = IcebergSnapshotSummary.OVERWRITE;
                    List<IcebergManifestEntry> newEntries = new ArrayList<>();
                    for (IcebergManifestEntry entry : entries) {
                        if (entry.isLive()) {
                            newEntries.add(
                                    new IcebergManifestEntry(
                                            removedFiles.containsKey(entry.file().filePath())
                                                    ? IcebergManifestEntry.Status.DELETED
                                                    : IcebergManifestEntry.Status.EXISTING,
                                            entry.snapshotId(),
                                            entry.sequenceNumber(),
                                            entry.fileSequenceNumber(),
                                            entry.file()));
                        }
                    }
                    newManifestFileMetas.addAll(
                            manifestFile.rollingWrite(newEntries.iterator(), currentSnapshotId));
                }
            } else {
                // partition of this file meta is not modified in this snapshot,
                // use this file meta again
                newManifestFileMetas.add(fileMeta);
            }
        }

        newManifestFileMetas.addAll(
                createNewlyAddedManifestFileMetas(addedFiles, currentSnapshotId));
        return Pair.of(newManifestFileMetas, snapshotSummary);
    }

    // -------------------------------------------------------------------------------------
    // Compact
    // -------------------------------------------------------------------------------------

    /**
     * 如果需要则合并 Manifest File。
     *
     * <p>合并策略：
     * <ul>
     *   <li>找出小于目标大小 2/3 的 Manifest
     *   <li>如果数量 >= 最小文件数 或 总大小 >= 目标大小
     *   <li>合并这些小文件为更大的文件
     *   <li>过滤已删除的条目
     *   <li>更新条目状态（ADDED -> EXISTING）
     * </ul>
     *
     * @param toCompact 待合并的 Manifest Meta 列表
     * @param currentSnapshotId 当前快照 ID
     * @return 合并后的 Manifest Meta 列表
     * @throws IOException 如果 I/O 操作失败
     */
    private List<IcebergManifestFileMeta> compactMetadataIfNeeded(
            List<IcebergManifestFileMeta> toCompact, long snapshotId) throws IOException {
        List<IcebergManifestFileMeta> result = new ArrayList<>();
        long targetSizeInBytes = table.coreOptions().manifestTargetSize().getBytes();

        List<IcebergManifestFileMeta> candidates = new ArrayList<>();
        long totalSizeInBytes = 0;
        for (IcebergManifestFileMeta meta : toCompact) {
            if (meta.manifestLength() < targetSizeInBytes * 2 / 3) {
                candidates.add(meta);
                totalSizeInBytes += meta.manifestLength();
            } else {
                result.add(meta);
            }
        }

        Options options = new Options(table.options());
        if (candidates.size() < options.get(IcebergOptions.COMPACT_MIN_FILE_NUM)) {
            return toCompact;
        }
        if (candidates.size() < options.get(IcebergOptions.COMPACT_MAX_FILE_NUM)
                && totalSizeInBytes < targetSizeInBytes) {
            return toCompact;
        }

        Function<IcebergManifestFileMeta, List<IcebergManifestEntry>> processor =
                meta -> {
                    List<IcebergManifestEntry> entries = new ArrayList<>();
                    for (IcebergManifestEntry entry :
                            IcebergManifestFile.create(table, pathFactory)
                                    .read(new Path(meta.manifestPath()).getName())) {
                        if (entry.fileSequenceNumber() == currentSnapshotId
                                || entry.status() == IcebergManifestEntry.Status.EXISTING) {
                            entries.add(entry);
                        } else {
                            // rewrite status if this entry is from an older snapshot
                            IcebergManifestEntry.Status newStatus;
                            if (entry.status() == IcebergManifestEntry.Status.ADDED) {
                                newStatus = IcebergManifestEntry.Status.EXISTING;
                            } else if (entry.status() == IcebergManifestEntry.Status.DELETED) {
                                continue;
                            } else {
                                throw new UnsupportedOperationException(
                                        "Unknown IcebergManifestEntry.Status " + entry.status());
                            }
                            entries.add(
                                    new IcebergManifestEntry(
                                            newStatus,
                                            entry.snapshotId(),
                                            entry.sequenceNumber(),
                                            entry.fileSequenceNumber(),
                                            entry.file()));
                        }
                    }
                    if (meta.sequenceNumber() == currentSnapshotId) {
                        // this file is created for this snapshot, so it is not recorded in any
                        // iceberg metas, we need to clean it
                        table.fileIO().deleteQuietly(new Path(meta.manifestPath()));
                    }
                    return entries;
                };
        Iterable<IcebergManifestEntry> newEntries =
                ManifestReadThreadPool.sequentialBatchedExecute(processor, candidates, null);
        result.addAll(manifestFile.rollingWrite(newEntries.iterator(), currentSnapshotId));
        return result;
    }

    // -------------------------------------------------------------------------------------
    // Expire
    // -------------------------------------------------------------------------------------

    /**
     * 判断快照是否应该过期。
     *
     * <p>过期条件：
     * <ul>
     *   <li>快照数量超过最大保留数
     *   <li>快照时间超过保留时间且快照数量超过最小保留数
     * </ul>
     *
     * @param snapshot 快照信息
     * @param currentSnapshotId 当前快照 ID
     * @return 是否应过期
     */
    private boolean shouldExpire(IcebergSnapshot snapshot, long currentSnapshotId) {
        Options options = new Options(table.options());
        if (snapshot.snapshotId()
                > currentSnapshotId - options.get(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN)) {
            return false;
        }
        if (snapshot.snapshotId()
                <= currentSnapshotId - options.get(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX)) {
            return true;
        }
        return snapshot.timestampMs()
                < System.currentTimeMillis()
                        - options.get(CoreOptions.SNAPSHOT_TIME_RETAINED).toMillis();
    }

    /**
     * 过期 Manifest List。
     *
     * <p>删除不再使用的 Manifest File。
     *
     * @param toExpire 要过期的 Manifest List 文件名
     * @param next 下一个 Manifest List 文件名（用于判断哪些 Manifest 仍在使用）
     */
    private void expireManifestList(String toExpire, String next) {
        Set<IcebergManifestFileMeta> metaInUse = new HashSet<>(manifestList.read(next));
        for (IcebergManifestFileMeta meta : manifestList.read(toExpire)) {
            if (metaInUse.contains(meta)) {
                continue;
            }
            table.fileIO().deleteQuietly(new Path(meta.manifestPath()));
        }
        table.fileIO().deleteQuietly(pathFactory.toManifestListPath(toExpire));
    }

    /**
     * 过期指定快照之前的所有元数据。
     *
     * @param snapshotId 快照 ID
     * @throws IOException 如果 I/O 操作失败
     */
    private void expireAllBefore(long snapshotId) throws IOException {
        Set<String> expiredManifestLists = new HashSet<>();
        Set<String> expiredManifestFileMetas = new HashSet<>();
        Iterator<Path> it =
                pathFactory.getAllMetadataPathBefore(table.fileIO(), snapshotId).iterator();

        while (it.hasNext()) {
            Path path = it.next();
            IcebergMetadata metadata = IcebergMetadata.fromPath(table.fileIO(), path);

            for (IcebergSnapshot snapshot : metadata.snapshots()) {
                Path listPath = new Path(snapshot.manifestList());
                String listName = listPath.getName();
                if (expiredManifestLists.contains(listName)) {
                    continue;
                }
                expiredManifestLists.add(listName);

                for (IcebergManifestFileMeta meta : manifestList.read(listName)) {
                    String metaName = new Path(meta.manifestPath()).getName();
                    if (expiredManifestFileMetas.contains(metaName)) {
                        continue;
                    }
                    expiredManifestFileMetas.add(metaName);
                    table.fileIO().deleteQuietly(new Path(meta.manifestPath()));
                }
                table.fileIO().deleteQuietly(listPath);
            }
        }
        deleteApplicableMetadataFiles(snapshotId);
    }

    /**
     * 删除可应用的元数据文件。
     *
     * <p>根据配置删除超过最大版本数的旧元数据文件。
     *
     * @param snapshotId 当前快照 ID
     * @throws IOException 如果 I/O 操作失败
     */
    private void deleteApplicableMetadataFiles(long snapshotId) throws IOException {
        Options options = new Options(table.options());
        if (options.get(IcebergOptions.METADATA_DELETE_AFTER_COMMIT)) {
            long earliestMetadataId =
                    snapshotId - options.get(IcebergOptions.METADATA_PREVIOUS_VERSIONS_MAX);
            if (earliestMetadataId > 0) {
                Iterator<Path> it =
                        pathFactory
                                .getAllMetadataPathBefore(table.fileIO(), earliestMetadataId)
                                .iterator();
                while (it.hasNext()) {
                    Path path = it.next();
                    table.fileIO().deleteQuietly(path);
                }
            }
        }
    }

    @Override
    public void notifyCreation(String tagName) {
        throw new UnsupportedOperationException(
                "IcebergCommitCallback notifyCreation requires a snapshot ID");
    }

    /**
     * 标签创建通知（带快照 ID）。
     *
     * <p>将标签信息添加到最新的 Iceberg 元数据中。
     * 仅当标签指向的快照在 Iceberg 中存在时才添加。
     *
     * @param tagName 标签名称
     * @param snapshotId 快照 ID
     */
    @Override
    public void notifyCreation(String tagName, long snapshotId) {
        try {
            Snapshot latestSnapshot = table.snapshotManager().latestSnapshot();
            if (latestSnapshot == null) {
                LOG.info(
                        "Latest Iceberg snapshot not found when creating tag {} for snapshot {}. Unable to create tag.",
                        tagName,
                        snapshotId);
                return;
            }

            Path baseMetadataPath = pathFactory.toMetadataPath(latestSnapshot.id());
            if (!table.fileIO().exists(baseMetadataPath)) {
                LOG.info(
                        "Iceberg metadata file {} not found when creating tag {} for snapshot {}. Unable to create tag.",
                        baseMetadataPath,
                        tagName,
                        snapshotId);
                return;
            }

            IcebergMetadata baseMetadata =
                    IcebergMetadata.fromPath(table.fileIO(), baseMetadataPath);

            // Tags can only be included in Iceberg if they point to an Iceberg snapshot that
            // exists. Otherwise an Iceberg client fails to parse the metadata and all reads fail.
            boolean tagSnapshotInIceberg = false;
            for (IcebergSnapshot snapshot : baseMetadata.snapshots()) {
                if (snapshot.snapshotId() == snapshotId) {
                    tagSnapshotInIceberg = true;
                    break;
                }
            }

            if (!tagSnapshotInIceberg) {
                LOG.warn(
                        "Snapshot {} does not exist in Iceberg metadata. Unable to create tag {}.",
                        snapshotId,
                        tagName);
                return;
            }

            baseMetadata.refs().put(tagName, new IcebergRef(snapshotId));

            IcebergMetadata metadata =
                    new IcebergMetadata(
                            baseMetadata.formatVersion(),
                            baseMetadata.tableUuid(),
                            baseMetadata.location(),
                            baseMetadata.currentSnapshotId(),
                            baseMetadata.lastColumnId(),
                            baseMetadata.schemas(),
                            baseMetadata.currentSchemaId(),
                            baseMetadata.partitionSpecs(),
                            baseMetadata.lastPartitionId(),
                            baseMetadata.snapshots(),
                            baseMetadata.currentSnapshotId(),
                            baseMetadata.refs());

            /*
            Overwrite the latest metadata file
            Currently the Paimon table snapshot id value is the same as the Iceberg metadata
            version number. Tag creation overwrites the latest metadata file to maintain this.
            There is no need to update the catalog after overwrite.
             */
            table.fileIO().overwriteFileUtf8(baseMetadataPath, metadata.toJson());
            LOG.info(
                    "Iceberg metadata file {} overwritten to add tag {} for snapshot {}.",
                    baseMetadataPath,
                    tagName,
                    snapshotId);

        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create tag " + tagName, e);
        }
    }

    /**
     * 标签删除通知。
     *
     * <p>从最新的 Iceberg 元数据中移除标签信息。
     *
     * @param tagName 标签名称
     */
    @Override
    public void notifyDeletion(String tagName) {
        try {
            Snapshot latestSnapshot = table.snapshotManager().latestSnapshot();
            if (latestSnapshot == null) {
                LOG.info(
                        "Latest Iceberg snapshot not found when deleting tag {}. Unable to delete tag.",
                        tagName);
                return;
            }

            Path baseMetadataPath = pathFactory.toMetadataPath(latestSnapshot.id());
            if (!table.fileIO().exists(baseMetadataPath)) {
                LOG.info(
                        "Iceberg metadata file {} not found when deleting tag {}. Unable to delete tag.",
                        baseMetadataPath,
                        tagName);
                return;
            }

            IcebergMetadata baseMetadata =
                    IcebergMetadata.fromPath(table.fileIO(), baseMetadataPath);

            baseMetadata.refs().remove(tagName);

            IcebergMetadata metadata =
                    new IcebergMetadata(
                            baseMetadata.formatVersion(),
                            baseMetadata.tableUuid(),
                            baseMetadata.location(),
                            baseMetadata.currentSnapshotId(),
                            baseMetadata.lastColumnId(),
                            baseMetadata.schemas(),
                            baseMetadata.currentSchemaId(),
                            baseMetadata.partitionSpecs(),
                            baseMetadata.lastPartitionId(),
                            baseMetadata.snapshots(),
                            baseMetadata.currentSnapshotId(),
                            baseMetadata.refs());

            /*
            Overwrite the latest metadata file
            Currently the Paimon table snapshot id value is the same as the Iceberg metadata
            version number. Tag creation overwrites the latest metadata file to maintain this.
            There is no need to update the catalog after overwrite.
             */
            table.fileIO().overwriteFileUtf8(baseMetadataPath, metadata.toJson());
            LOG.info(
                    "Iceberg metadata file {} overwritten to delete tag {}.",
                    baseMetadataPath,
                    tagName);

        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create tag " + tagName, e);
        }
    }

    // -------------------------------------------------------------------------------------
    // Deletion vectors
    // -------------------------------------------------------------------------------------

    /**
     * 检查是否需要将删除向量添加到 Iceberg。
     *
     * <p>需要满足：
     * <ul>
     *   <li>启用删除向量
     *   <li>使用 bitmap64 格式
     *   <li>Iceberg 格式版本为 V3
     * </ul>
     *
     * @return 是否需要添加 DV
     */
    private boolean needAddDvToIceberg() {
        CoreOptions options = table.coreOptions();
        // there may be dv indexes using bitmap32 in index files even if 'deletion-vectors.bitmap64'
        // is true, but analyzing all deletion vectors is very costly, so we do not check exactly
        // currently.
        return options.deletionVectorsEnabled()
                && options.deletionVectorBitmap64()
                && formatVersion == IcebergMetadata.FORMAT_VERSION_V3;
    }

    /**
     * 为删除向量创建 Manifest File Meta。
     *
     * <p>将所有 DV 索引转换为 Iceberg Position Deletes。
     *
     * @param snapshot 快照
     * @return DV Manifest File Meta 列表
     */
    private List<IcebergManifestFileMeta> createDvManifestFileMetas(Snapshot snapshot) {
        List<IcebergManifestEntry> icebergDvEntries = new ArrayList<>();

        long snapshotId = snapshot.id();
        List<IndexManifestEntry> newIndexes =
                indexFileHandler.scan(snapshot, DELETION_VECTORS_INDEX);
        if (newIndexes.isEmpty()) {
            return Collections.emptyList();
        }
        for (IndexManifestEntry entry : newIndexes) {
            LinkedHashMap<String, DeletionVectorMeta> dvMetas = entry.indexFile().dvRanges();
            Path bucketPath = fileStorePathFactory.bucketPath(entry.partition(), entry.bucket());
            if (dvMetas != null) {
                for (DeletionVectorMeta dvMeta : dvMetas.values()) {

                    // Iceberg will check the cardinality between deserialized dv and iceberg
                    // deletion file, so if deletionFile.cardinality() is null, we should stop
                    // synchronizing all dvs.
                    Preconditions.checkState(
                            dvMeta.cardinality() != null,
                            "cardinality in DeletionVector is null, stop generate dv for iceberg. "
                                    + "dataFile path is {}, indexFile path is {}",
                            new Path(bucketPath, dvMeta.dataFileName()),
                            indexFileHandler.filePath(entry).toString());

                    IcebergDataFileMeta deleteFileMeta =
                            IcebergDataFileMeta.createForDeleteFile(
                                    IcebergDataFileMeta.Content.POSITION_DELETES,
                                    indexFileHandler.filePath(entry).toString(),
                                    PUFFIN_FORMAT,
                                    entry.partition(),
                                    dvMeta.cardinality(),
                                    entry.indexFile().fileSize(),
                                    new Path(bucketPath, dvMeta.dataFileName()).toString(),
                                    (long) dvMeta.offset(),
                                    (long) dvMeta.length());

                    icebergDvEntries.add(
                            new IcebergManifestEntry(
                                    IcebergManifestEntry.Status.ADDED,
                                    snapshotId,
                                    snapshotId,
                                    snapshotId,
                                    deleteFileMeta));
                }
            }
        }

        if (icebergDvEntries.isEmpty()) {
            return Collections.emptyList();
        }

        return manifestFile.rollingWrite(
                icebergDvEntries.iterator(), snapshotId, IcebergManifestFileMeta.Content.DELETES);
    }

    // -------------------------------------------------------------------------------------
    // Utils
    // -------------------------------------------------------------------------------------

    /**
     * 检查格式版本是否相同。
     *
     * @param baseFormatVersion 基础格式版本
     * @return 是否相同
     */
    private boolean isSameFormatVersion(int baseFormatVersion) {
        if (baseFormatVersion != formatVersion) {
            Preconditions.checkArgument(
                    formatVersion > baseFormatVersion,
                    "format version in base metadata is {}, and it's bigger than the current format version {}, "
                            + "this is not allowed!");

            LOG.info(
                    "format version in base metadata is {}, and it's different from the current format version {}. "
                            + "New metadata will be recreated using format version {}.",
                    baseFormatVersion,
                    formatVersion,
                    formatVersion);
            return false;
        }
        return true;
    }

    /**
     * Schema 缓存类。
     *
     * <p>缓存 Iceberg Schema 以避免重复转换。
     */
    private class SchemaCache {

        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        Map<Long, IcebergSchema> schemas = new HashMap<>();

        /**
         * 获取指定 ID 的 Iceberg Schema。
         *
         * @param schemaId Schema ID
         * @return Iceberg Schema
         */
        private IcebergSchema get(long schemaId) {
            return schemas.computeIfAbsent(
                    schemaId, id -> IcebergSchema.create(schemaManager.schema(id)));
        }

        /**
         * 获取最新的 Schema ID。
         *
         * @return 最新 Schema ID
         */
        private long getLatestSchemaId() {
            return schemaManager.latest().get().id();
        }
    }
}
