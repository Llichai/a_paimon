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

package org.apache.paimon.manifest;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleStatsCollector;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.io.RollingFileWriterImpl;
import org.apache.paimon.io.SingleFileWriter;
import org.apache.paimon.operation.metrics.CacheMetrics;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.ObjectsFile;
import org.apache.paimon.utils.PathFactory;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.VersionedObjectSerializer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Manifest 文件读写器
 *
 * <p>ManifestFile 负责读写 Manifest 文件，存储多个 {@link ManifestEntry}。
 *
 * <p>三层元数据结构：
 * <ul>
 *   <li>Snapshot：指向一个 ManifestList 文件
 *   <li>ManifestList：包含多个 {@link ManifestFileMeta}
 *   <li>ManifestFile：包含多个 {@link ManifestEntry}（本类）
 *   <li>ManifestEntry：指向一个 {@link DataFileMeta}
 * </ul>
 *
 * <p>存储格式：
 * <ul>
 *   <li>默认使用 Avro 格式（可配置为 Parquet）
 *   <li>支持压缩（通过 compression 参数）
 *   <li>支持滚动写入（通过 RollingFileWriter）
 *   <li>支持缓存（通过 {@link ManifestEntryCache}）
 * </ul>
 *
 * <p>读取优化：
 * <ul>
 *   <li>缓存机制：{@link ManifestEntryCache} 缓存常用 Manifest
 *   <li>分区裁剪：通过 {@link PartitionPredicate} 过滤分区
 *   <li>桶裁剪：通过 {@link BucketFilter} 过滤桶
 *   <li>分段读取：{@link ManifestEntrySegments} 支持分段读取
 * </ul>
 *
 * <p>写入流程：
 * <pre>
 * 1. 创建 RollingFileWriter
 * 2. 写入 ManifestEntry 列表
 * 3. 生成 ManifestFileMeta（包含统计信息）
 * 4. 返回 ManifestFileMeta 列表
 * </pre>
 *
 * <p>使用场景：
 * <ul>
 *   <li>提交阶段：{@link FileStoreCommit} 写入 Manifest 文件
 *   <li>扫描阶段：{@link FileStoreScan} 读取 Manifest 文件
 *   <li>过期阶段：{@link SnapshotDeletion} 删除 Manifest 文件
 * </ul>
 *
 * <p>示例：
 * <pre>{@code
 * // 创建 ManifestFile
 * ManifestFile manifestFile = new ManifestFile.Factory(
 *     fileIO,
 *     schemaManager,
 *     partitionType,
 *     fileFormat,
 *     compression,
 *     pathFactory,
 *     suggestedFileSize,
 *     cache
 * ).create();
 *
 * // 写入 ManifestEntry 列表
 * List<ManifestFileMeta> metas = manifestFile.write(manifestEntries);
 *
 * // 读取 ManifestEntry 列表
 * List<ManifestEntry> entries = manifestFile.read(
 *     fileName,
 *     fileSize,
 *     partitionFilter,
 *     bucketFilter,
 *     readFilter,
 *     readTFilter
 * );
 * }</pre>
 */
public class ManifestFile extends ObjectsFile<ManifestEntry> {

    private final SchemaManager schemaManager;
    private final RowType partitionType;
    private final FormatWriterFactory writerFactory;
    private final long suggestedFileSize;

    private ManifestFile(
            FileIO fileIO,
            SchemaManager schemaManager,
            RowType partitionType,
            ManifestEntrySerializer serializer,
            RowType schema,
            FormatReaderFactory readerFactory,
            FormatWriterFactory writerFactory,
            String compression,
            PathFactory pathFactory,
            long suggestedFileSize,
            @Nullable SegmentsCache<Path> cache) {
        super(
                fileIO,
                serializer,
                schema,
                readerFactory,
                writerFactory,
                compression,
                pathFactory,
                cache);
        this.schemaManager = schemaManager;
        this.partitionType = partitionType;
        this.writerFactory = writerFactory;
        this.suggestedFileSize = suggestedFileSize;
    }

    /** 创建 ManifestEntryCache（覆盖父类方法） */
    @Override
    protected ManifestEntryCache createCache(
            @Nullable SegmentsCache<Path> cache, RowType formatType) {
        return new ManifestEntryCache(
                cache, serializer, formatType, super::fileSize, super::createIterator);
    }

    /** 设置缓存指标 */
    @Override
    public ManifestFile withCacheMetrics(@Nullable CacheMetrics cacheMetrics) {
        super.withCacheMetrics(cacheMetrics);
        return this;
    }

    /**
     * 读取 Manifest 文件中的 ManifestEntry 列表
     *
     * @param fileName Manifest 文件名
     * @param fileSize 文件大小（用于缓存，可为 null）
     * @param partitionFilter 分区过滤器（用于分区裁剪，可为 null）
     * @param bucketFilter 桶过滤器（用于桶裁剪，可为 null）
     * @param readFilter 行级过滤器
     * @param readTFilter ManifestEntry 级过滤器
     * @return ManifestEntry 列表
     */
    public List<ManifestEntry> read(
            String fileName,
            @Nullable Long fileSize,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable BucketFilter bucketFilter,
            Filter<InternalRow> readFilter,
            Filter<ManifestEntry> readTFilter) {
        try {
            Path path = pathFactory.toPath(fileName);
            // 如果启用缓存，使用缓存读取
            if (cache != null) {
                return cache.read(
                        path,
                        fileSize,
                        new ManifestEntryFilters(
                                partitionFilter, bucketFilter, readFilter, readTFilter));
            }

            // 否则直接读取
            return readFromIterator(
                    createIterator(path, fileSize), serializer, readFilter, readTFilter);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 获取建议的文件大小（用于测试） */
    @VisibleForTesting
    public long suggestedFileSize() {
        return suggestedFileSize;
    }

    /**
     * 读取 Manifest 文件中的 ExpireFileEntry 列表（用于过期）
     *
     * @param fileName Manifest 文件名
     * @param fileSize 文件大小（可为 null）
     * @return ExpireFileEntry 列表
     */
    public List<ExpireFileEntry> readExpireFileEntries(String fileName, @Nullable Long fileSize) {
        List<ManifestEntry> entries = read(fileName, fileSize);
        List<ExpireFileEntry> result = new ArrayList<>(entries.size());
        for (ManifestEntry entry : entries) {
            result.add(ExpireFileEntry.from(entry));
        }
        return result;
    }

    /**
     * 写入 ManifestEntry 列表到 Manifest 文件
     *
     * <p>NOTE: 此方法是原子的，要么全部成功，要么全部失败。
     *
     * @param entries ManifestEntry 列表
     * @return ManifestFileMeta 列表（可能包含多个文件，如果超过 suggestedFileSize）
     */
    public List<ManifestFileMeta> write(List<ManifestEntry> entries) {
        RollingFileWriter<ManifestEntry, ManifestFileMeta> writer = createRollingWriter();
        try {
            writer.write(entries);
            writer.close();
            return writer.result();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 创建滚动写入器（当文件超过 suggestedFileSize 时自动创建新文件）
     *
     * @return RollingFileWriter 实例
     */
    public RollingFileWriter<ManifestEntry, ManifestFileMeta> createRollingWriter() {
        return new RollingFileWriterImpl<>(
                () -> new ManifestEntryWriter(writerFactory, pathFactory.newPath(), compression),
                suggestedFileSize);
    }

    /**
     * 创建 ManifestEntryWriter（用于写入指定路径的 Manifest 文件）
     *
     * @param manifestPath Manifest 文件路径
     * @return ManifestEntryWriter 实例
     */
    public ManifestEntryWriter createManifestEntryWriter(Path manifestPath) {
        return new ManifestEntryWriter(writerFactory, manifestPath, compression);
    }

    /**
     * ManifestEntry 写入器
     *
     * <p>负责写入 ManifestEntry 并收集统计信息，生成 {@link ManifestFileMeta}。
     *
     * <p>收集的统计信息：
     * <ul>
     *   <li>numAddedFiles：新增文件数量
     *   <li>numDeletedFiles：删除文件数量
     *   <li>partitionStats：分区统计信息
     *   <li>schemaId：最大 Schema ID
     *   <li>minBucket、maxBucket：最小/最大桶号
     *   <li>minLevel、maxLevel：最小/最大层级
     *   <li>minRowId、maxRowId：最小/最大行 ID
     * </ul>
     */
    public class ManifestEntryWriter extends SingleFileWriter<ManifestEntry, ManifestFileMeta> {

        private final SimpleStatsCollector partitionStatsCollector;
        private final SimpleStatsConverter partitionStatsSerializer;

        private long numAddedFiles = 0;
        private long numDeletedFiles = 0;
        private long schemaId = Long.MIN_VALUE;
        private int minBucket = Integer.MAX_VALUE;
        private int maxBucket = Integer.MIN_VALUE;
        private int minLevel = Integer.MAX_VALUE;
        private int maxLevel = Integer.MIN_VALUE;
        private @Nullable RowIdStats rowIdStats = new RowIdStats();

        ManifestEntryWriter(FormatWriterFactory factory, Path path, String fileCompression) {
            super(
                    ManifestFile.this.fileIO,
                    factory,
                    path,
                    serializer::toRow,
                    fileCompression,
                    false);
            this.partitionStatsCollector = new SimpleStatsCollector(partitionType);
            this.partitionStatsSerializer = new SimpleStatsConverter(partitionType);
        }

        /**
         * 写入 ManifestEntry 并收集统计信息
         *
         * @param entry ManifestEntry
         */
        @Override
        public void write(ManifestEntry entry) throws IOException {
            super.write(entry);

            // 统计 ADD/DELETE 文件数量
            switch (entry.kind()) {
                case ADD:
                    numAddedFiles++;
                    break;
                case DELETE:
                    numDeletedFiles++;
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown entry kind: " + entry.kind());
            }

            // 收集统计信息
            schemaId = Math.max(schemaId, entry.file().schemaId());
            minBucket = Math.min(minBucket, entry.bucket());
            maxBucket = Math.max(maxBucket, entry.bucket());
            minLevel = Math.min(minLevel, entry.level());
            maxLevel = Math.max(maxLevel, entry.level());

            // 收集行 ID 统计信息（如果所有文件都有 firstRowId）
            if (rowIdStats != null) {
                Long firstRowId = entry.file().firstRowId();
                if (firstRowId == null) {
                    rowIdStats = null;
                } else {
                    rowIdStats.collect(firstRowId, entry.file().rowCount());
                }
            }

            // 收集分区统计信息
            partitionStatsCollector.collect(entry.partition());
        }

        /**
         * 生成 ManifestFileMeta
         *
         * @return ManifestFileMeta 实例
         */
        @Override
        public ManifestFileMeta result() throws IOException {
            return new ManifestFileMeta(
                    path.getName(),
                    outputBytes(),
                    numAddedFiles,
                    numDeletedFiles,
                    partitionStatsSerializer.toBinaryAllMode(partitionStatsCollector.extract()),
                    numAddedFiles + numDeletedFiles > 0
                            ? schemaId
                            : schemaManager.latest().get().id(),
                    minBucket,
                    maxBucket,
                    minLevel,
                    maxLevel,
                    rowIdStats == null ? null : rowIdStats.minRowId,
                    rowIdStats == null ? null : rowIdStats.maxRowId);
        }
    }

    /**
     * 行 ID 统计信息（用于 Row Tracking）
     *
     * <p>收集所有 ManifestEntry 的 minRowId 和 maxRowId。
     */
    private static class RowIdStats {

        private long minRowId = Long.MAX_VALUE;
        private long maxRowId = Long.MIN_VALUE;

        /**
         * 收集行 ID 统计信息
         *
         * @param firstRowId 第一行 ID
         * @param rowCount 行数
         */
        private void collect(long firstRowId, long rowCount) {
            minRowId = Math.min(minRowId, firstRowId);
            maxRowId = Math.max(maxRowId, firstRowId + rowCount - 1);
        }
    }

    /**
     * ManifestFile 工厂类
     *
     * <p>负责创建 ManifestFile 实例，配置文件格式、压缩、缓存等参数。
     *
     * <p>配置参数：
     * <ul>
     *   <li>fileIO：文件系统 IO
     *   <li>schemaManager：Schema 管理器
     *   <li>partitionType：分区类型
     *   <li>fileFormat：文件格式（Avro/Parquet）
     *   <li>compression：压缩算法
     *   <li>pathFactory：路径工厂
     *   <li>suggestedFileSize：建议文件大小
     *   <li>cache：Manifest 缓存（可为 null）
     * </ul>
     */
    public static class Factory {

        private final FileIO fileIO;
        private final SchemaManager schemaManager;
        private final RowType partitionType;
        private final FileFormat fileFormat;
        private final String compression;
        private final FileStorePathFactory pathFactory;
        private final long suggestedFileSize;
        @Nullable private final SegmentsCache<Path> cache;

        /**
         * 构造 ManifestFile 工厂
         *
         * @param fileIO 文件系统 IO
         * @param schemaManager Schema 管理器
         * @param partitionType 分区类型
         * @param fileFormat 文件格式（Avro/Parquet）
         * @param compression 压缩算法
         * @param pathFactory 路径工厂
         * @param suggestedFileSize 建议文件大小
         * @param cache Manifest 缓存（可为 null）
         */
        public Factory(
                FileIO fileIO,
                SchemaManager schemaManager,
                RowType partitionType,
                FileFormat fileFormat,
                String compression,
                FileStorePathFactory pathFactory,
                long suggestedFileSize,
                @Nullable SegmentsCache<Path> cache) {
            this.fileIO = fileIO;
            this.schemaManager = schemaManager;
            this.partitionType = partitionType;
            this.fileFormat = fileFormat;
            this.compression = compression;
            this.pathFactory = pathFactory;
            this.suggestedFileSize = suggestedFileSize;
            this.cache = cache;
        }

        /** 判断是否启用缓存 */
        public boolean isCacheEnabled() {
            return cache != null;
        }

        /**
         * 创建 ManifestFile 实例
         *
         * @return ManifestFile 实例
         */
        public ManifestFile create() {
            RowType entryType = VersionedObjectSerializer.versionType(ManifestEntry.SCHEMA);
            return new ManifestFile(
                    fileIO,
                    schemaManager,
                    partitionType,
                    new ManifestEntrySerializer(),
                    entryType,
                    fileFormat.createReaderFactory(entryType, entryType, new ArrayList<>()),
                    fileFormat.createWriterFactory(entryType),
                    compression,
                    pathFactory.manifestFileFactory(),
                    suggestedFileSize,
                    cache);
        }
    }
}
