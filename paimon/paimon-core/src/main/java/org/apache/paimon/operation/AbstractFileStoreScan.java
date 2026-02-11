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

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.manifest.BucketEntry;
import org.apache.paimon.manifest.BucketFilter;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileEntry.Identifier;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntrySerializer;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.metrics.ScanMetrics;
import org.apache.paimon.operation.metrics.ScanStats;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BiFilter;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.ListUtils;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.ManifestReadThreadPool.getExecutorService;
import static org.apache.paimon.utils.ManifestReadThreadPool.randomlyExecuteSequentialReturn;
import static org.apache.paimon.utils.ManifestReadThreadPool.sequentialBatchedExecute;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyOnlyExecute;

/**
 * {@link FileStoreScan} 的默认抽象实现。
 *
 * <p>实现了扫描的核心流程：
 * <ol>
 *   <li>根据过滤条件读取 Manifest 文件列表</li>
 *   <li>从 Manifest 文件中读取数据文件条目</li>
 *   <li>应用各种过滤器（分区、桶、层级、统计信息等）</li>
 *   <li>合并或保持原样返回文件条目列表</li>
 * </ol>
 *
 * <p>支持两种文件合并模式：
 * <ul>
 *   <li><b>合并模式（Merge）</b>：先读取所有删除条目，然后过滤掉已删除的文件。
 *       用于 ScanMode.ALL，需要合并增量和删除操作</li>
 *   <li><b>非合并模式（No Merge）</b>：直接读取所有条目，不做合并。
 *       用于增量扫描，保留所有的 ADD/DELETE 标记</li>
 * </ul>
 */
public abstract class AbstractFileStoreScan implements FileStoreScan {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractFileStoreScan.class);

    /** Manifest 读取器，负责读取和过滤 Manifest 文件 */
    private final ManifestsReader manifestsReader;
    /** 快照管理器，用于获取快照信息 */
    private final SnapshotManager snapshotManager;
    /** Manifest 文件工厂，用于创建 Manifest 文件读取器 */
    private final ManifestFile.Factory manifestFileFactory;
    /** 扫描并行度 */
    private final Integer parallelism;

    /** 表 Schema 缓存，避免重复读取 */
    private final ConcurrentMap<Long, TableSchema> tableSchemas;
    /** Schema 管理器 */
    private final SchemaManager schemaManager;
    /** 当前表 Schema */
    protected final TableSchema schema;

    /** 指定的快照 */
    private Snapshot specifiedSnapshot = null;
    /** 是否只读取实际存在的桶（用于延迟分桶表） */
    private boolean onlyReadRealBuckets = false;
    /** 指定的桶号 */
    private Integer specifiedBucket = null;
    /** 桶过滤器 */
    private Filter<Integer> bucketFilter = null;
    /** 总桶数感知的桶过滤器 */
    private BiFilter<Integer, Integer> totalAwareBucketFilter = null;
    /** 扫描模式（批量、增量、流式） */
    protected ScanMode scanMode = ScanMode.ALL;
    /** 指定的层级 */
    private Integer specifiedLevel = null;
    /** 层级过滤器 */
    private Filter<Integer> levelFilter = null;
    /** Manifest 条目过滤器 */
    private Filter<ManifestEntry> manifestEntryFilter = null;
    /** 文件名过滤器 */
    private Filter<String> fileNameFilter = null;

    /** 扫描指标收集器 */
    private ScanMetrics scanMetrics = null;
    /** 是否丢弃统计信息以减少内存 */
    private boolean dropStats;
    /** 行号范围过滤 */
    @Nullable protected List<Range> rowRanges;
    /** 最大行数限制 */
    @Nullable protected Long limit;

    public AbstractFileStoreScan(
            ManifestsReader manifestsReader,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            TableSchema schema,
            ManifestFile.Factory manifestFileFactory,
            @Nullable Integer parallelism) {
        this.manifestsReader = manifestsReader;
        this.snapshotManager = snapshotManager;
        this.schemaManager = schemaManager;
        this.schema = schema;
        this.manifestFileFactory = manifestFileFactory;
        this.tableSchemas = new ConcurrentHashMap<>();
        this.parallelism = parallelism;
        this.dropStats = false;
    }

    @Override
    public FileStoreScan withPartitionFilter(Predicate predicate) {
        manifestsReader.withPartitionFilter(predicate);
        return this;
    }

    @Override
    public FileStoreScan withPartitionFilter(List<BinaryRow> partitions) {
        manifestsReader.withPartitionFilter(partitions);
        return this;
    }

    @Override
    public FileStoreScan withPartitionsFilter(List<Map<String, String>> partitions) {
        manifestsReader.withPartitionsFilter(partitions);
        return this;
    }

    @Override
    public FileStoreScan withPartitionFilter(PartitionPredicate predicate) {
        manifestsReader.withPartitionFilter(predicate);
        return this;
    }

    @Override
    public FileStoreScan onlyReadRealBuckets() {
        manifestsReader.onlyReadRealBuckets();
        this.onlyReadRealBuckets = true;
        return this;
    }

    @Override
    public FileStoreScan withBucket(int bucket) {
        manifestsReader.withBucket(bucket);
        specifiedBucket = bucket;
        return this;
    }

    @Override
    public FileStoreScan withBucketFilter(Filter<Integer> bucketFilter) {
        this.bucketFilter = bucketFilter;
        return this;
    }

    @Override
    public FileStoreScan withTotalAwareBucketFilter(
            BiFilter<Integer, Integer> totalAwareBucketFilter) {
        this.totalAwareBucketFilter = totalAwareBucketFilter;
        return this;
    }

    @Override
    public FileStoreScan withPartitionBucket(BinaryRow partition, int bucket) {
        withPartitionFilter(Collections.singletonList(partition));
        withBucket(bucket);
        return this;
    }

    @Override
    public FileStoreScan withSnapshot(long snapshotId) {
        this.specifiedSnapshot = snapshotManager.snapshot(snapshotId);
        return this;
    }

    @Override
    public FileStoreScan withSnapshot(Snapshot snapshot) {
        this.specifiedSnapshot = snapshot;
        return this;
    }

    @Override
    public FileStoreScan withKind(ScanMode scanMode) {
        this.scanMode = scanMode;
        return this;
    }

    @Override
    public FileStoreScan withLevel(int level) {
        manifestsReader.withLevel(level);
        this.specifiedLevel = level;
        return this;
    }

    @Override
    public FileStoreScan withLevelFilter(Filter<Integer> levelFilter) {
        this.levelFilter = levelFilter;
        return this;
    }

    @Override
    public FileStoreScan withLevelMinMaxFilter(BiFilter<Integer, Integer> minMaxFilter) {
        manifestsReader.withLevelMinMaxFilter(minMaxFilter);
        return this;
    }

    @Override
    public FileStoreScan enableValueFilter() {
        return this;
    }

    @Override
    public FileStoreScan withManifestEntryFilter(Filter<ManifestEntry> filter) {
        this.manifestEntryFilter = filter;
        return this;
    }

    @Override
    public FileStoreScan withDataFileNameFilter(Filter<String> fileNameFilter) {
        this.fileNameFilter = fileNameFilter;
        return this;
    }

    @Override
    public FileStoreScan withMetrics(ScanMetrics metrics) {
        this.scanMetrics = metrics;
        return this;
    }

    @Override
    public FileStoreScan dropStats() {
        this.dropStats = true;
        return this;
    }

    @Override
    public FileStoreScan keepStats() {
        this.dropStats = false;
        return this;
    }

    @Override
    public FileStoreScan withRowRanges(List<Range> rowRanges) {
        this.rowRanges = rowRanges;
        manifestsReader.withRowRanges(rowRanges);
        return this;
    }

    @Override
    public FileStoreScan withReadType(RowType readType) {
        return this;
    }

    @Override
    public FileStoreScan withLimit(long limit) {
        this.limit = limit;
        return this;
    }

    @Nullable
    @Override
    public Integer parallelism() {
        return parallelism;
    }

    @Override
    public ManifestsReader manifestsReader() {
        return manifestsReader;
    }

    @Override
    public Plan plan() {
        long started = System.nanoTime();
        // 读取 Manifest 文件列表（已过滤）
        ManifestsReader.Result manifestsResult = readManifests();
        Snapshot snapshot = manifestsResult.snapshot;
        List<ManifestFileMeta> manifests = manifestsResult.filteredManifests;

        // 读取 Manifest 文件中的数据文件条目
        Iterator<ManifestEntry> iterator = readManifestEntries(manifests, false);

        // 转换为列表
        List<ManifestEntry> files = ListUtils.toList(iterator);
        // 如果启用了后置过滤，执行额外的过滤逻辑
        if (postFilterManifestEntriesEnabled()) {
            files = postFilterManifestEntries(files);
        }

        List<ManifestEntry> result = files;

        // 记录扫描耗时和结果数量
        long scanDuration = (System.nanoTime() - started) / 1_000_000;
        LOG.info(
                "File store scan plan completed in {} ms. Files size : {}",
                scanDuration,
                result.size());
        // 上报扫描指标
        if (scanMetrics != null) {
            long allDataFiles =
                    manifestsResult.allManifests.stream()
                            .mapToLong(f -> f.numAddedFiles() - f.numDeletedFiles())
                            .sum();
            scanMetrics.reportScan(
                    new ScanStats(
                            scanDuration,
                            snapshot == null ? 0 : snapshot.id(),
                            manifests.size(),
                            allDataFiles - result.size(),
                            result.size()));
        }

        // 返回扫描计划
        return new Plan() {
            @Nullable
            @Override
            public Long watermark() {
                return snapshot == null ? null : snapshot.watermark();
            }

            @Nullable
            @Override
            public Snapshot snapshot() {
                return snapshot;
            }

            @Override
            public List<ManifestEntry> files() {
                return result;
            }
        };
    }

    @Override
    public List<SimpleFileEntry> readSimpleEntries() {
        List<ManifestFileMeta> manifests = readManifests().filteredManifests;
        Iterator<SimpleFileEntry> iterator =
                scanMode == ScanMode.ALL
                        ? readAndMergeFileEntries(manifests, SimpleFileEntry::from, false)
                        : readAndNoMergeFileEntries(manifests, SimpleFileEntry::from, false);
        List<SimpleFileEntry> result = new ArrayList<>();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        return result;
    }

    @Override
    public List<PartitionEntry> readPartitionEntries() {
        List<ManifestFileMeta> manifests = readManifests().filteredManifests;
        // 使用并发 Map 收集分区条目
        Map<BinaryRow, PartitionEntry> partitions = new ConcurrentHashMap<>();
        // 并行读取 Manifest 文件并合并分区条目
        Consumer<ManifestFileMeta> processor =
                m -> PartitionEntry.merge(PartitionEntry.merge(readManifest(m)), partitions);
        randomlyOnlyExecute(getExecutorService(parallelism), processor, manifests);
        // 过滤掉空分区
        return partitions.values().stream()
                .filter(p -> p.fileCount() > 0)
                .collect(Collectors.toList());
    }

    @Override
    public List<BucketEntry> readBucketEntries() {
        List<ManifestFileMeta> manifests = readManifests().filteredManifests;
        // 使用并发 Map 收集桶条目
        Map<Pair<BinaryRow, Integer>, BucketEntry> buckets = new ConcurrentHashMap<>();
        // 并行读取 Manifest 文件并合并桶条目
        Consumer<ManifestFileMeta> processor =
                m -> BucketEntry.merge(BucketEntry.merge(readManifest(m)), buckets);
        randomlyOnlyExecute(getExecutorService(parallelism), processor, manifests);
        // 过滤掉空桶
        return buckets.values().stream()
                .filter(p -> p.fileCount() > 0)
                .collect(Collectors.toList());
    }

    @Override
    public Iterator<ManifestEntry> readFileIterator() {
        // 使用顺序模式减少内存占用，并且迭代器可以提前停止
        return readManifestEntries(readManifests().filteredManifests, true);
    }

    @Override
    public Iterator<ManifestEntry> readFileIterator(List<ManifestFileMeta> manifestFileMetas) {
        // 使用顺序模式减少内存占用，并且迭代器可以提前停止
        return readManifestEntries(manifestFileMetas, true);
    }

    /**
     * 读取 Manifest 条目的核心方法。
     *
     * @param manifests Manifest 文件列表
     * @param useSequential 是否使用顺序模式（减少内存占用）
     * @return Manifest 条目迭代器
     */
    public Iterator<ManifestEntry> readManifestEntries(
            List<ManifestFileMeta> manifests, boolean useSequential) {
        // 根据扫描模式选择合并或非合并读取
        return scanMode == ScanMode.ALL
                ? readAndMergeFileEntries(manifests, Function.identity(), useSequential)
                : readAndNoMergeFileEntries(manifests, Function.identity(), useSequential);
    }

    /**
     * 读取并合并文件条目（用于批量扫描）。
     *
     * <p>合并过程：
     * <ol>
     *   <li>先读取所有标记为 DELETE 的文件</li>
     *   <li>然后读取标记为 ADD 的文件</li>
     *   <li>过滤掉已被删除的文件</li>
     * </ol>
     *
     * @param manifests Manifest 文件列表
     * @param converter 转换函数，用于转换为目标类型
     * @param useSequential 是否使用顺序模式
     */
    private <T extends FileEntry> Iterator<T> readAndMergeFileEntries(
            List<ManifestFileMeta> manifests,
            Function<List<ManifestEntry>, List<T>> converter,
            boolean useSequential) {
        // 第一步：读取所有删除的文件标识
        Set<Identifier> deletedEntries =
                FileEntry.readDeletedEntries(
                        manifest -> readManifest(manifest, FileEntry.deletedFilter(), null),
                        manifests,
                        parallelism);

        // 第二步：过滤掉没有新增文件的 Manifest
        manifests =
                manifests.stream()
                        .filter(file -> file.numAddedFiles() > 0)
                        .collect(Collectors.toList());

        // 第三步：读取新增的文件并过滤掉已删除的
        Function<ManifestFileMeta, List<T>> processor =
                manifest ->
                        converter.apply(
                                readManifest(
                                        manifest,
                                        FileEntry.addFilter(),
                                        entry -> !deletedEntries.contains(entry.identifier())));
        // 根据模式选择执行方式
        if (useSequential) {
            return sequentialBatchedExecute(processor, manifests, parallelism).iterator();
        } else {
            return randomlyExecuteSequentialReturn(processor, manifests, parallelism);
        }
    }

    /**
     * 读取文件条目但不合并（用于增量扫描）。
     *
     * <p>直接读取所有条目，保留 ADD 和 DELETE 标记。
     */
    private <T extends FileEntry> Iterator<T> readAndNoMergeFileEntries(
            List<ManifestFileMeta> manifests,
            Function<List<ManifestEntry>, List<T>> converter,
            boolean useSequential) {
        Function<ManifestFileMeta, List<T>> reader =
                manifest -> converter.apply(readManifest(manifest));
        if (useSequential) {
            return sequentialBatchedExecute(reader, manifests, parallelism).iterator();
        } else {
            return randomlyExecuteSequentialReturn(reader, manifests, parallelism);
        }
    }

    /** 读取 Manifest 文件列表 */
    private ManifestsReader.Result readManifests() {
        return manifestsReader.read(specifiedSnapshot, scanMode);
    }

    // ------------------------------------------------------------------------
    // 线程安全方法开始：以下方法需要是线程安全的，因为它们会被多个线程调用
    // ------------------------------------------------------------------------

    /**
     * 获取指定 ID 的表 Schema。
     *
     * <p>注意：保持线程安全。使用缓存避免重复读取。
     */
    protected TableSchema scanTableSchema(long id) {
        return tableSchemas.computeIfAbsent(
                id, key -> key == schema.id() ? schema : schemaManager.schema(id));
    }

    /**
     * 根据统计信息过滤 Manifest 条目。
     *
     * <p>注意：保持线程安全。由子类实现具体的过滤逻辑。
     */
    protected abstract boolean filterByStats(ManifestEntry entry);

    /** 是否启用后置过滤 */
    protected boolean postFilterManifestEntriesEnabled() {
        return false;
    }

    /** 对 Manifest 条目进行后置过滤 */
    protected List<ManifestEntry> postFilterManifestEntries(List<ManifestEntry> entries) {
        throw new UnsupportedOperationException();
    }

    /**
     * 读取单个 Manifest 文件的内容。
     *
     * <p>注意：保持线程安全。
     */
    @Override
    public List<ManifestEntry> readManifest(ManifestFileMeta manifest) {
        return readManifest(manifest, null, null);
    }

    /**
     * 读取单个 Manifest 文件并应用过滤器。
     *
     * @param manifest Manifest 文件元数据
     * @param additionalFilter 额外的行级过滤器（InternalRow 级别，避免反序列化）
     * @param additionalTFilter 额外的条目级过滤器
     * @return 过滤后的 Manifest 条目列表
     */
    private List<ManifestEntry> readManifest(
            ManifestFileMeta manifest,
            @Nullable Filter<InternalRow> additionalFilter,
            @Nullable Filter<ManifestEntry> additionalTFilter) {
        // 读取 Manifest 文件，应用各种过滤器
        List<ManifestEntry> entries =
                manifestFileFactory
                        .create()
                        .withCacheMetrics(
                                scanMetrics != null ? scanMetrics.getCacheMetrics() : null)
                        .read(
                                manifest.fileName(),
                                manifest.fileSize(),
                                manifestsReader.partitionFilter(),
                                createBucketFilter(),
                                createEntryRowFilter().and(additionalFilter),
                                entry ->
                                        (additionalTFilter == null || additionalTFilter.test(entry))
                                                && (manifestEntryFilter == null
                                                        || manifestEntryFilter.test(entry))
                                                && filterByStats(entry));
        // 如果需要丢弃统计信息以减少内存
        if (dropStats) {
            List<ManifestEntry> copied = new ArrayList<>(entries.size());
            for (ManifestEntry entry : entries) {
                copied.add(dropStats(entry));
            }
            entries = copied;
        }
        return entries;
    }

    /** 丢弃条目的统计信息 */
    protected ManifestEntry dropStats(ManifestEntry entry) {
        return entry.copyWithoutStats();
    }

    /** 创建桶过滤器 */
    private BucketFilter createBucketFilter() {
        return BucketFilter.create(
                onlyReadRealBuckets, specifiedBucket, bucketFilter, totalAwareBucketFilter);
    }

    /**
     * 根据当前所需的分区和桶创建条目行过滤器。
     *
     * <p>实现为 {@link InternalRow} 是为了性能（无需反序列化）。
     * 在 InternalRow 级别进行过滤可以避免完整的对象创建开销。
     */
    private Filter<InternalRow> createEntryRowFilter() {
        // 获取各种字段的 Getter 函数
        Function<InternalRow, BinaryRow> partitionGetter =
                ManifestEntrySerializer.partitionGetter();
        Function<InternalRow, Integer> bucketGetter = ManifestEntrySerializer.bucketGetter();
        Function<InternalRow, Integer> totalBucketGetter =
                ManifestEntrySerializer.totalBucketGetter();
        Function<InternalRow, String> fileNameGetter = ManifestEntrySerializer.fileNameGetter();
        PartitionPredicate partitionFilter = manifestsReader.partitionFilter();
        Function<InternalRow, Integer> levelGetter = ManifestEntrySerializer.levelGetter();
        BucketFilter bucketFilter = createBucketFilter();
        // 返回组合过滤器
        return row -> {
            // 分区过滤
            if ((partitionFilter != null && !partitionFilter.test(partitionGetter.apply(row)))) {
                return false;
            }

            // 桶过滤
            if (bucketFilter != null) {
                int bucket = bucketGetter.apply(row);
                int totalBucket = totalBucketGetter.apply(row);
                if (!bucketFilter.test(bucket, totalBucket)) {
                    return false;
                }
            }

            // 层级过滤
            int level = levelGetter.apply(row);
            if (specifiedLevel != null && level != specifiedLevel) {
                return false;
            }

            if (levelFilter != null && !levelFilter.test(level)) {
                return false;
            }

            // 文件名过滤
            return fileNameFilter == null || fileNameFilter.test((fileNameGetter.apply(row)));
        };
    }

    // ------------------------------------------------------------------------
    // 线程安全方法结束
    // ------------------------------------------------------------------------
}
