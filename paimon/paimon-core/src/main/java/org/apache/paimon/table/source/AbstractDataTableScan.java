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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.snapshot.CompactedStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousCompactorStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousFromSnapshotFullStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousFromSnapshotStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousFromTimestampStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousLatestStartingScanner;
import org.apache.paimon.table.source.snapshot.EmptyResultStartingScanner;
import org.apache.paimon.table.source.snapshot.FileCreationTimeStartingScanner;
import org.apache.paimon.table.source.snapshot.FullCompactedStartingScanner;
import org.apache.paimon.table.source.snapshot.FullStartingScanner;
import org.apache.paimon.table.source.snapshot.IncrementalDeltaStartingScanner;
import org.apache.paimon.table.source.snapshot.IncrementalDiffStartingScanner;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.apache.paimon.table.source.snapshot.StaticFromSnapshotStartingScanner;
import org.apache.paimon.table.source.snapshot.StaticFromTagStartingScanner;
import org.apache.paimon.table.source.snapshot.StaticFromTimestampStartingScanner;
import org.apache.paimon.table.source.snapshot.StaticFromWatermarkStartingScanner;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

import static org.apache.paimon.CoreOptions.FULL_COMPACTION_DELTA_COMMITS;
import static org.apache.paimon.CoreOptions.IncrementalBetweenScanMode.DIFF;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 抽象数据表扫描类，位于 {@link FileStoreScan} 之上，提供输入分片生成功能。
 *
 * <p>AbstractDataTableScan 是所有数据表扫描实现的抽象基类，封装了通用的扫描逻辑，
 * 包括：过滤下推、分区裁剪、快照选择、分片生成等。
 *
 * <h3>架构层次</h3>
 * <pre>
 * DataTableScan (接口)
 *     ↓
 * AbstractDataTableScan (抽象基类，通用逻辑)
 *     ↓
 * ├── DataTableBatchScan (批量扫描)
 * └── DataTableStreamScan (流式扫描)
 * </pre>
 *
 * <h3>核心组件</h3>
 * <ul>
 *   <li><b>SnapshotReader</b>: 快照读取器，负责从快照读取数据文件</li>
 *   <li><b>StartingScanner</b>: 起始扫描器，确定从哪个快照开始扫描</li>
 *   <li><b>SplitGenerator</b>: 分片生成器，将数据文件转换为 Split</li>
 *   <li><b>TableQueryAuth</b>: 查询授权，控制数据访问权限</li>
 * </ul>
 *
 * <h3>主要功能</h3>
 * <ul>
 *   <li><b>过滤下推</b>: 将谓词下推到扫描层，利用统计信息过滤文件</li>
 *   <li><b>分区裁剪</b>: 根据分区条件过滤不需要扫描的分区</li>
 *   <li><b>快照选择</b>: 支持多种快照选择策略（时间旅行、增量读取等）</li>
 *   <li><b>分片生成</b>: 将数据文件打包成 Split，支持并行读取</li>
 *   <li><b>查询授权</b>: 支持细粒度的数据访问控制</li>
 * </ul>
 *
 * <h3>起始扫描器（StartingScanner）</h3>
 * <p>用于确定扫描的起始快照，支持多种策略：
 * <ul>
 *   <li><b>FullStartingScanner</b>: 全量扫描（从最新快照开始）</li>
 *   <li><b>StaticFromSnapshotStartingScanner</b>: 从指定快照开始</li>
 *   <li><b>StaticFromTimestampStartingScanner</b>: 从指定时间戳开始</li>
 *   <li><b>IncrementalDeltaStartingScanner</b>: 增量扫描（两个快照之间的变化）</li>
 *   <li><b>ContinuousLatestStartingScanner</b>: 持续扫描（从最新快照开始）</li>
 * </ul>
 *
 * <h3>使用流程</h3>
 * <pre>{@code
 * // 1. 创建扫描器
 * AbstractDataTableScan scan = new DataTableBatchScan(...);
 *
 * // 2. 配置扫描参数
 * scan.withFilter(predicate)                  // 设置过滤条件
 *     .withPartitionFilter(partitionFilter)   // 设置分区过滤
 *     .withReadType(projectedType)            // 设置列裁剪
 *     .withLimit(1000);                       // 设置行数限制
 *
 * // 3. 生成扫描计划
 * Plan plan = scan.plan();
 * List<Split> splits = plan.splits();
 * }</pre>
 *
 * <h3>子类实现</h3>
 * <ul>
 *   <li>{@link DataTableBatchScan}: 实现批量扫描逻辑（一次性读取）</li>
 *   <li>{@link DataTableStreamScan}: 实现流式扫描逻辑（持续读取）</li>
 * </ul>
 *
 * @see DataTableScan 数据表扫描接口
 * @see DataTableBatchScan 批量扫描实现
 * @see DataTableStreamScan 流式扫描实现
 * @see SnapshotReader 快照读取器
 * @see StartingScanner 起始扫描器
 */
abstract class AbstractDataTableScan implements DataTableScan {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractDataTableScan.class);

    protected final TableSchema schema;
    private final CoreOptions options;
    protected final SnapshotReader snapshotReader;
    private final TableQueryAuth queryAuth;

    @Nullable private RowType readType;

    protected AbstractDataTableScan(
            TableSchema schema,
            CoreOptions options,
            SnapshotReader snapshotReader,
            TableQueryAuth queryAuth) {
        this.schema = schema;
        this.options = options;
        this.snapshotReader = snapshotReader;
        this.queryAuth = queryAuth;
    }

    /**
     * 生成扫描计划（包含查询授权处理）。
     *
     * <p>该方法首先进行查询授权检查，然后生成扫描计划。
     * 如果授权结果需要转换计划（如限制访问范围），会应用转换。
     *
     * @return 扫描计划（可能经过授权转换）
     */
    @Override
    public final TableScan.Plan plan() {
        TableQueryAuthResult queryAuthResult = authQuery();
        Plan plan = planWithoutAuth();
        if (queryAuthResult != null) {
            plan = queryAuthResult.convertPlan(plan);
        }
        return plan;
    }

    /**
     * 生成扫描计划（不进行授权检查）。
     *
     * <p>由子类实现具体的扫描逻辑（批量扫描或流式扫描）。
     *
     * @return 扫描计划
     */
    protected abstract TableScan.Plan planWithoutAuth();

    /**
     * 设置过滤谓词（覆盖父接口方法）。
     *
     * @param predicate 过滤谓词
     * @return this
     */
    @Override
    public InnerTableScan withFilter(Predicate predicate) {
        snapshotReader.withFilter(predicate);
        return this;
    }

    /**
     * 设置桶过滤（只扫描指定的桶）。
     *
     * @param bucket 桶号
     * @return this
     */
    @Override
    public AbstractDataTableScan withBucket(int bucket) {
        snapshotReader.withBucket(bucket);
        return this;
    }

    /**
     * 设置桶过滤器（自定义桶过滤逻辑）。
     *
     * @param bucketFilter 桶过滤器
     * @return this
     */
    @Override
    public AbstractDataTableScan withBucketFilter(Filter<Integer> bucketFilter) {
        snapshotReader.withBucketFilter(bucketFilter);
        return this;
    }

    /**
     * 设置要读取的列类型（列裁剪）。
     *
     * @param readType 要读取的列类型
     * @return this
     */
    @Override
    public InnerTableScan withReadType(@Nullable RowType readType) {
        this.readType = readType;
        snapshotReader.withReadType(readType);
        return this;
    }

    /**
     * 设置分区过滤（使用分区键值对）。
     *
     * @param partitionSpec 分区键值对
     * @return this
     */
    @Override
    public AbstractDataTableScan withPartitionFilter(Map<String, String> partitionSpec) {
        snapshotReader.withPartitionFilter(partitionSpec);
        return this;
    }

    /**
     * 设置分区过滤（使用分区列表）。
     *
     * @param partitions 分区列表
     * @return this
     */
    @Override
    public AbstractDataTableScan withPartitionFilter(List<BinaryRow> partitions) {
        snapshotReader.withPartitionFilter(partitions);
        return this;
    }

    @Override
    public AbstractDataTableScan withPartitionsFilter(List<Map<String, String>> partitions) {
        snapshotReader.withPartitionsFilter(partitions);
        return this;
    }

    @Override
    public AbstractDataTableScan withPartitionFilter(PartitionPredicate partitionPredicate) {
        snapshotReader.withPartitionFilter(partitionPredicate);
        return this;
    }

    @Override
    public InnerTableScan withPartitionFilter(Predicate predicate) {
        snapshotReader.withPartitionFilter(predicate);
        return this;
    }

    @Override
    public AbstractDataTableScan withLevelFilter(Filter<Integer> levelFilter) {
        snapshotReader.withLevelFilter(levelFilter);
        return this;
    }

    public AbstractDataTableScan withMetricRegistry(MetricRegistry metricsRegistry) {
        snapshotReader.withMetricRegistry(metricsRegistry);
        return this;
    }

    @Nullable
    protected TableQueryAuthResult authQuery() {
        if (!options.queryAuthEnabled()) {
            return null;
        }
        return queryAuth.auth(readType == null ? null : readType.getFieldNames());
    }

    @Override
    public AbstractDataTableScan dropStats() {
        snapshotReader.dropStats();
        return this;
    }

    @Override
    public InnerTableScan withRowRanges(List<Range> rowRanges) {
        snapshotReader.withRowRanges(rowRanges);
        return this;
    }

    public SnapshotReader snapshotReader() {
        return snapshotReader;
    }

    public CoreOptions options() {
        return options;
    }

    protected StartingScanner createStartingScanner(boolean isStreaming) {
        SnapshotManager snapshotManager = snapshotReader.snapshotManager();
        ChangelogManager changelogManager = snapshotReader.changelogManager();
        CoreOptions.StreamScanMode type =
                options.toConfiguration().get(CoreOptions.STREAM_SCAN_MODE);
        switch (type) {
            case COMPACT_BUCKET_TABLE:
                checkArgument(
                        isStreaming, "Set 'streaming-compact' in batch mode. This is unexpected.");
                return new ContinuousCompactorStartingScanner(snapshotManager);
            case FILE_MONITOR:
                return new FullStartingScanner(snapshotManager);
        }

        // read from consumer id
        String consumerId = options.consumerId();
        if (isStreaming && consumerId != null && !options.consumerIgnoreProgress()) {
            ConsumerManager consumerManager = snapshotReader.consumerManager();
            Optional<Consumer> consumer = consumerManager.consumer(consumerId);
            if (consumer.isPresent()) {
                return new ContinuousFromSnapshotStartingScanner(
                        snapshotManager,
                        changelogManager,
                        consumer.get().nextSnapshot(),
                        options.changelogLifecycleDecoupled());
            }
        }

        CoreOptions.StartupMode startupMode = options.startupMode();
        switch (startupMode) {
            case LATEST_FULL:
                return new FullStartingScanner(snapshotManager);
            case LATEST:
                return isStreaming
                        ? new ContinuousLatestStartingScanner(snapshotManager)
                        : new FullStartingScanner(snapshotManager);
            case COMPACTED_FULL:
                if (options.changelogProducer() == ChangelogProducer.FULL_COMPACTION
                        || options.toConfiguration().contains(FULL_COMPACTION_DELTA_COMMITS)) {
                    int deltaCommits =
                            options.toConfiguration()
                                    .getOptional(FULL_COMPACTION_DELTA_COMMITS)
                                    .orElse(1);
                    return new FullCompactedStartingScanner(snapshotManager, deltaCommits);
                } else {
                    return new CompactedStartingScanner(snapshotManager);
                }
            case FROM_TIMESTAMP:
                String timestampStr = options.scanTimestamp();
                Long startupMillis = options.scanTimestampMills();
                if (startupMillis == null && timestampStr != null) {
                    startupMillis =
                            DateTimeUtils.parseTimestampData(timestampStr, 3, TimeZone.getDefault())
                                    .getMillisecond();
                }
                return isStreaming
                        ? new ContinuousFromTimestampStartingScanner(
                                snapshotManager,
                                changelogManager,
                                startupMillis,
                                options.changelogLifecycleDecoupled())
                        : new StaticFromTimestampStartingScanner(snapshotManager, startupMillis);
            case FROM_FILE_CREATION_TIME:
                Long fileCreationTimeMills = options.scanFileCreationTimeMills();
                return new FileCreationTimeStartingScanner(snapshotManager, fileCreationTimeMills);
            case FROM_CREATION_TIMESTAMP:
                Long creationTimeMills = options.scanCreationTimeMills();
                return createCreationTimestampStartingScanner(
                        snapshotManager,
                        changelogManager,
                        creationTimeMills,
                        options.changelogLifecycleDecoupled(),
                        isStreaming);

            case FROM_SNAPSHOT:
                if (options.scanSnapshotId() != null) {
                    return isStreaming
                            ? new ContinuousFromSnapshotStartingScanner(
                                    snapshotManager,
                                    changelogManager,
                                    options.scanSnapshotId(),
                                    options.changelogLifecycleDecoupled())
                            : new StaticFromSnapshotStartingScanner(
                                    snapshotManager, options.scanSnapshotId());
                } else if (options.scanWatermark() != null) {
                    checkArgument(!isStreaming, "Cannot scan from watermark in streaming mode.");
                    return new StaticFromWatermarkStartingScanner(
                            snapshotManager, options().scanWatermark());
                } else if (options.scanTagName() != null) {
                    checkArgument(!isStreaming, "Cannot scan from tag in streaming mode.");
                    return new StaticFromTagStartingScanner(
                            snapshotManager, options().scanTagName());
                } else {
                    throw new UnsupportedOperationException("Unknown snapshot read mode");
                }
            case FROM_SNAPSHOT_FULL:
                Long scanSnapshotId = options.scanSnapshotId();
                checkNotNull(
                        scanSnapshotId,
                        "scan.snapshot-id must be set when startupMode is FROM_SNAPSHOT_FULL.");
                return isStreaming
                        ? new ContinuousFromSnapshotFullStartingScanner(
                                snapshotManager, scanSnapshotId)
                        : new StaticFromSnapshotStartingScanner(snapshotManager, scanSnapshotId);
            case INCREMENTAL:
                checkArgument(!isStreaming, "Cannot read incremental in streaming mode.");
                return createIncrementalStartingScanner(snapshotManager);
            default:
                throw new UnsupportedOperationException(
                        "Unknown startup mode " + startupMode.name());
        }
    }

    public static StartingScanner createCreationTimestampStartingScanner(
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            long creationMillis,
            boolean changelogDecoupled,
            boolean isStreaming) {
        Long startingSnapshotPrevId =
                TimeTravelUtil.earlierThanTimeMills(
                        snapshotManager,
                        changelogManager,
                        creationMillis,
                        changelogDecoupled,
                        true);
        final StartingScanner scanner;
        Optional<Long> startingSnapshotId =
                Optional.ofNullable(startingSnapshotPrevId)
                        .map(id -> id + 1)
                        .filter(
                                id ->
                                        snapshotManager.snapshotExists(id)
                                                || changelogManager.longLivedChangelogExists(id));
        if (startingSnapshotId.isPresent()) {
            scanner =
                    isStreaming
                            ? new ContinuousFromSnapshotStartingScanner(
                                    snapshotManager,
                                    changelogManager,
                                    startingSnapshotId.get(),
                                    changelogDecoupled)
                            : new StaticFromSnapshotStartingScanner(
                                    snapshotManager, startingSnapshotId.get());
        } else {
            scanner = new FileCreationTimeStartingScanner(snapshotManager, creationMillis);
        }
        return scanner;
    }

    private StartingScanner createIncrementalStartingScanner(SnapshotManager snapshotManager) {
        Options conf = options.toConfiguration();

        if (conf.contains(CoreOptions.INCREMENTAL_BETWEEN)) {
            Pair<String, String> incrementalBetween = options.incrementalBetween();

            TagManager tagManager =
                    new TagManager(
                            snapshotManager.fileIO(),
                            snapshotManager.tablePath(),
                            snapshotManager.branch());
            Optional<Tag> startTag = tagManager.get(incrementalBetween.getLeft());
            Optional<Tag> endTag = tagManager.get(incrementalBetween.getRight());

            if (startTag.isPresent() && endTag.isPresent()) {
                if (options.incrementalBetweenTagToSnapshot()) {
                    CoreOptions.IncrementalBetweenScanMode scanMode =
                            options.incrementalBetweenScanMode();
                    return IncrementalDeltaStartingScanner.betweenSnapshotIds(
                            startTag.get().id(),
                            endTag.get().id(),
                            snapshotManager,
                            toSnapshotScanMode(scanMode));
                } else {
                    return IncrementalDiffStartingScanner.betweenTags(
                            startTag.get(), endTag.get(), snapshotManager, incrementalBetween);
                }
            } else {
                long startId, endId;
                try {
                    startId = Long.parseLong(incrementalBetween.getLeft());
                    endId = Long.parseLong(incrementalBetween.getRight());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Didn't find two tags for start '%s' and end '%s', and they are not two snapshot Ids. "
                                            + "Please set two tags or two snapshot Ids.",
                                    incrementalBetween.getLeft(), incrementalBetween.getRight()));
                }

                checkArgument(
                        endId >= startId,
                        "Ending snapshotId should >= starting snapshotId %s.",
                        endId,
                        startId);

                if (snapshotManager.earliestSnapshot() == null) {
                    LOG.warn("There is currently no snapshot. Waiting for snapshot generation.");
                    return new EmptyResultStartingScanner(snapshotManager);
                }

                if (startId == endId) {
                    return new EmptyResultStartingScanner(snapshotManager);
                }

                CoreOptions.IncrementalBetweenScanMode scanMode =
                        options.incrementalBetweenScanMode();
                return scanMode == DIFF
                        ? IncrementalDiffStartingScanner.betweenSnapshotIds(
                                startId, endId, snapshotManager)
                        : IncrementalDeltaStartingScanner.betweenSnapshotIds(
                                startId, endId, snapshotManager, toSnapshotScanMode(scanMode));
            }
        } else if (conf.contains(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP)) {
            String incrementalBetweenStr = options.incrementalBetweenTimestamp();
            String[] split = incrementalBetweenStr.split(",");
            if (split.length != 2) {
                throw new IllegalArgumentException(
                        "The incremental-between-timestamp must specific start(exclusive) and end timestamp. But is: "
                                + incrementalBetweenStr);
            }

            Pair<Long, Long> incrementalBetween;
            try {
                incrementalBetween = Pair.of(Long.parseLong(split[0]), Long.parseLong(split[1]));
            } catch (NumberFormatException nfe) {
                try {
                    long startTimestamp =
                            DateTimeUtils.parseTimestampData(split[0], 3, TimeZone.getDefault())
                                    .getMillisecond();
                    long endTimestamp =
                            DateTimeUtils.parseTimestampData(split[1], 3, TimeZone.getDefault())
                                    .getMillisecond();
                    incrementalBetween = Pair.of(startTimestamp, endTimestamp);
                } catch (Exception e) {
                    throw new IllegalArgumentException(
                            "The incremental-between-timestamp must specific start(exclusive) and end timestamp. But is: "
                                    + incrementalBetweenStr);
                }
            }

            Snapshot earliestSnapshot = snapshotManager.earliestSnapshot();
            Snapshot latestSnapshot = snapshotManager.latestSnapshot();
            if (earliestSnapshot == null || latestSnapshot == null) {
                return new EmptyResultStartingScanner(snapshotManager);
            }

            long startTimestamp = incrementalBetween.getLeft();
            long endTimestamp = incrementalBetween.getRight();
            checkArgument(
                    endTimestamp >= startTimestamp,
                    "Ending timestamp %s should be >= starting timestamp %s.",
                    endTimestamp,
                    startTimestamp);

            if (startTimestamp == endTimestamp
                    || startTimestamp > latestSnapshot.timeMillis()
                    || endTimestamp < earliestSnapshot.timeMillis()) {
                return new EmptyResultStartingScanner(snapshotManager);
            }

            CoreOptions.IncrementalBetweenScanMode scanMode = options.incrementalBetweenScanMode();

            return scanMode == DIFF
                    ? IncrementalDiffStartingScanner.betweenTimestamps(
                            startTimestamp, endTimestamp, snapshotManager)
                    : IncrementalDeltaStartingScanner.betweenTimestamps(
                            startTimestamp,
                            endTimestamp,
                            snapshotManager,
                            toSnapshotScanMode(scanMode));
        } else if (conf.contains(CoreOptions.INCREMENTAL_TO_AUTO_TAG)) {
            String endTag = options.incrementalToAutoTag();
            return IncrementalDiffStartingScanner.toEndAutoTag(snapshotManager, endTag, options);
        } else {
            throw new UnsupportedOperationException("Unknown incremental read mode.");
        }
    }

    private ScanMode toSnapshotScanMode(CoreOptions.IncrementalBetweenScanMode scanMode) {
        switch (scanMode) {
            case AUTO:
                return options.changelogProducer() == ChangelogProducer.NONE
                        ? ScanMode.DELTA
                        : ScanMode.CHANGELOG;
            case DELTA:
                return ScanMode.DELTA;
            case CHANGELOG:
                return ScanMode.CHANGELOG;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported incremental scan mode " + scanMode.name());
        }
    }
}
