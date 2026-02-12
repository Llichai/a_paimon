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

package org.apache.paimon.flink.action;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.MultiTableAppendCompactTask;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.compact.AppendTableCompact;
import org.apache.paimon.flink.sink.BucketsRowChannelComputer;
import org.apache.paimon.flink.sink.CombinedTableCompactorSink;
import org.apache.paimon.flink.sink.CompactorSinkBuilder;
import org.apache.paimon.flink.source.CombinedTableCompactorSourceBuilder;
import org.apache.paimon.flink.source.CompactorSourceBuilder;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.paimon.flink.sink.FlinkStreamPartitioner.partition;
import static org.apache.paimon.flink.sink.FlinkStreamPartitioner.rebalance;

/**
 * 数据库压缩操作 - 用于压缩数据库中的所有表。
 *
 * <p>CompactDatabaseAction 用于批量压缩数据库中的多个表，将小文件合并为大文件以提高查询性能和
 * 减少存储开销。支持通过正则表达式选择特定的表进行压缩，可配置压缩模式和分区隔离策略。
 *
 * <p>压缩模式：
 * <ul>
 *   <li><b>DIVIDED</b>: 分离模式，每个表的数据流分别处理（默认）
 *   <li><b>COMBINED</b>: 组合模式，所有表的数据流合并处理，提高资源利用率
 * </ul>
 *
 * <p>过滤配置：
 * <ul>
 *   <li><b>表过滤</b>: 使用正则表达式选择要压缩的表
 *   <li><b>数据库过滤</b>: 选择特定的数据库进行压缩
 *   <li><b>排除过滤</b>: 排除某些不需要压缩的表
 * </ul>
 *
 * <p>压缩配置：
 * <ul>
 *   <li><b>分区隔离</b>: 配置多久未更新的分区被视为闲置
 *   <li><b>完整压缩</b>: 是否进行完整压缩（压缩所有文件）还是增量压缩
 *   <li><b>表选项</b>: 为压缩指定特定的表选项
 * </ul>
 *
 * <p>应用场景：
 * <ul>
 *   <li><b>批量优化</b>: 定期压缩整个数据库以维持性能
 *   <li><b>存储清理</b>: 清理小文件，减少存储碎片
 *   <li><b>性能优化</b>: 通过减少文件数提高查询速度
 *   <li><b>定期维护</b>: 作为日常维护任务的一部分
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * CompactDatabaseAction action = new CompactDatabaseAction(catalogConfig);
 *
 * // 设置只压缩 analytics_db 数据库中的表
 * action.databasePattern("analytics_db");
 *
 * // 选择匹配 events_* 的表进行压缩
 * action.includingPattern("events_.*");
 *
 * // 排除临时表
 * action.excludingPattern("temp_.*");
 *
 * // 30 分钟未更新的分区视为闲置
 * action.partitionIdleTime("30 min");
 *
 * action.run();
 * }</pre>
 *
 * @see CompactAction
 * @see SortCompactAction
 */
public class CompactDatabaseAction extends ActionBase {
    private static final Logger LOG = LoggerFactory.getLogger(CompactDatabaseAction.class);

    /** 要压缩的表的匹配模式（默认匹配所有表） */
    private Pattern includingPattern = Pattern.compile(".*");
    /** 要排除的表的匹配模式（可选） */
    @Nullable private Pattern excludingPattern;
    /** 数据库的匹配模式（默认匹配所有数据库） */
    private Pattern databasePattern = Pattern.compile(".*");

    /** 数据库压缩模式：DIVIDED（分离）或 COMBINED（组合） */
    private MultiTablesSinkMode databaseCompactMode = MultiTablesSinkMode.DIVIDED;

    /** 缓存的表对象映射 */
    private final Map<String, FileStoreTable> tableMap = new HashMap<>();

    /** 应用于所有表的压缩选项 */
    private Options tableOptions = new Options();

    /** 分区闲置时间：超过此时间未更新的分区被视为闲置 */
    @Nullable private Duration partitionIdleTime = null;

    /** 是否执行完整压缩（压缩所有文件） */
    private Boolean fullCompaction;

    /** 是否使用流处理模式 */
    private boolean isStreaming;

    public CompactDatabaseAction(Map<String, String> catalogConfig) {
        super(catalogConfig);
    }

    public CompactDatabaseAction includingDatabases(@Nullable String includingDatabases) {
        if (includingDatabases != null) {
            this.databasePattern = Pattern.compile(includingDatabases);
        }
        return this;
    }

    public CompactDatabaseAction includingTables(@Nullable String includingTables) {
        if (includingTables != null) {
            this.includingPattern = Pattern.compile(includingTables);
        }
        return this;
    }

    public CompactDatabaseAction excludingTables(@Nullable String excludingTables) {
        this.excludingPattern = excludingTables == null ? null : Pattern.compile(excludingTables);
        return this;
    }

    public CompactDatabaseAction withDatabaseCompactMode(@Nullable String mode) {
        this.databaseCompactMode = MultiTablesSinkMode.fromString(mode);
        return this;
    }

    public CompactDatabaseAction withTableOptions(Map<String, String> tableOptions) {
        this.tableOptions = Options.fromMap(tableOptions);
        return this;
    }

    public CompactDatabaseAction withPartitionIdleTime(@Nullable Duration partitionIdleTime) {
        this.partitionIdleTime = partitionIdleTime;
        return this;
    }

    public CompactDatabaseAction withFullCompaction(boolean fullCompaction) {
        this.fullCompaction = fullCompaction;
        return this;
    }

    private boolean shouldCompactionTable(String paimonFullTableName) {
        boolean shouldCompaction = includingPattern.matcher(paimonFullTableName).matches();
        if (excludingPattern != null) {
            shouldCompaction =
                    shouldCompaction && !excludingPattern.matcher(paimonFullTableName).matches();
        }
        if (!shouldCompaction) {
            LOG.debug("Source table '{}' is excluded.", paimonFullTableName);
        }
        return shouldCompaction;
    }

    @Override
    public void build() {
        ReadableConfig conf = env.getConfiguration();
        isStreaming = conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING;

        if (fullCompaction == null) {
            fullCompaction = !isStreaming;
        }
        if (databaseCompactMode == MultiTablesSinkMode.DIVIDED) {
            buildForDividedMode();
        } else {
            buildForCombinedMode();
        }
    }

    private void buildForDividedMode() {
        try {
            List<String> databases = catalog.listDatabases();
            for (String databaseName : databases) {
                Matcher databaseMatcher = databasePattern.matcher(databaseName);
                if (databaseMatcher.matches()) {
                    List<String> tables = catalog.listTables(databaseName);
                    for (String tableName : tables) {
                        String fullTableName = String.format("%s.%s", databaseName, tableName);
                        if (shouldCompactionTable(fullTableName)) {
                            Table table =
                                    catalog.getTable(Identifier.create(databaseName, tableName));
                            if (!(table instanceof FileStoreTable)) {
                                LOG.error(
                                        String.format(
                                                "Only FileStoreTable supports compact action. The table type is '%s'.",
                                                table.getClass().getName()));
                                continue;
                            }
                            Map<String, String> dynamicOptions =
                                    new HashMap<>(tableOptions.toMap());
                            dynamicOptions.put(CoreOptions.WRITE_ONLY.key(), "false");
                            FileStoreTable fileStoreTable =
                                    (FileStoreTable) table.copy(dynamicOptions);
                            tableMap.put(fullTableName, fileStoreTable);
                        } else {
                            LOG.debug("The table {} is excluded.", fullTableName);
                        }
                    }
                }
            }
        } catch (Catalog.DatabaseNotExistException | Catalog.TableNotExistException e) {
            throw new RuntimeException(e);
        }

        Preconditions.checkState(
                !tableMap.isEmpty(),
                "no tables to be compacted. possible cause is that there are no tables detected after pattern matching");

        for (Map.Entry<String, FileStoreTable> entry : tableMap.entrySet()) {
            FileStoreTable fileStoreTable = entry.getValue();
            switch (fileStoreTable.bucketMode()) {
                case BUCKET_UNAWARE:
                    {
                        buildForUnawareBucketCompaction(env, entry.getKey(), fileStoreTable);
                        break;
                    }
                case HASH_FIXED:
                case HASH_DYNAMIC:
                default:
                    {
                        buildForTraditionalCompaction(env, entry.getKey(), fileStoreTable);
                    }
            }
        }
    }

    private void buildForCombinedMode() {

        CombinedTableCompactorSourceBuilder sourceBuilder =
                new CombinedTableCompactorSourceBuilder(
                                catalogLoader(),
                                databasePattern,
                                includingPattern,
                                excludingPattern,
                                tableOptions
                                        .get(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL)
                                        .toMillis())
                        .withPartitionIdleTime(partitionIdleTime);
        sourceBuilder.withTableOptions(tableOptions.toMap());

        Integer parallelism =
                tableOptions.get(FlinkConnectorOptions.SINK_PARALLELISM) == null
                        ? env.getParallelism()
                        : tableOptions.get(FlinkConnectorOptions.SINK_PARALLELISM);

        // multi bucket table which has multi bucket in a partition like fix bucket and dynamic
        // bucket
        DataStream<RowData> awareBucketTableSource =
                partition(
                        sourceBuilder
                                .withEnv(env)
                                .withContinuousMode(isStreaming)
                                .buildAwareBucketTableSource(),
                        new BucketsRowChannelComputer(),
                        parallelism);

        // unaware bucket table
        DataStream<MultiTableAppendCompactTask> unawareBucketTableSource =
                rebalance(
                        sourceBuilder
                                .withEnv(env)
                                .withContinuousMode(isStreaming)
                                .buildForUnawareBucketsTableSource(),
                        parallelism);

        new CombinedTableCompactorSink(catalogLoader(), tableOptions, fullCompaction)
                .sinkFrom(awareBucketTableSource, unawareBucketTableSource);
    }

    private void buildForTraditionalCompaction(
            StreamExecutionEnvironment env, String fullName, FileStoreTable table) {

        Preconditions.checkArgument(
                !(fullCompaction && isStreaming),
                "The full compact strategy is only supported in batch mode. Please add -Dexecution.runtime-mode=BATCH.");

        if (isStreaming) {
            // for completely asynchronous compaction
            HashMap<String, String> dynamicOptions =
                    new HashMap<String, String>() {
                        {
                            put(CoreOptions.NUM_SORTED_RUNS_STOP_TRIGGER.key(), "2147483647");
                            put(CoreOptions.SORT_SPILL_THRESHOLD.key(), "10");
                            put(CoreOptions.LOOKUP_WAIT.key(), "false");
                        }
                    };
            table = table.copy(dynamicOptions);
        }

        CompactorSourceBuilder sourceBuilder =
                new CompactorSourceBuilder(fullName, table)
                        .withPartitionIdleTime(partitionIdleTime);
        CompactorSinkBuilder sinkBuilder = new CompactorSinkBuilder(table, fullCompaction);

        DataStreamSource<RowData> source =
                sourceBuilder.withEnv(env).withContinuousMode(isStreaming).build();
        sinkBuilder.withInput(source).build();
    }

    private void buildForUnawareBucketCompaction(
            StreamExecutionEnvironment env, String fullName, FileStoreTable table) {
        AppendTableCompact unawareBucketCompactionTopoBuilder =
                new AppendTableCompact(env, fullName, table);

        unawareBucketCompactionTopoBuilder.withContinuousMode(isStreaming);
        unawareBucketCompactionTopoBuilder.withPartitionIdleTime(partitionIdleTime);
        unawareBucketCompactionTopoBuilder.build();
    }

    @Override
    public void run() throws Exception {
        build();
        execute("Compact database job");
    }
}
