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

package org.apache.paimon.flink.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.NestedProjectedRowData;
import org.apache.paimon.flink.Projection;
import org.apache.paimon.flink.sink.FlinkSink;
import org.apache.paimon.flink.source.align.AlignedContinuousFileStoreSource;
import org.apache.paimon.flink.source.operator.MonitorSource;
import org.apache.paimon.flink.utils.TableScanUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;
import static org.apache.paimon.flink.FlinkConnectorOptions.SOURCE_OPERATOR_UID_SUFFIX;
import static org.apache.paimon.flink.FlinkConnectorOptions.generateCustomUid;
import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;
import static org.apache.paimon.flink.utils.ParallelismUtils.forwardParallelism;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * Flink DataStream API 源构建器 - 用于创建 Paimon 表的 Flink 读取数据流。
 *
 * <p>FlinkSourceBuilder 提供了链式 API 来配置从 Paimon 表读取数据的各种参数，
 * 支持批处理和流处理两种模式，以及多种优化选项。
 *
 * <h3>核心功能</h3>
 * <ul>
 *   <li>批处理模式：{@link #buildStaticFileSource()} - 一次性读取表的全量快照
 *   <li>流处理模式：{@link #buildContinuousFileSource()} - 持续监听表的变更
 *   <li>对齐模式：{@link #buildAlignedContinuousFileSource()} - 支持 Checkpoint 对齐
 *   <li>专属分片生成：{@link #buildDedicatedSplitGenSource(boolean)} - 优化分片生成
 * </ul>
 *
 * <h3>配置选项</h3>
 * <ul>
 *   <li>数据流环境：{@link #env(StreamExecutionEnvironment)} - 设置 Flink 执行环境
 *   <li>源名称：{@link #sourceName(String)} - 设置数据流名称
 *   <li>有界性：{@link #sourceBounded(boolean)} - 指定是批处理还是流处理
 *   <li>列投影：{@link #projection(int[])} / {@link #projection(int[][])} - 选择要读取的列
 *   <li>过滤条件：{@link #predicate(Predicate)} - 添加行级过滤
 *   <li>分区过滤：{@link #partitionPredicate(PartitionPredicate)} - 添加分区级过滤
 *   <li>行限制：{@link #limit(Long)} - 限制读取的行数
 *   <li>并行度：{@link #sourceParallelism(Integer)} - 设置读取并行度
 *   <li>水位线策略：{@link #watermarkStrategy(WatermarkStrategy)} - 设置事件时间水位线
 *   <li>动态分区过滤：{@link #dynamicPartitionFilteringFields(List)} - 设置动态分区过滤字段
 * </ul>
 *
 * <h3>使用示例 - 批处理模式</h3>
 * <pre>{@code
 * StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 * Table table = catalog.getTable(new Identifier("db", "table"));
 *
 * DataStream<RowData> dataStream = new FlinkSourceBuilder(table)
 *     .env(env)
 *     .sourceBounded(true)
 *     .projection(0, 1, 2)          // 只读取前3列
 *     .predicate(predicate)           // 添加过滤条件
 *     .sourceParallelism(4)           // 4个并行任务
 *     .build();
 *
 * dataStream.print();
 * env.execute("Batch Read Paimon");
 * }</pre>
 *
 * <h3>使用示例 - 流处理模式</h3>
 * <pre>{@code
 * StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 * env.enableCheckpointing(60000);  // 每60秒检查点一次
 * Table table = catalog.getTable(new Identifier("db", "table"));
 *
 * DataStream<RowData> dataStream = new FlinkSourceBuilder(table)
 *     .env(env)
 *     .sourceBounded(false)           // 流处理模式
 *     .watermarkStrategy(strategy)    // 事件时间处理
 *     .build();
 *
 * dataStream.print();
 * env.execute("Streaming Read Paimon");
 * }</pre>
 *
 * <h3>有序性控制</h3>
 * <ul>
 *   <li>BUCKET_UNAWARE 模式：数据可能是无序的
 *   <li>HASH_FIXED 模式：如果启用了 BUCKET_APPEND_ORDERED，保证有序
 *   <li>主键表：总是有序的
 * </ul>
 *
 * <h3>与 FlinkSink 的关系</h3>
 * <p>FlinkSourceBuilder 和 {@link FlinkSink} 配套使用，形成完整的 ETL 管道：
 * <pre>{@code
 * // 读取
 * DataStream<RowData> source = new FlinkSourceBuilder(sourceTable)
 *     .env(env).sourceBounded(false).build();
 *
 * // 处理（业务逻辑）
 * DataStream<RowData> transformed = source.map(row -> {...});
 *
 * // 写入
 * new FlinkSink(sinkTable).sinkFrom(transformed);
 *
 * env.execute("ETL Pipeline");
 * }</pre>
 *
 * @since 0.8
 * @see FlinkSink Flink 汇聚实现
 */
public class FlinkSourceBuilder {

    /** 默认数据源名称 */
    private static final String SOURCE_NAME = "Source";

    /** Paimon 表对象 */
    private final Table table;

    /** 表配置选项 */
    private final Options conf;

    /** 是否为无序数据源（BUCKET_UNAWARE 模式或 HASH_FIXED 未排序模式） */
    private final boolean unordered;

    /** 数据源名称，用于 Flink UI 显示 */
    private String sourceName;

    /** 是否为有界数据源（true=批处理，false=流处理） */
    private Boolean sourceBounded;

    /** Flink 流执行环境 */
    private StreamExecutionEnvironment env;

    /** 列投影配置（支持嵌套投影） */
    @Nullable private int[][] projectedFields;

    /** 行级过滤谓词 */
    @Nullable private Predicate predicate;

    /** 分区级过滤谓词 */
    @Nullable private PartitionPredicate partitionPredicate;

    /** 并行度设置 */
    @Nullable private Integer parallelism;

    /** 行数限制 */
    @Nullable private Long limit;

    /** 水位线策略 */
    @Nullable private WatermarkStrategy<RowData> watermarkStrategy;

    /** 动态分区过滤信息 */
    @Nullable private DynamicPartitionFilteringInfo dynamicPartitionFilteringInfo;

    /**
     * 创建 FlinkSourceBuilder 实例。
     *
     * <p>初始化时会自动检测表的特性（是否为无序），
     * 并从表的选项中读取 Flink 连接器配置。
     *
     * @param table Paimon 表对象
     */
    public FlinkSourceBuilder(Table table) {
        this.table = table;
        this.sourceName = table.name();
        this.conf = Options.fromMap(table.options());
        this.unordered = unordered(table);
    }

    /**
     * 判断表是否为无序的。
     *
     * <p>无序表的特性：
     * <ul>
     *   <li>BUCKET_UNAWARE 模式（无桶划分）- 读取数据可能无序
     *   <li>HASH_FIXED 模式（固定哈希桶）且未启用 BUCKET_APPEND_ORDERED 时
     *   <li>主键表总是有序的，无论什么模式
     * </ul>
     *
     * @param table Paimon 表对象
     * @return true 如果表数据是无序的，false 如果有序
     */
    private static boolean unordered(Table table) {
        if (!(table instanceof FileStoreTable)) {
            return false;
        }

        if (!table.primaryKeys().isEmpty()) {
            return false;
        }

        BucketMode bucketMode = ((FileStoreTable) table).bucketMode();
        if (bucketMode == BucketMode.BUCKET_UNAWARE) {
            return true;
        } else if (bucketMode == BucketMode.HASH_FIXED) {
            return !Options.fromMap(table.options()).get(CoreOptions.BUCKET_APPEND_ORDERED);
        }

        return false;
    }

    /**
     * 设置 Flink 流执行环境。
     *
     * <p>这是必要的配置，会自动检测执行模式（批处理/流处理）。
     * 如果之前未显式设置 sourceBounded，则根据环境自动设置。
     *
     * @param env Flink 执行环境
     * @return this
     */
    public FlinkSourceBuilder env(StreamExecutionEnvironment env) {
        this.env = env;
        if (sourceBounded == null) {
            sourceBounded = !FlinkSink.isStreaming(env);
        }
        return this;
    }

    /**
     * 设置数据源名称（用于 Flink UI 显示）。
     *
     * @param name 数据源名称
     * @return this
     */
    public FlinkSourceBuilder sourceName(String name) {
        this.sourceName = name;
        return this;
    }

    /**
     * 设置是否为有界数据源（批处理 vs 流处理）。
     *
     * <ul>
     *   <li>true = 批处理模式，一次性读取表的快照
     *   <li>false = 流处理模式，持续监听表的变更
     * </ul>
     *
     * @param bounded 是否为有界数据源
     * @return this
     */
    public FlinkSourceBuilder sourceBounded(boolean bounded) {
        this.sourceBounded = bounded;
        return this;
    }

    public FlinkSourceBuilder projection(int[] projectedFields) {
        return projection(Projection.of(projectedFields).toNestedIndexes());
    }

    public FlinkSourceBuilder projection(int[][] projectedFields) {
        this.projectedFields = projectedFields;
        return this;
    }

    public FlinkSourceBuilder predicate(Predicate predicate) {
        this.predicate = predicate;
        return this;
    }

    public FlinkSourceBuilder partitionPredicate(PartitionPredicate partitionPredicate) {
        this.partitionPredicate = partitionPredicate;
        return this;
    }

    public FlinkSourceBuilder limit(@Nullable Long limit) {
        this.limit = limit;
        return this;
    }

    public FlinkSourceBuilder sourceParallelism(@Nullable Integer parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    public FlinkSourceBuilder watermarkStrategy(
            @Nullable WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
        return this;
    }

    public FlinkSourceBuilder dynamicPartitionFilteringFields(
            List<String> dynamicPartitionFilteringFields) {
        if (dynamicPartitionFilteringFields != null && !dynamicPartitionFilteringFields.isEmpty()) {
            checkState(
                    table instanceof FileStoreTable,
                    "Only Paimon FileStoreTable supports dynamic filtering but get %s.",
                    table.getClass().getName());

            this.dynamicPartitionFilteringInfo =
                    new DynamicPartitionFilteringInfo(
                            ((FileStoreTable) table).schema().logicalPartitionType(),
                            dynamicPartitionFilteringFields);
        }
        return this;
    }

    /**
     * 为建立 ReadBuilder 创建读取器。
     *
     * <p>该方法封装了常见的读取器配置逻辑：
     * <ul>
     *   <li>应用列投影（如果指定了）
     *   <li>应用行级过滤谓词
     *   <li>应用分区级过滤谓词
     *   <li>应用行数限制
     *   <li>删除统计信息（提升读取性能）
     * </ul>
     *
     * @param readType 要读取的列类型（null 表示读取所有列）
     * @return 配置完成的 ReadBuilder
     */
    private ReadBuilder createReadBuilder(@Nullable org.apache.paimon.types.RowType readType) {
        ReadBuilder readBuilder = table.newReadBuilder();
        if (readType != null) {
            readBuilder.withReadType(readType);
        }
        if (predicate != null) {
            readBuilder.withFilter(predicate);
        }
        if (partitionPredicate != null) {
            readBuilder.withPartitionFilter(partitionPredicate);
        }
        if (limit != null) {
            readBuilder.withLimit(limit.intValue());
        }
        return readBuilder.dropStats();
    }

    /**
     * 构建静态文件数据源（批处理模式）。
     *
     * <p>静态文件源会：
     * <ul>
     *   <li>读取表的某个快照的全量数据
     *   <li>支持分片枚举器批处理大小配置
     *   <li>支持分片分配模式选择
     *   <li>支持动态分区过滤
     * </ul>
     *
     * @return 批处理 RowData 数据流
     */
    private DataStream<RowData> buildStaticFileSource() {
        Options options = Options.fromMap(table.options());
        return toDataStream(
                new StaticFileStoreSource(
                        createReadBuilder(projectedRowType()),
                        limit,
                        options.get(FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_BATCH_SIZE),
                        options.get(FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_ASSIGN_MODE),
                        dynamicPartitionFilteringInfo,
                        outerProject(),
                        options.get(CoreOptions.BLOB_AS_DESCRIPTOR)));
    }

    /**
     * 构建持续文件数据源（流处理模式）。
     *
     * <p>持续文件源会：
     * <ul>
     *   <li>持续监听 Paimon 表的变更
     *   <li>读取新增或修改的数据
     *   <li>支持无序读取优化
     * </ul>
     *
     * @return 流处理 RowData 数据流
     */
    private DataStream<RowData> buildContinuousFileSource() {
        return toDataStream(
                new ContinuousFileStoreSource(
                        createReadBuilder(projectedRowType()),
                        table.options(),
                        limit,
                        unordered,
                        outerProject()));
    }

    /**
     * 构建对齐模式的持续数据源。
     *
     * <p>对齐模式支持 Flink Checkpoint 对齐，要求：
     * <ul>
     *   <li>Checkpoint 必须启用
     *   <li>最多一个并发检查点
     *   <li>不支持非对齐检查点
     *   <li>只支持 EXACTLY_ONCE 模式
     * </ul>
     *
     * @return 对齐模式的 RowData 数据流
     */
    private DataStream<RowData> buildAlignedContinuousFileSource() {
        assertStreamingConfigurationForAlignMode(env);
        return toDataStream(
                new AlignedContinuousFileStoreSource(
                        createReadBuilder(projectedRowType()),
                        table.options(),
                        limit,
                        unordered,
                        outerProject()));
    }

    /**
     * 将 Flink Source 转换为 DataStream。
     *
     * <p>该方法处理：
     * <ul>
     *   <li>应用水位线策略（如果指定）
     *   <li>设置操作符 UID（如果配置了）
     *   <li>设置并行度（如果指定）
     * </ul>
     *
     * @param source Flink 源对象
     * @return DataStream<RowData>
     */
    private DataStream<RowData> toDataStream(Source<RowData, ?, ?> source) {
        DataStreamSource<RowData> dataStream =
                env.fromSource(
                        source,
                        watermarkStrategy == null
                                ? WatermarkStrategy.noWatermarks()
                                : watermarkStrategy,
                        sourceName,
                        produceTypeInfo());

        String uidSuffix = table.options().get(SOURCE_OPERATOR_UID_SUFFIX.key());
        if (!StringUtils.isNullOrWhitespaceOnly(uidSuffix)) {
            dataStream =
                    (DataStreamSource<RowData>)
                            dataStream.uid(generateCustomUid(SOURCE_NAME, table.name(), uidSuffix));
        }

        if (parallelism != null) {
            dataStream.setParallelism(parallelism);
        }
        return dataStream;
    }

    /**
     * 获取数据流的类型信息。
     *
     * <p>根据列投影生成对应的 RowType，用于 Flink 类型推断。
     *
     * @return 数据流元素的 TypeInformation
     */
    private TypeInformation<RowData> produceTypeInfo() {
        RowType rowType = toLogicalType(table.rowType());
        LogicalType produceType =
                Optional.ofNullable(projectedFields)
                        .map(Projection::of)
                        .map(p -> p.project(rowType))
                        .orElse(rowType);
        return InternalTypeInfo.of(produceType);
    }

    private @Nullable org.apache.paimon.types.RowType projectedRowType() {
        return Optional.ofNullable(projectedFields)
                .map(Projection::of)
                .map(p -> p.project(table.rowType()))
                .orElse(null);
    }

    private @Nullable NestedProjectedRowData outerProject() {
        return Optional.ofNullable(projectedFields)
                .map(Projection::of)
                .map(p -> p.getOuterProjectRow(table.rowType()))
                .orElse(null);
    }

    @VisibleForTesting
    public boolean isUnordered() {
        return unordered;
    }

    /**
     * 构建最终的数据源并转换为 Row 类型。
     *
     * <p>与 {@link #build()} 类似，但会将 RowData 转换为 Flink 外部 Row 格式，
     * 便于与传统 Flink 生态集成。
     *
     * @return 包含 Flink Row 的数据流
     */
    public DataStream<Row> buildForRow() {
        DataType rowType = fromLogicalToDataType(toLogicalType(table.rowType()));
        DataType[] fieldDataTypes = rowType.getChildren().toArray(new DataType[0]);

        DataFormatConverters.RowConverter converter =
                new DataFormatConverters.RowConverter(fieldDataTypes);
        DataStream<RowData> source = build();
        SingleOutputStreamOperator<Row> result =
                source.map((MapFunction<RowData, Row>) converter::toExternal)
                        .returns(ExternalTypeInfo.of(rowType));
        forwardParallelism(result, source);
        return result;
    }

    /**
     * 构建最终的数据源。
     *
     * <p>该方法会根据配置选择合适的读取策略：
     * <ul>
     *   <li>批处理模式：静态文件源 or 专属分片生成源
     *   <li>流处理模式：对齐模式 or 专属分片生成源 or 持续文件源
     *   <li>会验证必要的配置（如 CONSUMER_EXPIRATION_TIME）
     * </ul>
     *
     * <p>返回的 DataStream 会包含应用了投影和过滤的 RowData。
     *
     * @return 配置完成的 RowData 数据流
     * @throws IllegalArgumentException 如果配置不合法
     */
    public DataStream<RowData> build() {
        if (env == null) {
            throw new IllegalArgumentException("StreamExecutionEnvironment should not be null.");
        }
        if (conf.contains(CoreOptions.CONSUMER_ID)
                && !conf.contains(CoreOptions.CONSUMER_EXPIRATION_TIME)) {
            throw new IllegalArgumentException(
                    "You need to configure 'consumer.expiration-time' (ALTER TABLE) and restart your write job for it"
                            + " to take effect, when you need consumer-id feature. This is to prevent consumers from leaving"
                            + " too many snapshots that could pose a risk to the file system.");
        }

        if (sourceBounded) {
            if (conf.get(FlinkConnectorOptions.SCAN_DEDICATED_SPLIT_GENERATION)) {
                return buildDedicatedSplitGenSource(true);
            }
            return buildStaticFileSource();
        }
        TableScanUtils.streamingReadingValidate(table);

        if (conf.get(FlinkConnectorOptions.SOURCE_CHECKPOINT_ALIGN_ENABLED)) {
            return buildAlignedContinuousFileSource();
        } else if (conf.contains(CoreOptions.CONSUMER_ID)
                && conf.get(CoreOptions.CONSUMER_CONSISTENCY_MODE)
                        == CoreOptions.ConsumerMode.EXACTLY_ONCE) {
            return buildDedicatedSplitGenSource(false);
        } else {
            return buildContinuousFileSource();
        }
    }

    private DataStream<RowData> buildDedicatedSplitGenSource(boolean isBounded) {
        DataStream<RowData> dataStream;
        if (limit != null && !isBounded) {
            throw new IllegalArgumentException(
                    "Cannot limit streaming source, please use batch execution mode.");
        }
        dataStream =
                MonitorSource.buildSource(
                        env,
                        sourceName,
                        produceTypeInfo(),
                        createReadBuilder(projectedRowType()),
                        conf.get(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL).toMillis(),
                        watermarkStrategy == null,
                        conf.get(FlinkConnectorOptions.READ_SHUFFLE_BUCKET_WITH_PARTITION),
                        unordered,
                        outerProject(),
                        isBounded,
                        limit);
        if (parallelism != null) {
            dataStream.getTransformation().setParallelism(parallelism);
        }
        if (watermarkStrategy != null) {
            dataStream = dataStream.assignTimestampsAndWatermarks(watermarkStrategy);
        }
        return dataStream;
    }

    private void assertStreamingConfigurationForAlignMode(StreamExecutionEnvironment env) {
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkArgument(
                checkpointConfig.isCheckpointingEnabled(),
                "The align mode of paimon source is only supported when checkpoint enabled. Please set "
                        + "execution.checkpointing.interval larger than 0");
        checkArgument(
                checkpointConfig.getMaxConcurrentCheckpoints() == 1,
                "The align mode of paimon source supports at most one ongoing checkpoint at the same time. Please set "
                        + "execution.checkpointing.max-concurrent-checkpoints to 1");
        checkArgument(
                checkpointConfig.getCheckpointTimeout()
                        > conf.get(FlinkConnectorOptions.SOURCE_CHECKPOINT_ALIGN_TIMEOUT)
                                .toMillis(),
                "The align mode of paimon source requires that the timeout of checkpoint is greater than the timeout of the source's snapshot alignment. Please increase "
                        + "execution.checkpointing.timeout or decrease "
                        + FlinkConnectorOptions.SOURCE_CHECKPOINT_ALIGN_TIMEOUT.key());
        checkArgument(
                !env.getCheckpointConfig().isUnalignedCheckpointsEnabled(),
                "The align mode of paimon source currently does not support unaligned checkpoints. Please set "
                        + "execution.checkpointing.unaligned.enabled to false.");
        checkArgument(
                env.getCheckpointConfig().getCheckpointingMode() == CheckpointingMode.EXACTLY_ONCE,
                "The align mode of paimon source currently only supports EXACTLY_ONCE checkpoint mode. Please set "
                        + "execution.checkpointing.mode to exactly-once");
    }
}
