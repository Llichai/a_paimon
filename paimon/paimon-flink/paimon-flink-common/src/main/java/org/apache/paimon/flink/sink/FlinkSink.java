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

package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions.TagCreationMode;
import org.apache.paimon.flink.compact.changelog.ChangelogCompactCoordinateOperator;
import org.apache.paimon.flink.compact.changelog.ChangelogCompactSortOperator;
import org.apache.paimon.flink.compact.changelog.ChangelogCompactWorkerOperator;
import org.apache.paimon.flink.compact.changelog.ChangelogTaskTypeInfo;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.EitherTypeInfo;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import static org.apache.paimon.CoreOptions.createCommitUser;
import static org.apache.paimon.flink.FlinkConnectorOptions.END_INPUT_WATERMARK;
import static org.apache.paimon.flink.FlinkConnectorOptions.PRECOMMIT_COMPACT;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_AUTO_TAG_FOR_SAVEPOINT;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_COMMITTER_CPU;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_COMMITTER_MEMORY;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_COMMITTER_OPERATOR_CHAINING;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_MANAGED_WRITER_BUFFER_MEMORY;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_OPERATOR_UID_SUFFIX;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_USE_MANAGED_MEMORY;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_WRITER_CPU;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_WRITER_MEMORY;
import static org.apache.paimon.flink.FlinkConnectorOptions.generateCustomUid;
import static org.apache.paimon.flink.utils.ManagedMemoryUtils.declareManagedMemory;
import static org.apache.paimon.flink.utils.ParallelismUtils.forwardParallelism;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Paimon Flink 汇聚抽象基类 - 提供数据写入 Paimon 表的核心能力。
 *
 * <p>FlinkSink 将 Flink DataStream 中的数据写入到 Paimon 表，支持：
 * <ul>
 *   <li>批处理和流处理模式
 *   <li>主键表和追加表
 *   <li>列式存储和行式存储
 *   <li>多种索引格式（Parquet、ORC、Avro）
 *   <li>精确一次语义（EXACTLY_ONCE）
 *   <li>事务提交
 * </ul>
 *
 * <h3>工作流程</h3>
 * <p>写入过程分为两个阶段：
 * <pre>
 * 1. 写入阶段（doWrite）
 *    ├─ 数据通过 WriterOperator 写入本地 WriteBuffer
 *    ├─ 定期 flush WriteBuffer，生成 DataFile
 *    ├─ 将 DataFile 元数据封装为 Committable
 *    └─ 发送 Committable 到 Committer 算子
 *
 * 2. 提交阶段（doCommit）
 *    ├─ GlobalCommitter 接收所有并行 Writer 的 Committable
 *    ├─ 按事务顺序合并 Committable
 *    ├─ 创建新的 Snapshot（原子操作）
 *    └─ 完成提交（Checkpoint 时）
 * </pre>
 *
 * <h3>核心特性</h3>
 * <ul>
 *   <li><b>精确一次语义</b>：通过 Flink Checkpoint 和 Snapshot 原子性保证
 *   <li><b>写入优化</b>：支持 pre-commit compaction，在提交前合并小文件
 *   <li><b>资源隔离</b>：支持为 Writer 和 Committer 分配独立的 CPU 和内存
 *   <li><b>标签自动创建</b>：支持在 Savepoint 时自动创建标签
 *   <li><b>Write-only 模式</b>：支持只写不读的优化模式
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 * env.enableCheckpointing(60000);  // 每60秒checkpoint
 * env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
 *
 * // 创建源数据
 * DataStream<RowData> source = ...;
 *
 * // 获取目标 Paimon 表
 * FileStoreTable table = catalog.getTable(new Identifier("db", "sink_table"));
 *
 * // 创建汇聚并写入
 * RowDataSink sink = new RowDataSink(table);
 * sink.sinkFrom(source);
 *
 * env.execute("Streaming ETL");
 * }</pre>
 *
 * <h3>与 FlinkSourceBuilder 的关系</h3>
 * <p>FlinkSourceBuilder 和 FlinkSink 配套使用，形成 ETL 管道：
 * <pre>{@code
 * // 源表
 * DataStream<RowData> source = new FlinkSourceBuilder(sourceTable)
 *     .env(env)
 *     .sourceBounded(false)
 *     .build();
 *
 * // 处理逻辑
 * DataStream<RowData> processed = source.map(row -> {...});
 *
 * // 写入目标表
 * FlinkSink sink = new RowDataSink(sinkTable);
 * sink.sinkFrom(processed);
 * }</pre>
 *
 * <h3>配置选项</h3>
 * 通过表选项配置汇聚行为：
 * <ul>
 *   <li>sink.parallelism: 写入并行度
 *   <li>sink.committer-memory: Committer 内存大小
 *   <li>sink.committer-cpu: Committer CPU 核心数
 *   <li>sink.writer-memory: Writer 内存大小
 *   <li>sink.writer-cpu: Writer CPU 核心数
 *   <li>sink.auto-tag-for-savepoint: Savepoint 时自动创建标签
 *   <li>write-only: 只写模式（不生成 Snapshot）
 * </ul>
 *
 * <h3>实现类</h3>
 * <ul>
 *   <li>{@link org.apache.paimon.flink.sink.RowDataSink}: 通用的 RowData 汇聚
 *   <li>其他特定格式的汇聚实现（如 CDC、Iceberg 等）
 * </ul>
 *
 * @param <T> 输入数据流中的元素类型
 */
public abstract class FlinkSink<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Writer 算子的默认名称 */
    private static final String WRITER_NAME = "Writer";

    /** Write-only 模式 Writer 算子的名称 */
    private static final String WRITER_WRITE_ONLY_NAME = "Writer(write-only)";

    /** 全局提交器算子的名称 */
    private static final String GLOBAL_COMMITTER_NAME = "Global Committer";

    /** 目标 Paimon 表 */
    protected final FileStoreTable table;

    /** 是否忽略之前的文件（新表 vs 已有数据的表） */
    private final boolean ignorePreviousFiles;

    /**
     * 创建 FlinkSink 实例。
     *
     * @param table 目标 Paimon 表
     * @param ignorePreviousFiles 是否忽略之前已存在的文件（true 适用于新表，false 用于现有表）
     */
    public FlinkSink(FileStoreTable table, boolean ignorePreviousFiles) {
        this.table = table;
        this.ignorePreviousFiles = ignorePreviousFiles;
    }

    /**
     * 从数据流写入到 Paimon 表（自动生成 commitUser）。
     *
     * <p>该方法是 {@link #sinkFrom(DataStream, String)} 的简化版本，
     * 自动为新作业生成 commitUser。Checkpoint 恢复时会使用保存的 commitUser。
     *
     * <p>内部会调用两个阶段：
     * <ul>
     *   <li>doWrite：数据写入 WriteBuffer 并生成 Committable
     *   <li>doCommit：Committable 合并并提交（创建新 Snapshot）
     * </ul>
     *
     * @param input 输入数据流
     * @return DataStreamSink
     */
    public DataStreamSink<?> sinkFrom(DataStream<T> input) {
        // This commitUser is valid only for new jobs.
        // After the job starts, this commitUser will be recorded into the states of write and
        // commit operators.
        // When the job restarts, commitUser will be recovered from states and this value is
        // ignored.
        return sinkFrom(input, createCommitUser(table.coreOptions().toConfiguration()));
    }

    /**
     * 从数据流写入到 Paimon 表（指定 commitUser）。
     *
     * <p>commitUser 用于标识提交者身份，在多个写入作业同时写入时用于区分。
     *
     * @param input 输入数据流
     * @param initialCommitUser 初始 commitUser（仅对新作业有效）
     * @return DataStreamSink
     */
    public DataStreamSink<?> sinkFrom(DataStream<T> input, String initialCommitUser) {
        // do the actually writing action, no snapshot generated in this stage
        DataStream<Committable> written = doWrite(input, initialCommitUser, null);
        // commit the committable to generate a new snapshot
        return doCommit(written, initialCommitUser);
    }

    /**
     * 检查数据流是否包含 SinkMaterializer 算子。
     *
     * <p>SinkMaterializer 通常由 SQL Table API 自动添加。
     * 需要此信息来判断是否应启用特定的优化。
     *
     * @param input 输入数据流
     * @return 是否存在 SinkMaterializer
     */
    private boolean hasSinkMaterializer(DataStream<T> input) {
        // traverse the transformation graph with breadth first search
        Set<Integer> visited = new HashSet<>();
        Queue<Transformation<?>> queue = new LinkedList<>();
        queue.add(input.getTransformation());
        visited.add(input.getTransformation().getId());
        while (!queue.isEmpty()) {
            Transformation<?> transformation = queue.poll();
            if (transformation.getName().startsWith("SinkMaterializer")) {
                return true;
            }
            for (Transformation<?> prev : transformation.getInputs()) {
                if (!visited.contains(prev.getId())) {
                    queue.add(prev);
                    visited.add(prev.getId());
                }
            }
        }
        return false;
    }

    /**
     * 执行写入操作（第一阶段）。
     *
     * <p>该方法：
     * <ul>
     *   <li>创建 WriterOperator，将数据写入 WriteBuffer
     *   <li>周期性 flush WriteBuffer，生成 DataFile
     *   <li>如果启用 pre-commit compaction，则在提交前合并小文件
     *   <li>返回 Committable 供后续提交
     * </ul>
     *
     * <p>资源配置：
     * <ul>
     *   <li>并行度：如果 parallelism 为 null，则继承输入流的并行度
     *   <li>托管内存：根据 SINK_USE_MANAGED_MEMORY 配置
     *   <li>CPU 和内存限制：通过 SINK_WRITER_CPU 和 SINK_WRITER_MEMORY 配置
     * </ul>
     *
     * @param input 输入数据流
     * @param commitUser commit 用户标识
     * @param parallelism 写入并行度（null 表示继承输入流）
     * @return Committable 数据流
     */
    public DataStream<Committable> doWrite(
            DataStream<T> input, String commitUser, @Nullable Integer parallelism) {
        StreamExecutionEnvironment env = input.getExecutionEnvironment();
        boolean isStreaming = isStreaming(input);

        boolean writeOnly = table.coreOptions().writeOnly();
        SingleOutputStreamOperator<Committable> written =
                input.transform(
                        (writeOnly ? WRITER_WRITE_ONLY_NAME : WRITER_NAME) + " : " + table.name(),
                        new CommittableTypeInfo(),
                        createWriteOperatorFactory(
                                StoreSinkWrite.createWriteProvider(
                                        table,
                                        env.getCheckpointConfig(),
                                        isStreaming,
                                        ignorePreviousFiles,
                                        hasSinkMaterializer(input)),
                                commitUser));
        if (parallelism == null) {
            forwardParallelism(written, input);
        } else {
            written.setParallelism(parallelism);
        }

        Options options = Options.fromMap(table.options());

        String uidSuffix = options.get(SINK_OPERATOR_UID_SUFFIX);
        if (options.get(SINK_OPERATOR_UID_SUFFIX) != null) {
            written = written.uid(generateCustomUid(WRITER_NAME, table.name(), uidSuffix));
        }

        if (options.get(SINK_USE_MANAGED_MEMORY)) {
            declareManagedMemory(written, options.get(SINK_MANAGED_WRITER_BUFFER_MEMORY));
        }

        configureSlotSharingGroup(
                written, options.get(SINK_WRITER_CPU), options.get(SINK_WRITER_MEMORY));

        if (!table.primaryKeys().isEmpty() && options.get(PRECOMMIT_COMPACT)) {
            SingleOutputStreamOperator<Committable> beforeSort =
                    written.transform(
                                    "Changelog Compact Coordinator",
                                    new EitherTypeInfo<>(
                                            new CommittableTypeInfo(), new ChangelogTaskTypeInfo()),
                                    new ChangelogCompactCoordinateOperator(table.coreOptions()))
                            .forceNonParallel()
                            .transform(
                                    "Changelog Compact Worker",
                                    new CommittableTypeInfo(),
                                    new ChangelogCompactWorkerOperator(table));
            forwardParallelism(beforeSort, written);

            written =
                    beforeSort
                            .transform(
                                    "Changelog Sort by Creation Time",
                                    new CommittableTypeInfo(),
                                    new ChangelogCompactSortOperator())
                            .forceNonParallel();
        }

        return written;
    }

    /**
     * 执行提交操作（第二阶段）。
     *
     * <p>该方法：
     * <ul>
     *   <li>创建 GlobalCommitter，接收并合并所有 Writer 的 Committable
     *   <li>按事务顺序提交 Committable
     *   <li>原子创建新 Snapshot（所有数据文件都可见后）
     *   <li>完成后更新所有 Reader 和 CDC 消费者的状态
     * </ul>
     *
     * @param written Committable 数据流
     * @param commitUser commit 用户标识
     * @return 汇聚结果
     */
    public DataStreamSink<?> doCommit(DataStream<Committable> written, String commitUser) {
        StreamExecutionEnvironment env = written.getExecutionEnvironment();
        ReadableConfig conf = env.getConfiguration();
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        boolean streamingCheckpointEnabled =
                isStreaming(written) && checkpointConfig.isCheckpointingEnabled();
        if (streamingCheckpointEnabled) {
            assertStreamingConfiguration(env);
        }

        Options options = Options.fromMap(table.options());
        OneInputStreamOperatorFactory<Committable, Committable> committerOperator =
                createCommitterOperatorFactory(
                        streamingCheckpointEnabled, commitUser, options.get(END_INPUT_WATERMARK));

        if (options.get(SINK_AUTO_TAG_FOR_SAVEPOINT)) {
            committerOperator =
                    new AutoTagForSavepointCommitterOperatorFactory<>(
                            (CommitterOperatorFactory<Committable, ManifestCommittable>)
                                    committerOperator,
                            table::snapshotManager,
                            table::tagManager,
                            () -> table.store().newTagDeletion(),
                            () -> table.store().createTagCallbacks(table),
                            table.coreOptions().tagDefaultTimeRetained());
        }
        if (conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.BATCH
                && table.coreOptions().tagCreationMode() == TagCreationMode.BATCH) {
            committerOperator =
                    new BatchWriteGeneratorTagOperatorFactory<>(
                            (CommitterOperatorFactory<Committable, ManifestCommittable>)
                                    committerOperator,
                            table);
        }
        SingleOutputStreamOperator<?> committed =
                written.transform(
                                GLOBAL_COMMITTER_NAME + " : " + table.name(),
                                new CommittableTypeInfo(),
                                committerOperator)
                        .setParallelism(1)
                        .setMaxParallelism(1);
        if (options.get(SINK_OPERATOR_UID_SUFFIX) != null) {
            committed =
                    committed.uid(
                            generateCustomUid(
                                    GLOBAL_COMMITTER_NAME,
                                    table.name(),
                                    options.get(SINK_OPERATOR_UID_SUFFIX)));
        }
        if (!options.get(SINK_COMMITTER_OPERATOR_CHAINING)) {
            committed = committed.startNewChain();
        }
        configureSlotSharingGroup(
                committed, options.get(SINK_COMMITTER_CPU), options.get(SINK_COMMITTER_MEMORY));
        return committed.sinkTo(new DiscardingSink<>()).name("end").setParallelism(1);
    }

    public static void configureSlotSharingGroup(
            SingleOutputStreamOperator<?> operator,
            double cpuCores,
            @Nullable MemorySize heapMemory) {
        if (heapMemory == null) {
            return;
        }

        SlotSharingGroup slotSharingGroup =
                SlotSharingGroup.newBuilder(operator.getName())
                        .setCpuCores(cpuCores)
                        .setTaskHeapMemory(
                                new org.apache.flink.configuration.MemorySize(
                                        heapMemory.getBytes()))
                        .build();
        operator.slotSharingGroup(slotSharingGroup);
    }

    public static void assertStreamingConfiguration(StreamExecutionEnvironment env) {
        checkArgument(
                !env.getCheckpointConfig().isUnalignedCheckpointsEnabled(),
                "Paimon sink currently does not support unaligned checkpoints. Please set "
                        + "execution.checkpointing.unaligned.enabled to false.");
        checkArgument(
                env.getCheckpointConfig().getCheckpointingMode() == CheckpointingMode.EXACTLY_ONCE,
                "Paimon sink currently only supports EXACTLY_ONCE checkpoint mode. Please set "
                        + "execution.checkpointing.mode to exactly-once");
    }

    public static void assertBatchAdaptiveParallelism(
            StreamExecutionEnvironment env, int sinkParallelism) {
        String msg =
                "Paimon Sink does not support Flink's Adaptive Parallelism mode. "
                        + "Please manually turn it off or set Paimon `sink.parallelism` manually.";
        assertBatchAdaptiveParallelism(env, sinkParallelism, msg);
    }

    public static void assertBatchAdaptiveParallelism(
            StreamExecutionEnvironment env, int sinkParallelism, String exceptionMsg) {
        try {
            checkArgument(
                    sinkParallelism != -1 || !AdaptiveParallelism.isEnabled(env), exceptionMsg);
        } catch (NoClassDefFoundError ignored) {
            // before 1.17, there is no adaptive parallelism
        }
    }

    protected CommitterOperatorFactory<Committable, ManifestCommittable>
            createCommitterOperatorFactory(
                    boolean streamingCheckpointEnabled,
                    String commitUser,
                    @Nullable Long endInputWatermark) {
        return new CommitterOperatorFactory<>(
                streamingCheckpointEnabled,
                true,
                commitUser,
                createCommitterFactory(),
                createCommittableStateManager(),
                endInputWatermark);
    }

    protected abstract OneInputStreamOperatorFactory<T, Committable> createWriteOperatorFactory(
            StoreSinkWrite.Provider writeProvider, String commitUser);

    protected abstract Committer.Factory<Committable, ManifestCommittable> createCommitterFactory();

    protected abstract CommittableStateManager<ManifestCommittable> createCommittableStateManager();

    public static boolean isStreaming(DataStream<?> input) {
        return isStreaming(input.getExecutionEnvironment());
    }

    public static boolean isStreaming(StreamExecutionEnvironment env) {
        return env.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                == RuntimeExecutionMode.STREAMING;
    }
}
