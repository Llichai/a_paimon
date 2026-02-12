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
import org.apache.paimon.CoreOptions.OrderType;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.sink.SortCompactSinkBuilder;
import org.apache.paimon.flink.sorter.TableSortInfo;
import org.apache.paimon.flink.sorter.TableSorter;
import org.apache.paimon.flink.source.FlinkSourceBuilder;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 排序压缩操作。
 *
 * <p>用于在压缩 Paimon 表数据时，同时按照指定的列进行排序。这是 CompactAction 的增强版本，
 * 在重新整合数据文件的同时，根据配置的排序策略重新排序数据，以优化查询性能。
 *
 * <p>主要特性：
 * <ul>
 *   <li>在压缩数据的同时进行排序</li>
 *   <li>支持多列排序</li>
 *   <li>支持自定义排序策略（ASC/DESC）</li>
 *   <li>改善查询性能，特别是范围查询</li>
 *   <li>Flink 集群执行操作，支持分布式处理</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 *     // 执行排序压缩，按 user_id 升序、timestamp 降序排列
 *     SortCompactAction action = new SortCompactAction(
 *         "mydb",
 *         "mytable",
 *         catalogConfig,
 *         new HashMap<String, String>() {{
 *             put("sort.strategy", "order_by");
 *             put("sort.columns", "user_id ASC,timestamp DESC");
 *         }}
 *     )
 *     .withOrder("user_id ASC,timestamp DESC")
 *     .withStrategy("order_by");
 *
 *     action.run();
 * }</pre>
 *
 * <p>参数说明：
 * <ul>
 *   <li>database: 目标数据库名称</li>
 *   <li>tableName: 目标表名称</li>
 *   <li>sortStrategy: 排序策略（如 "order_by" 等）</li>
 *   <li>orderColumns: 排序列列表，格式为 "col1 ASC,col2 DESC"</li>
 * </ul>
 *
 * <p>排序压缩过程：
 * <ul>
 *   <li>读取表的所有数据文件</li>
 *   <li>按照指定的排序列进行全局排序</li>
 *   <li>生成排序后的新数据文件</li>
 *   <li>按照表的分桶策略重新分桶</li>
 *   <li>提交新快照，原数据自动清理</li>
 * </ul>
 *
 * <p>性能提升场景：
 * <ul>
 *   <li>时间序列数据按时间戳排序，提升时间范围查询性能</li>
 *   <li>按用户 ID 排序，提升用户维度的点查询性能</li>
 *   <li>按多个列排序，支持复杂查询场景优化</li>
 *   <li>频繁的范围扫描和分析查询</li>
 * </ul>
 *
 * <p>注意事项：
 * <ul>
 *   <li>排序列应该选择高频查询的条件列</li>
 *   <li>排序结果的行成本较高，应按需使用</li>
 *   <li>大表的排序压缩需要大量的 Flink 集群资源</li>
 *   <li>排序过程中表处于不可写状态</li>
 *   <li>排序完成后性能改善的程度取决于数据分布和查询模式</li>
 * </ul>
 *
 * <p>与常规 CompactAction 的区别：
 * <ul>
 *   <li>排序压缩会修改数据顺序，可能增加写入成本</li>
 *   <li>适合读多写少的场景</li>
 *   <li>查询性能通常优于普通压缩</li>
 * </ul>
 *
 * @see CompactAction
 * @see TableSorter
 * @since 0.1
 */
public class SortCompactAction extends CompactAction {

    private static final Logger LOG = LoggerFactory.getLogger(SortCompactAction.class);

    private String sortStrategy;
    private List<String> orderColumns;

    public SortCompactAction(
            String database,
            String tableName,
            Map<String, String> catalogConfig,
            Map<String, String> tableConf) {
        super(database, tableName, catalogConfig, tableConf);
        table = table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));
    }

    @Override
    public void run() throws Exception {
        build();
        execute("Sort Compact Job");
    }

    @Override
    public void build() throws Exception {
        // only support batch sort yet
        if (env.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                != RuntimeExecutionMode.BATCH) {
            LOG.warn(
                    "Sort Compact only support batch mode yet. Please add -Dexecution.runtime-mode=BATCH. The action this time will shift to batch mode forcely.");
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        }
        FileStoreTable fileStoreTable = (FileStoreTable) table;

        if (fileStoreTable.coreOptions().dataEvolutionEnabled()) {
            throw new UnsupportedOperationException("Data Evolution table cannot be sorted!");
        }

        if (fileStoreTable.bucketMode() != BucketMode.BUCKET_UNAWARE
                && fileStoreTable.bucketMode() != BucketMode.HASH_DYNAMIC) {
            throw new IllegalArgumentException("Sort Compact only supports bucket=-1 yet.");
        }
        Map<String, String> tableConfig = fileStoreTable.options();
        FlinkSourceBuilder sourceBuilder =
                new FlinkSourceBuilder(fileStoreTable)
                        .sourceName(
                                ObjectIdentifier.of(
                                                catalogName,
                                                identifier.getDatabaseName(),
                                                identifier.getObjectName())
                                        .asSummaryString());

        sourceBuilder.partitionPredicate(getPartitionPredicate());

        String scanParallelism = tableConfig.get(FlinkConnectorOptions.SCAN_PARALLELISM.key());
        if (scanParallelism != null) {
            sourceBuilder.sourceParallelism(Integer.parseInt(scanParallelism));
        }

        DataStream<RowData> source = sourceBuilder.env(env).sourceBounded(true).build();
        int localSampleMagnification =
                ((FileStoreTable) table).coreOptions().getLocalSampleMagnification();
        if (localSampleMagnification < 20) {
            throw new IllegalArgumentException(
                    String.format(
                            "the config '%s=%d' should not be set too small,greater than or equal to 20 is needed.",
                            CoreOptions.SORT_COMPACTION_SAMPLE_MAGNIFICATION.key(),
                            localSampleMagnification));
        }
        String sinkParallelismValue =
                table.options().get(FlinkConnectorOptions.SINK_PARALLELISM.key());
        final int sinkParallelism =
                sinkParallelismValue == null
                        ? source.getParallelism()
                        : Integer.parseInt(sinkParallelismValue);
        TableSortInfo sortInfo =
                new TableSortInfo.Builder()
                        .setSortColumns(orderColumns)
                        .setSortStrategy(OrderType.of(sortStrategy))
                        .setSinkParallelism(sinkParallelism)
                        .setLocalSampleSize(sinkParallelism * localSampleMagnification)
                        .setGlobalSampleSize(sinkParallelism * 1000)
                        .setRangeNumber(sinkParallelism * 10)
                        .build();

        TableSorter sorter = TableSorter.getSorter(env, source, fileStoreTable, sortInfo);

        new SortCompactSinkBuilder(fileStoreTable)
                .forCompact(true)
                .forRowData(sorter.sort())
                .overwrite()
                .build();
    }

    public SortCompactAction withOrderStrategy(String sortStrategy) {
        this.sortStrategy = sortStrategy;
        return this;
    }

    public SortCompactAction withOrderColumns(String... orderColumns) {
        return withOrderColumns(Arrays.asList(orderColumns));
    }

    public SortCompactAction withOrderColumns(List<String> orderColumns) {
        this.orderColumns = orderColumns.stream().map(String::trim).collect(Collectors.toList());
        return this;
    }
}
