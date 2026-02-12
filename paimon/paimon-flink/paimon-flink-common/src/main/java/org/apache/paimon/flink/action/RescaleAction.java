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
import org.apache.paimon.Snapshot;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.flink.source.FlinkSourceBuilder;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * 重新扩展（重新分桶）操作。
 *
 * <p>用于调整 Paimon 表的分桶数量，从而改变数据在桶中的分布。当表的写入并行度、数据分布不均匀，
 * 或需要调整查询性能时，可以使用本操作重新分配数据的桶结构。
 *
 * <p>主要特性：
 * <ul>
 *   <li>支持修改指定分区的分桶数量</li>
 *   <li>支持指定扫描和写入的并行度</li>
 *   <li>通过重新分配实现数据重新分布</li>
 *   <li>支持特定分区的增量重扩展</li>
 *   <li>Flink 集群执行操作，支持分布式处理</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 *     // 重新扩展指定分区，修改分桶数为 16
 *     RescaleAction action = new RescaleAction(
 *         "mydb",
 *         "mytable",
 *         catalogConfig
 *     )
 *     .withBucketNum(16)                      // 设置新的分桶数
 *     .withPartition("year", "2024")          // 指定分区
 *     .withPartition("month", "01")
 *     .withScanParallelism(8)                 // 扫描并行度
 *     .withSinkParallelism(8);                // 写入并行度
 *
 *     action.run();
 * }</pre>
 *
 * <p>参数说明：
 * <ul>
 *   <li>databaseName: 目标数据库名称</li>
 *   <li>tableName: 目标表名称</li>
 *   <li>bucketNum: 新的分桶数量</li>
 *   <li>partition: 分区值映射，指定要重扩展的分区</li>
 *   <li>scanParallelism: 扫描并行度（可选）</li>
 *   <li>sinkParallelism: 写入并行度（可选）</li>
 * </ul>
 *
 * <p>重扩展过程：
 * <ul>
 *   <li>扫描指定分区的现有数据文件</li>
 *   <li>根据新分桶数重新计算数据的桶位置</li>
 *   <li>并行读取原数据文件</li>
 *   <li>根据新分桶规则写入新的数据文件</li>
 *   <li>提交新快照，原数据自动清理</li>
 * </ul>
 *
 * <p>典型应用场景：
 * <ul>
 *   <li>业务增长导致写入并行度需要调整</li>
 *   <li>数据分布不均匀，某些桶特别大</li>
 *   <li>查询性能下降，需要优化数据分布</li>
 *   <li>从低并行度逐步扩展到高并行度</li>
 * </ul>
 *
 * <p>注意事项：
 * <ul>
 *   <li>重扩展仅适用于固定桶模式的表（BucketMode.FIXED）</li>
 *   <li>执行过程会产生临时数据文件，需要足够的存储空间</li>
 *   <li>大表的重扩展可能需要较长时间和较高的 Flink 集群资源</li>
 *   <li>重扩展期间表可以继续读取，但不能进行写入操作</li>
 *   <li>如果中止操作，应清理临时文件</li>
 * </ul>
 *
 * @see TableActionBase
 * @since 0.1
 */
public class RescaleAction extends TableActionBase {

    private @Nullable Integer bucketNum;
    private Map<String, String> partition = new HashMap<>();
    private @Nullable Integer scanParallelism;
    private @Nullable Integer sinkParallelism;

    public RescaleAction(String databaseName, String tableName, Map<String, String> catalogConfig) {
        super(databaseName, tableName, catalogConfig);
    }

    public RescaleAction withBucketNum(int bucketNum) {
        this.bucketNum = bucketNum;
        return this;
    }

    public RescaleAction withPartition(Map<String, String> partition) {
        this.partition = partition;
        return this;
    }

    public RescaleAction withScanParallelism(int scanParallelism) {
        this.scanParallelism = scanParallelism;
        return this;
    }

    public RescaleAction withSinkParallelism(int sinkParallelism) {
        this.sinkParallelism = sinkParallelism;
        return this;
    }

    @Override
    public void build() throws Exception {
        Configuration flinkConf = new Configuration();
        flinkConf.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        env.configure(flinkConf);

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        Optional<Snapshot> optionalSnapshot = fileStoreTable.latestSnapshot();
        if (!optionalSnapshot.isPresent()) {
            throw new IllegalArgumentException(
                    "Table " + table.fullName() + " has no snapshot. No need to rescale.");
        }
        Snapshot snapshot = optionalSnapshot.get();

        // If someone commits while the rescale job is running, this commit will be lost.
        // So we use strict mode to make sure nothing is lost.
        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(
                CoreOptions.COMMIT_STRICT_MODE_LAST_SAFE_SNAPSHOT.key(),
                String.valueOf(snapshot.id()));
        fileStoreTable = fileStoreTable.copy(dynamicOptions);

        PartitionPredicate partitionPredicate =
                PartitionPredicate.fromMap(
                        fileStoreTable.schema().logicalPartitionType(),
                        partition,
                        fileStoreTable.coreOptions().partitionDefaultName());

        DataStream<RowData> source =
                new FlinkSourceBuilder(fileStoreTable)
                        .env(env)
                        .sourceBounded(true)
                        .sourceParallelism(
                                scanParallelism == null
                                        ? currentBucketNum(snapshot)
                                        : scanParallelism)
                        .partitionPredicate(partitionPredicate)
                        .build();

        Map<String, String> bucketOptions = new HashMap<>(fileStoreTable.options());
        if (bucketNum == null) {
            Preconditions.checkArgument(
                    fileStoreTable.coreOptions().bucket() != BucketMode.POSTPONE_BUCKET,
                    "When rescaling postpone bucket tables, you must provide the resulting bucket number.");
        } else {
            bucketOptions.put(CoreOptions.BUCKET.key(), String.valueOf(bucketNum));
        }
        FileStoreTable rescaledTable =
                fileStoreTable.copy(fileStoreTable.schema().copy(bucketOptions));
        new FlinkSinkBuilder(rescaledTable)
                .overwrite(partition)
                .parallelism(sinkParallelism == null ? bucketNum : sinkParallelism)
                .forRowData(source)
                .build();
    }

    @Override
    public void run() throws Exception {
        build();
        env.execute("Rescale : " + table.fullName());
    }

    private int currentBucketNum(Snapshot snapshot) {
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        Iterator<ManifestEntry> it =
                fileStoreTable
                        .newSnapshotReader()
                        .withSnapshot(snapshot)
                        .withPartitionFilter(partition)
                        .onlyReadRealBuckets()
                        .readFileIterator();
        Preconditions.checkArgument(
                it.hasNext(),
                "The specified partition does not have any data files. No need to rescale.");
        return it.next().totalBuckets();
    }
}
