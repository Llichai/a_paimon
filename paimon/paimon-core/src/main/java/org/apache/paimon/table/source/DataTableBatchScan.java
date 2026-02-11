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
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.SortValue;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.apache.paimon.table.source.snapshot.StartingScanner.ScannedResult;
import org.apache.paimon.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.apache.paimon.table.source.PushDownUtils.minmaxAvailable;

/**
 * 批量扫描实现，用于一次性读取完整的快照数据。
 *
 * <p>DataTableBatchScan 是 {@link TableScan} 的批量扫描实现，适用于批处理场景。
 * 与流式扫描不同，批量扫描只执行一次，读取指定快照的完整数据。
 *
 * <h3>批量扫描 vs 流式扫描</h3>
 * <ul>
 *   <li><b>DataTableBatchScan</b>: 批量扫描
 *       <ul>
 *         <li>只执行一次扫描</li>
 *         <li>读取单个快照的完整数据</li>
 *         <li>适用于批处理、离线分析</li>
 *       </ul>
 *   </li>
 *   <li><b>DataTableStreamScan</b>: 流式扫描
 *       <ul>
 *         <li>持续跟踪新快照</li>
 *         <li>每次返回新产生的数据</li>
 *         <li>适用于实时处理、CDC</li>
 *       </ul>
 *   </li>
 * </ul>
 *
 * <h3>主要功能</h3>
 * <ul>
 *   <li><b>快照选择</b>: 支持多种快照选择策略（最新、指定时间、指定版本等）</li>
 *   <li><b>Limit 优化</b>: 支持 Limit 下推，减少读取的数据量</li>
 *   <li><b>TopN 优化</b>: 支持 TopN 查询优化（排序 + Limit）</li>
 *   <li><b>跳过 Level 0</b>: 可选跳过 Level 0 文件，减少读取量（主键表）</li>
 *   <li><b>延迟分桶表优化</b>: 只读取实际存在的桶</li>
 * </ul>
 *
 * <h3>起始扫描器（StartingScanner）</h3>
 * <p>批量扫描使用起始扫描器确定要读取的快照：
 * <ul>
 *   <li><b>FullStartingScanner</b>: 读取最新快照（默认）</li>
 *   <li><b>StaticFromSnapshotStartingScanner</b>: 读取指定快照</li>
 *   <li><b>StaticFromTimestampStartingScanner</b>: 读取指定时间的快照</li>
 *   <li><b>IncrementalDeltaStartingScanner</b>: 读取两个快照之间的变化</li>
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 批量扫描（读取最新快照）
 * DataTableBatchScan scan = new DataTableBatchScan(...);
 * Plan plan = scan.plan();
 *
 * // 批量扫描（读取指定快照）
 * scan.withStartingScanner(
 *     new StaticFromSnapshotStartingScanner(10)
 * );
 * Plan plan = scan.plan();
 *
 * // 批量扫描（增量读取）
 * scan.withStartingScanner(
 *     new IncrementalDeltaStartingScanner(5, 10)
 * );
 * Plan plan = scan.plan();
 * }</pre>
 *
 * <h3>优化选项</h3>
 * <ul>
 *   <li><b>batch-scan-skip-level0</b>: 跳过 Level 0 文件（主键表，减少读取量）</li>
 *   <li><b>scan.push-down.limit</b>: Limit 下推（提前终止扫描）</li>
 *   <li><b>scan.mode</b>: 扫描模式（ALL / DELTA / CHANGELOG）</li>
 * </ul>
 *
 * @see DataTableScan 数据表扫描接口
 * @see AbstractDataTableScan 抽象基类
 * @see DataTableStreamScan 流式扫描实现
 * @see StartingScanner 起始扫描器
 */
public class DataTableBatchScan extends AbstractDataTableScan {

    private static final Logger LOG = LoggerFactory.getLogger(DataTableBatchScan.class);

    private StartingScanner startingScanner;
    private boolean hasNext;

    private Integer pushDownLimit;
    private TopN topN;

    private final SchemaManager schemaManager;

    public DataTableBatchScan(
            TableSchema schema,
            SchemaManager schemaManager,
            CoreOptions options,
            SnapshotReader snapshotReader,
            TableQueryAuth queryAuth) {
        super(schema, options, snapshotReader, queryAuth);

        this.hasNext = true;
        this.schemaManager = schemaManager;
        if (!schema.primaryKeys().isEmpty() && options.batchScanSkipLevel0()) {
            if (options.toConfiguration()
                    .get(CoreOptions.BATCH_SCAN_MODE)
                    .equals(CoreOptions.BatchScanMode.NONE)) {
                snapshotReader.withLevelFilter(level -> level > 0).enableValueFilter();
            }
        }
        if (options.bucket() == BucketMode.POSTPONE_BUCKET) {
            snapshotReader.onlyReadRealBuckets();
        }
    }

    @Override
    public InnerTableScan withFilter(Predicate predicate) {
        super.withFilter(predicate);
        return this;
    }

    @Override
    public InnerTableScan withLimit(int limit) {
        this.pushDownLimit = limit;
        snapshotReader.withLimit(limit);
        return this;
    }

    @Override
    public InnerTableScan withTopN(TopN topN) {
        this.topN = topN;
        return this;
    }

    @Override
    protected TableScan.Plan planWithoutAuth() {
        if (startingScanner == null) {
            startingScanner = createStartingScanner(false);
        }

        if (hasNext) {
            hasNext = false;
            Optional<StartingScanner.Result> pushed = applyPushDownLimit();
            if (pushed.isPresent()) {
                return DataFilePlan.fromResult(pushed.get());
            }
            pushed = applyPushDownTopN();
            if (pushed.isPresent()) {
                return DataFilePlan.fromResult(pushed.get());
            }
            return DataFilePlan.fromResult(startingScanner.scan(snapshotReader));
        } else {
            throw new EndOfScanException();
        }
    }

    @Override
    public List<PartitionEntry> listPartitionEntries() {
        if (startingScanner == null) {
            startingScanner = createStartingScanner(false);
        }
        return startingScanner.scanPartitions(snapshotReader);
    }

    private Optional<StartingScanner.Result> applyPushDownLimit() {
        if (pushDownLimit == null) {
            return Optional.empty();
        }

        StartingScanner.Result result = startingScanner.scan(snapshotReader);
        if (!(result instanceof ScannedResult)) {
            return Optional.of(result);
        }

        long scannedRowCount = 0;
        SnapshotReader.Plan plan = ((ScannedResult) result).plan();
        List<DataSplit> splits = plan.dataSplits();
        LOG.info("Applying limit pushdown. Original splits count: {}", splits.size());
        if (splits.isEmpty()) {
            return Optional.of(result);
        }

        List<Split> limitedSplits = new ArrayList<>();
        for (DataSplit dataSplit : splits) {
            OptionalLong mergedRowCount = dataSplit.mergedRowCount();
            if (mergedRowCount.isPresent()) {
                limitedSplits.add(dataSplit);
                scannedRowCount += mergedRowCount.getAsLong();
                if (scannedRowCount >= pushDownLimit) {
                    SnapshotReader.Plan newPlan =
                            new PlanImpl(plan.watermark(), plan.snapshotId(), limitedSplits);
                    LOG.info(
                            "Limit pushdown applied successfully. Original splits: {}, Limited splits: {}, Pushdown limit: {}",
                            splits.size(),
                            limitedSplits.size(),
                            pushDownLimit);
                    return Optional.of(new ScannedResult(newPlan));
                }
            }
        }
        return Optional.of(result);
    }

    private Optional<StartingScanner.Result> applyPushDownTopN() {
        if (topN == null
                || pushDownLimit != null
                || !schema.primaryKeys().isEmpty()
                || options().deletionVectorsEnabled()) {
            return Optional.empty();
        }

        List<SortValue> orders = topN.orders();
        if (orders.size() != 1) {
            return Optional.empty();
        }

        if (topN.limit() > 100) {
            return Optional.empty();
        }

        SortValue order = orders.get(0);
        DataType type = order.field().type();
        if (!minmaxAvailable(type)) {
            return Optional.empty();
        }

        StartingScanner.Result result = startingScanner.scan(snapshotReader.keepStats());
        if (!(result instanceof ScannedResult)) {
            return Optional.of(result);
        }

        SnapshotReader.Plan plan = ((ScannedResult) result).plan();
        List<DataSplit> splits = plan.dataSplits();
        if (splits.isEmpty()) {
            return Optional.of(result);
        }

        TopNDataSplitEvaluator evaluator = new TopNDataSplitEvaluator(schema, schemaManager);
        List<Split> topNSplits = new ArrayList<>(evaluator.evaluate(order, topN.limit(), splits));
        SnapshotReader.Plan newPlan = new PlanImpl(plan.watermark(), plan.snapshotId(), topNSplits);
        return Optional.of(new ScannedResult(newPlan));
    }

    @Override
    public DataTableScan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        snapshotReader.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
        return this;
    }
}
