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

package org.apache.paimon.globalindex;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.RowIdPredicateVisitor;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableBatchScan;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.paimon.globalindex.GlobalIndexScanBuilder.parallelScan;
import static org.apache.paimon.table.SpecialFields.ROW_ID;

/**
 * 数据演化批量扫描器。
 *
 * <p>该类用于支持数据演化表的批量扫描操作，集成了全局索引功能以优化查询性能。
 *
 * <h3>核心功能：</h3>
 * <ul>
 *   <li>支持基于全局索引的行范围过滤
 *   <li>支持向量搜索和分数返回
 *   <li>自动处理 RowId 谓词并转换为行范围
 *   <li>将扫描结果封装为带索引信息的 Split
 * </ul>
 *
 * <h3>工作流程：</h3>
 * <ol>
 *   <li>接收查询谓词和向量搜索条件
 *   <li>从谓词中提取 RowId 范围信息
 *   <li>评估全局索引以获取匹配的行范围
 *   <li>基于行范围进行数据扫描
 *   <li>将扫描结果封装为 IndexedSplit 以支持分数排序
 * </ol>
 *
 * <h3>使用场景：</h3>
 * <ul>
 *   <li>支持全局索引的表进行点查询优化
 *   <li>向量相似度搜索场景
 *   <li>基于行号的精确范围扫描
 * </ul>
 *
 * <h3>设计模式：</h3>
 * <ul>
 *   <li>装饰器模式：包装 DataTableBatchScan 以增强功能
 *   <li>委托模式：大部分操作委托给内部的 batchScan 实现
 * </ul>
 */
public class DataEvolutionBatchScan implements DataTableScan {

    /** 文件存储表实例 */
    private final FileStoreTable table;

    /** 内部批量扫描器 */
    private final DataTableBatchScan batchScan;

    /** 过滤谓词 */
    private Predicate filter;

    /** 向量搜索条件 */
    private VectorSearch vectorSearch;

    /** 下推的行范围列表 */
    private List<Range> pushedRowRanges;

    /** 全局索引评估结果 */
    private GlobalIndexResult globalIndexResult;

    /**
     * 构造数据演化批量扫描器。
     *
     * @param wrapped 文件存储表实例
     * @param batchScan 内部批量扫描器
     */
    public DataEvolutionBatchScan(FileStoreTable wrapped, DataTableBatchScan batchScan) {
        this.table = wrapped;
        this.batchScan = batchScan;
    }

    /**
     * 设置分片信息，用于并行扫描。
     *
     * @param indexOfThisSubtask 当前子任务索引
     * @param numberOfParallelSubtasks 并行子任务总数
     * @return 当前扫描器实例
     */
    @Override
    public DataTableScan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        return batchScan.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
    }

    /**
     * 设置过滤谓词。
     *
     * <p>该方法会自动处理 RowId 谓词：
     * <ul>
     *   <li>提取 RowId 范围信息并转换为行范围
     *   <li>从谓词中移除 RowId 过滤条件
     *   <li>将剩余谓词传递给内部扫描器
     * </ul>
     *
     * @param predicate 过滤谓词
     * @return 当前扫描器实例
     */
    @Override
    public InnerTableScan withFilter(Predicate predicate) {
        if (predicate == null) {
            return this;
        }

        predicate.visit(new RowIdPredicateVisitor()).ifPresent(this::withRowRanges);
        predicate = removeRowIdFilter(predicate);
        this.filter = predicate;
        batchScan.withFilter(predicate);
        return this;
    }

    /**
     * 递归移除谓词中的 RowId 过滤条件。
     *
     * <p>实现逻辑：
     * <ul>
     *   <li>对于叶子谓词：检查是否包含 RowId 字段，是则返回 null
     *   <li>对于复合谓词：递归处理所有子谓词，重新构建不含 RowId 的新谓词
     * </ul>
     *
     * @param filter 待处理的谓词
     * @return 移除 RowId 后的谓词，若完全移除则返回 null
     */
    private Predicate removeRowIdFilter(Predicate filter) {
        // 叶子谓词：检查是否为 RowId 字段
        if (filter instanceof LeafPredicate
                && ((LeafPredicate) filter).fieldNames().contains(ROW_ID.name())) {
            return null;
        } else if (filter instanceof CompoundPredicate) {
            // 复合谓词：递归处理子谓词
            CompoundPredicate compoundPredicate = (CompoundPredicate) filter;

            List<Predicate> newChildren = new ArrayList<>();
            for (Predicate child : compoundPredicate.children()) {
                Predicate newChild = removeRowIdFilter(child);
                if (newChild != null) {
                    newChildren.add(newChild);
                }
            }

            // 根据剩余子谓词数量构建新谓词
            if (newChildren.isEmpty()) {
                return null;
            } else if (newChildren.size() == 1) {
                return newChildren.get(0);
            } else {
                return new CompoundPredicate(compoundPredicate.function(), newChildren);
            }
        }
        return filter;
    }

    @Override
    public InnerTableScan withVectorSearch(VectorSearch vectorSearch) {
        this.vectorSearch = vectorSearch;
        batchScan.withVectorSearch(vectorSearch);
        return this;
    }

    @Override
    public InnerTableScan withReadType(@Nullable RowType readType) {
        batchScan.withReadType(readType);
        return this;
    }

    @Override
    public InnerTableScan withBucket(int bucket) {
        batchScan.withBucket(bucket);
        return this;
    }

    @Override
    public InnerTableScan withTopN(TopN topN) {
        batchScan.withTopN(topN);
        return this;
    }

    @Override
    public InnerTableScan dropStats() {
        batchScan.dropStats();
        return this;
    }

    @Override
    public InnerTableScan withMetricRegistry(MetricRegistry metricsRegistry) {
        batchScan.withMetricRegistry(metricsRegistry);
        return this;
    }

    @Override
    public InnerTableScan withLimit(int limit) {
        batchScan.withLimit(limit);
        return this;
    }

    @Override
    public InnerTableScan withPartitionFilter(Map<String, String> partitionSpec) {
        batchScan.withPartitionFilter(partitionSpec);
        return this;
    }

    @Override
    public InnerTableScan withPartitionFilter(List<BinaryRow> partitions) {
        batchScan.withPartitionFilter(partitions);
        return this;
    }

    @Override
    public InnerTableScan withPartitionsFilter(List<Map<String, String>> partitions) {
        batchScan.withPartitionsFilter(partitions);
        return this;
    }

    @Override
    public InnerTableScan withPartitionFilter(PartitionPredicate partitionPredicate) {
        batchScan.withPartitionFilter(partitionPredicate);
        return this;
    }

    @Override
    public InnerTableScan withBucketFilter(Filter<Integer> bucketFilter) {
        batchScan.withBucketFilter(bucketFilter);
        return this;
    }

    @Override
    public InnerTableScan withLevelFilter(Filter<Integer> levelFilter) {
        batchScan.withLevelFilter(levelFilter);
        return this;
    }

    /**
     * 设置行范围过滤条件。
     *
     * <p>注意：该方法与 {@link #withGlobalIndexResult} 互斥，不能同时使用。
     *
     * @param rowRanges 行范围列表
     * @return 当前扫描器实例
     * @throws IllegalStateException 如果已经设置了全局索引结果
     */
    @Override
    public InnerTableScan withRowRanges(List<Range> rowRanges) {
        if (rowRanges == null) {
            return this;
        }

        this.pushedRowRanges = rowRanges;
        if (globalIndexResult != null) {
            throw new IllegalStateException("Cannot push row ranges after global index eval.");
        }
        return this;
    }

    /**
     * 直接设置全局索引评估结果。
     *
     * <p>该方法允许外部系统自行计算索引结果并传入，跳过内部的索引评估过程。
     *
     * @param globalIndexResult 全局索引评估结果
     * @return 当前扫描器实例
     * @throws IllegalStateException 如果已经设置了行范围
     */
    public InnerTableScan withGlobalIndexResult(GlobalIndexResult globalIndexResult) {
        this.globalIndexResult = globalIndexResult;
        if (pushedRowRanges != null) {
            throw new IllegalStateException("");
        }
        return this;
    }

    @Override
    public List<PartitionEntry> listPartitionEntries() {
        return batchScan.listPartitionEntries();
    }

    /**
     * 生成扫描计划。
     *
     * <p>执行流程：
     * <ol>
     *   <li>优先使用已设置的行范围（pushedRowRanges）
     *   <li>若无行范围，则评估全局索引获取行范围
     *   <li>若无全局索引结果，则执行普通扫描
     *   <li>基于行范围生成 DataSplit
     *   <li>将 DataSplit 封装为 IndexedSplit 以支持分数排序
     * </ol>
     *
     * @return 包含 Split 列表的扫描计划
     */
    @Override
    public Plan plan() {
        List<Range> rowRanges = this.pushedRowRanges;
        ScoreGetter scoreGetter = null;

        // 如果没有预设的行范围，则评估全局索引
        if (rowRanges == null) {
            Optional<GlobalIndexResult> indexResult = evalGlobalIndex();
            if (indexResult.isPresent()) {
                GlobalIndexResult result = indexResult.get();
                rowRanges = result.results().toRangeList();
                // 如果索引结果包含分数信息，提取分数获取器
                if (result instanceof ScoredGlobalIndexResult) {
                    scoreGetter = ((ScoredGlobalIndexResult) result).scoreGetter();
                }
            }
        }

        // 如果没有行范围过滤，执行普通扫描
        if (rowRanges == null) {
            return batchScan.plan();
        }

        // 基于行范围执行扫描并封装为索引 Split
        List<Split> splits = batchScan.withRowRanges(rowRanges).plan().splits();
        return wrapToIndexSplits(splits, rowRanges, scoreGetter);
    }

    /**
     * 评估全局索引以获取匹配的行范围。
     *
     * <p>评估条件：
     * <ul>
     *   <li>已设置过滤谓词或向量搜索条件
     *   <li>表启用了全局索引功能
     *   <li>存在已索引的行范围数据
     * </ul>
     *
     * <p>评估流程：
     * <ol>
     *   <li>获取分区谓词和快照信息
     *   <li>扫描索引文件获取已索引的行范围
     *   <li>并行评估每个分片的索引数据
     *   <li>合并已索引和未索引的行范围
     * </ol>
     *
     * @return 全局索引评估结果，若不满足评估条件则返回 empty
     */
    private Optional<GlobalIndexResult> evalGlobalIndex() {
        // 优先使用预设的全局索引结果
        if (this.globalIndexResult != null) {
            return Optional.of(globalIndexResult);
        }
        // 检查是否有过滤条件
        if (filter == null && vectorSearch == null) {
            return Optional.empty();
        }
        // 检查表是否启用了全局索引
        if (!table.coreOptions().globalIndexEnabled()) {
            return Optional.empty();
        }
        // 获取分区谓词
        PartitionPredicate partitionPredicate =
                batchScan.snapshotReader().manifestsReader().partitionFilter();
        GlobalIndexScanBuilder indexScanBuilder = table.store().newGlobalIndexScanBuilder();
        Snapshot snapshot = TimeTravelUtil.tryTravelOrLatest(table);
        indexScanBuilder.withPartitionPredicate(partitionPredicate).withSnapshot(snapshot);
        // 获取所有已索引的行范围
        List<Range> indexedRowRanges = indexScanBuilder.shardList();
        if (indexedRowRanges.isEmpty()) {
            return Optional.empty();
        }

        // 计算未索引的行范围
        Long nextRowId = Objects.requireNonNull(snapshot.nextRowId());
        List<Range> nonIndexedRowRanges = new Range(0, nextRowId - 1).exclude(indexedRowRanges);
        // 并行扫描已索引的行范围
        Optional<GlobalIndexResult> resultOptional =
                parallelScan(
                        indexedRowRanges,
                        indexScanBuilder,
                        filter,
                        vectorSearch,
                        table.coreOptions().globalIndexThreadNum());
        if (!resultOptional.isPresent()) {
            return Optional.empty();
        }

        // 合并未索引的行范围到结果中
        GlobalIndexResult result = resultOptional.get();
        if (!nonIndexedRowRanges.isEmpty()) {
            for (Range range : nonIndexedRowRanges) {
                result.or(GlobalIndexResult.fromRange(range));
            }
        }

        return Optional.of(result);
    }

    /**
     * 将普通 Split 封装为带索引信息的 IndexedSplit。
     *
     * <p>封装过程：
     * <ol>
     *   <li>计算每个数据文件的行范围
     *   <li>与期望的行范围求交集，过滤不需要的数据
     *   <li>若有分数信息，为每个匹配行分配分数
     *   <li>创建 IndexedSplit 并添加到结果列表
     * </ol>
     *
     * @param splits 原始 Split 列表
     * @param rowRanges 期望的行范围列表
     * @param scoreGetter 分数获取器，可为 null
     * @return 包含 IndexedSplit 的扫描计划
     */
    private static Plan wrapToIndexSplits(
            List<Split> splits, List<Range> rowRanges, ScoreGetter scoreGetter) {
        List<Split> indexedSplits = new ArrayList<>();
        for (Split split : splits) {
            DataSplit dataSplit = (DataSplit) split;
            // 计算每个数据文件的行范围
            List<Range> fileRanges = new ArrayList<>();
            for (DataFileMeta file : dataSplit.dataFiles()) {
                fileRanges.add(
                        new Range(
                                file.nonNullFirstRowId(),
                                file.nonNullFirstRowId() + file.rowCount() - 1));
            }

            // 合并相邻的文件行范围
            fileRanges = Range.mergeSortedAsPossible(fileRanges);

            // 计算文件范围与期望范围的交集
            List<Range> expected = Range.and(fileRanges, rowRanges);

            // 如果有分数信息，为每个匹配行分配分数
            float[] scores = null;
            if (scoreGetter != null) {
                int size = expected.stream().mapToInt(r -> (int) (r.count())).sum();
                scores = new float[size];

                int index = 0;
                for (Range range : expected) {
                    for (long i = range.from; i <= range.to; i++) {
                        scores[index++] = scoreGetter.score(i);
                    }
                }
            }

            // 创建带索引信息的 Split
            indexedSplits.add(new IndexedSplit(dataSplit, expected, scores));
        }
        return () -> indexedSplits;
    }
}
