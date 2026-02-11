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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 读取构建器接口，用于构建 {@link TableScan} 和 {@link TableRead}。
 *
 * <p>ReadBuilder 是创建表扫描和读取器的入口点，提供了流式 API 来配置读取参数，
 * 如过滤条件、列裁剪、分区过滤等。
 *
 * <h3>分布式读取示例</h3>
 * <pre>{@code
 * // 1. 创建 ReadBuilder（可序列化）
 * Table table = catalog.getTable(...);
 * ReadBuilder builder = table.newReadBuilder()
 *     .withFilter(...)
 *     .withReadType(...);
 *
 * // 2. 在协调器（Coordinator/Driver）中规划分片
 * List<Split> splits = builder.newScan().plan().splits();
 *
 * // 3. 将分片分配给不同的任务
 *
 * // 4. 在任务中读取分片
 * TableRead read = builder.newRead();
 * RecordReader<InternalRow> reader = read.createReader(split);
 * reader.forEachRemaining(...);
 * }</pre>
 *
 * <h3>流式扫描示例</h3>
 * <p>{@link #newStreamScan()} 会创建流式扫描，支持持续规划：
 * <pre>{@code
 * TableScan scan = builder.newStreamScan();
 * while (true) {
 *     List<Split> splits = scan.plan().splits();
 *     // 处理分片...
 * }
 * }</pre>
 *
 * <h3>核心功能</h3>
 * <ul>
 *   <li><b>过滤下推</b>: 通过 {@link #withFilter(Predicate)} 下推过滤条件</li>
 *   <li><b>列裁剪</b>: 通过 {@link #withReadType(RowType)} 或 {@link #withProjection(int[])} 指定读取列</li>
 *   <li><b>分区裁剪</b>: 通过 {@link #withPartitionFilter} 过滤分区</li>
 *   <li><b>桶过滤</b>: 通过 {@link #withBucket(int)} 或 {@link #withBucketFilter} 过滤桶</li>
 *   <li><b>Limit 优化</b>: 通过 {@link #withLimit(int)} 限制读取行数</li>
 *   <li><b>TopN 优化</b>: 通过 {@link #withTopN(TopN)} 执行排序 + Limit</li>
 *   <li><b>分片（Shard）</b>: 通过 {@link #withShard} 分配读取任务</li>
 * </ul>
 *
 * <h3>过滤保证</h3>
 * <p>注意：{@link #withFilter} 会尽可能过滤数据，但不保证是完全过滤。
 * 用户需要重新检查所有返回的记录（因为某些过滤条件可能无法通过统计信息判断）。
 *
 * <h3>InternalRow 注意事项</h3>
 * <p>重要：{@link InternalRow} 不能保存在内存中。它可能在内部被重用，
 * 因此需要将其转换为自己的数据结构或复制它。
 *
 * <h3>构建器方法</h3>
 * <ul>
 *   <li>{@link #newScan()}: 创建批量扫描（一次性读取）</li>
 *   <li>{@link #newStreamScan()}: 创建流式扫描（持续读取）</li>
 *   <li>{@link #newRead()}: 创建读取器（读取 Split）</li>
 * </ul>
 *
 * <h3>分片 vs 桶过滤</h3>
 * <p>注意：{@link #withShard} 和 {@link #withBucketFilter} 不能同时使用。
 * 原因：分片和桶过滤是不同的逻辑机制，同时应用会引入冲突的选择标准。
 *
 * @see TableScan 表扫描接口
 * @see TableRead 表读取接口
 * @see ReadBuilderImpl 实现类
 * @since 0.4.0
 */
@Public
public interface ReadBuilder extends Serializable {

    /** 获取表名（用于标识表）。 */
    String tableName();

    /** 获取读取的行类型（包含列裁剪后的结果）。 */
    RowType readType();

    /**
     * 应用过滤条件到读取器，减少产生的记录数。
     *
     * <p>该方法是 {@link #withFilter(Predicate)} 的便捷方法，
     * 会将多个谓词合并为一个 AND 谓词。
     *
     * <p>注意：此接口会尽可能过滤记录，但某些产生的记录可能不满足所有谓词。
     * 用户需要重新检查所有记录。
     *
     * @param predicates 谓词列表（null 或空列表表示不过滤）
     * @return this
     */
    default ReadBuilder withFilter(List<Predicate> predicates) {
        if (predicates == null || predicates.isEmpty()) {
            return this;
        }
        return withFilter(PredicateBuilder.and(predicates));
    }

    /**
     * 下推过滤条件，尽可能过滤数据，但不保证完全过滤。
     *
     * <p>过滤条件会应用在两个阶段：
     * <ul>
     *   <li><b>扫描阶段</b>: 利用文件统计信息过滤文件</li>
     *   <li><b>读取阶段</b>: 如果调用了 {@link TableRead#executeFilter()}，也会过滤行数据</li>
     * </ul>
     *
     * @param predicate 过滤谓词
     * @return this
     */
    ReadBuilder withFilter(Predicate predicate);

    /**
     * 下推分区过滤（使用单个分区）。
     *
     * @param partitionSpec 分区键值对
     * @return this
     */
    ReadBuilder withPartitionFilter(Map<String, String> partitionSpec);

    /**
     * 下推分区过滤（使用分区谓词）。
     *
     * @param partitionPredicate 分区谓词
     * @return this
     */
    ReadBuilder withPartitionFilter(PartitionPredicate partitionPredicate);

    /**
     * 设置桶过滤（只读取指定的桶）。
     *
     * @param bucket 桶号
     * @return this
     */
    ReadBuilder withBucket(int bucket);

    /**
     * 下推桶过滤器（自定义桶过滤逻辑）。
     *
     * <p>注意：此方法不能与 {@link #withShard(int, int)} 同时使用。
     *
     * <p>原因：桶过滤和分片是不同的逻辑机制。同时应用两种方法会引入冲突的选择标准。
     *
     * @param bucketFilter 桶过滤器
     * @return this
     */
    ReadBuilder withBucketFilter(Filter<Integer> bucketFilter);

    /**
     * 下推读取行类型，支持嵌套行裁剪。
     *
     * <p>通过指定 readType，只读取需要的列，支持嵌套字段的裁剪。
     *
     * @param readType 读取行类型
     * @return this
     * @since 1.0.0
     */
    ReadBuilder withReadType(RowType readType);

    /**
     * 应用列投影到读取器。
     *
     * <p>如果需要嵌套行裁剪，请使用 {@link #withReadType(RowType)} 代替。
     *
     * @param projection 列索引数组
     * @return this
     */
    ReadBuilder withProjection(int[] projection);

    /**
     * 下推行数限制（Limit 优化）。
     *
     * @param limit 最大行数
     * @return this
     */
    ReadBuilder withLimit(int limit);

    /**
     * 下推 TopN 过滤（排序 + Limit）。
     *
     * <p>会尽可能过滤数据，但不保证完全过滤。
     *
     * @param topN TopN 查询条件
     * @return this
     */
    ReadBuilder withTopN(TopN topN);

    /**
     * 指定要读取的分片（shard），分配分片的文件供读取。
     *
     * <p>注意：此方法不能与 {@link #withBucketFilter(Filter)} 同时使用。
     *
     * <p>原因：分片和桶过滤是不同的逻辑机制。同时应用两种方法会引入冲突的选择标准。
     *
     * @param indexOfThisSubtask 当前任务的索引
     * @param numberOfParallelSubtasks 并行任务的总数
     * @return this
     */
    ReadBuilder withShard(int indexOfThisSubtask, int numberOfParallelSubtasks);

    /**
     * 指定要读取的行 ID 范围。
     *
     * <p>通常用于读取数据演化表中的特定行。
     *
     * @param rowRanges 要读取的行 ID 范围
     * @return this
     */
    ReadBuilder withRowRanges(List<Range> rowRanges);

    /**
     * 下推向量检索到读取器。
     *
     * @param vectorSearch 向量检索条件
     * @return this
     */
    ReadBuilder withVectorSearch(VectorSearch vectorSearch);

    /**
     * 删除扫描计划结果中的统计信息。
     *
     * @return this
     */
    ReadBuilder dropStats();

    /**
     * 创建 {@link TableScan} 执行批量规划（一次性读取）。
     *
     * @return 批量扫描器
     */
    TableScan newScan();

    /**
     * 创建 {@link StreamTableScan} 执行流式规划（持续读取）。
     *
     * @return 流式扫描器
     */
    StreamTableScan newStreamScan();

    /**
     * 创建 {@link TableRead} 读取 {@link Split}。
     *
     * @return 表读取器
     */
    TableRead newRead();
}
