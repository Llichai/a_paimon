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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/**
 * 内部表扫描接口，扩展 {@link TableScan}，支持过滤条件和优化参数的下推。
 *
 * <p>InnerTableScan 是 TableScan 的增强版本，提供了更多的配置选项，
 * 主要用于内部实现和高级用户，支持各种查询优化技术。
 *
 * <h3>与 TableScan 的关系</h3>
 * <ul>
 *   <li><b>TableScan</b>: 公共接口，提供基本的扫描功能</li>
 *   <li><b>InnerTableScan</b>: 内部接口，提供过滤下推、列裁剪等高级功能</li>
 * </ul>
 *
 * <h3>主要功能</h3>
 * <ul>
 *   <li><b>谓词下推</b>: 将过滤条件下推到扫描阶段，减少读取的数据量</li>
 *   <li><b>分区过滤</b>: 支持多种方式过滤分区（表达式、分区列表等）</li>
 *   <li><b>桶过滤</b>: 只扫描指定的桶，减少扫描范围</li>
 *   <li><b>列裁剪</b>: 指定要读取的列，减少 IO 量</li>
 *   <li><b>Limit 优化</b>: 限制读取的行数</li>
 *   <li><b>TopN 优化</b>: 支持 TopN 查询优化</li>
 *   <li><b>向量检索</b>: 支持向量相似度搜索</li>
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * InnerTableScan scan = (InnerTableScan) table.newScan();
 *
 * // 配置过滤和优化参数
 * Plan plan = scan
 *     .withFilter(predicate)                    // 谓词下推
 *     .withPartitionFilter(partitionPredicate)  // 分区过滤
 *     .withBucket(5)                            // 只扫描桶 5
 *     .withLimit(1000)                          // 限制 1000 行
 *     .withReadType(projectedType)              // 列裁剪
 *     .plan();
 * }</pre>
 *
 * <h3>实现类</h3>
 * <ul>
 *   <li>{@link AbstractDataTableScan}: 数据表扫描的抽象基类</li>
 *   <li>{@link DataTableBatchScan}: 批量扫描实现</li>
 *   <li>{@link DataTableStreamScan}: 流式扫描实现</li>
 * </ul>
 *
 * @see TableScan 基础扫描接口
 * @see InnerTableRead 内部读取接口（支持过滤和投影下推）
 */
public interface InnerTableScan extends TableScan {

    /**
     * 设置数据过滤谓词（用于过滤数据行）。
     *
     * <p>谓词会在两个阶段应用：
     * <ul>
     *   <li><b>扫描阶段</b>: 利用文件统计信息（min/max/null count）过滤文件</li>
     *   <li><b>读取阶段</b>: 如果调用了 {@link InnerTableRead#executeFilter()}，也会过滤行数据</li>
     * </ul>
     *
     * <h3>示例</h3>
     * <pre>{@code
     * // age > 18 AND city = 'Beijing'
     * Predicate predicate = PredicateBuilder.and(
     *     new LeafPredicate(Equal.GREATER_THAN, 0, DataTypes.INT(), "age", Collections.singletonList(18)),
     *     new LeafPredicate(Equal.EQUAL, 1, DataTypes.STRING(), "city", Collections.singletonList("Beijing"))
     * );
     * scan.withFilter(predicate);
     * }</pre>
     *
     * @param predicate 过滤谓词
     * @return this
     */
    InnerTableScan withFilter(Predicate predicate);

    /**
     * 设置向量检索条件（用于向量相似度搜索）。
     *
     * <p>向量检索用于在向量列上执行相似度搜索，通常用于 AI 应用：
     * <ul>
     *   <li>图像检索（找到相似的图像向量）</li>
     *   <li>文本检索（找到语义相似的文本向量）</li>
     * </ul>
     *
     * @param vectorSearch 向量检索条件
     * @return this
     */
    default InnerTableScan withVectorSearch(VectorSearch vectorSearch) {
        return this;
    }

    /**
     * 设置要读取的列类型（列裁剪）。
     *
     * <p>通过指定 readType，只读取需要的列，可以：
     * <ul>
     *   <li>减少 IO 量（不读取不需要的列）</li>
     *   <li>减少内存占用</li>
     *   <li>提升查询性能</li>
     * </ul>
     *
     * <h3>示例</h3>
     * <pre>{@code
     * // 表结构: (id INT, name STRING, age INT, address STRING)
     * // 只读取 id 和 name 列
     * RowType readType = RowType.of(DataTypes.INT(), DataTypes.STRING());
     * scan.withReadType(readType);
     * }</pre>
     *
     * @param readType 要读取的列类型（null 表示读取所有列）
     * @return this
     */
    default InnerTableScan withReadType(@Nullable RowType readType) {
        return this;
    }

    /**
     * 设置读取行数限制（Limit 优化）。
     *
     * <p>限制最多读取的行数，可以提前终止扫描，适用于：
     * <ul>
     *   <li>查询只需要部分数据（如 SELECT ... LIMIT 10）</li>
     *   <li>数据采样</li>
     * </ul>
     *
     * @param limit 最大行数
     * @return this
     */
    default InnerTableScan withLimit(int limit) {
        return this;
    }

    /**
     * 设置分区过滤（使用分区键值对）。
     *
     * <p>只扫描指定的单个分区。
     *
     * <h3>示例</h3>
     * <pre>{@code
     * // 只扫描分区 dt=2024-01-01, hour=10
     * Map<String, String> partition = new HashMap<>();
     * partition.put("dt", "2024-01-01");
     * partition.put("hour", "10");
     * scan.withPartitionFilter(partition);
     * }</pre>
     *
     * @param partitionSpec 分区键值对
     * @return this
     */
    default InnerTableScan withPartitionFilter(Map<String, String> partitionSpec) {
        return this;
    }

    /**
     * 设置分区过滤（使用分区列表）。
     *
     * <p>只扫描指定的多个分区。
     *
     * @param partitions 分区列表，每个分区用键值对表示
     * @return this
     */
    default InnerTableScan withPartitionsFilter(List<Map<String, String>> partitions) {
        return this;
    }

    /**
     * 设置分区过滤（使用二进制分区键列表）。
     *
     * @param partitions 二进制分区键列表
     * @return this
     */
    default InnerTableScan withPartitionFilter(List<BinaryRow> partitions) {
        return this;
    }

    /**
     * 设置分区过滤（使用分区谓词）。
     *
     * <p>PartitionPredicate 支持对分区列的复杂过滤条件。
     *
     * @param partitionPredicate 分区谓词
     * @return this
     */
    default InnerTableScan withPartitionFilter(PartitionPredicate partitionPredicate) {
        return this;
    }

    /**
     * 设置分区过滤（使用通用谓词）。
     *
     * <p>使用普通的 Predicate 过滤分区列。
     *
     * @param predicate 分区列的过滤谓词
     * @return this
     */
    default InnerTableScan withPartitionFilter(Predicate predicate) {
        return this;
    }

    /**
     * 设置行范围过滤（用于按行号范围扫描）。
     *
     * <p>行范围可用于分页查询或并行扫描。
     *
     * @param rowRanges 行范围列表
     * @return this
     */
    default InnerTableScan withRowRanges(List<Range> rowRanges) {
        return this;
    }

    /**
     * 设置桶过滤（只扫描指定的单个桶）。
     *
     * <p>Paimon 使用桶（bucket）来分片数据。通过指定桶号，
     * 可以只扫描该桶的数据，适用于：
     * <ul>
     *   <li>并行读取（每个任务读取不同的桶）</li>
     *   <li>按主键过滤（主键哈希到特定桶）</li>
     * </ul>
     *
     * @param bucket 桶号
     * @return this
     */
    default InnerTableScan withBucket(int bucket) {
        return this;
    }

    /**
     * 设置桶过滤器（自定义桶过滤逻辑）。
     *
     * <p>使用 Filter 接口自定义桶过滤条件。
     *
     * @param bucketFilter 桶过滤器
     * @return this
     */
    default InnerTableScan withBucketFilter(Filter<Integer> bucketFilter) {
        return this;
    }

    /**
     * 设置层级过滤器（只扫描指定层级的文件）。
     *
     * <p>LSM 树结构中，数据文件分布在不同层级（Level 0, 1, 2...）。
     * 通过层级过滤，可以只扫描特定层级的文件。
     *
     * @param levelFilter 层级过滤器
     * @return this
     */
    default InnerTableScan withLevelFilter(Filter<Integer> levelFilter) {
        return this;
    }

    /**
     * 设置监控指标注册器（覆盖父接口方法）。
     *
     * @param metricRegistry 指标注册器
     * @return this
     */
    @Override
    default InnerTableScan withMetricRegistry(MetricRegistry metricRegistry) {
        // do nothing, should implement this if need
        return this;
    }

    /**
     * 设置 TopN 优化（用于 ORDER BY ... LIMIT 查询）。
     *
     * <p>TopN 优化可以在扫描阶段就进行排序和限制，减少读取的数据量。
     *
     * @param topN TopN 查询条件
     * @return this
     */
    default InnerTableScan withTopN(TopN topN) {
        return this;
    }

    /**
     * 丢弃统计信息（不使用统计信息进行文件过滤）。
     *
     * <p>在某些场景下，统计信息可能不准确或不可用，
     * 可以调用此方法禁用基于统计信息的优化。
     *
     * @return this
     */
    default InnerTableScan dropStats() {
        // do nothing, should implement this if need
        return this;
    }
}
