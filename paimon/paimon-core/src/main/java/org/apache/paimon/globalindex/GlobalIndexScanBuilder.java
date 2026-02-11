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
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.ManifestReadThreadPool.randomlyExecuteSequentialReturn;

/**
 * 全局索引扫描构建器接口。
 *
 * <p>用于构建全局索引扫描器，支持多种过滤条件和并行扫描能力。
 *
 * <h3>主要功能：</h3>
 * <ul>
 *   <li>配置扫描快照和分区谓词
 *   <li>指定扫描的行范围
 *   <li>构建行范围全局索引扫描器
 *   <li>获取索引分片列表
 *   <li>并行扫描多个行范围
 * </ul>
 *
 * <h3>Builder模式方法：</h3>
 * <ul>
 *   <li>{@link #withSnapshot}: 设置扫描快照
 *   <li>{@link #withPartitionPredicate}: 设置分区谓词
 *   <li>{@link #withRowRange}: 设置行范围
 *   <li>{@link #build}: 构建扫描器
 *   <li>{@link #shardList}: 获取分片列表
 * </ul>
 *
 * <h3>并行扫描：</h3>
 * <p>{@link #parallelScan} 静态方法支持对多个行范围进行并行索引扫描，
 * 并合并结果，适用于大规模数据的索引查询场景。
 */
public interface GlobalIndexScanBuilder {

    /**
     * 设置扫描的快照ID。
     *
     * @param snapshotId 快照ID
     * @return 当前构建器实例
     */
    GlobalIndexScanBuilder withSnapshot(long snapshotId);

    /**
     * 设置扫描的快照对象。
     *
     * @param snapshot 快照对象
     * @return 当前构建器实例
     */
    GlobalIndexScanBuilder withSnapshot(Snapshot snapshot);

    /**
     * 设置分区谓词过滤条件。
     *
     * @param partitionPredicate 分区谓词
     * @return 当前构建器实例
     */
    GlobalIndexScanBuilder withPartitionPredicate(PartitionPredicate partitionPredicate);

    /**
     * 设置扫描的行范围。
     *
     * @param rowRange 行范围
     * @return 当前构建器实例
     */
    GlobalIndexScanBuilder withRowRange(Range rowRange);

    /**
     * 构建行范围全局索引扫描器。
     *
     * @return 行范围全局索引扫描器实例
     */
    RowRangeGlobalIndexScanner build();

    /**
     * 获取已排序且无重叠的行范围分片列表。
     *
     * <p>该列表表示当前快照中所有已建立索引的行范围，可用于：
     * <ul>
     *   <li>确定索引覆盖范围
     *   <li>分配并行扫描任务
     *   <li>计算未索引的数据范围
     * </ul>
     *
     * @return 已排序且无重叠的行范围列表
     */
    List<Range> shardList();

    /**
     * 并行扫描多个行范围的全局索引。
     *
     * <p>该方法会：
     * <ol>
     *   <li>为每个行范围创建独立的扫描器
     *   <li>使用线程池并行执行索引扫描
     *   <li>合并所有扫描结果
     *   <li>对于没有索引结果的范围，使用完整范围
     * </ol>
     *
     * <h3>算法流程：</h3>
     * <pre>
     * 1. 创建扫描器列表: ranges -> scanners
     * 2. 并行执行扫描: scanners -> results
     * 3. 合并结果:
     *    - 有结果: 使用扫描结果
     *    - 无结果: 使用完整范围
     * 4. 返回合并后的全局索引结果
     * </pre>
     *
     * @param ranges 待扫描的行范围列表
     * @param globalIndexScanBuilder 全局索引扫描构建器
     * @param filter 过滤谓词
     * @param vectorSearch 向量搜索条件（可为null）
     * @param threadNum 并行线程数
     * @return 合并后的全局索引结果，如果所有扫描都无结果则返回 empty
     */
    static Optional<GlobalIndexResult> parallelScan(
            final List<Range> ranges,
            final GlobalIndexScanBuilder globalIndexScanBuilder,
            final Predicate filter,
            @Nullable final VectorSearch vectorSearch,
            final Integer threadNum) {
        // 为每个范围创建扫描器
        List<RowRangeGlobalIndexScanner> scanners =
                ranges.stream()
                        .map(globalIndexScanBuilder::withRowRange)
                        .map(GlobalIndexScanBuilder::build)
                        .collect(Collectors.toList());

        try {
            List<Optional<GlobalIndexResult>> rowsResults = new ArrayList<>();
            // 并行执行扫描任务
            Iterator<Optional<GlobalIndexResult>> resultIterators =
                    randomlyExecuteSequentialReturn(
                            scanner -> {
                                Optional<GlobalIndexResult> result =
                                        scanner.scan(filter, vectorSearch);
                                return Collections.singletonList(result);
                            },
                            scanners,
                            threadNum);
            // 收集所有扫描结果
            while (resultIterators.hasNext()) {
                rowsResults.add(resultIterators.next());
            }
            // 如果所有扫描都无结果，返回 empty
            if (rowsResults.stream().noneMatch(Optional::isPresent)) {
                return Optional.empty();
            }

            // 合并所有扫描结果
            GlobalIndexResult globalIndexResult = GlobalIndexResult.createEmpty();

            for (int i = 0; i < ranges.size(); i++) {
                if (rowsResults.get(i).isPresent()) {
                    // 使用扫描结果
                    globalIndexResult = globalIndexResult.or(rowsResults.get(i).get());
                } else {
                    // 使用完整范围
                    globalIndexResult =
                            globalIndexResult.or(GlobalIndexResult.fromRange(ranges.get(i)));
                }
            }
            return Optional.of(globalIndexResult);
        } finally {
            // 关闭所有扫描器
            IOUtils.closeAllQuietly(scanners);
        }
    }
}
