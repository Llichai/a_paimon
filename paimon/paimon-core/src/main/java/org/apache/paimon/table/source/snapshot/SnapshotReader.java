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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.BucketEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.ManifestsReader;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BiFilter;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 快照读取器接口
 *
 * <p>该接口定义了从指定 {@link Snapshot} 读取数据分片（Split）的核心功能。
 *
 * <p><b>核心职责：</b>
 * <ol>
 *   <li>配置读取参数（快照、过滤条件、扫描模式等）
 *   <li>执行扫描并生成读取计划（Plan）
 *   <li>将读取计划转换为数据分片（Split）
 * </ol>
 *
 * <p><b>主要方法分类：</b>
 * <table border="1">
 *   <tr>
 *     <th>方法类型</th>
 *     <th>方法示例</th>
 *     <th>说明</th>
 *   </tr>
 *   <tr>
 *     <td>配置方法</td>
 *     <td>withSnapshot, withFilter, withMode</td>
 *     <td>设置读取参数，返回 this 支持链式调用</td>
 *   </tr>
 *   <tr>
 *     <td>执行方法</td>
 *     <td>read, readChanges, readIncrementalDiff</td>
 *     <td>执行扫描，返回 Plan</td>
 *   </tr>
 *   <tr>
 *     <td>元数据方法</td>
 *     <td>partitions, partitionEntries, bucketEntries</td>
 *     <td>查询分区/桶信息</td>
 *   </tr>
 *   <tr>
 *     <td>访问器方法</td>
 *     <td>snapshotManager, splitGenerator</td>
 *     <td>获取内部组件</td>
 *   </tr>
 * </table>
 *
 * <p><b>使用示例：</b>
 * <pre>
 * // 1. 批量读取：读取完整快照
 * Plan plan = snapshotReader
 *     .withSnapshot(snapshotId)
 *     .withMode(ScanMode.ALL)
 *     .withFilter(predicate)
 *     .read();
 *
 * // 2. 流式读取：读取增量变更
 * Plan plan = snapshotReader
 *     .withSnapshot(snapshotId)
 *     .withMode(ScanMode.DELTA)
 *     .readChanges();
 *
 * // 3. 增量比对：读取两个快照之间的差异
 * Plan plan = snapshotReader
 *     .withSnapshot(endSnapshot)
 *     .readIncrementalDiff(startSnapshot);
 * </pre>
 *
 * <p><b>与其他组件的关系：</b>
 * <ul>
 *   <li>{@link org.apache.paimon.table.source.TableScan}：使用 SnapshotReader 生成扫描计划
 *   <li>{@link StartingScanner}：在流式读取的起始阶段使用 SnapshotReader
 *   <li>{@link FollowUpScanner}：在流式读取的持续阶段使用 SnapshotReader
 *   <li>{@link org.apache.paimon.operation.FileStoreScan}：底层文件扫描接口
 * </ul>
 *
 * <p><b>实现类：</b>
 * <ul>
 *   <li>{@link SnapshotReaderImpl}：默认实现
 * </ul>
 *
 * @see SnapshotReaderImpl
 * @see Plan
 * @see org.apache.paimon.table.source.TableScan
 */
public interface SnapshotReader {

    /** 获取并行度配置，如果未配置则返回 null */
    @Nullable
    Integer parallelism();

    /** 获取快照管理器，用于访问和管理快照 */
    SnapshotManager snapshotManager();

    /** 获取 Changelog 管理器，用于访问和管理 changelog 文件 */
    ChangelogManager changelogManager();

    /** 获取 Manifest 读取器，用于读取 manifest 文件 */
    ManifestsReader manifestsReader();

    /** 读取指定 manifest 文件中的所有条目 */
    List<ManifestEntry> readManifest(ManifestFileMeta manifest);

    /** 获取消费者管理器，用于管理流式读取的消费位置 */
    ConsumerManager consumerManager();

    /** 获取分片生成器，用于将文件转换为读取分片 */
    SplitGenerator splitGenerator();

    /** 获取文件路径工厂，用于生成文件和目录路径 */
    FileStorePathFactory pathFactory();

    /** 设置要读取的快照 ID */
    SnapshotReader withSnapshot(long snapshotId);

    /** 设置要读取的快照对象 */
    SnapshotReader withSnapshot(Snapshot snapshot);

    /** 设置数据过滤谓词（自动分离分区过滤和数据过滤） */
    SnapshotReader withFilter(Predicate predicate);

    /** 设置分区过滤条件（通过分区键值对） */
    SnapshotReader withPartitionFilter(Map<String, String> partitionSpec);

    /** 设置分区过滤谓词 */
    SnapshotReader withPartitionFilter(Predicate predicate);

    /** 设置要读取的分区列表 */
    SnapshotReader withPartitionFilter(List<BinaryRow> partitions);

    /** 设置分区过滤器 */
    SnapshotReader withPartitionFilter(PartitionPredicate partitionPredicate);

    /** 设置多个分区过滤条件 */
    SnapshotReader withPartitionsFilter(List<Map<String, String>> partitions);

    /** 设置扫描模式（ALL/DELTA/CHANGELOG） */
    SnapshotReader withMode(ScanMode scanMode);

    /** 设置只读取指定 LSM 层级的文件 */
    SnapshotReader withLevel(int level);

    /** 设置层级过滤器 */
    SnapshotReader withLevelFilter(Filter<Integer> levelFilter);

    /** 设置层级最小最大值过滤器 */
    SnapshotReader withLevelMinMaxFilter(BiFilter<Integer, Integer> minMaxFilter);

    /** 启用值过滤（基于统计信息进行数据过滤） */
    SnapshotReader enableValueFilter();

    /** 设置 Manifest 条目过滤器 */
    SnapshotReader withManifestEntryFilter(Filter<ManifestEntry> filter);

    /** 设置只读取指定桶号的数据 */
    SnapshotReader withBucket(int bucket);

    /** 只读取真实桶（跳过延迟桶） */
    SnapshotReader onlyReadRealBuckets();

    /** 设置桶号过滤器 */
    SnapshotReader withBucketFilter(Filter<Integer> bucketFilter);

    /** 设置数据文件名过滤器 */
    SnapshotReader withDataFileNameFilter(Filter<String> fileNameFilter);

    /** 丢弃统计信息（减少内存占用） */
    SnapshotReader dropStats();

    /** 保留统计信息 */
    SnapshotReader keepStats();

    /** 设置分片策略（用于分布式读取） */
    SnapshotReader withShard(int indexOfThisSubtask, int numberOfParallelSubtasks);

    /** 设置度量注册器 */
    SnapshotReader withMetricRegistry(MetricRegistry registry);

    /** 设置行范围过滤（用于主键范围查询） */
    SnapshotReader withRowRanges(List<Range> rowRanges);

    /** 设置读取类型（投影下推） */
    SnapshotReader withReadType(RowType readType);

    /** 设置读取记录数上限 */
    SnapshotReader withLimit(int limit);

    /**
     * 读取完整快照
     *
     * <p>根据扫描模式（ScanMode）读取数据：
     * <ul>
     *   <li>ScanMode.ALL：读取 baseManifestList，返回完整快照数据
     *   <li>ScanMode.DELTA：读取 deltaManifestList，返回增量数据
     *   <li>ScanMode.CHANGELOG：读取 changelogManifestList，返回变更日志
     * </ul>
     *
     * @return 读取计划（包含分片列表）
     */
    Plan read();

    /**
     * 读取文件变更
     *
     * <p>读取当前快照相对于前一个快照的文件变更，返回增量分片。
     * 使用 {@link org.apache.paimon.table.source.IncrementalSplit} 表示变更。
     *
     * @return 读取计划（包含 before/after 文件）
     */
    Plan readChanges();

    /**
     * 读取增量差异
     *
     * <p>读取两个快照之间的数据差异，用于增量查询。
     *
     * @param before 起始快照
     * @return 读取计划（包含差异数据）
     */
    Plan readIncrementalDiff(Snapshot before);

    /** 列出所有分区 */
    List<BinaryRow> partitions();

    /** 列出分区条目（包含统计信息） */
    List<PartitionEntry> partitionEntries();

    /** 列出桶条目（包含统计信息） */
    List<BucketEntry> bucketEntries();

    /** 返回文件迭代器（用于遍历所有文件） */
    Iterator<ManifestEntry> readFileIterator();

    /**
     * 扫描结果计划
     *
     * <p>包含本次扫描的所有信息：
     * <ul>
     *   <li>快照 ID
     *   <li>水位线
     *   <li>数据分片列表
     * </ul>
     */
    interface Plan extends TableScan.Plan {

        /** 获取水位线（如果有） */
        @Nullable
        Long watermark();

        /**
         * 获取快照 ID
         *
         * <p>如果表为空或者使用了 manifest list 则返回 null
         */
        @Nullable
        Long snapshotId();

        /** 获取结果分片列表 */
        List<Split> splits();

        /** 获取数据分片列表（类型转换辅助方法） */
        @SuppressWarnings({"unchecked", "rawtypes"})
        default List<DataSplit> dataSplits() {
            return (List) splits();
        }
    }
}
