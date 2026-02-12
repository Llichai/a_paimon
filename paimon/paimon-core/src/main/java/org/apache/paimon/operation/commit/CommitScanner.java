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

package org.apache.paimon.operation.commit;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.ScanMode;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;

/**
 * 提交扫描器
 *
 * <p>用于在提交过程中扫描Manifest条目的工具类。
 *
 * <h2>功能概述</h2>
 * <p>该类提供以下扫描功能：
 * <ul>
 *   <li><b>增量扫描</b>：扫描两个快照之间的增量变更
 *   <li><b>分区扫描</b>：扫描指定分区的所有文件
 *   <li><b>覆盖写入扫描</b>：为覆盖写入操作扫描需要删除的文件
 * </ul>
 *
 * <h2>扫描类型</h2>
 * <p>支持多种扫描模式：
 * <ul>
 *   <li><b>DELTA模式</b>：只扫描增量变更
 *   <li><b>ALL模式</b>：扫描所有文件
 * </ul>
 *
 * <h2>使用场景</h2>
 * <p>该类主要用于：
 * <ul>
 *   <li>冲突检测：扫描基线快照和增量变更
 *   <li>覆盖写入：扫描需要删除的旧文件
 *   <li>数据验证：检查分区内的文件完整性
 * </ul>
 *
 * <h2>性能优化</h2>
 * <p>支持以下优化选项：
 * <ul>
 *   <li><b>统计信息删除</b>：对于DELETE条目，可以删除统计信息减少存储
 *   <li><b>分区过滤</b>：只扫描指定分区，减少扫描范围
 *   <li><b>Bucket过滤</b>：只扫描有效的Bucket，跳过延迟Bucket
 * </ul>
 *
 * @see FileStoreScan 文件存储扫描接口
 * @see IndexManifestFile 索引Manifest文件操作
 * @see ScanMode 扫描模式
 */
public class CommitScanner {

    /** 文件存储扫描器 */
    private final FileStoreScan scan;

    /** 索引Manifest文件操作接口 */
    private final IndexManifestFile indexManifestFile;

    /**
     * 构造提交扫描器
     *
     * @param scan 文件存储扫描器
     * @param indexManifestFile 索引Manifest文件操作接口
     * @param options 核心配置选项
     */
    public CommitScanner(
            FileStoreScan scan, IndexManifestFile indexManifestFile, CoreOptions options) {
        this.scan = scan;
        this.indexManifestFile = indexManifestFile;
        // DELETE类型的Manifest条目中的统计信息是无用的，可以删除
        if (options.manifestDeleteFileDropStats()) {
            this.scan.dropStats();
        }
    }

    /**
     * 读取增量变更
     *
     * <p>读取两个快照之间的所有增量变更，支持按分区过滤。
     *
     * <p>扫描范围：从 {@code from.id() + 1} 到 {@code to.id()}
     *
     * <h3>使用场景</h3>
     * <ul>
     *   <li>冲突检测：扫描基线快照之后的所有变更
     *   <li>增量合并：合并多个快照的变更
     * </ul>
     *
     * @param from 起始快照（不包含）
     * @param to 结束快照（包含）
     * @param changedPartitions 变更的分区列表，用于过滤
     * @return 简化的文件条目列表
     */
    public List<SimpleFileEntry> readIncrementalChanges(
            Snapshot from, Snapshot to, List<BinaryRow> changedPartitions) {
        List<SimpleFileEntry> entries = new ArrayList<>();
        for (long i = from.id() + 1; i <= to.id(); i++) {
            List<SimpleFileEntry> delta =
                    scan.withSnapshot(i)
                            .withKind(ScanMode.DELTA)
                            .withPartitionFilter(changedPartitions)
                            .readSimpleEntries();
            entries.addAll(delta);
        }
        return entries;
    }

    /**
     * 读取增量条目
     *
     * <p>读取指定快照中指定分区的增量变更条目。
     *
     * <p>与 {@link #readIncrementalChanges} 的区别：
     * <ul>
     *   <li>返回完整的 {@link ManifestEntry}，包含更多元数据
     *   <li>只读取单个快照的变更
     *   <li>通过计划接口获取，支持更多过滤选项
     * </ul>
     *
     * @param snapshot 要扫描的快照
     * @param changedPartitions 变更的分区列表
     * @return Manifest条目列表
     */
    public List<ManifestEntry> readIncrementalEntries(
            Snapshot snapshot, List<BinaryRow> changedPartitions) {
        return scan.withSnapshot(snapshot)
                .withKind(ScanMode.DELTA)
                .withPartitionFilter(changedPartitions)
                .plan()
                .files();
    }

    /**
     * 读取变更分区中的所有条目
     *
     * <p>读取指定快照中变更分区的所有文件条目（不仅是增量）。
     *
     * <h3>使用场景</h3>
     * <ul>
     *   <li>冲突检测：读取基线快照中的所有文件
     *   <li>数据验证：检查分区完整性
     * </ul>
     *
     * <h3>与增量扫描的区别</h3>
     * <ul>
     *   <li>使用 {@link ScanMode#ALL} 扫描所有文件
     *   <li>返回简化的文件条目
     *   <li>只扫描指定分区
     * </ul>
     *
     * @param snapshot 要扫描的快照
     * @param changedPartitions 变更的分区列表
     * @return 简化的文件条目列表
     * @throws RuntimeException 如果读取失败
     */
    public List<SimpleFileEntry> readAllEntriesFromChangedPartitions(
            Snapshot snapshot, List<BinaryRow> changedPartitions) {
        try {
            return scan.withSnapshot(snapshot)
                    .withKind(ScanMode.ALL)
                    .withPartitionFilter(changedPartitions)
                    .readSimpleEntries();
        } catch (Throwable e) {
            throw new RuntimeException("Cannot read manifest entries from changed partitions.", e);
        }
    }

    /**
     * 读取覆盖写入的变更
     *
     * <p>为覆盖写入操作生成提交变更，包括删除旧文件和添加新文件。
     *
     * <h3>覆盖写入流程</h3>
     * <ol>
     *   <li>扫描最新快照中匹配分区过滤器的所有文件
     *   <li>为这些文件生成DELETE条目
     *   <li>为索引文件生成DELETE条目
     *   <li>添加新的数据文件和索引文件
     * </ol>
     *
     * <h3>Bucket过滤</h3>
     * <p>对于非延迟Bucket表：
     * <ul>
     *   <li>只删除Bucket >= 0的文件
     *   <li>跳过Bucket = -2（延迟Bucket）的文件
     * </ul>
     *
     * <h3>索引处理</h3>
     * <p>如果快照包含索引Manifest：
     * <ul>
     *   <li>读取所有索引条目
     *   <li>为匹配分区过滤器的索引生成DELETE条目
     * </ul>
     *
     * @param numBucket Bucket数量，用于判断是否是延迟Bucket表
     * @param changes 新文件的Manifest条目
     * @param indexFiles 新索引文件的条目
     * @param latestSnapshot 最新快照，如果不存在则为null
     * @param partitionFilter 分区过滤器，如果为null则覆盖所有分区
     * @return 包含删除和添加操作的提交变更
     */
    public CommitChanges readOverwriteChanges(
            int numBucket,
            List<ManifestEntry> changes,
            List<IndexManifestEntry> indexFiles,
            @Nullable Snapshot latestSnapshot,
            @Nullable PartitionPredicate partitionFilter) {
        List<ManifestEntry> changesWithOverwrite = new ArrayList<>();
        List<IndexManifestEntry> indexChangesWithOverwrite = new ArrayList<>();
        if (latestSnapshot != null) {
            scan.withSnapshot(latestSnapshot)
                    .withPartitionFilter(partitionFilter)
                    .withKind(ScanMode.ALL);
            if (numBucket != BucketMode.POSTPONE_BUCKET) {
                // Bucket = -2只能在延迟Bucket表中被覆盖
                scan.withBucketFilter(bucket -> bucket >= 0);
            }
            List<ManifestEntry> currentEntries = scan.plan().files();
            for (ManifestEntry entry : currentEntries) {
                changesWithOverwrite.add(
                        ManifestEntry.create(
                                FileKind.DELETE,
                                entry.partition(),
                                entry.bucket(),
                                entry.totalBuckets(),
                                entry.file()));
            }

            // 收集索引文件
            if (latestSnapshot.indexManifest() != null) {
                List<IndexManifestEntry> entries =
                        indexManifestFile.read(latestSnapshot.indexManifest());
                for (IndexManifestEntry entry : entries) {
                    if (partitionFilter == null || partitionFilter.test(entry.partition())) {
                        indexChangesWithOverwrite.add(entry.toDeleteEntry());
                    }
                }
            }
        }
        changesWithOverwrite.addAll(changes);
        indexChangesWithOverwrite.addAll(indexFiles);
        return new CommitChanges(changesWithOverwrite, emptyList(), indexChangesWithOverwrite);
    }
}
