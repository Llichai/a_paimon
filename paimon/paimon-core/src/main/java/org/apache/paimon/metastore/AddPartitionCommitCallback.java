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

package org.apache.paimon.metastore;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.table.PartitionHandler;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.utils.InternalRowPartitionComputer;

import org.apache.paimon.shade.guava30.com.google.common.cache.Cache;
import org.apache.paimon.shade.guava30.com.google.common.cache.CacheBuilder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 将新创建的分区添加到元数据存储的 {@link CommitCallback} 实现。
 *
 * <p><b>功能说明：</b>
 * 在提交成功后，自动将新增的分区信息同步到外部元数据存储（如 Hive Metastore），
 * 使得外部系统（如 Hive、Presto）能够感知到新分区的存在。
 *
 * <p><b>工作流程：</b>
 * <ol>
 *   <li>监听提交事件，收集新增的分区
 *   <li>过滤已处理的分区（通过缓存避免重复处理）
 *   <li>调用 {@link PartitionHandler} 在元数据存储中创建分区
 *   <li>更新缓存，记录已处理的分区
 * </ol>
 *
 * <p><b>缓存机制：</b>
 * 使用 Guava Cache 缓存已处理的分区：
 * <ul>
 *   <li>过期时间：30分钟访问过期
 *   <li>最大容量：300个分区
 *   <li>使用软引用：支持内存回收
 * </ul>
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li>Hive 集成：同步分区到 Hive Metastore
 *   <li>混合查询：支持 Hive/Presto 查询 Paimon 表
 *   <li>兼容性：保持与传统数仓工具的兼容
 * </ul>
 *
 * @see PartitionHandler
 * @see CommitCallback
 */
public class AddPartitionCommitCallback implements CommitCallback {

    /**
     * 分区缓存，避免重复处理同一分区。
     *
     * <p>缓存配置：
     * <ul>
     *   <li>过期策略：30分钟访问后过期
     *   <li>最大大小：300个条目
     *   <li>值类型：软引用（支持GC）
     * </ul>
     */
    private final Cache<BinaryRow, Boolean> cache =
            CacheBuilder.newBuilder()
                    // avoid extreme situations
                    .expireAfterAccess(Duration.ofMinutes(30))
                    // estimated cache size
                    .maximumSize(300)
                    .softValues()
                    .build();

    /** 分区处理器，用于在元数据存储中创建分区。 */
    private final PartitionHandler partitionHandler;

    /** 分区计算器，用于将二进制分区转换为分区值。 */
    private final InternalRowPartitionComputer partitionComputer;

    /**
     * 构造函数。
     *
     * @param partitionHandler 分区处理器
     * @param partitionComputer 分区计算器
     */
    public AddPartitionCommitCallback(
            PartitionHandler partitionHandler, InternalRowPartitionComputer partitionComputer) {
        this.partitionHandler = partitionHandler;
        this.partitionComputer = partitionComputer;
    }

    @Override
    public void call(
            List<SimpleFileEntry> baseFiles,
            List<ManifestEntry> deltaFiles,
            List<IndexManifestEntry> indexFiles,
            Snapshot snapshot) {
        Set<BinaryRow> partitions =
                deltaFiles.stream()
                        .filter(e -> FileKind.ADD.equals(e.kind()))
                        .map(ManifestEntry::partition)
                        .collect(Collectors.toSet());
        addPartitions(partitions);
    }

    @Override
    public void retry(ManifestCommittable committable) {
        Set<BinaryRow> partitions =
                committable.fileCommittables().stream()
                        .map(CommitMessage::partition)
                        .collect(Collectors.toSet());
        addPartitions(partitions);
    }

    private void addPartitions(Set<BinaryRow> partitions) {
        try {
            List<BinaryRow> newPartitions = new ArrayList<>();
            for (BinaryRow partition : partitions) {
                if (!cache.get(partition, () -> false)) {
                    newPartitions.add(partition);
                }
            }
            if (!newPartitions.isEmpty()) {
                partitionHandler.createPartitions(
                        newPartitions.stream()
                                .map(partitionComputer::generatePartValues)
                                .collect(Collectors.toList()));
                newPartitions.forEach(partition -> cache.put(partition, true));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        partitionHandler.close();
    }
}
