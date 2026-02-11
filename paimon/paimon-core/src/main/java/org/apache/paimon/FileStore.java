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

package org.apache.paimon;

import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.GlobalIndexScanBuilder;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.ChangelogDeletion;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.FileStoreWrite;
import org.apache.paimon.operation.PartitionExpire;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.operation.TagDeletion;
import org.apache.paimon.partition.PartitionExpireStrategy;
import org.apache.paimon.service.ServiceManager;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.tag.TagAutoManager;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;

/**
 * 文件存储接口
 *
 * <p>FileStore 是 Paimon 存储层的核心抽象，定义了数据读写、扫描、提交等核心操作。
 *
 * <p>架构层次：
 * <ul>
 *   <li>FileStore（本接口）：顶层抽象
 *   <li>AbstractFileStore：通用实现基类
 *   <li>KeyValueFileStore：Primary Key 表实现（支持 MergeTree）
 *   <li>AppendOnlyFileStore：Append-Only 表实现
 * </ul>
 *
 * <p>核心功能模块：
 * <ul>
 *   <li>扫描：{@link #newScan()} - 创建文件扫描器
 *   <li>读取：{@link #newRead()} - 创建数据读取器
 *   <li>写入：{@link #newWrite(String)} - 创建数据写入器
 *   <li>提交：{@link #newCommit(String, FileStoreTable)} - 创建提交器
 *   <li>删除：{@link #newSnapshotDeletion()} - 创建快照删除器
 * </ul>
 *
 * <p>实现类负责：
 * <ul>
 *   <li>管理 Manifest 文件、Snapshot 文件和 Changelog 文件
 *   <li>提供数据的增删改查接口
 *   <li>支持 Schema 演化
 *   <li>支持分区过期和标签管理
 * </ul>
 *
 * @param <T> 读写记录的类型（KeyValue 或 InternalRow）
 */
public interface FileStore<T> {

    /**
     * 获取路径工厂，用于生成数据文件、Manifest 文件等的路径
     *
     * @return 路径工厂实例
     */
    FileStorePathFactory pathFactory();

    /**
     * 获取快照管理器，用于读取和写入快照文件
     *
     * @return 快照管理器实例
     */
    SnapshotManager snapshotManager();

    /**
     * 获取 Changelog 管理器，用于读取和写入 Changelog 文件
     *
     * @return Changelog 管理器实例
     */
    ChangelogManager changelogManager();

    /**
     * 获取分区类型
     *
     * @return 分区字段的行类型
     */
    RowType partitionType();

    /**
     * 获取分区计算器，用于计算记录的分区路径
     *
     * @return 分区计算器实例
     */
    InternalRowPartitionComputer partitionComputer();

    /**
     * 获取表的配置选项
     *
     * @return 核心配置选项
     */
    CoreOptions options();

    /**
     * 获取分桶模式
     *
     * <p>可能的值：
     * <ul>
     *   <li>HASH_FIXED：固定哈希分桶（bucket >= 0）
     *   <li>HASH_DYNAMIC：动态哈希分桶（bucket = -1，无跨分区更新）
     *   <li>KEY_DYNAMIC：主键动态分桶（bucket = -1，跨分区更新）
     *   <li>BUCKET_UNAWARE：无分桶（仅 Append-Only 表）
     *   <li>POSTPONE_MODE：延迟分桶模式（bucket = -2）
     * </ul>
     *
     * @return 分桶模式
     */
    BucketMode bucketMode();

    /**
     * 创建文件扫描器，用于扫描表中的数据文件
     *
     * <p>扫描器负责：
     * <ul>
     *   <li>读取 Manifest 文件
     *   <li>应用谓词过滤
     *   <li>生成数据分片
     * </ul>
     *
     * @return 文件扫描器实例
     */
    FileStoreScan newScan();

    /**
     * 获取 Manifest List 工厂
     *
     * @return Manifest List 工厂实例
     */
    ManifestList.Factory manifestListFactory();

    /**
     * 获取 Manifest File 工厂
     *
     * @return Manifest File 工厂实例
     */
    ManifestFile.Factory manifestFileFactory();

    /**
     * 获取索引 Manifest 工厂
     *
     * @return 索引 Manifest 工厂实例
     */
    IndexManifestFile.Factory indexManifestFileFactory();

    /**
     * 创建索引文件处理器，用于管理删除向量等索引文件
     *
     * @return 索引文件处理器实例
     */
    IndexFileHandler newIndexFileHandler();

    /**
     * 创建统计文件处理器，用于管理统计信息文件
     *
     * @return 统计文件处理器实例
     */
    StatsFileHandler newStatsFileHandler();

    /**
     * 创建数据读取器，用于读取数据文件
     *
     * <p>读取器负责：
     * <ul>
     *   <li>从数据文件读取记录
     *   <li>执行 MergeTree 合并（KeyValue 表）
     *   <li>应用投影和过滤
     * </ul>
     *
     * @return 数据读取器实例
     */
    SplitRead<T> newRead();

    /**
     * 创建数据写入器（使用默认 writeId）
     *
     * @param commitUser 提交用户标识
     * @return 数据写入器实例
     */
    FileStoreWrite<T> newWrite(String commitUser);

    /**
     * 创建数据写入器（指定 writeId）
     *
     * <p>writeId 用于标识不同的写入实例，在分布式写入时区分不同 Writer。
     *
     * @param commitUser 提交用户标识
     * @param writeId 写入器 ID（可选）
     * @return 数据写入器实例
     */
    FileStoreWrite<T> newWrite(String commitUser, @Nullable Integer writeId);

    /**
     * 创建提交器，用于将写入的数据提交到表
     *
     * <p>提交器负责：
     * <ul>
     *   <li>生成新的快照
     *   <li>更新 Manifest 文件
     *   <li>执行冲突检测
     *   <li>触发回调（如 Metastore 同步）
     * </ul>
     *
     * @param commitUser 提交用户标识
     * @param table 表实例
     * @return 提交器实例
     */
    FileStoreCommit newCommit(String commitUser, FileStoreTable table);

    /**
     * 创建快照删除器，用于删除过期快照及其关联的数据文件
     *
     * @return 快照删除器实例
     */
    SnapshotDeletion newSnapshotDeletion();

    /**
     * 创建 Changelog 删除器，用于删除过期的 Changelog 文件
     *
     * @return Changelog 删除器实例
     */
    ChangelogDeletion newChangelogDeletion();

    /**
     * 创建标签管理器，用于创建和管理快照标签
     *
     * @return 标签管理器实例
     */
    TagManager newTagManager();

    /**
     * 创建标签删除器，用于删除标签及其关联的数据文件
     *
     * @return 标签删除器实例
     */
    TagDeletion newTagDeletion();

    /**
     * 创建分区过期器（使用默认配置）
     *
     * <p>如果表没有分区字段或未配置过期时间，返回 null。
     *
     * @param commitUser 提交用户标识
     * @param table 表实例
     * @return 分区过期器实例（可能为 null）
     */
    @Nullable
    PartitionExpire newPartitionExpire(String commitUser, FileStoreTable table);

    /**
     * 创建分区过期器（指定配置）
     *
     * @param commitUser 提交用户标识
     * @param table 表实例
     * @param expirationTime 分区过期时间
     * @param checkInterval 检查间隔
     * @param expireStrategy 过期策略
     * @return 分区过期器实例（可能为 null）
     */
    @Nullable
    PartitionExpire newPartitionExpire(
            String commitUser,
            FileStoreTable table,
            Duration expirationTime,
            Duration checkInterval,
            PartitionExpireStrategy expireStrategy);

    /**
     * 创建标签自动管理器，用于自动创建和删除标签
     *
     * @param table 表实例
     * @return 标签自动管理器实例
     */
    TagAutoManager newTagAutoManager(FileStoreTable table);

    /**
     * 创建服务管理器，用于管理后台服务
     *
     * @return 服务管理器实例
     */
    ServiceManager newServiceManager();

    /**
     * 合并 Schema，支持添加新字段
     *
     * @param rowType 新的行类型
     * @param allowExplicitCast 是否允许显式类型转换
     * @return 是否成功合并
     */
    boolean mergeSchema(RowType rowType, boolean allowExplicitCast);

    /**
     * 创建标签回调，在标签创建时触发（如同步到 Metastore）
     *
     * @param table 表实例
     * @return 标签回调列表
     */
    List<TagCallback> createTagCallbacks(FileStoreTable table);

    /**
     * 设置 Manifest 缓存，用于加速 Manifest 文件的读取
     *
     * @param manifestCache Manifest 缓存实例
     */
    void setManifestCache(SegmentsCache<Path> manifestCache);

    /**
     * 设置快照缓存，用于加速快照文件的读取
     *
     * @param cache 快照缓存实例
     */
    void setSnapshotCache(Cache<Path, Snapshot> cache);

    /**
     * 创建全局索引扫描构建器，用于扫描全局索引
     *
     * @return 全局索引扫描构建器实例
     */
    GlobalIndexScanBuilder newGlobalIndexScanBuilder();
}
