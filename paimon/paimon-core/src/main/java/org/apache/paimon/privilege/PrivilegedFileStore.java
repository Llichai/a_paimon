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

package org.apache.paimon.privilege;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
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
 * 带权限检查的 {@link FileStore} 实现。
 *
 * <p>这是一个装饰器模式的实现,在原始FileStore的基础上添加权限检查功能。
 * 所有涉及数据访问的操作都会在执行前进行权限验证。
 *
 * <h2>权限检查策略</h2>
 * <ul>
 *   <li><b>读操作</b> - 需要SELECT权限
 *     <ul>
 *       <li>newScan() - 创建扫描器</li>
 *       <li>newRead() - 创建读取器</li>
 *       <li>newServiceManager() - 创建服务管理器</li>
 *     </ul>
 *   </li>
 *   <li><b>写操作</b> - 需要INSERT权限
 *     <ul>
 *       <li>newWrite() - 创建写入器</li>
 *       <li>newCommit() - 创建提交器</li>
 *       <li>newSnapshotDeletion() - 删除快照</li>
 *       <li>newChangelogDeletion() - 删除变更日志</li>
 *       <li>newTagManager() - 管理标签</li>
 *       <li>newTagDeletion() - 删除标签</li>
 *       <li>newPartitionExpire() - 分区过期</li>
 *       <li>newTagAutoManager() - 自动标签管理</li>
 *       <li>mergeSchema() - 合并Schema</li>
 *     </ul>
 *   </li>
 *   <li><b>读写操作</b> - 需要SELECT或INSERT权限之一
 *     <ul>
 *       <li>snapshotManager() - 访问快照管理器</li>
 *       <li>changelogManager() - 访问变更日志管理器</li>
 *     </ul>
 *   </li>
 *   <li><b>无需权限</b> - 元数据查询
 *     <ul>
 *       <li>pathFactory() - 路径工厂</li>
 *       <li>partitionType() - 分区类型</li>
 *       <li>partitionComputer() - 分区计算器</li>
 *       <li>options() - 配置选项</li>
 *       <li>bucketMode() - 分桶模式</li>
 *       <li>各种工厂方法</li>
 *     </ul>
 *   </li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 获取带权限检查的FileStore
 * FileStore<?> fileStore = privilegedTable.store();
 *
 * // 读取数据 - 需要SELECT权限
 * FileStoreScan scan = fileStore.newScan();
 * SplitRead<?> read = fileStore.newRead();
 *
 * // 写入数据 - 需要INSERT权限
 * FileStoreWrite<?> write = fileStore.newWrite("user");
 * FileStoreCommit commit = fileStore.newCommit("user", table);
 * }</pre>
 *
 * <h2>异常处理</h2>
 * <ul>
 *   <li>{@link NoPrivilegeException} - 用户没有执行操作所需的权限</li>
 * </ul>
 *
 * @param <T> 记录类型
 * @see PrivilegedFileStoreTable
 * @see PrivilegeChecker
 */
public class PrivilegedFileStore<T> implements FileStore<T> {

    private final FileStore<T> wrapped;
    private final PrivilegeChecker privilegeChecker;
    private final Identifier identifier;

    /**
     * 构造带权限检查的FileStore。
     *
     * @param wrapped 被包装的原始FileStore
     * @param privilegeChecker 权限检查器
     * @param identifier 表标识符
     */
    public PrivilegedFileStore(
            FileStore<T> wrapped, PrivilegeChecker privilegeChecker, Identifier identifier) {
        this.wrapped = wrapped;
        this.privilegeChecker = privilegeChecker;
        this.identifier = identifier;
    }

    @Override
    public FileStorePathFactory pathFactory() {
        return wrapped.pathFactory();
    }

    @Override
    public SnapshotManager snapshotManager() {
        privilegeChecker.assertCanSelectOrInsert(identifier);
        return wrapped.snapshotManager();
    }

    @Override
    public ChangelogManager changelogManager() {
        privilegeChecker.assertCanSelectOrInsert(identifier);
        return wrapped.changelogManager();
    }

    @Override
    public RowType partitionType() {
        return wrapped.partitionType();
    }

    @Override
    public InternalRowPartitionComputer partitionComputer() {
        return wrapped.partitionComputer();
    }

    @Override
    public CoreOptions options() {
        return wrapped.options();
    }

    @Override
    public BucketMode bucketMode() {
        return wrapped.bucketMode();
    }

    @Override
    public FileStoreScan newScan() {
        privilegeChecker.assertCanSelect(identifier);
        return wrapped.newScan();
    }

    @Override
    public ManifestList.Factory manifestListFactory() {
        return wrapped.manifestListFactory();
    }

    @Override
    public ManifestFile.Factory manifestFileFactory() {
        return wrapped.manifestFileFactory();
    }

    @Override
    public IndexManifestFile.Factory indexManifestFileFactory() {
        return wrapped.indexManifestFileFactory();
    }

    @Override
    public IndexFileHandler newIndexFileHandler() {
        return wrapped.newIndexFileHandler();
    }

    @Override
    public StatsFileHandler newStatsFileHandler() {
        return wrapped.newStatsFileHandler();
    }

    @Override
    public SplitRead<T> newRead() {
        privilegeChecker.assertCanSelect(identifier);
        return wrapped.newRead();
    }

    @Override
    public FileStoreWrite<T> newWrite(String commitUser) {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newWrite(commitUser);
    }

    @Override
    public FileStoreWrite<T> newWrite(String commitUser, @Nullable Integer writeId) {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newWrite(commitUser, writeId);
    }

    @Override
    public FileStoreCommit newCommit(String commitUser, FileStoreTable table) {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newCommit(commitUser, table);
    }

    @Override
    public SnapshotDeletion newSnapshotDeletion() {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newSnapshotDeletion();
    }

    @Override
    public ChangelogDeletion newChangelogDeletion() {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newChangelogDeletion();
    }

    @Override
    public TagManager newTagManager() {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newTagManager();
    }

    @Override
    public TagDeletion newTagDeletion() {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newTagDeletion();
    }

    @Nullable
    @Override
    public PartitionExpire newPartitionExpire(String commitUser, FileStoreTable table) {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newPartitionExpire(commitUser, table);
    }

    @Override
    public PartitionExpire newPartitionExpire(
            String commitUser,
            FileStoreTable table,
            Duration expirationTime,
            Duration checkInterval,
            PartitionExpireStrategy expireStrategy) {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newPartitionExpire(
                commitUser, table, expirationTime, checkInterval, expireStrategy);
    }

    @Override
    public TagAutoManager newTagAutoManager(FileStoreTable table) {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newTagAutoManager(table);
    }

    @Override
    public ServiceManager newServiceManager() {
        privilegeChecker.assertCanSelect(identifier);
        return wrapped.newServiceManager();
    }

    @Override
    public boolean mergeSchema(RowType rowType, boolean allowExplicitCast) {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.mergeSchema(rowType, allowExplicitCast);
    }

    @Override
    public List<TagCallback> createTagCallbacks(FileStoreTable table) {
        return wrapped.createTagCallbacks(table);
    }

    @Override
    public void setManifestCache(SegmentsCache<Path> manifestCache) {
        wrapped.setManifestCache(manifestCache);
    }

    @Override
    public void setSnapshotCache(Cache<Path, Snapshot> cache) {
        wrapped.setSnapshotCache(cache);
    }

    @Override
    public GlobalIndexScanBuilder newGlobalIndexScanBuilder() {
        return wrapped.newGlobalIndexScanBuilder();
    }
}
