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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.sink.RowKeyExtractor;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.sink.WriteSelector;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.tag.TagAutoManager;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.DVMetaCache;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.SimpleFileReader;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * 委托模式 FileStoreTable 实现 - 通过组合方式扩展 FileStoreTable
 *
 * <p>DelegatedFileStoreTable 是一个抽象基类，使用<b>委托模式（Delegation Pattern）</b>
 * 包装了一个内部的 FileStoreTable 实例（wrapped），并将所有方法调用转发给它。
 *
 * <p><b>为什么需要委托模式？</b>
 * <ul>
 *   <li><b>避免继承层次过深</b>：通过组合而非继承扩展功能
 *   <li><b>选择性覆盖</b>：子类可以只覆盖需要修改的方法，其他方法自动转发
 *   <li><b>多重装饰</b>：可以层层包装，添加多种行为（如缓存、日志、权限检查）
 *   <li><b>运行时灵活性</b>：可以在运行时动态替换被委托的对象
 * </ul>
 *
 * <p><b>委托的所有方法（>40个）：</b>
 * <ul>
 *   <li><b>元数据访问</b>：name、fullName、uuid、schema、location、fileIO
 *   <li><b>管理器</b>：snapshotManager、changelogManager、schemaManager、consumerManager、tagManager、branchManager
 *   <li><b>快照操作</b>：latestSnapshot、snapshot、rollbackTo
 *   <li><b>Tag 操作</b>：createTag、deleteTag、renameTag、replaceTag、rollbackTo(tagName)
 *   <li><b>Branch 操作</b>：createBranch、deleteBranch、fastForward
 *   <li><b>过期操作</b>：newExpireSnapshots、newExpireChangelog
 *   <li><b>读写操作</b>：newScan、newStreamScan、newRead、newWrite、newCommit
 *   <li><b>缓存设置</b>：setManifestCache、setSnapshotCache、setStatsCache、setDVMetaCache
 *   <li><b>其他</b>：store、coreOptions、manifestListReader、manifestFileReader、indexManifestFileReader
 * </ul>
 *
 * <p><b>典型子类及其覆盖的方法：</b>
 * <ul>
 *   <li><b>{@link FallbackReadFileStoreTable}</b>：覆盖 newScan、newRead、copy 等方法，实现主分支/回退分支切换逻辑
 *   <li><b>{@link ChainGroupReadTable}</b>：覆盖 newScan、newRead 方法，实现链式分组读取
 *   <li><b>SystemTable</b>：覆盖 newScan、newRead 方法，实现系统表查询
 * </ul>
 *
 * <p><b>装饰器模式与委托模式的区别：</b>
 * <ul>
 *   <li><b>装饰器模式</b>：在原有功能基础上<b>增强</b>行为（before/after logic）
 *   <li><b>委托模式</b>：完全<b>替换</b>行为或仅转发调用
 *   <li>DelegatedFileStoreTable 更接近<b>代理模式</b>（Proxy Pattern）
 * </ul>
 *
 * <p><b>使用示例：</b>
 * <pre>{@code
 * // 1. 创建一个添加日志的委托表
 * public class LoggingFileStoreTable extends DelegatedFileStoreTable {
 *     public LoggingFileStoreTable(FileStoreTable wrapped) {
 *         super(wrapped);
 *     }
 *
 *     @Override
 *     public DataTableScan newScan() {
 *         LOG.info("Creating new scan for table: {}", wrapped.name());
 *         return wrapped.newScan(); // 转发调用
 *     }
 *
 *     @Override
 *     public InnerTableRead newRead() {
 *         LOG.info("Creating new read for table: {}", wrapped.name());
 *         return wrapped.newRead();
 *     }
 *
 *     // 其他方法自动继承默认的转发行为
 * }
 *
 * // 2. 使用
 * FileStoreTable baseTable = ...;
 * FileStoreTable loggingTable = new LoggingFileStoreTable(baseTable);
 * loggingTable.newScan(); // 会打印日志，然后调用 baseTable.newScan()
 * }</pre>
 *
 * <p><b>注意事项：</b>
 * <ul>
 *   <li>equals 方法基于 wrapped 对象比较，确保语义正确
 *   <li>所有方法都是直接转发，子类可以选择性覆盖
 *   <li>不要创建循环委托（A 委托 B，B 又委托 A）
 * </ul>
 *
 * @see FallbackReadFileStoreTable
 * @see ChainGroupReadTable
 */
public abstract class DelegatedFileStoreTable implements FileStoreTable {

    /** 被委托的内部 FileStoreTable 实例 */
    protected final FileStoreTable wrapped;

    /**
     * 构造委托表
     *
     * @param wrapped 被委托的 FileStoreTable 实例
     */
    public DelegatedFileStoreTable(FileStoreTable wrapped) {
        this.wrapped = wrapped;
    }

    /**
     * 获取被委托的表
     *
     * @return 内部的 FileStoreTable 实例
     */
    public FileStoreTable wrapped() {
        return wrapped;
    }

    @Override
    public String name() {
        return wrapped.name();
    }

    @Override
    public String fullName() {
        return wrapped.fullName();
    }

    @Override
    public String uuid() {
        return wrapped.uuid();
    }

    @Override
    public SnapshotReader newSnapshotReader() {
        return wrapped.newSnapshotReader();
    }

    @Override
    public CoreOptions coreOptions() {
        return wrapped.coreOptions();
    }

    @Override
    public SnapshotManager snapshotManager() {
        return wrapped.snapshotManager();
    }

    @Override
    public ChangelogManager changelogManager() {
        return wrapped.changelogManager();
    }

    @Override
    public SchemaManager schemaManager() {
        return wrapped.schemaManager();
    }

    @Override
    public ConsumerManager consumerManager() {
        return wrapped.consumerManager();
    }

    @Override
    public TagManager tagManager() {
        return wrapped.tagManager();
    }

    @Override
    public BranchManager branchManager() {
        return wrapped.branchManager();
    }

    @Override
    public Path location() {
        return wrapped.location();
    }

    @Override
    public FileIO fileIO() {
        return wrapped.fileIO();
    }

    @Override
    public void setManifestCache(SegmentsCache<Path> manifestCache) {
        wrapped.setManifestCache(manifestCache);
    }

    @Nullable
    @Override
    public SegmentsCache<Path> getManifestCache() {
        return wrapped.getManifestCache();
    }

    @Override
    public void setSnapshotCache(Cache<Path, Snapshot> cache) {
        wrapped.setSnapshotCache(cache);
    }

    @Override
    public void setStatsCache(Cache<String, Statistics> cache) {
        wrapped.setStatsCache(cache);
    }

    @Override
    public void setDVMetaCache(DVMetaCache cache) {
        wrapped.setDVMetaCache(cache);
    }

    @Override
    public TableSchema schema() {
        return wrapped.schema();
    }

    @Override
    public FileStore<?> store() {
        return wrapped.store();
    }

    @Override
    public CatalogEnvironment catalogEnvironment() {
        return wrapped.catalogEnvironment();
    }

    @Override
    public Optional<Statistics> statistics() {
        return wrapped.statistics();
    }

    @Override
    public Optional<Snapshot> latestSnapshot() {
        return wrapped.latestSnapshot();
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        return wrapped.snapshot(snapshotId);
    }

    @Override
    public SimpleFileReader<ManifestFileMeta> manifestListReader() {
        return wrapped.manifestListReader();
    }

    @Override
    public SimpleFileReader<ManifestEntry> manifestFileReader() {
        return wrapped.manifestFileReader();
    }

    @Override
    public SimpleFileReader<IndexManifestEntry> indexManifestFileReader() {
        return wrapped.indexManifestFileReader();
    }

    @Override
    public void rollbackTo(long snapshotId) {
        wrapped.rollbackTo(snapshotId);
    }

    @Override
    public void createTag(String tagName) {
        wrapped.createTag(tagName);
    }

    @Override
    public void createTag(String tagName, long fromSnapshotId) {
        wrapped.createTag(tagName, fromSnapshotId);
    }

    @Override
    public void createTag(String tagName, Duration timeRetained) {
        wrapped.createTag(tagName, timeRetained);
    }

    @Override
    public void createTag(String tagName, long fromSnapshotId, Duration timeRetained) {
        wrapped.createTag(tagName, fromSnapshotId, timeRetained);
    }

    @Override
    public TagAutoManager newTagAutoManager() {
        return wrapped.newTagAutoManager();
    }

    @Override
    public void renameTag(String tagName, String targetTagName) {
        wrapped.renameTag(tagName, targetTagName);
    }

    @Override
    public void replaceTag(String tagName, Long fromSnapshotId, Duration timeRetained) {
        wrapped.replaceTag(tagName, fromSnapshotId, timeRetained);
    }

    @Override
    public void deleteTag(String tagName) {
        wrapped.deleteTag(tagName);
    }

    @Override
    public void rollbackTo(String tagName) {
        wrapped.rollbackTo(tagName);
    }

    @Override
    public void createBranch(String branchName) {
        wrapped.createBranch(branchName);
    }

    @Override
    public void createBranch(String branchName, String tagName) {
        wrapped.createBranch(branchName, tagName);
    }

    @Override
    public void deleteBranch(String branchName) {
        wrapped.deleteBranch(branchName);
    }

    @Override
    public void fastForward(String branchName) {
        wrapped.fastForward(branchName);
    }

    @Override
    public ExpireSnapshots newExpireSnapshots() {
        return wrapped.newExpireSnapshots();
    }

    @Override
    public ExpireSnapshots newExpireChangelog() {
        return wrapped.newExpireChangelog();
    }

    @Override
    public DataTableScan newScan() {
        return wrapped.newScan();
    }

    @Override
    public StreamDataTableScan newStreamScan() {
        return wrapped.newStreamScan();
    }

    @Override
    public InnerTableRead newRead() {
        return wrapped.newRead();
    }

    @Override
    public Optional<WriteSelector> newWriteSelector() {
        return wrapped.newWriteSelector();
    }

    @Override
    public TableWriteImpl<?> newWrite(String commitUser) {
        return wrapped.newWrite(commitUser);
    }

    @Override
    public TableWriteImpl<?> newWrite(String commitUser, @Nullable Integer writeId) {
        return wrapped.newWrite(commitUser, writeId);
    }

    @Override
    public TableCommitImpl newCommit(String commitUser) {
        return wrapped.newCommit(commitUser);
    }

    @Override
    public LocalTableQuery newLocalTableQuery() {
        return wrapped.newLocalTableQuery();
    }

    @Override
    public boolean supportStreamingReadOverwrite() {
        return wrapped.supportStreamingReadOverwrite();
    }

    @Override
    public RowKeyExtractor createRowKeyExtractor() {
        return wrapped.createRowKeyExtractor();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DelegatedFileStoreTable that = (DelegatedFileStoreTable) o;
        return Objects.equals(wrapped, that.wrapped);
    }
}
