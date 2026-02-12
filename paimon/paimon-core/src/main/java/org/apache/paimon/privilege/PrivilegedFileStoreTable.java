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

import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.DelegatedFileStoreTable;
import org.apache.paimon.table.ExpireSnapshots;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.query.LocalTableQuery;
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
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 带权限检查的 {@link FileStoreTable} 实现。
 *
 * <p>这是一个装饰器模式的实现,在原始FileStoreTable的基础上添加权限检查功能。
 * 所有涉及表操作的方法都会在执行前进行权限验证。
 *
 * <h2>权限检查策略</h2>
 * <ul>
 *   <li><b>查询操作</b> - 需要SELECT权限
 *     <ul>
 *       <li>newScan() - 创建批量扫描</li>
 *       <li>newStreamScan() - 创建流式扫描</li>
 *       <li>newRead() - 创建读取器</li>
 *       <li>newSnapshotReader() - 创建快照读取器</li>
 *       <li>statistics() - 获取统计信息</li>
 *       <li>newLocalTableQuery() - 创建本地查询</li>
 *       <li>branchManager() - 访问分支管理器(需要同时有INSERT权限)</li>
 *     </ul>
 *   </li>
 *   <li><b>写入操作</b> - 需要INSERT权限
 *     <ul>
 *       <li>newWrite() - 创建写入器</li>
 *       <li>newCommit() - 创建提交器</li>
 *       <li>newWriteSelector() - 创建写入选择器</li>
 *       <li>rollbackTo() - 回滚到指定快照或标签</li>
 *       <li>createTag() - 创建标签</li>
 *       <li>deleteTag() - 删除标签</li>
 *       <li>renameTag() - 重命名标签</li>
 *       <li>createBranch() - 创建分支</li>
 *       <li>deleteBranch() - 删除分支</li>
 *       <li>fastForward() - 快进分支</li>
 *       <li>newExpireSnapshots() - 过期快照</li>
 *       <li>newExpireChangelog() - 过期变更日志</li>
 *       <li>tagManager() - 访问标签管理器</li>
 *       <li>newTagAutoManager() - 创建自动标签管理器</li>
 *     </ul>
 *   </li>
 *   <li><b>读写操作</b> - 需要SELECT或INSERT权限之一
 *     <ul>
 *       <li>snapshotManager() - 访问快照管理器</li>
 *       <li>changelogManager() - 访问变更日志管理器</li>
 *       <li>latestSnapshot() - 获取最新快照</li>
 *       <li>snapshot() - 获取指定快照</li>
 *     </ul>
 *   </li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 从PrivilegedCatalog获取表(自动包装为PrivilegedFileStoreTable)
 * Table table = catalog.getTable(Identifier.create("db", "table"));
 *
 * // 读取数据 - 需要SELECT权限
 * DataTableScan scan = ((FileStoreTable) table).newScan();
 * InnerTableRead read = ((FileStoreTable) table).newRead();
 *
 * // 写入数据 - 需要INSERT权限
 * TableWriteImpl<?> write = ((FileStoreTable) table).newWrite("user");
 * TableCommitImpl commit = ((FileStoreTable) table).newCommit("user");
 *
 * // 标签管理 - 需要INSERT权限
 * ((FileStoreTable) table).createTag("tag1");
 * ((FileStoreTable) table).deleteTag("tag1");
 * }</pre>
 *
 * <h2>表复制与权限</h2>
 * <p>所有的copy方法都会返回新的PrivilegedFileStoreTable实例,保留相同的权限检查器:
 * <ul>
 *   <li>{@link #copy(Map)} - 使用动态选项复制</li>
 *   <li>{@link #copy(TableSchema)} - 使用新Schema复制</li>
 *   <li>{@link #copyWithoutTimeTravel(Map)} - 复制但不带时间旅行</li>
 *   <li>{@link #copyWithLatestSchema()} - 使用最新Schema复制</li>
 *   <li>{@link #switchToBranch(String)} - 切换到指定分支</li>
 * </ul>
 *
 * <h2>与FileStore的集成</h2>
 * <p>{@link #store()} 方法返回 {@link PrivilegedFileStore},确保通过FileStore进行的
 * 底层操作也会受到权限检查的保护。
 *
 * <h2>异常处理</h2>
 * <ul>
 *   <li>{@link NoPrivilegeException} - 用户没有执行操作所需的权限</li>
 * </ul>
 *
 * @see PrivilegedCatalog
 * @see PrivilegedFileStore
 * @see PrivilegeChecker
 */
public class PrivilegedFileStoreTable extends DelegatedFileStoreTable {

    protected final PrivilegeChecker privilegeChecker;
    protected final Identifier identifier;

    /**
     * 构造带权限检查的FileStoreTable。
     *
     * @param wrapped 被包装的原始FileStoreTable
     * @param privilegeChecker 权限检查器
     * @param identifier 表标识符
     */
    protected PrivilegedFileStoreTable(
            FileStoreTable wrapped, PrivilegeChecker privilegeChecker, Identifier identifier) {
        super(wrapped);
        this.privilegeChecker = privilegeChecker;
        this.identifier = identifier;
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
    public Optional<Snapshot> latestSnapshot() {
        privilegeChecker.assertCanSelectOrInsert(identifier);
        return wrapped.latestSnapshot();
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        privilegeChecker.assertCanSelectOrInsert(identifier);
        return wrapped.snapshot(snapshotId);
    }

    @Override
    public SnapshotReader newSnapshotReader() {
        privilegeChecker.assertCanSelect(identifier);
        return wrapped.newSnapshotReader();
    }

    @Override
    public TagManager tagManager() {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.tagManager();
    }

    @Override
    public BranchManager branchManager() {
        privilegeChecker.assertCanSelect(identifier);
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.branchManager();
    }

    @Override
    public FileStore<?> store() {
        return new PrivilegedFileStore<>(wrapped.store(), privilegeChecker, identifier);
    }

    @Override
    public Optional<Statistics> statistics() {
        privilegeChecker.assertCanSelect(identifier);
        return wrapped.statistics();
    }

    @Override
    public void rollbackTo(long snapshotId) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.rollbackTo(snapshotId);
    }

    @Override
    public void createTag(String tagName) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.createTag(tagName);
    }

    @Override
    public void createTag(String tagName, long fromSnapshotId) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.createTag(tagName, fromSnapshotId);
    }

    @Override
    public void createTag(String tagName, Duration timeRetained) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.createTag(tagName, timeRetained);
    }

    @Override
    public void renameTag(String tagName, String targetTagName) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.renameTag(tagName, targetTagName);
    }

    @Override
    public void createTag(String tagName, long fromSnapshotId, Duration timeRetained) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.createTag(tagName, fromSnapshotId, timeRetained);
    }

    @Override
    public TagAutoManager newTagAutoManager() {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newTagAutoManager();
    }

    @Override
    public void deleteTag(String tagName) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.deleteTag(tagName);
    }

    @Override
    public void rollbackTo(String tagName) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.rollbackTo(tagName);
    }

    @Override
    public void createBranch(String branchName) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.createBranch(branchName);
    }

    @Override
    public void createBranch(String branchName, String tagName) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.createBranch(branchName, tagName);
    }

    @Override
    public void deleteBranch(String branchName) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.deleteBranch(branchName);
    }

    @Override
    public void fastForward(String branchName) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.fastForward(branchName);
    }

    @Override
    public ExpireSnapshots newExpireSnapshots() {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newExpireSnapshots();
    }

    @Override
    public ExpireSnapshots newExpireChangelog() {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newExpireChangelog();
    }

    @Override
    public DataTableScan newScan() {
        privilegeChecker.assertCanSelect(identifier);
        return wrapped.newScan();
    }

    @Override
    public StreamDataTableScan newStreamScan() {
        privilegeChecker.assertCanSelect(identifier);
        return wrapped.newStreamScan();
    }

    @Override
    public InnerTableRead newRead() {
        privilegeChecker.assertCanSelect(identifier);
        return wrapped.newRead();
    }

    @Override
    public Optional<WriteSelector> newWriteSelector() {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newWriteSelector();
    }

    @Override
    public TableWriteImpl<?> newWrite(String commitUser) {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newWrite(commitUser);
    }

    @Override
    public TableWriteImpl<?> newWrite(String commitUser, @Nullable Integer writeId) {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newWrite(commitUser, writeId);
    }

    @Override
    public TableCommitImpl newCommit(String commitUser) {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newCommit(commitUser);
    }

    @Override
    public LocalTableQuery newLocalTableQuery() {
        privilegeChecker.assertCanSelect(identifier);
        return wrapped.newLocalTableQuery();
    }

    // ======================= equals ============================

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrivilegedFileStoreTable that = (PrivilegedFileStoreTable) o;
        return Objects.equals(wrapped, that.wrapped)
                && Objects.equals(privilegeChecker, that.privilegeChecker)
                && Objects.equals(identifier, that.identifier);
    }

    // ======================= copy ============================

    @Override
    public PrivilegedFileStoreTable copy(Map<String, String> dynamicOptions) {
        return new PrivilegedFileStoreTable(
                wrapped.copy(dynamicOptions), privilegeChecker, identifier);
    }

    @Override
    public PrivilegedFileStoreTable copy(TableSchema newTableSchema) {
        return new PrivilegedFileStoreTable(
                wrapped.copy(newTableSchema), privilegeChecker, identifier);
    }

    @Override
    public PrivilegedFileStoreTable copyWithoutTimeTravel(Map<String, String> dynamicOptions) {
        return new PrivilegedFileStoreTable(
                wrapped.copyWithoutTimeTravel(dynamicOptions), privilegeChecker, identifier);
    }

    @Override
    public PrivilegedFileStoreTable copyWithLatestSchema() {
        return new PrivilegedFileStoreTable(
                wrapped.copyWithLatestSchema(), privilegeChecker, identifier);
    }

    @Override
    public PrivilegedFileStoreTable switchToBranch(String branchName) {
        return new PrivilegedFileStoreTable(
                wrapped.switchToBranch(branchName), privilegeChecker, identifier);
    }

    /**
     * 包装FileStoreTable为PrivilegedFileStoreTable。
     *
     * @param table 原始FileStoreTable
     * @param privilegeChecker 权限检查器
     * @param identifier 表标识符
     * @return 带权限检查的FileStoreTable
     */
    public static PrivilegedFileStoreTable wrap(
            FileStoreTable table, PrivilegeChecker privilegeChecker, Identifier identifier) {
        return new PrivilegedFileStoreTable(table, privilegeChecker, identifier);
    }
}
