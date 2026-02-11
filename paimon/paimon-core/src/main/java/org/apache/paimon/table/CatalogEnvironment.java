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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;
import org.apache.paimon.catalog.CatalogSnapshotCommit;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.RenamingSnapshotCommit;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.catalog.TableRollback;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.table.source.TableQueryAuth;
import org.apache.paimon.tag.SnapshotLoaderImpl;
import org.apache.paimon.utils.SnapshotLoader;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Optional;

/**
 * 表中的 Catalog 环境，包含锁工厂、Metastore 客户端工厂等。
 *
 * <p>CatalogEnvironment 封装了表与 Catalog 之间的交互所需的环境信息：
 * <ul>
 *   <li>表标识符（Identifier）：数据库名、表名、分支名等
 *   <li>UUID：表的全局唯一标识
 *   <li>Catalog Loader：用于加载 Catalog 实例
 *   <li>锁工厂（Lock Factory）：用于创建分布式锁
 *   <li>锁上下文（Lock Context）：锁的配置信息
 *   <li>Catalog Context：Catalog 的配置上下文
 *   <li>版本管理支持：是否支持 Catalog 级别的版本管理
 * </ul>
 *
 * <h3>主要功能</h3>
 * <ul>
 *   <li>分区处理：创建 PartitionHandler，用于管理 Catalog 中的分区
 *   <li>快照提交：根据版本管理支持情况创建不同的 SnapshotCommit
 *   <li>表回滚：支持 Catalog 级别的表回滚
 *   <li>快照加载：创建 SnapshotLoader，从 Catalog 加载快照
 *   <li>查询授权：创建 TableQueryAuth，用于表查询的授权检查
 * </ul>
 *
 * <h3>版本管理支持</h3>
 * <p>如果 Catalog 支持版本管理（supportsVersionManagement=true）：
 * <ul>
 *   <li>使用 CatalogSnapshotCommit：快照提交委托给 Catalog
 *   <li>使用 Catalog 的回滚功能：支持跨分支的回滚
 *   <li>不需要文件系统级别的锁：由 Catalog 保证事务性
 * </ul>
 *
 * <p>如果 Catalog 不支持版本管理：
 * <ul>
 *   <li>使用 RenamingSnapshotCommit：基于文件系统的快照提交
 *   <li>使用文件系统锁：通过 CatalogLockFactory 创建
 *   <li>不支持 Catalog 级别的回滚
 * </ul>
 *
 * @see Identifier 表标识符
 * @see CatalogLoader Catalog 加载器
 * @see CatalogLockFactory Catalog 锁工厂
 */
public class CatalogEnvironment implements Serializable {

    private static final long serialVersionUID = 2L;

    /** 表的标识符，可能为 null（不通过 Catalog 创建的表）。 */
    @Nullable private final Identifier identifier;

    /** 表的 UUID，可能为 null（使用默认 UUID）。 */
    @Nullable private final String uuid;

    /** Catalog 加载器，用于重新加载 Catalog 实例。 */
    @Nullable private final CatalogLoader catalogLoader;

    /** Catalog 锁工厂，用于创建分布式锁。 */
    @Nullable private final CatalogLockFactory lockFactory;

    /** Catalog 锁上下文，包含锁的配置信息。 */
    @Nullable private final CatalogLockContext lockContext;

    /** Catalog 上下文，包含 Catalog 的配置信息。 */
    @Nullable private final CatalogContext catalogContext;

    /** 是否支持 Catalog 级别的版本管理。 */
    private final boolean supportsVersionManagement;

    /**
     * 构造 CatalogEnvironment。
     *
     * @param identifier 表标识符
     * @param uuid 表 UUID
     * @param catalogLoader Catalog 加载器
     * @param lockFactory 锁工厂
     * @param lockContext 锁上下文
     * @param catalogContext Catalog 上下文
     * @param supportsVersionManagement 是否支持版本管理
     */
    public CatalogEnvironment(
            @Nullable Identifier identifier,
            @Nullable String uuid,
            @Nullable CatalogLoader catalogLoader,
            @Nullable CatalogLockFactory lockFactory,
            @Nullable CatalogLockContext lockContext,
            @Nullable CatalogContext catalogContext,
            boolean supportsVersionManagement) {
        this.identifier = identifier;
        this.uuid = uuid;
        this.catalogLoader = catalogLoader;
        this.lockFactory = lockFactory;
        this.lockContext = lockContext;
        this.catalogContext = catalogContext;
        this.supportsVersionManagement = supportsVersionManagement;
    }

    /**
     * 创建空的 CatalogEnvironment。
     *
     * <p>空环境用于不通过 Catalog 创建的表，所有字段都为 null。
     *
     * @return 空的 CatalogEnvironment 实例
     */
    public static CatalogEnvironment empty() {
        return new CatalogEnvironment(null, null, null, null, null, null, false);
    }

    /**
     * 返回表的标识符。
     *
     * @return Identifier，可能为 null
     */
    @Nullable
    public Identifier identifier() {
        return identifier;
    }

    /**
     * 返回表的 UUID。
     *
     * @return UUID 字符串，可能为 null
     */
    @Nullable
    public String uuid() {
        return uuid;
    }

    /**
     * 创建分区处理器。
     *
     * <p>PartitionHandler 用于管理 Catalog 中的分区元数据，支持：
     * <ul>
     *   <li>添加分区
     *   <li>删除分区
     *   <li>列举分区
     * </ul>
     *
     * @return PartitionHandler 实例，如果没有 CatalogLoader 则返回 null
     */
    @Nullable
    public PartitionHandler partitionHandler() {
        if (catalogLoader == null) {
            return null;
        }
        Catalog catalog = catalogLoader.load();
        return PartitionHandler.create(catalog, identifier);
    }

    /**
     * 返回是否支持版本管理。
     *
     * @return true 如果 Catalog 支持版本管理
     */
    public boolean supportsVersionManagement() {
        return supportsVersionManagement;
    }

    /**
     * 创建快照提交器。
     *
     * <p>根据版本管理支持情况返回不同的 SnapshotCommit 实现：
     * <ul>
     *   <li>支持版本管理：使用 CatalogSnapshotCommit（委托给 Catalog）
     *   <li>不支持版本管理：使用 RenamingSnapshotCommit（基于文件系统）
     * </ul>
     *
     * @param snapshotManager 快照管理器
     * @return SnapshotCommit 实例，可能为 null
     */
    @Nullable
    public SnapshotCommit snapshotCommit(SnapshotManager snapshotManager) {
        SnapshotCommit snapshotCommit;
        if (catalogLoader != null && supportsVersionManagement) {
            // 使用 Catalog 提交
            snapshotCommit = new CatalogSnapshotCommit(catalogLoader.load(), identifier, uuid);
        } else {
            // 使用文件系统提交 + 锁
            Lock lock =
                    Optional.ofNullable(lockFactory)
                            .map(factory -> factory.createLock(lockContext))
                            .map(l -> Lock.fromCatalog(l, identifier))
                            .orElseGet(Lock::empty);
            snapshotCommit = new RenamingSnapshotCommit(snapshotManager, lock);
        }
        return snapshotCommit;
    }

    /**
     * 创建表回滚器（Catalog 级别）。
     *
     * <p>只有支持版本管理的 Catalog 才能提供表回滚功能。
     *
     * @return TableRollback 实例，如果不支持版本管理则返回 null
     */
    @Nullable
    public TableRollback catalogTableRollback() {
        if (catalogLoader != null && supportsVersionManagement) {
            Catalog catalog = catalogLoader.load();
            return (instant, fromSnapshot) -> {
                try {
                    catalog.rollbackTo(identifier, instant, fromSnapshot);
                } catch (Catalog.TableNotExistException e) {
                    throw new RuntimeException(e);
                }
            };
        }
        return null;
    }

    /**
     * 创建快照加载器。
     *
     * <p>SnapshotLoader 用于从 Catalog 加载快照信息。
     *
     * @return SnapshotLoader 实例，如果没有 CatalogLoader 则返回 null
     */
    @Nullable
    public SnapshotLoader snapshotLoader() {
        if (catalogLoader == null) {
            return null;
        }
        return new SnapshotLoaderImpl(catalogLoader, identifier);
    }

    /**
     * 返回锁工厂。
     *
     * @return CatalogLockFactory，可能为 null
     */
    @Nullable
    public CatalogLockFactory lockFactory() {
        return lockFactory;
    }

    /**
     * 返回锁上下文。
     *
     * @return CatalogLockContext，可能为 null
     */
    @Nullable
    public CatalogLockContext lockContext() {
        return lockContext;
    }

    /**
     * 返回 Catalog 加载器。
     *
     * @return CatalogLoader，可能为 null
     */
    @Nullable
    public CatalogLoader catalogLoader() {
        return catalogLoader;
    }

    /**
     * 返回 Catalog 上下文。
     *
     * @return CatalogContext，可能为 null
     */
    @Nullable
    public CatalogContext catalogContext() {
        return catalogContext;
    }

    /**
     * 复制 CatalogEnvironment 并修改表标识符。
     *
     * <p>用于切换分支时创建新的 CatalogEnvironment。
     *
     * @param identifier 新的表标识符
     * @return 新的 CatalogEnvironment 实例
     */
    public CatalogEnvironment copy(Identifier identifier) {
        return new CatalogEnvironment(
                identifier,
                uuid,
                catalogLoader,
                lockFactory,
                lockContext,
                catalogContext,
                supportsVersionManagement);
    }

    /**
     * 创建表查询授权器。
     *
     * <p>TableQueryAuth 用于在查询时进行授权检查，支持：
     * <ul>
     *   <li>行级权限过滤
     *   <li>列级权限控制
     *   <li>数据脱敏
     * </ul>
     *
     * <p>授权由配置项 {@code query-auth.enabled} 控制。
     *
     * @param options 核心配置选项
     * @return TableQueryAuth 实例
     */
    public TableQueryAuth tableQueryAuth(CoreOptions options) {
        if (!options.queryAuthEnabled() || catalogLoader == null) {
            // 不启用授权或没有 Catalog
            return select -> null;
        }
        return select -> {
            try (Catalog catalog = catalogLoader.load()) {
                return catalog.authTableQuery(identifier, select);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }
}
