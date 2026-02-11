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

package org.apache.paimon.catalog;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.utils.DVMetaCache;
import org.apache.paimon.utils.SegmentsCache;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Ticker;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Weigher;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.options.CatalogOptions.CACHE_DV_MAX_NUM;
import static org.apache.paimon.options.CatalogOptions.CACHE_ENABLED;
import static org.apache.paimon.options.CatalogOptions.CACHE_EXPIRE_AFTER_ACCESS;
import static org.apache.paimon.options.CatalogOptions.CACHE_EXPIRE_AFTER_WRITE;
import static org.apache.paimon.options.CatalogOptions.CACHE_MANIFEST_MAX_MEMORY;
import static org.apache.paimon.options.CatalogOptions.CACHE_MANIFEST_SMALL_FILE_MEMORY;
import static org.apache.paimon.options.CatalogOptions.CACHE_MANIFEST_SMALL_FILE_THRESHOLD;
import static org.apache.paimon.options.CatalogOptions.CACHE_PARTITION_MAX_NUM;
import static org.apache.paimon.options.CatalogOptions.CACHE_SNAPSHOT_MAX_NUM_PER_TABLE;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 带缓存的 Catalog 包装器
 *
 * <p>CachingCatalog 是一个装饰器模式实现,为任意 Catalog 添加缓存功能,大幅提升元数据查询性能。
 *
 * <p>缓存内容:
 * <ul>
 *   <li><b>数据库缓存</b>: 缓存 {@link Database} 对象
 *   <li><b>表缓存</b>: 缓存 {@link Table} 对象
 *   <li><b>Manifest 缓存</b>: 缓存 Manifest 文件内容（使用 {@link SegmentsCache}）
 *   <li><b>分区缓存</b>: 缓存分区列表（可选,默认关闭）
 *   <li><b>删除向量缓存</b>: 缓存删除向量元数据（可选）
 * </ul>
 *
 * <p>缓存策略:
 * <ul>
 *   <li><b>过期策略</b>:
 *     <ul>
 *       <li>读后过期: {@code cache.expire-after-access}（默认 10 分钟）
 *       <li>写后过期: {@code cache.expire-after-write}（默认 10 分钟）
 *     </ul>
 *   <li><b>失效策略</b>:
 *     <ul>
 *       <li>软引用: 使用 Caffeine 的 softValues(),内存不足时自动清理
 *       <li>主动失效: 修改操作（create/drop/alter）自动使缓存失效
 *     </ul>
 *   <li><b>容量限制</b>:
 *     <ul>
 *       <li>Manifest: 基于内存大小限制（{@code cache.manifest-small-file-memory}）
 *       <li>分区: 基于数量限制（{@code cache.partition-max-num}）
 *       <li>快照: 每个表最多缓存的快照数（{@code cache.snapshot-max-num-per-table}）
 *     </ul>
 * </ul>
 *
 * <p>性能优化:
 * <ul>
 *   <li><b>元数据查询</b>: 避免重复访问 Hive Metastore/JDBC 等外部系统
 *   <li><b>Manifest 文件</b>: 缓存小文件内容,减少远程 I/O
 *   <li><b>分区列表</b>: 对于大表,分区列表查询开销大,缓存可显著提升性能
 *   <li><b>Table 对象</b>: 缓存 Table 实例,避免重复构造 FileStoreTable
 * </ul>
 *
 * <p>缓存一致性:
 * <ul>
 *   <li><b>单进程</b>: 同一进程内保证一致性（修改操作自动失效）
 *   <li><b>多进程</b>: 依赖过期时间,可能存在一定延迟（最多 10 分钟）
 *   <li><b>建议</b>: 对于读多写少的场景,延迟通常可接受
 * </ul>
 *
 * <p>配置示例:
 * <pre>
 * # 启用缓存
 * cache.enabled = true
 *
 * # 读后 5 分钟过期
 * cache.expire-after-access = 5 min
 *
 * # 写后 5 分钟过期
 * cache.expire-after-write = 5 min
 *
 * # Manifest 小文件缓存 256MB
 * cache.manifest-small-file-memory = 256 mb
 *
 * # 最多缓存 100 个分区列表
 * cache.partition-max-num = 100
 *
 * # 每个表最多缓存 20 个快照
 * cache.snapshot-max-num-per-table = 20
 * </pre>
 *
 * <p>使用场景:
 * <ul>
 *   <li><b>读密集型</b>: 查询远多于写入的场景
 *   <li><b>高延迟存储</b>: Hive Metastore、JDBC、对象存储等访问延迟高
 *   <li><b>大规模表</b>: 表数量多,频繁访问元数据
 *   <li><b>分析查询</b>: Flink/Spark 任务需要频繁读取表信息
 * </ul>
 *
 * <p>注意事项:
 * <ul>
 *   <li><b>内存占用</b>: 缓存会占用 JVM 堆内存,需根据实际情况调整大小
 *   <li><b>数据延迟</b>: 分区缓存可能导致看不到最新数据（最多延迟 = 过期时间）
 *   <li><b>外部修改</b>: 如果有其他进程直接修改文件系统,缓存无法感知
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 1. 创建基础 Catalog
 * Catalog baseCatalog = new FileSystemCatalog(fileIO, warehouse);
 *
 * // 2. 包装成缓存 Catalog
 * Options options = new Options();
 * options.set(CACHE_ENABLED, true);
 * options.set(CACHE_EXPIRE_AFTER_ACCESS, Duration.ofMinutes(5));
 * Catalog catalog = CachingCatalog.tryToCreate(baseCatalog, options);
 *
 * // 3. 使用 - 第二次访问会从缓存读取
 * Table table1 = catalog.getTable(identifier);  // 从 Metastore 读取
 * Table table2 = catalog.getTable(identifier);  // 从缓存读取（快速）
 *
 * // 4. 修改后缓存自动失效
 * catalog.alterTable(identifier, changes, false);
 * Table table3 = catalog.getTable(identifier);  // 重新从 Metastore 读取
 * }</pre>
 *
 * <p>与 Flink/Spark 集成:
 * <p>在 Flink/Spark 中使用 CachingCatalog 可显著提升性能:
 * <pre>{@code
 * // Flink SQL
 * CREATE CATALOG paimon_catalog WITH (
 *   'type' = 'paimon',
 *   'warehouse' = 'hdfs:///warehouse',
 *   'cache.enabled' = 'true',
 *   'cache.expire-after-access' = '5 min'
 * );
 *
 * // 多次查询同一个表,元数据从缓存读取
 * SELECT * FROM paimon_catalog.db.table WHERE ...
 * }</pre>
 *
 * @see DelegateCatalog
 * @see SegmentsCache
 * @see CachingCatalogLoader
 */
public class CachingCatalog extends DelegateCatalog {

    /** Catalog 配置选项 */
    private final Options options;

    /** 读后过期时间 */
    private final Duration expireAfterAccess;

    /** 写后过期时间 */
    private final Duration expireAfterWrite;

    /** 每个表最多缓存的快照数量 */
    private final int snapshotMaxNumPerTable;

    /** 最多缓存的分区数量 */
    private final long cachedPartitionMaxNum;

    /** 数据库缓存 */
    protected Cache<String, Database> databaseCache;

    /** 表缓存 */
    protected Cache<Identifier, Table> tableCache;

    /** Manifest 文件缓存（缓存小文件内容） */
    @Nullable protected final SegmentsCache<Path> manifestCache;

    /** 分区缓存（注意: 会影响数据延迟） */
    @Nullable protected Cache<Identifier, List<Partition>> partitionCache;

    /** 删除向量元数据缓存 */
    @Nullable protected DVMetaCache dvMetaCache;

    /**
     * 构造函数
     *
     * <p>初始化各种缓存组件:
     * <ul>
     *   <li>Manifest 缓存: 根据内存配置创建 {@link SegmentsCache}
     *   <li>过期时间: 从配置读取读后/写后过期时间
     *   <li>容量限制: 从配置读取各缓存的容量限制
     * </ul>
     *
     * @param wrapped 被包装的 Catalog 实例
     * @param options 配置选项
     * @throws IllegalArgumentException 如果过期时间配置为负数或 0
     */
    public CachingCatalog(Catalog wrapped, Options options) {
        super(wrapped);
        this.options = options;
        MemorySize manifestMaxMemory = options.get(CACHE_MANIFEST_SMALL_FILE_MEMORY);
        long manifestCacheThreshold = options.get(CACHE_MANIFEST_SMALL_FILE_THRESHOLD).getBytes();
        Optional<MemorySize> maxMemory = options.getOptional(CACHE_MANIFEST_MAX_MEMORY);
        if (maxMemory.isPresent() && maxMemory.get().compareTo(manifestMaxMemory) > 0) {
            // 缓存所有 Manifest 文件
            manifestMaxMemory = maxMemory.get();
            manifestCacheThreshold = Long.MAX_VALUE;
        }

        this.expireAfterAccess = options.get(CACHE_EXPIRE_AFTER_ACCESS);
        if (expireAfterAccess.isZero() || expireAfterAccess.isNegative()) {
            throw new IllegalArgumentException(
                    "When 'cache.expire-after-access' is set to negative or 0, the catalog cache should be disabled.");
        }
        this.expireAfterWrite = options.get(CACHE_EXPIRE_AFTER_WRITE);
        if (expireAfterWrite.isZero() || expireAfterWrite.isNegative()) {
            throw new IllegalArgumentException(
                    "When 'cache.expire-after-write' is set to negative or 0, the catalog cache should be disabled.");
        }

        this.snapshotMaxNumPerTable = options.get(CACHE_SNAPSHOT_MAX_NUM_PER_TABLE);
        this.manifestCache = SegmentsCache.create(manifestMaxMemory, manifestCacheThreshold);

        this.cachedPartitionMaxNum = options.get(CACHE_PARTITION_MAX_NUM);

        int cacheDvMaxNum = options.get(CACHE_DV_MAX_NUM);
        if (cacheDvMaxNum > 0) {
            this.dvMetaCache = new DVMetaCache(cacheDvMaxNum);
        }
        init(Ticker.systemTicker());
    }

    /**
     * 初始化各种缓存
     *
     * <p>使用 Caffeine 创建缓存实例,配置:
     * <ul>
     *   <li>softValues: 使用软引用,内存不足时自动清理
     *   <li>expireAfterAccess: 读后过期
     *   <li>expireAfterWrite: 写后过期
     *   <li>executor: 使用当前线程执行清理任务（避免线程池开销）
     * </ul>
     *
     * @param ticker 时间源（用于测试）
     */
    @VisibleForTesting
    void init(Ticker ticker) {
        // 数据库缓存
        this.databaseCache =
                Caffeine.newBuilder()
                        .softValues()
                        .executor(Runnable::run)
                        .expireAfterAccess(expireAfterAccess)
                        .expireAfterWrite(expireAfterWrite)
                        .ticker(ticker)
                        .build();

        // 表缓存
        this.tableCache =
                Caffeine.newBuilder()
                        .softValues()
                        .executor(Runnable::run)
                        .expireAfterAccess(expireAfterAccess)
                        .expireAfterWrite(expireAfterWrite)
                        .ticker(ticker)
                        .build();

        // 分区缓存（可选）
        this.partitionCache =
                cachedPartitionMaxNum == 0
                        ? null
                        : Caffeine.newBuilder()
                                .softValues()
                                .executor(Runnable::run)
                                .expireAfterAccess(expireAfterAccess)
                                .expireAfterWrite(expireAfterWrite)
                                .weigher(
                                        (Weigher<Identifier, List<Partition>>)
                                                (identifier, v) -> v.size())
                                .maximumWeight(cachedPartitionMaxNum)
                                .ticker(ticker)
                                .build();
    }

    /**
     * 获取表缓存（用于测试）
     *
     * @return 表缓存实例
     */
    @VisibleForTesting
    public Cache<Identifier, Table> tableCache() {
        return tableCache;
    }

    /**
     * 尝试创建缓存 Catalog
     *
     * <p>如果配置了 {@code cache.enabled=true},则包装成 CachingCatalog;
     * 否则返回原始 Catalog。
     *
     * @param catalog 原始 Catalog
     * @param options 配置选项
     * @return 可能被包装的 Catalog
     */
    public static Catalog tryToCreate(Catalog catalog, Options options) {
        if (!options.get(CACHE_ENABLED)) {
            return catalog;
        }

        return new CachingCatalog(catalog, options);
    }

    @Override
    public CatalogLoader catalogLoader() {
        return new CachingCatalogLoader(wrapped.catalogLoader(), options);
    }

    /**
     * 获取数据库（带缓存）
     *
     * <p>执行流程:
     * <ol>
     *   <li>先从缓存查找
     *   <li>缓存命中: 直接返回
     *   <li>缓存未命中: 从底层 Catalog 读取,并放入缓存
     * </ol>
     *
     * @param databaseName 数据库名称
     * @return Database 对象
     * @throws DatabaseNotExistException 如果数据库不存在
     */
    @Override
    public Database getDatabase(String databaseName) throws DatabaseNotExistException {
        Database database = databaseCache.getIfPresent(databaseName);
        if (database != null) {
            return database;
        }

        database = super.getDatabase(databaseName);
        databaseCache.put(databaseName, database);
        return database;
    }

    /**
     * 删除数据库（自动失效缓存）
     *
     * <p>删除成功后:
     * <ul>
     *   <li>使数据库缓存失效
     *   <li>如果是级联删除,使该数据库下所有表的缓存失效
     * </ul>
     *
     * @param name 数据库名称
     * @param ignoreIfNotExists 如果不存在是否忽略
     * @param cascade 是否级联删除表
     * @throws DatabaseNotExistException 如果数据库不存在
     * @throws DatabaseNotEmptyException 如果数据库不为空且 cascade=false
     */
    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        super.dropDatabase(name, ignoreIfNotExists, cascade);
        databaseCache.invalidate(name);
        if (cascade) {
            List<Identifier> tables = new ArrayList<>();
            for (Identifier identifier : tableCache.asMap().keySet()) {
                if (identifier.getDatabaseName().equals(name)) {
                    tables.add(identifier);
                }
            }
            tables.forEach(tableCache::invalidate);
        }
    }

    @Override
    public void alterDatabase(String name, List<PropertyChange> changes, boolean ignoreIfNotExists)
            throws DatabaseNotExistException {
        super.alterDatabase(name, changes, ignoreIfNotExists);
        databaseCache.invalidate(name);
    }

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws TableNotExistException {
        super.dropTable(identifier, ignoreIfNotExists);
        invalidateTable(identifier);

        // clear all branch tables of this table
        for (Identifier i : tableCache.asMap().keySet()) {
            if (identifier.getTableName().equals(i.getTableName())
                    && identifier.getDatabaseName().equals(i.getDatabaseName())) {
                tableCache.invalidate(i);
            }
        }
    }

    @Override
    public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException {
        super.renameTable(fromTable, toTable, ignoreIfNotExists);
        invalidateTable(fromTable);
    }

    @Override
    public void alterTable(
            Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        super.alterTable(identifier, changes, ignoreIfNotExists);
        invalidateTable(identifier);
    }

    @Override
    public Table getTable(Identifier identifier) throws TableNotExistException {
        Table table = tableCache.getIfPresent(identifier);
        if (table != null) {
            return table;
        }

        // For system table, do not cache it directly. Instead, cache the origin table and then wrap
        // it to generate the system table.
        if (identifier.isSystemTable()) {
            Identifier originIdentifier =
                    new Identifier(
                            identifier.getDatabaseName(),
                            identifier.getTableName(),
                            identifier.getBranchName(),
                            null);
            Table originTable = getTable(originIdentifier);
            table =
                    SystemTableLoader.load(
                            checkNotNull(identifier.getSystemTableName()),
                            (FileStoreTable) originTable);
            if (table == null) {
                throw new TableNotExistException(identifier);
            }
            return table;
        }

        table = wrapped.getTable(identifier);
        putTableCache(identifier, table);
        return table;
    }

    private void putTableCache(Identifier identifier, Table table) {
        if (table instanceof FileStoreTable) {
            FileStoreTable storeTable = (FileStoreTable) table;
            storeTable.setSnapshotCache(
                    Caffeine.newBuilder()
                            .softValues()
                            .expireAfterAccess(expireAfterAccess)
                            .expireAfterWrite(expireAfterWrite)
                            .maximumSize(snapshotMaxNumPerTable)
                            .executor(Runnable::run)
                            .build());
            storeTable.setStatsCache(
                    Caffeine.newBuilder()
                            .softValues()
                            .expireAfterAccess(expireAfterAccess)
                            .expireAfterWrite(expireAfterWrite)
                            .maximumSize(5)
                            .executor(Runnable::run)
                            .build());
            if (manifestCache != null) {
                storeTable.setManifestCache(manifestCache);
            }
            if (dvMetaCache != null) {
                storeTable.setDVMetaCache(dvMetaCache);
            }
        }

        tableCache.put(identifier, table);
    }

    @Override
    public List<Partition> listPartitions(Identifier identifier) throws TableNotExistException {
        if (partitionCache == null) {
            return wrapped.listPartitions(identifier);
        }

        List<Partition> result = partitionCache.getIfPresent(identifier);
        if (result == null) {
            result = wrapped.listPartitions(identifier);
            partitionCache.put(identifier, result);
        }
        return result;
    }

    @Override
    public void dropPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        wrapped.dropPartitions(identifier, partitions);
        if (partitionCache != null) {
            partitionCache.invalidate(identifier);
        }
    }

    @Override
    public void alterPartitions(Identifier identifier, List<PartitionStatistics> partitions)
            throws TableNotExistException {
        wrapped.alterPartitions(identifier, partitions);
        if (partitionCache != null) {
            partitionCache.invalidate(identifier);
        }
    }

    @Override
    public void invalidateTable(Identifier identifier) {
        tableCache.invalidate(identifier);
        if (partitionCache != null) {
            partitionCache.invalidate(identifier);
        }
    }

    // ================================== Cache Public API
    // ================================================

    /**
     * Partition cache will affect the latency of table, so refresh method is provided for compute
     * engine.
     */
    public void refreshPartitions(Identifier identifier) throws TableNotExistException {
        if (partitionCache != null) {
            List<Partition> result = wrapped.listPartitions(identifier);
            partitionCache.put(identifier, result);
        }
    }

    /**
     * Cache sizes for compute engine. This method can let the outside know the specific usage of
     * cache.
     */
    public CacheSizes estimatedCacheSizes() {
        long databaseCacheSize = databaseCache.estimatedSize();
        long tableCacheSize = tableCache.estimatedSize();
        long manifestCacheSize = 0L;
        long manifestCacheBytes = 0L;
        if (manifestCache != null) {
            manifestCacheSize = manifestCache.estimatedSize();
            manifestCacheBytes = manifestCache.totalCacheBytes();
        }
        long partitionCacheSize = 0L;
        if (partitionCache != null) {
            for (Map.Entry<Identifier, List<Partition>> entry : partitionCache.asMap().entrySet()) {
                partitionCacheSize += entry.getValue().size();
            }
        }
        return new CacheSizes(
                databaseCacheSize,
                tableCacheSize,
                manifestCacheSize,
                manifestCacheBytes,
                partitionCacheSize);
    }

    /** Cache sizes of a caching catalog. */
    public static class CacheSizes {

        private final long databaseCacheSize;
        private final long tableCacheSize;
        private final long manifestCacheSize;
        private final long manifestCacheBytes;
        private final long partitionCacheSize;

        public CacheSizes(
                long databaseCacheSize,
                long tableCacheSize,
                long manifestCacheSize,
                long manifestCacheBytes,
                long partitionCacheSize) {
            this.databaseCacheSize = databaseCacheSize;
            this.tableCacheSize = tableCacheSize;
            this.manifestCacheSize = manifestCacheSize;
            this.manifestCacheBytes = manifestCacheBytes;
            this.partitionCacheSize = partitionCacheSize;
        }

        public long databaseCacheSize() {
            return databaseCacheSize;
        }

        public long tableCacheSize() {
            return tableCacheSize;
        }

        public long manifestCacheSize() {
            return manifestCacheSize;
        }

        public long manifestCacheBytes() {
            return manifestCacheBytes;
        }

        public long partitionCacheSize() {
            return partitionCacheSize;
        }
    }
}
