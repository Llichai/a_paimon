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

package org.apache.paimon.options;

import org.apache.paimon.table.CatalogTableType;

import java.time.Duration;

import static org.apache.paimon.options.ConfigOptions.key;

/**
 * Catalog 的配置选项类。
 *
 * <p>该类定义了 Paimon Catalog 的所有配置选项,包括:
 * <ul>
 *   <li>基础配置: warehouse、metastore、uri 等
 *   <li>表类型配置: table.type
 *   <li>锁配置: lock.enabled、lock.type 等
 *   <li>缓存配置: cache-enabled、cache.expire-after-access 等
 *   <li>连接池配置: client-pool-size
 *   <li>格式表配置: format-table.enabled
 *   <li>文件 I/O 配置: resolving-file-io.enabled、file-io.allow-cache
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建配置
 * Options options = new Options();
 * options.set(CatalogOptions.WAREHOUSE, "/path/to/warehouse");
 * options.set(CatalogOptions.METASTORE, "filesystem");
 * options.set(CatalogOptions.CACHE_ENABLED, true);
 *
 * // 使用配置创建 Catalog
 * Catalog catalog = CatalogFactory.createCatalog(options);
 * }</pre>
 */
public class CatalogOptions {

    /** Catalog 的仓库根路径 */
    public static final ConfigOption<String> WAREHOUSE =
            ConfigOptions.key("warehouse")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The warehouse root path of catalog.");

    /** Catalog 的元数据存储类型,支持 filesystem、hive 和 jdbc */
    public static final ConfigOption<String> METASTORE =
            ConfigOptions.key("metastore")
                    .stringType()
                    .defaultValue("filesystem")
                    .withDescription(
                            "Metastore of paimon catalog, supports filesystem, hive and jdbc.");

    /** 元数据存储服务器的 URI */
    public static final ConfigOption<String> URI =
            ConfigOptions.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Uri of metastore server.");

    /** 表类型配置 */
    public static final ConfigOption<CatalogTableType> TABLE_TYPE =
            ConfigOptions.key("table.type")
                    .enumType(CatalogTableType.class)
                    .defaultValue(CatalogTableType.MANAGED)
                    .withDescription("Type of table.");

    /** 是否启用 Catalog 锁 */
    public static final ConfigOption<Boolean> LOCK_ENABLED =
            ConfigOptions.key("lock.enabled")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("Enable Catalog Lock.");

    /** Catalog 锁的类型,例如 'hive'、'zookeeper' */
    public static final ConfigOption<String> LOCK_TYPE =
            ConfigOptions.key("lock.type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Lock Type for Catalog, such as 'hive', 'zookeeper'.");

    /** 重试检查锁时的最大睡眠时间 */
    public static final ConfigOption<Duration> LOCK_CHECK_MAX_SLEEP =
            key("lock-check-max-sleep")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(8))
                    .withDescription("The maximum sleep time when retrying to check the lock.");

    /** 获取锁的最大等待时间 */
    public static final ConfigOption<Duration> LOCK_ACQUIRE_TIMEOUT =
            key("lock-acquire-timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(8))
                    .withDescription("The maximum time to wait for acquiring the lock.");

    /** 配置连接池的大小 */
    public static final ConfigOption<Integer> CLIENT_POOL_SIZE =
            key("client-pool-size")
                    .intType()
                    .defaultValue(2)
                    .withDescription("Configure the size of the connection pool.");

    /** 是否启用缓存,控制 Catalog 是否缓存数据库、表、清单和分区 */
    public static final ConfigOption<Boolean> CACHE_ENABLED =
            key("cache-enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Controls whether the catalog will cache databases, tables, manifests and partitions.");

    /** 缓存过期策略:在最后一次访问后经过指定时间后过期 */
    public static final ConfigOption<Duration> CACHE_EXPIRE_AFTER_ACCESS =
            key("cache.expire-after-access")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(10))
                    .withFallbackKeys("cache.expiration-interval")
                    .withDescription(
                            "Cache expiration policy: marks cache entries to expire after a specified duration has passed since their last access.");

    /** 缓存过期策略:在最后一次刷新后经过指定时间后过期 */
    public static final ConfigOption<Duration> CACHE_EXPIRE_AFTER_WRITE =
            key("cache.expire-after-write")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(30))
                    .withDescription(
                            "Cache expiration policy: marks cache entries to expire after a specified duration has passed since their last refresh.");

    /** 控制 Catalog 中缓存的分区的最大数量 */
    public static final ConfigOption<Long> CACHE_PARTITION_MAX_NUM =
            key("cache.partition.max-num")
                    .longType()
                    .defaultValue(0L)
                    .withDescription(
                            "Controls the max number for which partitions in the catalog are cached.");

    /** 控制用于缓存小清单文件的缓存内存 */
    public static final ConfigOption<MemorySize> CACHE_MANIFEST_SMALL_FILE_MEMORY =
            key("cache.manifest.small-file-memory")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription("Controls the cache memory to cache small manifest files.");

    /** 控制小清单文件的阈值 */
    public static final ConfigOption<MemorySize> CACHE_MANIFEST_SMALL_FILE_THRESHOLD =
            key("cache.manifest.small-file-threshold")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(1))
                    .withDescription("Controls the threshold of small manifest file.");

    /** 控制缓存清单内容的最大内存 */
    public static final ConfigOption<MemorySize> CACHE_MANIFEST_MAX_MEMORY =
            key("cache.manifest.max-memory")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription("Controls the maximum memory to cache manifest content.");

    /** 控制每个表在 Catalog 中缓存的快照的最大数量 */
    public static final ConfigOption<Integer> CACHE_SNAPSHOT_MAX_NUM_PER_TABLE =
            key("cache.snapshot.max-num-per-table")
                    .intType()
                    .defaultValue(20)
                    .withDescription(
                            "Controls the max number for snapshots per table in the catalog are cached.");

    /** 控制可以缓存的删除向量元数据的最大数量 */
    public static final ConfigOption<Integer> CACHE_DV_MAX_NUM =
            key("cache.deletion-vectors.max-num")
                    .intType()
                    .defaultValue(100_000)
                    .withDescription(
                            "Controls the maximum number of deletion vector meta that can be cached.");

    /** 指示此 Catalog 是否区分大小写 */
    public static final ConfigOption<Boolean> CASE_SENSITIVE =
            ConfigOptions.key("case-sensitive")
                    .booleanType()
                    .noDefaultValue()
                    .withFallbackKeys("allow-upper-case")
                    .withDescription("Indicates whether this catalog is case-sensitive.");

    /** 是否将所有表属性同步到 Hive 元数据存储 */
    public static final ConfigOption<Boolean> SYNC_ALL_PROPERTIES =
            ConfigOptions.key("sync-all-properties")
                    .booleanType()
                    // 我们应该将默认值设置为 true,以防止 hive 元数据存储丢失表属性
                    .defaultValue(true)
                    .withDescription("Sync all table properties to hive metastore");

    /** 是否支持格式表,格式表对应常规的 csv、parquet 或 orc 表,允许读写操作 */
    public static final ConfigOption<Boolean> FORMAT_TABLE_ENABLED =
            ConfigOptions.key("format-table.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to support format tables, format table corresponds to a regular csv, parquet or orc table, allowing read and write operations. "
                                    + "However, during these processes, it does not connect to the metastore; hence, newly added partitions will not be reflected in"
                                    + " the metastore and need to be manually added as separate partition operations.");

    /** 是否启用解析文件 I/O,启用后可以读写外部存储路径 */
    public static final ConfigOption<Boolean> RESOLVING_FILE_IO_ENABLED =
            ConfigOptions.key("resolving-file-io.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to enable resolving fileio, when this option is enabled, in conjunction with the table's property data-file.external-paths, "
                                    + "Paimon can read and write to external storage paths, such as OSS or S3. "
                                    + "In order to access these external paths correctly, you also need to configure the corresponding access key and secret key.");

    /** 是否允许在文件 I/O 实现中使用静态缓存 */
    public static final ConfigOption<Boolean> FILE_IO_ALLOW_CACHE =
            ConfigOptions.key("file-io.allow-cache")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to allow static cache in file io implementation. If not allowed, this means that "
                                    + "there may be a large number of FileIO instances generated, enabling caching can "
                                    + "lead to resource leakage.");
}
