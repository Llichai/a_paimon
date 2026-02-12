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

package org.apache.paimon.lookup.rocksdb;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.lookup.StateFactory;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TtlDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * 基于 RocksDB 的状态工厂实现,用于创建持久化的键值状态.
 *
 * <p>RocksDBStateFactory 负责初始化 RocksDB 实例,并创建各种类型的 RocksDB 状态。
 * RocksDB 是一个高性能的嵌入式键值存储引擎,基于 LSM-tree 数据结构,
 * 支持 TB 级数据存储,特别适合大规模状态管理。
 *
 * <h2>主要特性:</h2>
 * <ul>
 *   <li><b>持久化</b>: 数据存储在磁盘,支持进程重启后恢复
 *   <li><b>大容量</b>: 支持 TB 级状态数据,不受 JVM 堆大小限制
 *   <li><b>列族隔离</b>: 使用列族(ColumnFamily)隔离不同的状态
 *   <li><b>TTL 支持</b>: 可选的 TTL(Time-To-Live)功能,自动过期数据
 *   <li><b>批量加载</b>: 支持通过 SST 文件批量导入数据
 * </ul>
 *
 * <h2>RocksDB 架构:</h2>
 * <pre>{@code
 * RocksDB 实例
 *   ├── 默认列族 (default)
 *   ├── 自定义列族 1 (状态1)
 *   ├── 自定义列族 2 (状态2)
 *   └── ...
 *
 * 内存结构:
 *   ├── MemTable (活跃写缓冲)
 *   ├── Immutable MemTable (只读写缓冲)
 *   └── Block Cache (读缓存)
 *
 * 磁盘结构:
 *   ├── SST Level 0 (最新数据)
 *   ├── SST Level 1
 *   ├── SST Level 2
 *   └── ... (LSM-tree 层级结构)
 * }</pre>
 *
 * <h2>LSM-tree 原理:</h2>
 * <ul>
 *   <li>写入数据先进入 MemTable(内存)
 *   <li>MemTable 满后转为 Immutable MemTable,并刷新到 Level 0 SST 文件
 *   <li>后台自动进行 Compaction(合并),将小文件合并为大文件,并移到更高层级
 *   <li>查询时从 MemTable 开始,逐层向下查找,直到找到数据或确认不存在
 * </ul>
 *
 * <h2>配置选项:</h2>
 * <ul>
 *   <li><b>压缩类型</b>: LZ4(默认),Snappy,Zlib 等
 *   <li><b>合并策略</b>: Level(默认),Universal,FIFO,None
 *   <li><b>写缓冲大小</b>: 默认 64MB
 *   <li><b>Block Cache 大小</b>: 默认 128MB
 *   <li><b>Bloom Filter</b>: 可选,用于快速判断键是否存在
 * </ul>
 *
 * <h2>Merge Operator:</h2>
 * <p>RocksDB 支持 Merge 操作,用于高效地追加数据(如 ListState)。
 * Merge 操作会将多个值合并为一个,避免了读-改-写的开销。
 * 默认使用 "stringappendtest" merge operator。
 *
 * <h2>TTL 支持:</h2>
 * <p>如果创建工厂时指定了 TTL,则会使用 {@link TtlDB} 代替普通的 RocksDB。
 * TTL 功能会自动删除过期的数据,适合缓存场景。
 *
 * <h2>本地库加载:</h2>
 * <p>RocksDB 需要加载本地库(JNI)。工厂会在初始化时尝试加载本地库,
 * 如果加载失败会重试最多 3 次。可以通过环境变量 ROCKSDB_SHAREDLIB_DIR 指定库路径。
 *
 * <h2>性能特点:</h2>
 * <ul>
 *   <li><b>写入吞吐</b>: 高(MemTable 批量刷新)
 *   <li><b>写放大</b>: 中等(LSM-tree 需要多次合并)
 *   <li><b>读取延迟</b>: 微秒级(可能需要多次磁盘查找)
 *   <li><b>空间放大</b>: 低(压缩后约为实际数据的 1.2-1.5 倍)
 * </ul>
 *
 * <h2>与 InMemory 工厂的对比:</h2>
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>RocksDB</th>
 *     <th>InMemory</th>
 *   </tr>
 *   <tr>
 *     <td>初始化开销</td>
 *     <td>较大(加载库)</td>
 *     <td>极小</td>
 *   </tr>
 *   <tr>
 *     <td>容量限制</td>
 *     <td>几乎无限(TB级)</td>
 *     <td>几百 MB</td>
 *   </tr>
 *   <tr>
 *     <td>访问延迟</td>
 *     <td>微秒级</td>
 *     <td>纳秒级</td>
 *   </tr>
 *   <tr>
 *     <td>持久化</td>
 *     <td>支持</td>
 *     <td>不支持</td>
 *   </tr>
 *   <tr>
 *     <td>批量加载</td>
 *     <td>SST 文件导入</td>
 *     <td>无优化</td>
 *   </tr>
 * </table>
 *
 * <h2>使用场景:</h2>
 * <ul>
 *   <li>大维度表数据(GB-TB 级)
 *   <li>需要持久化状态数据
 *   <li>状态数据量超过 JVM 堆大小
 *   <li>需要 TTL 功能的缓存场景
 * </ul>
 *
 * <h2>使用示例:</h2>
 * <pre>{@code
 * // 创建 RocksDB 状态工厂
 * Options options = new Options();
 * StateFactory factory = new RocksDBStateFactory(
 *     "/tmp/rocksdb",        // 数据存储路径
 *     options,               // Paimon 配置
 *     Duration.ofHours(24)   // TTL(可选)
 * );
 *
 * // 创建单值状态
 * ValueState<Integer, String> valueState = factory.valueState(
 *     "user-info",
 *     IntSerializer.INSTANCE,
 *     StringSerializer.INSTANCE,
 *     10000L  // LRU 缓存大小
 * );
 *
 * // 写入和查询数据
 * valueState.put(1, "Alice");
 * String user = valueState.get(1);  // "Alice"
 *
 * // 关闭工厂,释放资源
 * factory.close();
 * }</pre>
 *
 * @see RocksDBValueState 单值状态实现
 * @see RocksDBListState 列表状态实现
 * @see RocksDBSetState 集合状态实现
 * @see RocksDBOptions RocksDB 配置选项
 */
public class RocksDBStateFactory implements StateFactory {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBStateFactory.class);

    /** Merge Operator 名称,用于 ListState 的追加操作. */
    public static final String MERGE_OPERATOR_NAME = "stringappendtest";

    /** RocksDB 本地库加载的最大重试次数. */
    private static final int ROCKSDB_LIB_LOADING_ATTEMPTS = 3;

    /** RocksDB 本地库是否已初始化的标志. */
    private static boolean rocksDbInitialized = false;

    /** RocksDB 选项. */
    private final Options options;

    /** RocksDB 数据存储路径. */
    private final String path;

    /** 列族选项. */
    private final ColumnFamilyOptions columnFamilyOptions;

    /** RocksDB 实例. */
    private RocksDB db;

    /**
     * 构造 RocksDB 状态工厂.
     *
     * <p>初始化 RocksDB 实例,配置数据库和列族选项。如果指定了 TTL,则创建支持 TTL 的数据库实例。
     *
     * <h3>初始化流程:</h3>
     * <ol>
     *   <li>确保 RocksDB 本地库已加载
     *   <li>创建数据库选项(DBOptions)和列族选项(ColumnFamilyOptions)
     *   <li>打开 RocksDB 实例(普通或 TTL 模式)
     * </ol>
     *
     * @param path RocksDB 数据存储路径
     * @param conf Paimon 配置选项
     * @param ttlSecs TTL 时长,如果为 null 则不启用 TTL
     * @throws IOException 如果初始化过程发生错误
     */
    public RocksDBStateFactory(
            String path, org.apache.paimon.options.Options conf, @Nullable Duration ttlSecs)
            throws IOException {
        try {
            ensureRocksDBIsLoaded();
        } catch (Throwable e) {
            throw new IOException("Could not load the native RocksDB library", e);
        }
        DBOptions dbOptions =
                RocksDBOptions.createDBOptions(
                        new DBOptions()
                                .setUseFsync(false)
                                .setStatsDumpPeriodSec(0)
                                .setCreateIfMissing(true),
                        conf);
        this.path = path;
        this.columnFamilyOptions =
                RocksDBOptions.createColumnOptions(new ColumnFamilyOptions(), conf)
                        .setMergeOperatorName(MERGE_OPERATOR_NAME);

        this.options = new Options(dbOptions, columnFamilyOptions);
        try {
            this.db =
                    ttlSecs == null
                            ? RocksDB.open(options, path)
                            : TtlDB.open(options, path, (int) ttlSecs.getSeconds(), false);
        } catch (RocksDBException e) {
            throw new IOException("Error while opening RocksDB instance.", e);
        }
    }

    /** 获取 RocksDB 实例. */
    public RocksDB db() {
        return db;
    }

    /** 获取 RocksDB 选项. */
    public Options options() {
        return options;
    }

    /** 获取 RocksDB 存储路径. */
    public String path() {
        return path;
    }

    @Override
    public <K, V> RocksDBValueState<K, V> valueState(
            String name,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize)
            throws IOException {
        return new RocksDBValueState<>(
                this, createColumnFamily(name), keySerializer, valueSerializer, lruCacheSize);
    }

    @Override
    public <K, V> RocksDBSetState<K, V> setState(
            String name,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize)
            throws IOException {
        return new RocksDBSetState<>(
                this, createColumnFamily(name), keySerializer, valueSerializer, lruCacheSize);
    }

    @Override
    public <K, V> RocksDBListState<K, V> listState(
            String name,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize)
            throws IOException {

        return new RocksDBListState<>(
                this, createColumnFamily(name), keySerializer, valueSerializer, lruCacheSize);
    }

    /**
     * 返回是否偏好批量加载.
     *
     * <p>对于 RocksDB,批量加载通过直接导入 SST 文件可以显著提高性能,因此返回 true。
     *
     * @return true,表示偏好批量加载
     */
    @Override
    public boolean preferBulkLoad() {
        return true;
    }

    /**
     * 创建列族(ColumnFamily).
     *
     * <p>列族用于隔离不同的状态,每个状态对应一个列族。
     *
     * @param name 列族名称
     * @return 列族句柄
     * @throws IOException 如果创建失败
     */
    private ColumnFamilyHandle createColumnFamily(String name) throws IOException {
        try {
            return db.createColumnFamily(
                    new ColumnFamilyDescriptor(
                            name.getBytes(StandardCharsets.UTF_8), columnFamilyOptions));
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    /**
     * 关闭状态工厂,释放 RocksDB 资源.
     *
     * @throws IOException 如果关闭过程发生 I/O 错误
     */
    @Override
    public void close() throws IOException {
        if (db != null) {
            db.close();
            db = null;
        }
    }

    // ------------------------------------------------------------------------
    //  static library loading utilities
    // ------------------------------------------------------------------------

    /**
     * 确保 RocksDB 本地库已加载.
     *
     * <p>RocksDB 需要加载 JNI 本地库。该方法会尝试加载库,如果失败会重试最多 3 次。
     * 可以通过环境变量 ROCKSDB_SHAREDLIB_DIR 指定库路径。
     *
     * @throws IOException 如果加载失败
     */
    @VisibleForTesting
    static void ensureRocksDBIsLoaded() throws IOException {
        synchronized (RocksDBStateFactory.class) {
            if (!rocksDbInitialized) {
                LOG.info("Attempting to load RocksDB native library");

                Throwable lastException = null;
                for (int attempt = 1; attempt <= ROCKSDB_LIB_LOADING_ATTEMPTS; attempt++) {
                    try {
                        // keep same with RocksDB.loadLibrary
                        final String tmpDir = System.getenv("ROCKSDB_SHAREDLIB_DIR");
                        // explicitly load the JNI dependency if it has not been loaded before
                        NativeLibraryLoader.getInstance().loadLibrary(tmpDir);

                        // this initialization here should validate that the loading succeeded
                        RocksDB.loadLibrary();

                        // seems to have worked
                        LOG.info("Successfully loaded RocksDB native library");
                        rocksDbInitialized = true;
                        return;
                    } catch (Throwable t) {
                        lastException = t;
                        LOG.debug("RocksDB JNI library loading attempt {} failed", attempt, t);
                        // try to force RocksDB to attempt reloading the library
                        try {
                            resetRocksDBLoadedFlag();
                        } catch (Throwable tt) {
                            LOG.debug(
                                    "Failed to reset 'initialized' flag in RocksDB native code loader",
                                    tt);
                        }
                    }
                }
                throw new IOException("Could not load the native RocksDB library", lastException);
            }
        }
    }

    /**
     * 重置 RocksDB 加载标志(用于重试).
     *
     * @throws Exception 如果重置失败
     */
    @VisibleForTesting
    static void resetRocksDBLoadedFlag() throws Exception {
        final Field initField =
                org.rocksdb.NativeLibraryLoader.class.getDeclaredField("initialized");
        initField.setAccessible(true);
        initField.setBoolean(null, false);
    }

    /**
     * 返回 RocksDB 是否已初始化.
     *
     * @return 如果已初始化返回 true,否则返回 false
     */
    public static boolean rocksDbInitialized() {
        return rocksDbInitialized;
    }
}
