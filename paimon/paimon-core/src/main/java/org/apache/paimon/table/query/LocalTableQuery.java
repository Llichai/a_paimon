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

package org.apache.paimon.table.query;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.LookupFile;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.mergetree.lookup.LookupSerializerFactory;
import org.apache.paimon.mergetree.lookup.PersistValueProcessor;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.paimon.lookup.LookupStoreFactory.bfGenerator;
import static org.apache.paimon.mergetree.LookupFile.localFilePrefix;

/**
 * {@link TableQuery} 的本地缓存实现,支持在本地缓存数据和文件。
 *
 * <p>LocalTableQuery 是 TableQuery 接口的主要实现,提供了高性能的主键查找能力。
 * 它通过在本地维护一个表数据的视图,并缓存文件和数据,来加速查找操作。
 *
 * <h3>核心特性</h3>
 * <ul>
 *   <li><b>多层查找</b>: 基于 LSM 树结构,支持在多个层级中查找数据</li>
 *   <li><b>文件缓存</b>: 使用 Caffeine 缓存频繁访问的文件,减少磁盘 I/O</li>
 *   <li><b>数据缓存</b>: 支持将热点数据缓存到内存,提高查询性能</li>
 *   <li><b>增量刷新</b>: 支持增量更新文件列表,保持查询视图的实时性</li>
 *   <li><b>分区和分桶</b>: 为每个分区和分桶维护独立的查找层级</li>
 * </ul>
 *
 * <h3>查找流程</h3>
 * <ol>
 *   <li>根据分区键和分桶号定位到对应的 LookupLevels</li>
 *   <li>从配置的起始层级(startLevel)开始查找</li>
 *   <li>在每个层级的文件中查找匹配的主键</li>
 *   <li>找到后返回数据行,未找到或已删除则返回 null</li>
 * </ol>
 *
 * <h3>与其他组件的关系</h3>
 * <ul>
 *   <li>实现 {@link TableQuery} 接口,提供统一的查询能力</li>
 *   <li>使用 {@link org.apache.paimon.mergetree.LookupLevels} 管理多层级索引</li>
 *   <li>使用 {@link org.apache.paimon.lookup.LookupStoreFactory} 创建查找存储</li>
 *   <li>使用 {@link KeyValueFileReaderFactory} 读取底层数据文件</li>
 *   <li>依赖 {@link org.apache.paimon.disk.IOManager} 进行 I/O 管理</li>
 * </ul>
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li>维表关联(Dimension Table Join)</li>
 *   <li>数据去重和校验</li>
 *   <li>实时点查询服务</li>
 *   <li>流式处理中的状态查找</li>
 * </ul>
 *
 * <h3>线程安全性</h3>
 * <p>当前 lookup 方法使用 synchronized 保证线程安全,未来计划支持多线程并发查询。
 *
 * @see TableQuery
 * @see org.apache.paimon.mergetree.LookupLevels
 * @see org.apache.paimon.lookup.LookupStoreFactory
 */
public class LocalTableQuery implements TableQuery {

    /** 表视图,按分区和分桶组织的查找层级映射。键为分区,值为桶号到查找层级的映射。 */
    private final Map<BinaryRow, Map<Integer, LookupLevels<KeyValue>>> tableView;

    /** 核心配置选项,控制查找行为和缓存策略。 */
    private final CoreOptions options;

    /** 主键比较器供应商,用于创建主键比较器以确定记录顺序。 */
    private final Supplier<Comparator<InternalRow>> keyComparatorSupplier;

    /** 读取器工厂构建器,用于创建文件读取器。 */
    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;

    /** 查找存储工厂,用于创建不同类型的查找存储(如 RocksDB)。 */
    private final LookupStoreFactory lookupStoreFactory;

    /** 查找起始层级,由配置决定。如果需要 lookup 功能则从第 1 层开始,否则从第 0 层开始。 */
    private final int startLevel;

    /** I/O 管理器,用于管理临时文件和磁盘空间。 */
    private IOManager ioManager;

    /** 查找文件缓存,缓存本地查找文件以加速查询。 */
    @Nullable private Cache<String, LookupFile> lookupFileCache;

    /** 表的行类型,包含所有字段的类型信息。 */
    private final RowType rowType;

    /** 分区类型,定义分区字段的类型信息。 */
    private final RowType partitionType;

    /** 缓存行过滤器,可选,用于过滤哪些数据需要缓存。 */
    @Nullable private Filter<InternalRow> cacheRowFilter;

    /**
     * 构造一个本地表查询实例。
     *
     * <p>该构造方法会初始化查找所需的所有组件,包括:
     * <ul>
     *   <li>验证表必须是带主键的表(KeyValueFileStore)</li>
     *   <li>创建文件读取器工厂构建器</li>
     *   <li>初始化主键比较器</li>
     *   <li>创建查找存储工厂(可能使用 RocksDB 等)</li>
     *   <li>确定查找起始层级</li>
     * </ul>
     *
     * @param table 要查询的文件存储表,必须是带主键的表
     * @throws UnsupportedOperationException 如果表不是 KeyValueFileStore 类型(即不带主键)
     */
    public LocalTableQuery(FileStoreTable table) {
        this.options = table.coreOptions();
        this.tableView = new HashMap<>();
        FileStore<?> tableStore = table.store();
        if (!(tableStore instanceof KeyValueFileStore)) {
            throw new UnsupportedOperationException(
                    "Table Query only supports table with primary key.");
        }
        KeyValueFileStore store = (KeyValueFileStore) tableStore;

        this.readerFactoryBuilder = store.newReaderFactoryBuilder();
        this.rowType = table.schema().logicalRowType();
        this.partitionType = table.schema().logicalPartitionType();
        RowType keyType = readerFactoryBuilder.keyType();
        this.keyComparatorSupplier = new KeyComparatorSupplier(readerFactoryBuilder.keyType());
        this.lookupStoreFactory =
                LookupStoreFactory.create(
                        options,
                        new CacheManager(
                                options.lookupCacheMaxMemory(),
                                options.lookupCacheHighPrioPoolRatio()),
                        new RowCompactedSerializer(keyType).createSliceComparator());
        startLevel = options.needLookup() ? 1 : 0;
    }

    /**
     * 增量刷新文件列表。
     *
     * <p>当表的文件发生变化时,需要调用此方法更新查找层级的文件列表。
     * 该方法支持增量更新,只处理变化的文件,避免全量重建。
     *
     * <p>处理逻辑:
     * <ul>
     *   <li>如果是首次为该分区桶创建查找层级,忽略 beforeFiles(视为删除操作),只添加新文件</li>
     *   <li>如果查找层级已存在,则更新层级结构:删除 beforeFiles,添加 dataFiles</li>
     * </ul>
     *
     * @param partition 分区键
     * @param bucket 分桶编号
     * @param beforeFiles 更新前的文件列表(需要从层级中删除的文件)
     * @param dataFiles 更新后的文件列表(需要添加到层级中的文件)
     */
    public void refreshFiles(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> beforeFiles,
            List<DataFileMeta> dataFiles) {
        LookupLevels<KeyValue> lookupLevels =
                tableView.computeIfAbsent(partition, k -> new HashMap<>()).get(bucket);
        if (lookupLevels == null) {
            // Initial phase: ignore beforeFiles as they represent deletions from previous state
            newLookupLevels(partition, bucket, dataFiles);
        } else {
            lookupLevels.getLevels().update(beforeFiles, dataFiles);
        }
    }

    /**
     * 为指定分区和分桶创建新的查找层级。
     *
     * <p>该方法负责初始化一个新的 LookupLevels 实例,设置所有必要的组件:
     * <ul>
     *   <li>创建 LSM 树层级结构</li>
     *   <li>初始化文件读取器工厂</li>
     *   <li>设置文件缓存(如果尚未创建)</li>
     *   <li>配置查找存储和布隆过滤器</li>
     * </ul>
     *
     * @param partition 分区键
     * @param bucket 分桶编号
     * @param dataFiles 该分区桶的数据文件列表
     */
    private void newLookupLevels(BinaryRow partition, int bucket, List<DataFileMeta> dataFiles) {
        Levels levels = new Levels(keyComparatorSupplier.get(), dataFiles, options.numLevels());
        // TODO pass DeletionVector factory
        KeyValueFileReaderFactory factory =
                readerFactoryBuilder.build(partition, bucket, DeletionVector.emptyFactory());
        Options options = this.options.toConfiguration();
        if (lookupFileCache == null) {
            lookupFileCache =
                    LookupFile.createCache(
                            options.get(CoreOptions.LOOKUP_CACHE_FILE_RETENTION),
                            options.get(CoreOptions.LOOKUP_CACHE_MAX_DISK_SIZE));
        }

        RowType readValueType = readerFactoryBuilder.readValueType();
        LookupLevels<KeyValue> lookupLevels =
                new LookupLevels<>(
                        schemaId -> readValueType,
                        0L,
                        levels,
                        keyComparatorSupplier.get(),
                        readerFactoryBuilder.keyType(),
                        PersistValueProcessor.factory(readValueType),
                        LookupSerializerFactory.INSTANCE.get(),
                        file -> {
                            RecordReader<KeyValue> reader = factory.createRecordReader(file);
                            if (cacheRowFilter != null) {
                                reader =
                                        reader.filter(
                                                keyValue -> cacheRowFilter.test(keyValue.value()));
                            }
                            return reader;
                        },
                        file ->
                                Preconditions.checkNotNull(ioManager, "IOManager is required.")
                                        .createChannel(
                                                localFilePrefix(
                                                        partitionType, partition, bucket, file))
                                        .getPathFile(),
                        lookupStoreFactory,
                        bfGenerator(options),
                        lookupFileCache);

        tableView.computeIfAbsent(partition, k -> new HashMap<>()).put(bucket, lookupLevels);
    }

    /**
     * 执行主键查找操作。
     *
     * <p>根据分区、分桶和主键信息,在对应的查找层级中查找匹配的数据行。
     * 查找从 startLevel 开始,遍历 LSM 树的各个层级直到找到匹配的记录。
     *
     * <p>查找结果:
     * <ul>
     *   <li>如果找到匹配的记录且未被删除,返回对应的数据行</li>
     *   <li>如果记录已被删除(valueKind 为 RETRACT),返回 null</li>
     *   <li>如果未找到匹配的记录,返回 null</li>
     *   <li>如果对应的分区或分桶不存在,返回 null</li>
     * </ul>
     *
     * @param partition 分区键,用于定位数据所在的分区
     * @param bucket 分桶编号,用于定位数据所在的桶
     * @param key 主键行数据,用于在桶内精确定位记录
     * @return 返回查找到的数据行,如果不存在或已被删除则返回 null
     * @throws IOException 如果读取文件时发生 I/O 错误
     *
     * @todo 移除 synchronized,支持多线程并发查找
     */
    /** TODO remove synchronized and supports multiple thread to lookup. */
    @Nullable
    @Override
    public synchronized InternalRow lookup(BinaryRow partition, int bucket, InternalRow key)
            throws IOException {
        Map<Integer, LookupLevels<KeyValue>> buckets = tableView.get(partition);
        if (buckets == null || buckets.isEmpty()) {
            return null;
        }
        LookupLevels<KeyValue> lookupLevels = buckets.get(bucket);
        if (lookupLevels == null) {
            return null;
        }

        KeyValue kv = lookupLevels.lookup(key, startLevel);
        if (kv == null || kv.valueKind().isRetract()) {
            return null;
        } else {
            return kv.value();
        }
    }

    /**
     * 设置值投影,指定需要读取的字段。
     *
     * <p>通过投影可以减少实际读取的字段数量,提高查询性能。
     * 投影后的字段会更新到读取器工厂构建器中。
     *
     * @param projection 字段索引数组,指定需要返回的字段位置
     * @return 返回当前实例,支持链式调用
     */
    @Override
    public LocalTableQuery withValueProjection(int[] projection) {
        this.readerFactoryBuilder.withReadValueType(rowType.project(projection));
        return this;
    }

    /**
     * 设置 I/O 管理器。
     *
     * <p>I/O 管理器用于管理临时文件和磁盘空间,在创建本地查找文件时需要使用。
     *
     * @param ioManager I/O 管理器实例
     * @return 返回当前实例,支持链式调用
     */
    public LocalTableQuery withIOManager(IOManager ioManager) {
        this.ioManager = ioManager;
        return this;
    }

    /**
     * 设置缓存行过滤器。
     *
     * <p>过滤器用于决定哪些数据行需要被缓存到本地。
     * 只有满足过滤条件的数据才会被缓存,从而节省内存和磁盘空间。
     *
     * @param cacheRowFilter 行过滤器,对不满足条件的行返回 false
     * @return 返回当前实例,支持链式调用
     */
    public LocalTableQuery withCacheRowFilter(Filter<InternalRow> cacheRowFilter) {
        this.cacheRowFilter = cacheRowFilter;
        return this;
    }

    /**
     * 创建值序列化器。
     *
     * <p>根据当前的读取类型(考虑投影后的类型)创建对应的序列化器。
     *
     * @return 返回内部行序列化器
     */
    @Override
    public InternalRowSerializer createValueSerializer() {
        return InternalSerializers.create(readerFactoryBuilder.readValueType());
    }

    /**
     * 关闭查询实例,释放所有资源。
     *
     * <p>该方法会:
     * <ul>
     *   <li>关闭所有分区和分桶的查找层级</li>
     *   <li>清空文件缓存</li>
     *   <li>清空表视图</li>
     * </ul>
     *
     * @throws IOException 如果关闭资源时发生 I/O 错误
     */
    @Override
    public void close() throws IOException {
        for (Map.Entry<BinaryRow, Map<Integer, LookupLevels<KeyValue>>> buckets :
                tableView.entrySet()) {
            for (Map.Entry<Integer, LookupLevels<KeyValue>> bucket :
                    buckets.getValue().entrySet()) {
                bucket.getValue().close();
            }
        }
        if (lookupFileCache != null) {
            lookupFileCache.invalidateAll();
        }
        tableView.clear();
    }
}
