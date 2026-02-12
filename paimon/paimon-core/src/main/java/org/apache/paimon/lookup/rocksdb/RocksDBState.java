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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.lookup.ByteArray;
import org.apache.paimon.lookup.State;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * 基于 RocksDB 的状态抽象基类,提供持久化的键值存储.
 *
 * <p>RocksDBState 是所有 RocksDB 状态实现的基础类,使用 RocksDB 作为底层存储引擎,
 * 支持大规模状态数据的持久化存储。数据存储在堆外内存和磁盘上,不受 JVM 堆大小限制。
 *
 * <h2>主要特性:</h2>
 * <ul>
 *   <li><b>持久化存储</b>: 数据存储在磁盘,支持 TB 级状态
 *   <li><b>堆外内存</b>: 使用堆外内存,不占用 JVM 堆空间
 *   <li><b>LRU 缓存</b>: 内置 Caffeine 缓存,减少磁盘访问
 *   <li><b>列族隔离</b>: 使用 RocksDB 列族(ColumnFamily)隔离不同的状态
 *   <li><b>禁用 WAL</b>: 禁用写前日志(WAL)以提高写入性能
 * </ul>
 *
 * <h2>存储架构:</h2>
 * <pre>{@code
 * RocksDB 实例
 *   ├── 列族 1 (状态1)
 *   ├── 列族 2 (状态2)
 *   └── ...
 *
 * 内存层:
 *   ├── Caffeine LRU 缓存 (热点数据)
 *   ├── RocksDB MemTable (写缓冲)
 *   └── Block Cache (读缓冲)
 *
 * 磁盘层:
 *   └── SST 文件 (LSM-tree 结构)
 * }</pre>
 *
 * <h2>缓存机制:</h2>
 * <ul>
 *   <li><b>Caffeine 缓存</b>: 应用层 LRU 缓存,缓存反序列化后的对象,避免重复反序列化
 *   <li><b>Soft Values</b>: 使用软引用,在内存紧张时可被 GC 回收
 *   <li><b>同步执行器</b>: 使用同步执行器(Runnable::run),避免额外的线程开销
 * </ul>
 *
 * <h2>写入优化:</h2>
 * <ul>
 *   <li><b>禁用 WAL</b>: 禁用写前日志,提高写入性能(可能丢失少量未刷盘数据)
 *   <li><b>批量写入</b>: 支持通过 BulkLoader 批量导入 SST 文件
 * </ul>
 *
 * <h2>性能特点:</h2>
 * <ul>
 *   <li><b>查询延迟</b>: 微秒级(缓存命中时纳秒级)
 *   <li><b>写入延迟</b>: 微秒级(写 MemTable)
 *   <li><b>内存占用</b>: 可控(通过配置 MemTable、Block Cache 大小)
 *   <li><b>磁盘占用</b>: 实际数据大小 * 1.2-1.5 倍(LSM-tree 放大)
 * </ul>
 *
 * <h2>与 InMemory 状态的对比:</h2>
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>RocksDB</th>
 *     <th>InMemory</th>
 *   </tr>
 *   <tr>
 *     <td>存储位置</td>
 *     <td>堆外内存 + 磁盘</td>
 *     <td>JVM 堆内存</td>
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
 *     <td>GC 压力</td>
 *     <td>低</td>
 *     <td>高</td>
 *   </tr>
 * </table>
 *
 * <h2>使用场景:</h2>
 * <ul>
 *   <li>大维度表数据(GB-TB 级)
 *   <li>需要持久化状态数据
 *   <li>状态数据量超过 JVM 堆大小
 *   <li>需要跨进程共享状态
 * </ul>
 *
 * <h2>典型子类:</h2>
 * <ul>
 *   <li>RocksDBValueState: 单值状态,键值一对一映射
 *   <li>RocksDBListState: 列表状态,使用 merge 操作追加值
 *   <li>RocksDBSetState: 集合状态,使用前缀扫描获取值集合
 * </ul>
 *
 * @param <K> 键的类型
 * @param <V> 值的类型
 * @param <CacheV> 缓存值的类型(可能与 V 不同,如 Reference、List 等)
 * @see RocksDBValueState 单值状态实现
 * @see RocksDBListState 列表状态实现
 * @see RocksDBSetState 集合状态实现
 * @see RocksDBStateFactory RocksDB 状态工厂
 */
public abstract class RocksDBState<K, V, CacheV> implements State<K, V> {

    /** RocksDB 状态工厂,提供 RocksDB 实例和配置. */
    protected final RocksDBStateFactory stateFactory;

    /** RocksDB 实例. */
    protected final RocksDB db;

    /** 写入选项,禁用 WAL 以提高性能. */
    protected final WriteOptions writeOptions;

    /** 列族句柄,用于隔离不同的状态. */
    protected final ColumnFamilyHandle columnFamily;

    /** 键的序列化器. */
    protected final Serializer<K> keySerializer;

    /** 值的序列化器. */
    protected final Serializer<V> valueSerializer;

    /** 键的序列化输出视图,复用以减少对象创建. */
    protected final DataOutputSerializer keyOutView;

    /** 值的反序列化输入视图,复用以减少对象创建. */
    protected final DataInputDeserializer valueInputView;

    /** 值的序列化输出视图,复用以减少对象创建. */
    protected final DataOutputSerializer valueOutputView;

    /** LRU 缓存,用于缓存热点数据,使用软引用避免内存溢出. */
    protected final Cache<ByteArray, CacheV> cache;

    /**
     * 构造 RocksDB 状态基类.
     *
     * <p>初始化序列化器、输入输出视图、写入选项和 LRU 缓存。
     *
     * <h3>写入选项配置:</h3>
     * <ul>
     *   <li>禁用 WAL(Write-Ahead Log): 提高写入性能,但可能丢失少量未刷盘数据
     * </ul>
     *
     * <h3>缓存配置:</h3>
     * <ul>
     *   <li>使用软引用(softValues): 在内存紧张时可被 GC 回收
     *   <li>LRU 淘汰策略: 当缓存满时淘汰最久未使用的条目
     *   <li>同步执行器: 避免额外的线程开销
     * </ul>
     *
     * @param stateFactory RocksDB 状态工厂
     * @param columnFamily 列族句柄
     * @param keySerializer 键的序列化器
     * @param valueSerializer 值的序列化器
     * @param lruCacheSize LRU 缓存的最大条目数
     */
    public RocksDBState(
            RocksDBStateFactory stateFactory,
            ColumnFamilyHandle columnFamily,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize) {
        this.stateFactory = stateFactory;
        this.db = stateFactory.db();
        this.columnFamily = columnFamily;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.keyOutView = new DataOutputSerializer(32);
        this.valueInputView = new DataInputDeserializer();
        this.valueOutputView = new DataOutputSerializer(32);
        this.writeOptions = new WriteOptions().setDisableWAL(true);
        this.cache =
                Caffeine.newBuilder()
                        .softValues()
                        .maximumSize(lruCacheSize)
                        .executor(Runnable::run)
                        .build();
    }

    /**
     * 将键对象序列化为字节数组.
     *
     * @param key 要序列化的键对象
     * @return 序列化后的字节数组
     * @throws IOException 如果序列化过程发生 I/O 错误
     */
    @Override
    public byte[] serializeKey(K key) throws IOException {
        keyOutView.clear();
        keySerializer.serialize(key, keyOutView);
        return keyOutView.getCopyOfBuffer();
    }

    /**
     * 将值对象序列化为字节数组.
     *
     * @param value 要序列化的值对象
     * @return 序列化后的字节数组
     * @throws IOException 如果序列化过程发生 I/O 错误
     */
    @Override
    public byte[] serializeValue(V value) throws IOException {
        valueOutputView.clear();
        valueSerializer.serialize(value, valueOutputView);
        return valueOutputView.getCopyOfBuffer();
    }

    /**
     * 从字节数组反序列化值对象.
     *
     * @param valueBytes 序列化的字节数组
     * @return 反序列化后的值对象
     * @throws IOException 如果反序列化过程发生 I/O 错误
     */
    @Override
    public V deserializeValue(byte[] valueBytes) throws IOException {
        valueInputView.setBuffer(valueBytes);
        return valueSerializer.deserialize(valueInputView);
    }

    /**
     * 将字节数组包装为 ByteArray.
     *
     * <p>ByteArray 提供了正确的 equals 和 hashCode 实现,可以用作 HashMap 的键。
     *
     * @param bytes 字节数组
     * @return ByteArray 实例
     */
    protected ByteArray wrap(byte[] bytes) {
        return new ByteArray(bytes);
    }

    /**
     * 将字节数组包装为 Reference.
     *
     * <p>Reference 用于区分"值不存在"和"值为空"两种情况。
     *
     * @param bytes 字节数组,可能为 null
     * @return Reference 实例
     */
    protected Reference ref(byte[] bytes) {
        return new Reference(bytes);
    }

    /**
     * 创建 RocksDB 批量加载器.
     *
     * <p>批量加载器使用 RocksDB 的 SstFileWriter 直接生成 SST 文件,
     * 然后通过 ingestExternalFile API 将文件导入到数据库,避免了常规写入的写放大。
     *
     * @return RocksDB 批量加载器实例
     */
    public RocksDBBulkLoader createBulkLoader() {
        return new RocksDBBulkLoader(db, stateFactory.options(), columnFamily, stateFactory.path());
    }

    /**
     * 创建用于批量加载排序的外部排序缓冲区.
     *
     * <p>该方法创建一个二进制外部排序缓冲区,用于在批量加载前对键值对进行排序。
     * 排序是批量加载的前提条件,因为 SST 文件要求键必须有序。
     *
     * <h3>排序配置:</h3>
     * <ul>
     *   <li>排序键: 第一列(键字节数组)
     *   <li>写缓冲区大小: options.writeBufferSize() / 2
     *   <li>页大小: options.pageSize()
     *   <li>溢写压缩: options.spillCompressOptions()
     * </ul>
     *
     * @param ioManager I/O 管理器,用于管理临时文件
     * @param options 核心配置选项
     * @return 二进制外部排序缓冲区
     */
    public static BinaryExternalSortBuffer createBulkLoadSorter(
            IOManager ioManager, CoreOptions options) {
        return BinaryExternalSortBuffer.create(
                ioManager,
                RowType.of(DataTypes.BYTES(), DataTypes.BYTES()),
                new int[] {0},
                options.writeBufferSize() / 2,
                options.pageSize(),
                options.localSortMaxNumFileHandles(),
                options.spillCompressOptions(),
                options.writeBufferSpillDiskSize(),
                options.sequenceFieldSortOrderIsAscending());
    }

    /**
     * 引用包装类,用于区分"值不存在"和"值为空"两种情况.
     *
     * <p>在缓存中,我们需要区分以下两种情况:
     * <ul>
     *   <li>键不存在: Reference(null)
     *   <li>键存在但值为空字节数组: Reference(new byte[0])
     * </ul>
     *
     * <p>如果直接使用 byte[],无法区分这两种情况(都是 null 或空数组)。
     */
    protected static class Reference {

        /** 字节数组,null 表示键不存在. */
        @Nullable protected final byte[] bytes;

        /**
         * 构造引用.
         *
         * @param bytes 字节数组,null 表示键不存在
         */
        protected Reference(@Nullable byte[] bytes) {
            this.bytes = bytes;
        }

        /**
         * 判断值是否存在.
         *
         * @return 如果值存在返回 true,否则返回 false
         */
        public boolean isPresent() {
            return bytes != null;
        }
    }
}
