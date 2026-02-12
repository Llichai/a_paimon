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

import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.lookup.ByteArray;
import org.apache.paimon.lookup.ValueState;

import org.rocksdb.ColumnFamilyHandle;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 基于 RocksDB 的单值状态实现.
 *
 * <p>RocksDBValueState 使用 RocksDB 存储键值对,支持大规模持久化状态数据。
 * 结合 Caffeine LRU 缓存,在保证容量的同时提供较好的查询性能。
 *
 * <h2>存储机制:</h2>
 * <ul>
 *   <li>数据直接存储在 RocksDB 的列族中
 *   <li>查询时先查缓存,缓存未命中则查 RocksDB
 *   <li>写入时同时更新 RocksDB 和缓存
 *   <li>删除时先检查键是否存在,存在才执行删除并更新缓存
 * </ul>
 *
 * <h2>性能特点:</h2>
 * <ul>
 *   <li><b>查询延迟</b>: 缓存命中时纳秒级,未命中时微秒级
 *   <li><b>写入延迟</b>: 微秒级(写 MemTable)
 *   <li><b>容量</b>: 几乎无限(受磁盘空间限制)
 * </ul>
 *
 * @param <K> 键的类型
 * @param <V> 值的类型
 * @see RocksDBState RocksDB 状态基类
 * @see ValueState 单值状态接口
 */
public class RocksDBValueState<K, V> extends RocksDBState<K, V, RocksDBState.Reference>
        implements ValueState<K, V> {

    public RocksDBValueState(
            RocksDBStateFactory stateFactory,
            ColumnFamilyHandle columnFamily,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize) {
        super(stateFactory, columnFamily, keySerializer, valueSerializer, lruCacheSize);
    }

    @Nullable
    @Override
    public V get(K key) throws IOException {
        try {
            Reference valueRef = get(wrap(serializeKey(key)));
            return valueRef.isPresent() ? deserializeValue(valueRef.bytes) : null;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private Reference get(ByteArray keyBytes) throws Exception {
        Reference valueRef = cache.getIfPresent(keyBytes);
        if (valueRef == null) {
            valueRef = ref(db.get(columnFamily, keyBytes.bytes));
            cache.put(keyBytes, valueRef);
        }

        return valueRef;
    }

    @Override
    public void put(K key, V value) throws IOException {
        checkArgument(value != null);

        try {
            byte[] keyBytes = serializeKey(key);
            byte[] valueBytes = serializeValue(value);
            db.put(columnFamily, writeOptions, keyBytes, valueBytes);
            cache.put(wrap(keyBytes), ref(valueBytes));
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void delete(K key) throws IOException {
        try {
            byte[] keyBytes = serializeKey(key);
            ByteArray keyByteArray = wrap(keyBytes);
            if (get(keyByteArray).isPresent()) {
                db.delete(columnFamily, writeOptions, keyBytes);
                cache.put(keyByteArray, ref(null));
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
