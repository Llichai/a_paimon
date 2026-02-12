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
import org.apache.paimon.lookup.SetState;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 基于 RocksDB 的集合状态实现.
 *
 * <p>RocksDBSetState 使用复合键(键 + 值)存储集合中的每个元素,
 * 通过前缀扫描实现集合查询操作。
 *
 * <h2>存储机制:</h2>
 * <ul>
 *   <li><b>复合键设计</b>: 将 (key, value) 组合为 RocksDB 的键,值为空字节数组
 *   <li><b>前缀扫描</b>: 查询时使用键前缀扫描所有匹配的值
 *   <li><b>自然排序</b>: RocksDB 的键是有序的,天然支持排序查询
 *   <li><b>缓存策略</b>: 缓存查询结果(字节数组列表),添加/删除时失效缓存
 * </ul>
 *
 * <h2>性能特点:</h2>
 * <ul>
 *   <li><b>添加/删除延迟</b>: 微秒级(单次 put/delete 操作)
 *   <li><b>查询延迟</b>: 毫秒级(需要前缀扫描)
 *   <li><b>缓存失效</b>: 每次修改都会使缓存失效
 * </ul>
 *
 * <h2>使用约束:</h2>
 * <ul>
 *   <li>查询性能随集合大小线性增长(O(n))
 *   <li>建议单个键对应的值数量不超过 1000 个
 * </ul>
 *
 * @param <K> 键的类型
 * @param <V> 值的类型
 * @see RocksDBState RocksDB 状态基类
 * @see SetState 集合状态接口
 */
public class RocksDBSetState<K, V> extends RocksDBState<K, V, List<byte[]>>
        implements SetState<K, V> {

    private static final byte[] EMPTY = new byte[0];

    public RocksDBSetState(
            RocksDBStateFactory stateFactory,
            ColumnFamilyHandle columnFamily,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize) {
        super(stateFactory, columnFamily, keySerializer, valueSerializer, lruCacheSize);
    }

    @Override
    public List<V> get(K key) throws IOException {
        ByteArray keyBytes = wrap(serializeKey(key));
        List<byte[]> valueBytes = cache.getIfPresent(keyBytes);
        if (valueBytes == null) {
            valueBytes = new ArrayList<>();
            try (RocksIterator iterator = db.newIterator(columnFamily)) {
                iterator.seek(keyBytes.bytes);

                while (iterator.isValid() && startWithKeyPrefix(keyBytes.bytes, iterator.key())) {
                    byte[] rawKeyBytes = iterator.key();
                    byte[] value =
                            Arrays.copyOfRange(
                                    rawKeyBytes, keyBytes.bytes.length, rawKeyBytes.length);
                    valueBytes.add(value);
                    iterator.next();
                }
            }
            cache.put(keyBytes, valueBytes);
        }

        List<V> values = new ArrayList<>(valueBytes.size());
        for (byte[] value : valueBytes) {
            valueInputView.setBuffer(value);
            values.add(valueSerializer.deserialize(valueInputView));
        }
        return values;
    }

    @Override
    public void retract(K key, V value) throws IOException {
        try {
            byte[] bytes = invalidKeyAndGetKVBytes(key, value);
            if (db.get(columnFamily, bytes) != null) {
                db.delete(columnFamily, writeOptions, bytes);
            }
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void add(K key, V value) throws IOException {
        try {
            byte[] bytes = invalidKeyAndGetKVBytes(key, value);
            db.put(columnFamily, writeOptions, bytes, EMPTY);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    private byte[] invalidKeyAndGetKVBytes(K key, V value) throws IOException {
        checkArgument(value != null);

        keyOutView.clear();
        keySerializer.serialize(key, keyOutView);

        // it is hard to maintain cache, invalidate the key.
        cache.invalidate(wrap(keyOutView.getCopyOfBuffer()));

        valueSerializer.serialize(value, keyOutView);
        return keyOutView.getCopyOfBuffer();
    }

    private boolean startWithKeyPrefix(byte[] keyPrefixBytes, byte[] rawKeyBytes) {
        if (rawKeyBytes.length < keyPrefixBytes.length) {
            return false;
        }

        for (int i = keyPrefixBytes.length; --i >= 0; ) {
            if (rawKeyBytes[i] != keyPrefixBytes[i]) {
                return false;
            }
        }

        return true;
    }
}
