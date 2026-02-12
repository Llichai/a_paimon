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

package org.apache.paimon.lookup.memory;

import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.lookup.ByteArray;
import org.apache.paimon.lookup.ValueBulkLoader;
import org.apache.paimon.lookup.ValueState;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.lookup.ByteArray.wrapBytes;

/**
 * 基于内存 HashMap 的单值状态实现.
 *
 * <p>InMemoryValueState 使用标准的 Java {@link HashMap} 存储键值对,
 * 键为序列化后的字节数组(包装为 ByteArray),值为序列化后的字节数组。
 *
 * <h2>存储结构:</h2>
 * <pre>{@code
 * HashMap<ByteArray, byte[]>
 *   键: ByteArray(序列化的键字节数组)
 *   值: byte[](序列化的值字节数组)
 * }</pre>
 *
 * <h2>主要特性:</h2>
 * <ul>
 *   <li><b>高性能</b>: 基于 HashMap,查询和写入时间复杂度为 O(1)
 *   <li><b>序列化存储</b>: 键和值都以序列化字节数组形式存储,节省内存
 *   <li><b>可空值</b>: 支持查询不存在的键,返回 null
 *   <li><b>完整 CRUD</b>: 支持 get、put、delete 操作
 * </ul>
 *
 * <h2>内存开销:</h2>
 * <ul>
 *   <li>HashMap 本身的开销: 约 32 字节 + 数组开销
 *   <li>每个键值对的开销: ByteArray 对象 + byte[] 对象 + HashMap Entry
 *   <li>总开销约为: 实际数据大小 * 2.5 倍
 * </ul>
 *
 * <h2>性能特点:</h2>
 * <ul>
 *   <li><b>查询延迟</b>: 纳秒级(HashMap.get + 反序列化)
 *   <li><b>写入延迟</b>: 纳秒级(序列化 + HashMap.put)
 *   <li><b>内存占用</b>: 高(包含 HashMap 和序列化数据的开销)
 *   <li><b>GC 压力</b>: 中等(频繁的序列化/反序列化会产生临时对象)
 * </ul>
 *
 * <h2>批量加载:</h2>
 * <p>虽然提供了批量加载器,但由于 HashMap 的插入本身就很快,批量加载并没有显著的性能优势。
 * 批量加载器主要用于保持接口一致性。
 *
 * <h2>使用场景:</h2>
 * <ul>
 *   <li>维度表数据量小于 100MB
 *   <li>需要极低延迟的键值查询
 *   <li>数据不需要持久化
 *   <li>一对一的键值映射关系
 * </ul>
 *
 * <h2>使用示例:</h2>
 * <pre>{@code
 * // 创建内存单值状态
 * StateFactory factory = new InMemoryStateFactory();
 * ValueState<Integer, String> state = factory.valueState(
 *     "user-cache",
 *     IntSerializer.INSTANCE,
 *     StringSerializer.INSTANCE,
 *     0L
 * );
 *
 * // 写入数据
 * state.put(1, "Alice");
 * state.put(2, "Bob");
 * state.put(3, "Charlie");
 *
 * // 查询数据
 * String user1 = state.get(1);  // "Alice"
 * String user4 = state.get(4);  // null
 *
 * // 删除数据
 * state.delete(2);
 * String user2 = state.get(2);  // null
 *
 * // 批量加载(可选)
 * ValueBulkLoader loader = state.createBulkLoader();
 * loader.write(serializeKey(10), serializeValue("David"));
 * loader.write(serializeKey(11), serializeValue("Eve"));
 * loader.finish();
 * }</pre>
 *
 * @param <K> 键的类型
 * @param <V> 值的类型
 * @see InMemoryState 内存状态基类
 * @see ValueState 单值状态接口
 * @see InMemoryListState 内存列表状态
 * @see InMemorySetState 内存集合状态
 */
public class InMemoryValueState<K, V> extends InMemoryState<K, V> implements ValueState<K, V> {

    /** 存储键值对的 HashMap,键为序列化的字节数组,值为序列化的字节数组. */
    private final Map<ByteArray, byte[]> values;

    /**
     * 构造内存单值状态.
     *
     * @param keySerializer 键的序列化器
     * @param valueSerializer 值的序列化器
     */
    public InMemoryValueState(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(keySerializer, valueSerializer);
        this.values = new HashMap<>();
    }

    /**
     * 根据键获取对应的值.
     *
     * <p>执行流程:
     * <ol>
     *   <li>将键对象序列化为字节数组
     *   <li>用字节数组包装为 ByteArray
     *   <li>从 HashMap 中查找对应的值字节数组
     *   <li>如果存在,反序列化为值对象;否则返回 null
     * </ol>
     *
     * <p>时间复杂度: O(1) - HashMap 查找 + 序列化/反序列化
     *
     * @param key 要查询的键
     * @return 键对应的值,如果键不存在则返回 null
     * @throws IOException 如果序列化或反序列化过程发生 I/O 错误
     */
    @Override
    public @Nullable V get(K key) throws IOException {
        byte[] bytes = values.get(wrapBytes(serializeKey(key)));
        if (bytes == null) {
            return null;
        }
        return deserializeValue(bytes);
    }

    /**
     * 插入或更新键值对.
     *
     * <p>执行流程:
     * <ol>
     *   <li>将键对象序列化为字节数组,包装为 ByteArray
     *   <li>将值对象序列化为字节数组
     *   <li>将键值对存入 HashMap
     * </ol>
     *
     * <p>如果键已存在,旧值会被覆盖。
     *
     * <p>时间复杂度: O(1) - 序列化 + HashMap 插入
     *
     * @param key 要插入或更新的键
     * @param value 要存储的值
     * @throws IOException 如果序列化过程发生 I/O 错误
     */
    @Override
    public void put(K key, V value) throws IOException {
        values.put(wrapBytes(serializeKey(key)), serializeValue(value));
    }

    /**
     * 删除指定键及其对应的值.
     *
     * <p>执行流程:
     * <ol>
     *   <li>将键对象序列化为字节数组,包装为 ByteArray
     *   <li>从 HashMap 中移除该键
     * </ol>
     *
     * <p>如果键不存在,该操作不会产生任何影响。
     *
     * <p>时间复杂度: O(1) - 序列化 + HashMap 删除
     *
     * @param key 要删除的键
     * @throws IOException 如果序列化过程发生 I/O 错误
     */
    @Override
    public void delete(K key) throws IOException {
        values.remove(wrapBytes(serializeKey(key)));
    }

    /**
     * 创建批量加载器.
     *
     * <p>返回的批量加载器直接将序列化后的键值对写入 HashMap。
     * 对于内存状态,批量加载没有显著的性能优势,主要用于保持接口一致性。
     *
     * <p>批量加载器的 finish() 方法为空操作,因为内存状态不需要刷新或构建索引。
     *
     * @return 批量加载器实例
     */
    @Override
    public ValueBulkLoader createBulkLoader() {
        return new ValueBulkLoader() {

            /**
             * 写入一个键值对.
             *
             * @param key 序列化后的键字节数组
             * @param value 序列化后的值字节数组
             */
            @Override
            public void write(byte[] key, byte[] value) {
                values.put(wrapBytes(key), value);
            }

            /**
             * 完成批量加载.
             *
             * <p>对于内存状态,该方法为空操作。
             */
            @Override
            public void finish() {}
        };
    }
}
