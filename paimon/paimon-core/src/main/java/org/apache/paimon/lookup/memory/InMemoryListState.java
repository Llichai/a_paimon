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
import org.apache.paimon.lookup.ListBulkLoader;
import org.apache.paimon.lookup.ListState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.lookup.ByteArray.wrapBytes;

/**
 * 基于内存 HashMap + ArrayList 的列表状态实现.
 *
 * <p>InMemoryListState 使用 {@link HashMap} 存储键到列表的映射,
 * 每个键对应一个 {@link ArrayList},用于存储该键的所有值(序列化后的字节数组)。
 *
 * <h2>存储结构:</h2>
 * <pre>{@code
 * HashMap<ByteArray, List<byte[]>>
 *   键: ByteArray(序列化的键字节数组)
 *   值: ArrayList<byte[]>(序列化的值列表)
 * }</pre>
 *
 * <h2>主要特性:</h2>
 * <ul>
 *   <li><b>一对多映射</b>: 每个键对应一个值列表
 *   <li><b>保持顺序</b>: ArrayList 保持值的插入顺序
 *   <li><b>允许重复</b>: 列表中可以包含重复的值
 *   <li><b>仅追加</b>: 只支持添加操作,不支持删除单个值
 * </ul>
 *
 * <h2>内存开销:</h2>
 * <ul>
 *   <li>HashMap 本身的开销: 约 32 字节 + 数组开销
 *   <li>每个键的开销: ByteArray 对象 + ArrayList 对象 + HashMap Entry
 *   <li>每个值的开销: byte[] 对象 + ArrayList 槽位
 *   <li>总开销约为: 实际数据大小 * 3 倍
 * </ul>
 *
 * <h2>性能特点:</h2>
 * <ul>
 *   <li><b>添加延迟</b>: 纳秒级(HashMap.computeIfAbsent + ArrayList.add)
 *   <li><b>查询延迟</b>: 微秒级(HashMap.get + 反序列化整个列表)
 *   <li><b>内存占用</b>: 高(包含 HashMap、ArrayList 和序列化数据的开销)
 *   <li><b>GC 压力</b>: 中等到高(特别是查询时需要反序列化整个列表)
 * </ul>
 *
 * <h2>与 SetState 的区别:</h2>
 * <ul>
 *   <li>ListState 保持插入顺序,SetState 按字节序排序
 *   <li>ListState 允许重复值,SetState 保证值唯一性
 *   <li>ListState 只能追加,SetState 支持添加和撤回
 * </ul>
 *
 * <h2>使用场景:</h2>
 * <ul>
 *   <li>一对多关系数据,如订单明细、用户行为记录
 *   <li>需要保持插入顺序的场景
 *   <li>允许重复值的聚合场景
 *   <li>总数据量小于 100MB
 * </ul>
 *
 * <h2>性能优化建议:</h2>
 * <ul>
 *   <li>避免单个键对应过多的值(建议不超过 1000 个)
 *   <li>避免频繁查询,因为每次查询都需要反序列化整个列表
 *   <li>如果需要去重,使用 SetState 代替
 * </ul>
 *
 * <h2>使用示例:</h2>
 * <pre>{@code
 * // 创建内存列表状态
 * StateFactory factory = new InMemoryStateFactory();
 * ListState<Integer, String> state = factory.listState(
 *     "order-items",
 *     IntSerializer.INSTANCE,
 *     StringSerializer.INSTANCE,
 *     0L
 * );
 *
 * // 添加数据
 * state.add(1001, "item-A");
 * state.add(1001, "item-B");
 * state.add(1001, "item-C");
 * state.add(1001, "item-A");  // 允许重复
 *
 * // 查询数据(返回完整列表)
 * List<String> items = state.get(1001);
 * // ["item-A", "item-B", "item-C", "item-A"]
 *
 * // 批量加载
 * ListBulkLoader loader = state.createBulkLoader();
 * loader.write(
 *     serializeKey(1002),
 *     Arrays.asList(
 *         serializeValue("item-X"),
 *         serializeValue("item-Y")
 *     )
 * );
 * loader.finish();
 * }</pre>
 *
 * @param <K> 键的类型
 * @param <V> 值的类型
 * @see InMemoryState 内存状态基类
 * @see ListState 列表状态接口
 * @see InMemoryValueState 内存单值状态
 * @see InMemorySetState 内存集合状态
 */
public class InMemoryListState<K, V> extends InMemoryState<K, V> implements ListState<K, V> {

    /** 存储键到值列表的映射,键为序列化的字节数组,值为序列化的字节数组列表. */
    private final Map<ByteArray, List<byte[]>> values;

    /**
     * 构造内存列表状态.
     *
     * @param keySerializer 键的序列化器
     * @param valueSerializer 值的序列化器
     */
    public InMemoryListState(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(keySerializer, valueSerializer);
        this.values = new HashMap<>();
    }

    /**
     * 向指定键对应的列表中添加一个值.
     *
     * <p>执行流程:
     * <ol>
     *   <li>将键和值分别序列化为字节数组
     *   <li>使用 computeIfAbsent 获取或创建该键对应的列表
     *   <li>将值字节数组追加到列表末尾
     * </ol>
     *
     * <p>如果键不存在,会自动创建一个新的 ArrayList。
     *
     * <p>时间复杂度: O(1) - 序列化 + HashMap 查找 + ArrayList 追加
     *
     * @param key 键
     * @param value 要添加到列表的值
     * @throws IOException 如果序列化过程发生 I/O 错误
     */
    @Override
    public void add(K key, V value) throws IOException {
        byte[] keyBytes = serializeKey(key);
        byte[] valueBytes = serializeValue(value);
        values.computeIfAbsent(wrapBytes(keyBytes), k -> new ArrayList<>()).add(valueBytes);
    }

    /**
     * 获取指定键对应的完整值列表.
     *
     * <p>执行流程:
     * <ol>
     *   <li>将键序列化为字节数组
     *   <li>从 HashMap 中查找对应的字节数组列表
     *   <li>逐个反序列化字节数组为值对象
     *   <li>返回反序列化后的值列表
     * </ol>
     *
     * <p>如果键不存在,返回空列表(而非 null)。
     *
     * <p>注意: 该方法会反序列化整个列表,如果列表很大,性能开销会比较大。
     *
     * <p>时间复杂度: O(n) - n 为列表中值的数量
     *
     * @param key 键
     * @return 键对应的值列表,如果键不存在则返回空列表
     * @throws IOException 如果序列化或反序列化过程发生 I/O 错误
     */
    @Override
    public List<V> get(K key) throws IOException {
        List<byte[]> list = this.values.get(wrapBytes(serializeKey(key)));
        List<V> result = new ArrayList<>();
        if (list != null) {
            for (byte[] value : list) {
                result.add(deserializeValue(value));
            }
        }
        return result;
    }

    /**
     * 创建批量加载器.
     *
     * <p>返回的批量加载器直接将序列化后的键和值列表写入 HashMap。
     * 注意批量加载器会复制值列表,避免外部修改影响内部状态。
     *
     * @return 批量加载器实例
     */
    @Override
    public ListBulkLoader createBulkLoader() {
        return new ListBulkLoader() {

            /**
             * 写入一个键及其对应的值列表.
             *
             * <p>该方法会复制值列表,避免外部修改影响内部状态。
             *
             * @param key 序列化后的键字节数组
             * @param value 序列化后的值列表,列表中每个元素都是字节数组
             */
            @Override
            public void write(byte[] key, List<byte[]> value) {
                // copy the list, outside will reuse list
                values.put(wrapBytes(key), new ArrayList<>(value));
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
