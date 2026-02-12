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
import org.apache.paimon.lookup.SetState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.paimon.lookup.ByteArray.wrapBytes;

/**
 * 基于内存 HashMap + TreeSet 的集合状态实现.
 *
 * <p>InMemorySetState 使用 {@link HashMap} 存储键到集合的映射,
 * 每个键对应一个 {@link TreeSet},用于存储该键的所有唯一值(序列化后的字节数组)。
 *
 * <h2>存储结构:</h2>
 * <pre>{@code
 * HashMap<ByteArray, TreeSet<ByteArray>>
 *   键: ByteArray(序列化的键字节数组)
 *   值: TreeSet<ByteArray>(序列化的值集合,自动排序和去重)
 * }</pre>
 *
 * <h2>主要特性:</h2>
 * <ul>
 *   <li><b>一对多映射</b>: 每个键对应一个值集合
 *   <li><b>自动去重</b>: TreeSet 保证值的唯一性
 *   <li><b>自动排序</b>: TreeSet 按字节数组的字典序自动排序
 *   <li><b>双向操作</b>: 支持 add(添加)和 retract(撤回)操作
 * </ul>
 *
 * <h2>内存开销:</h2>
 * <ul>
 *   <li>HashMap 本身的开销: 约 32 字节 + 数组开销
 *   <li>每个键的开销: ByteArray 对象 + TreeSet 对象 + HashMap Entry
 *   <li>每个值的开销: ByteArray 对象 + TreeSet 节点(红黑树节点,约 40 字节)
 *   <li>总开销约为: 实际数据大小 * 4 倍
 * </ul>
 *
 * <h2>性能特点:</h2>
 * <ul>
 *   <li><b>添加延迟</b>: 微秒级(HashMap.computeIfAbsent + TreeSet.add 红黑树插入)
 *   <li><b>撤回延迟</b>: 微秒级(HashMap.get + TreeSet.remove 红黑树删除)
 *   <li><b>查询延迟</b>: 微秒级(HashMap.get + 反序列化整个集合)
 *   <li><b>内存占用</b>: 很高(包含 HashMap、TreeSet 红黑树节点和序列化数据的开销)
 *   <li><b>GC 压力</b>: 高(特别是查询时需要反序列化整个集合)
 * </ul>
 *
 * <h2>与 ListState 的区别:</h2>
 * <ul>
 *   <li>SetState 按字节序排序,ListState 保持插入顺序
 *   <li>SetState 保证值唯一性,ListState 允许重复
 *   <li>SetState 支持撤回,ListState 只能追加
 *   <li>SetState 使用 TreeSet(O(log n)),ListState 使用 ArrayList(O(1))
 * </ul>
 *
 * <h2>排序规则:</h2>
 * <p>值按序列化后的字节数组进行字典序比较(无符号字节比较)。
 * 例如:
 * <ul>
 *   <li>整数: [1] < [2] < [10] < [100] (按字节序,非数值序)
 *   <li>字符串: "a" < "ab" < "b" (按字典序)
 * </ul>
 *
 * <h2>使用场景:</h2>
 * <ul>
 *   <li>需要去重的一对多关系,如用户标签、商品类别
 *   <li>需要保持排序的集合数据
 *   <li>支持动态增删的集合状态
 *   <li>总数据量小于 100MB
 * </ul>
 *
 * <h2>性能优化建议:</h2>
 * <ul>
 *   <li>避免单个键对应过多的值(建议不超过 500 个,因为 TreeSet 操作为 O(log n))
 *   <li>避免频繁查询,因为每次查询都需要反序列化整个集合
 *   <li>如果不需要排序和撤回,使用 ListState 性能更好
 *   <li>如果值很多,考虑使用 RocksDBSetState
 * </ul>
 *
 * <h2>使用示例:</h2>
 * <pre>{@code
 * // 创建内存集合状态
 * StateFactory factory = new InMemoryStateFactory();
 * SetState<Integer, String> state = factory.setState(
 *     "user-tags",
 *     IntSerializer.INSTANCE,
 *     StringSerializer.INSTANCE,
 *     0L
 * );
 *
 * // 添加标签
 * state.add(100, "vip");
 * state.add(100, "active");
 * state.add(100, "vip");  // 重复添加,不会增加第二次
 *
 * // 查询标签(按字典序返回)
 * List<String> tags = state.get(100);
 * // ["active", "vip"] (按字典序排序)
 *
 * // 撤回标签
 * state.retract(100, "vip");
 * tags = state.get(100);
 * // ["active"]
 *
 * // 再次添加
 * state.add(100, "premium");
 * tags = state.get(100);
 * // ["active", "premium"]
 * }</pre>
 *
 * @param <K> 键的类型
 * @param <V> 值的类型
 * @see InMemoryState 内存状态基类
 * @see SetState 集合状态接口
 * @see InMemoryValueState 内存单值状态
 * @see InMemoryListState 内存列表状态
 */
public class InMemorySetState<K, V> extends InMemoryState<K, V> implements SetState<K, V> {

    /** 存储键到值集合的映射,键为序列化的字节数组,值为有序去重的字节数组集合. */
    private final Map<ByteArray, TreeSet<ByteArray>> values;

    /**
     * 构造内存集合状态.
     *
     * @param keySerializer 键的序列化器
     * @param valueSerializer 值的序列化器
     */
    public InMemorySetState(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(keySerializer, valueSerializer);
        this.values = new HashMap<>();
    }

    /**
     * 获取指定键对应的所有值,并按字节数组排序返回.
     *
     * <p>执行流程:
     * <ol>
     *   <li>将键序列化为字节数组
     *   <li>从 HashMap 中查找对应的 TreeSet
     *   <li>遍历 TreeSet(已排序),逐个反序列化字节数组为值对象
     *   <li>返回反序列化后的值列表
     * </ol>
     *
     * <p>返回的列表中的值是唯一的,并按序列化后字节数组的字典序排列。
     * 如果键不存在,返回空列表(而非 null)。
     *
     * <p>时间复杂度: O(n) - n 为集合中值的数量
     *
     * @param key 键
     * @return 键对应的值列表(已排序且去重),如果键不存在则返回空列表
     * @throws IOException 如果序列化或反序列化过程发生 I/O 错误
     */
    @Override
    public List<V> get(K key) throws IOException {
        Set<ByteArray> set = values.get(wrapBytes(serializeKey(key)));
        List<V> result = new ArrayList<>();
        if (set != null) {
            for (ByteArray value : set) {
                result.add(deserializeValue(value.bytes));
            }
        }
        return result;
    }

    /**
     * 撤回(删除)指定键对应集合中的一个值.
     *
     * <p>执行流程:
     * <ol>
     *   <li>将键序列化为字节数组
     *   <li>从 HashMap 中获取对应的 TreeSet
     *   <li>将值序列化为字节数组
     *   <li>从 TreeSet 中移除该值
     * </ol>
     *
     * <p>如果值不存在于集合中,该操作不会产生影响。
     *
     * <p>时间复杂度: O(log n) - TreeSet 删除操作
     *
     * @param key 键
     * @param value 要撤回的值
     * @throws IOException 如果序列化过程发生 I/O 错误
     */
    @Override
    public void retract(K key, V value) throws IOException {
        values.get(wrapBytes(serializeKey(key))).remove(wrapBytes(serializeValue(value)));
    }

    /**
     * 向指定键对应的集合中添加一个值.
     *
     * <p>执行流程:
     * <ol>
     *   <li>将键和值分别序列化为字节数组
     *   <li>使用 computeIfAbsent 获取或创建该键对应的 TreeSet
     *   <li>将值字节数组添加到 TreeSet 中(自动去重和排序)
     * </ol>
     *
     * <p>如果值已存在于集合中,该操作不会重复添加。
     * TreeSet 会自动保持值的字典序排序。
     *
     * <p>时间复杂度: O(log n) - TreeSet 插入操作
     *
     * @param key 键
     * @param value 要添加的值
     * @throws IOException 如果序列化过程发生 I/O 错误
     */
    @Override
    public void add(K key, V value) throws IOException {
        byte[] keyBytes = serializeKey(key);
        byte[] valueBytes = serializeValue(value);
        values.computeIfAbsent(wrapBytes(keyBytes), k -> new TreeSet<>())
                .add(wrapBytes(valueBytes));
    }
}
