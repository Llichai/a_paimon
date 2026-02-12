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

package org.apache.paimon.data;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * 通用Map数据结构,包装Java Map实现{@link InternalMap}接口。
 *
 * <h2>设计目标</h2>
 * <p>GenericMap是{@link InternalMap}的通用实现,用于表示{@link MapType}或{@link MultisetType}类型的数据。
 * 与{@link BinaryMap}相比:
 *
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>GenericMap</th>
 *     <th>BinaryMap</th>
 *   </tr>
 *   <tr>
 *     <td>存储方式</td>
 *     <td>包装Java Map</td>
 *     <td>二进制内存段</td>
 *   </tr>
 *   <tr>
 *     <td>适用场景</td>
 *     <td>灵活操作,易于使用</td>
 *     <td>高性能,零拷贝</td>
 *   </tr>
 *   <tr>
 *     <td>内存效率</td>
 *     <td>较低(Java对象开销)</td>
 *     <td>高(紧凑二进制格式)</td>
 *   </tr>
 *   <tr>
 *     <td>序列化性能</td>
 *     <td>需要转换</td>
 *     <td>零拷贝序列化</td>
 *   </tr>
 * </table>
 *
 * <h2>数据约束</h2>
 * <ul>
 *   <li>所有键必须是内部数据结构(见{@link InternalRow})</li>
 *   <li>所有值必须是内部数据结构</li>
 *   <li>所有键必须是相同类型</li>
 *   <li>所有值必须是相同类型</li>
 *   <li>键和值都可以为null(表示SQL NULL)</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建GenericMap
 * Map<BinaryString, Integer> javaMap = new HashMap<>();
 * javaMap.put(BinaryString.fromString("key1"), 100);
 * javaMap.put(BinaryString.fromString("key2"), 200);
 * GenericMap map = new GenericMap(javaMap);
 *
 * // 访问map
 * int size = map.size();
 * Object value = map.get(BinaryString.fromString("key1")); // 100
 * boolean contains = map.contains(BinaryString.fromString("key1")); // true
 *
 * // 获取键值数组
 * InternalArray keys = map.keyArray();
 * InternalArray values = map.valueArray();
 * }</pre>
 *
 * <h2>内部数据结构</h2>
 * <p>内部数据结构是指Paimon内部表示数据类型的对象,包括:
 * <ul>
 *   <li>基本类型: Boolean, Byte, Short, Integer, Long, Float, Double</li>
 *   <li>字符串: {@link BinaryString}</li>
 *   <li>二进制: byte[]</li>
 *   <li>数值: {@link Decimal}</li>
 *   <li>时间: {@link Timestamp}</li>
 *   <li>复杂类型: {@link InternalRow}, {@link InternalArray}, {@link InternalMap}</li>
 * </ul>
 *
 * <h2>相等性比较</h2>
 * <p>实现了深度相等比较,能够正确处理:
 * <ul>
 *   <li>byte[]值的深度比较(使用Objects.deepEquals)</li>
 *   <li>null值的正确处理</li>
 *   <li>嵌套复杂类型的递归比较</li>
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public final class GenericMap implements InternalMap, Serializable {

    private static final long serialVersionUID = 1L;

    /** 包装的Java Map,包含实际的键值对数据 */
    private final Map<?, ?> map;

    /**
     * 使用给定的Java Map创建GenericMap实例。
     *
     * <p>重要约束:
     * <ul>
     *   <li>map中的所有键必须是内部数据结构</li>
     *   <li>map中的所有值必须是内部数据结构</li>
     *   <li>不会拷贝map,直接引用传入的实例</li>
     * </ul>
     *
     * @param map 包含内部数据结构的Java Map
     */
    public GenericMap(Map<?, ?> map) {
        this.map = map;
    }

    /**
     * 返回指定键映射的值。
     *
     * <p>如果map中不包含该键的映射,返回null。
     * 注意区分: 返回null可能表示键不存在,或键存在但值为null。
     *
     * @param key 要查找的键(必须是内部数据结构)
     * @return 键对应的值(内部数据结构),如果键不存在则返回null
     */
    public Object get(Object key) {
        return map.get(key);
    }

    /**
     * 检查map是否包含指定的键。
     *
     * @param key 要检查的键
     * @return 如果包含该键返回true,否则返回false
     */
    public boolean contains(Object key) {
        return map.containsKey(key);
    }

    /**
     * 返回map的大小(键值对数量)。
     *
     * @return 键值对的数量
     */
    @Override
    public int size() {
        return map.size();
    }

    /**
     * 返回包含所有键的数组。
     *
     * <p>将map的键集合转换为对象数组,然后包装为{@link GenericArray}。
     * 键的顺序取决于底层map的实现(通常是插入顺序或无序)。
     *
     * @return 包含所有键的InternalArray
     */
    @Override
    public InternalArray keyArray() {
        Object[] keys = map.keySet().toArray();
        return new GenericArray(keys);
    }

    /**
     * 返回包含所有值的数组。
     *
     * <p>将map的值集合转换为对象数组,然后包装为{@link GenericArray}。
     * 值的顺序与键数组对应。
     *
     * @return 包含所有值的InternalArray
     */
    @Override
    public InternalArray valueArray() {
        Object[] values = map.values().toArray();
        return new GenericArray(values);
    }

    /**
     * 比较此GenericMap与另一个对象是否相等。
     *
     * <p>相等性判断规则:
     * <ol>
     *   <li>如果是同一个对象,返回true</li>
     *   <li>如果对方不是GenericMap,返回false</li>
     *   <li>使用深度相等比较map内容(支持byte[]值)</li>
     * </ol>
     *
     * @param o 要比较的对象
     * @return 如果相等返回true,否则返回false
     */
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof GenericMap)) {
            return false;
        }
        // 使用深度相等比较,正确处理byte[]值
        return deepEquals(map, ((GenericMap) o).map);
    }

    /**
     * 深度比较两个Map是否相等。
     *
     * <p>此方法参考了HashMap.equals的实现,但使用{@link Objects#deepEquals}
     * 来比较值,以正确处理byte[]等数组类型。
     *
     * <p>比较过程:
     * <ol>
     *   <li>检查大小是否相等</li>
     *   <li>遍历第一个map的所有条目</li>
     *   <li>对于每个条目,检查第二个map是否有相同的键值对</li>
     *   <li>使用deepEquals比较值,支持数组等复杂类型</li>
     * </ol>
     *
     * @param m1 第一个Map
     * @param m2 第二个Map
     * @param <K> 键类型
     * @param <V> 值类型
     * @return 如果两个Map深度相等返回true
     */
    private static <K, V> boolean deepEquals(Map<K, V> m1, Map<?, ?> m2) {
        // 参考HashMap.equals实现,但使用deepEquals比较
        if (m1.size() != m2.size()) {
            return false;
        }
        try {
            for (Map.Entry<K, V> e : m1.entrySet()) {
                K key = e.getKey();
                V value = e.getValue();
                if (value == null) {
                    if (!(m2.get(key) == null && m2.containsKey(key))) {
                        return false;
                    }
                } else {
                    if (!Objects.deepEquals(value, m2.get(key))) {
                        return false;
                    }
                }
            }
        } catch (ClassCastException | NullPointerException unused) {
            return false;
        }
        return true;
    }

    /**
     * 计算此GenericMap的哈希码。
     *
     * <p>哈希码计算规则:
     * <ul>
     *   <li>只基于键计算哈希码(因为值可能包含byte[])</li>
     *   <li>对所有键的哈希码求和(乘以31)</li>
     *   <li>确保相等的map有相同的哈希码</li>
     * </ul>
     *
     * <p>注意: 由于值可能包含byte[],为了避免哈希码不一致,
     * 只使用键来计算哈希码。
     *
     * @return 哈希码
     */
    @Override
    public int hashCode() {
        int result = 0;
        for (Object key : map.keySet()) {
            // 只包含键,因为值可能包含byte[]
            result += 31 * Objects.hashCode(key);
        }
        return result;
    }
}
