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
import org.apache.paimon.lookup.ListState;
import org.apache.paimon.lookup.SetState;
import org.apache.paimon.lookup.StateFactory;
import org.apache.paimon.lookup.ValueState;

import java.io.IOException;

/**
 * 基于内存的状态工厂实现,用于创建各种类型的内存状态.
 *
 * <p>InMemoryStateFactory 创建的所有状态都将数据存储在 JVM 堆内存中,
 * 使用标准的 Java 集合类(HashMap、ArrayList、TreeSet)作为底层存储。
 *
 * <h2>主要特性:</h2>
 * <ul>
 *   <li><b>轻量级</b>: 无需初始化外部依赖,创建开销极小
 *   <li><b>高性能</b>: 基于 JVM 堆内存,访问延迟在纳秒级
 *   <li><b>简单</b>: 不需要配置参数,开箱即用
 *   <li><b>无持久化</b>: 数据仅存在于内存,进程重启后丢失
 * </ul>
 *
 * <h2>创建的状态类型:</h2>
 * <ul>
 *   <li>ValueState: 基于 HashMap 实现,键值一对一映射
 *   <li>ListState: 基于 HashMap + ArrayList 实现,键对应值列表
 *   <li>SetState: 基于 HashMap + TreeSet 实现,键对应有序集合
 * </ul>
 *
 * <h2>性能特点:</h2>
 * <ul>
 *   <li><b>查询延迟</b>: 纳秒级(HashMap 查找)
 *   <li><b>写入延迟</b>: 纳秒级(HashMap 插入)
 *   <li><b>内存开销</b>: HashMap 的开销 + 序列化数据的开销
 *   <li><b>容量限制</b>: 受 JVM 堆大小限制,通常不超过几百 MB
 * </ul>
 *
 * <h2>与 RocksDB 工厂的对比:</h2>
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>InMemory</th>
 *     <th>RocksDB</th>
 *   </tr>
 *   <tr>
 *     <td>存储位置</td>
 *     <td>JVM 堆内存</td>
 *     <td>堆外内存 + 磁盘</td>
 *   </tr>
 *   <tr>
 *     <td>容量限制</td>
 *     <td>几百 MB</td>
 *     <td>几乎无限(TB级)</td>
 *   </tr>
 *   <tr>
 *     <td>访问延迟</td>
 *     <td>纳秒级</td>
 *     <td>微秒级</td>
 *   </tr>
 *   <tr>
 *     <td>持久化</td>
 *     <td>不支持</td>
 *     <td>支持</td>
 *   </tr>
 *   <tr>
 *     <td>批量加载</td>
 *     <td>无优化</td>
 *     <td>SST 文件导入</td>
 *   </tr>
 *   <tr>
 *     <td>初始化开销</td>
 *     <td>极小</td>
 *     <td>较大(加载库)</td>
 *   </tr>
 * </table>
 *
 * <h2>使用场景:</h2>
 * <ul>
 *   <li><b>小维度表</b>: 维度表数据量小于 100MB
 *   <li><b>低延迟要求</b>: 对查询延迟要求极高(纳秒级)
 *   <li><b>临时状态</b>: 不需要持久化的临时计算状态
 *   <li><b>测试开发</b>: 快速原型开发和单元测试
 * </ul>
 *
 * <h2>不适用场景:</h2>
 * <ul>
 *   <li>状态数据量超过 JVM 堆大小的 30%
 *   <li>需要持久化状态数据
 *   <li>需要跨进程共享状态
 * </ul>
 *
 * <h2>使用示例:</h2>
 * <pre>{@code
 * // 创建内存状态工厂
 * StateFactory factory = new InMemoryStateFactory();
 *
 * // 创建单值状态
 * ValueState<Integer, String> valueState = factory.valueState(
 *     "user-info",
 *     IntSerializer.INSTANCE,
 *     StringSerializer.INSTANCE,
 *     10000L  // LRU 缓存大小(对内存状态无效)
 * );
 *
 * // 写入和查询数据
 * valueState.put(1, "Alice");
 * String user = valueState.get(1);  // "Alice"
 *
 * // 不需要关闭,没有外部资源
 * factory.close();
 * }</pre>
 *
 * @see InMemoryValueState 单值状态实现
 * @see InMemoryListState 列表状态实现
 * @see InMemorySetState 集合状态实现
 * @see org.apache.paimon.lookup.rocksdb.RocksDBStateFactory RocksDB 状态工厂
 */
public class InMemoryStateFactory implements StateFactory {

    /**
     * 创建基于内存的单值状态.
     *
     * <p>创建的状态使用 {@link HashMap} 存储键值对,键为 {@link org.apache.paimon.lookup.ByteArray} 包装的字节数组,
     * 值为序列化后的字节数组。
     *
     * <h3>参数说明:</h3>
     * <ul>
     *   <li>name: 状态名称,对内存状态无实际作用,仅用于标识
     *   <li>lruCacheSize: LRU 缓存大小,对内存状态无效(因为所有数据都在内存中)
     * </ul>
     *
     * @param <K> 键的类型
     * @param <V> 值的类型
     * @param name 状态名称
     * @param keySerializer 键的序列化器
     * @param valueSerializer 值的序列化器
     * @param lruCacheSize LRU 缓存大小(对内存状态无效)
     * @return 内存单值状态实例
     */
    @Override
    public <K, V> ValueState<K, V> valueState(
            String name,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize) {
        return new InMemoryValueState<>(keySerializer, valueSerializer);
    }

    /**
     * 创建基于内存的集合状态.
     *
     * <p>创建的状态使用 {@link HashMap} + {@link java.util.TreeSet} 存储键到集合的映射。
     * TreeSet 保证值的有序性和唯一性。
     *
     * @param <K> 键的类型
     * @param <V> 值的类型
     * @param name 状态名称
     * @param keySerializer 键的序列化器
     * @param valueSerializer 值的序列化器
     * @param lruCacheSize LRU 缓存大小(对内存状态无效)
     * @return 内存集合状态实例
     */
    @Override
    public <K, V> SetState<K, V> setState(
            String name,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize) {
        return new InMemorySetState<>(keySerializer, valueSerializer);
    }

    /**
     * 创建基于内存的列表状态.
     *
     * <p>创建的状态使用 {@link HashMap} + {@link java.util.ArrayList} 存储键到列表的映射。
     * ArrayList 保持插入顺序,允许重复值。
     *
     * @param <K> 键的类型
     * @param <V> 值的类型
     * @param name 状态名称
     * @param keySerializer 键的序列化器
     * @param valueSerializer 值的序列化器
     * @param lruCacheSize LRU 缓存大小(对内存状态无效)
     * @return 内存列表状态实例
     */
    @Override
    public <K, V> ListState<K, V> listState(
            String name,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize) {
        return new InMemoryListState<>(keySerializer, valueSerializer);
    }

    /**
     * 返回是否偏好批量加载.
     *
     * <p>对于内存状态,批量加载没有性能优势,因为内存写入本身就很快。
     * 因此返回 false,表示不偏好批量加载。
     *
     * @return false,表示不偏好批量加载
     */
    @Override
    public boolean preferBulkLoad() {
        return false;
    }

    /**
     * 关闭状态工厂.
     *
     * <p>对于内存状态工厂,没有需要关闭的外部资源,该方法为空实现。
     *
     * @throws IOException 如果关闭过程发生 I/O 错误(实际上不会抛出)
     */
    @Override
    public void close() throws IOException {}
}
