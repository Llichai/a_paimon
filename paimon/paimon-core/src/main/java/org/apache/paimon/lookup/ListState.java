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

package org.apache.paimon.lookup;

import java.io.IOException;
import java.util.List;

/**
 * 列表状态接口,用于存储键到值列表的映射关系.
 *
 * <p>ListState 为每个键维护一个有序的值列表,支持向列表追加元素并查询整个列表。
 * 该接口类似于 Flink 的 ListState,但针对 Paimon 的 Lookup 操作进行了优化。
 *
 * <h2>主要特性:</h2>
 * <ul>
 *   <li><b>列表存储</b>: 每个键对应一个值的有序列表
 *   <li><b>仅追加</b>: 只支持向列表末尾添加元素,不支持删除或修改
 *   <li><b>完整查询</b>: 每次查询返回键对应的完整值列表
 *   <li><b>批量加载</b>: 提供 BulkLoader 用于高效的初始化数据加载
 * </ul>
 *
 * <h2>使用场景:</h2>
 * <ul>
 *   <li><b>一对多关系</b>: 存储一个键对应多个值的关系,如订单明细、用户行为记录等
 *   <li><b>时间序列</b>: 存储按时间顺序的事件列表
 *   <li><b>聚合操作</b>: 收集同一个键的多个值,用于后续聚合计算
 *   <li><b>历史追踪</b>: 保留键的历史值变化记录
 * </ul>
 *
 * <h2>典型实现:</h2>
 * <ul>
 *   <li>RocksDBListState: 基于 RocksDB 的持久化实现,使用前缀扫描来获取列表
 *   <li>InMemoryListState: 基于内存的实现,使用 ArrayList 存储列表
 * </ul>
 *
 * <h2>与 SetState 的区别:</h2>
 * <ul>
 *   <li>ListState 保持插入顺序,SetState 按字节序排序
 *   <li>ListState 允许重复值,SetState 保证值唯一性
 *   <li>ListState 只能追加,SetState 支持添加和删除
 * </ul>
 *
 * <h2>使用示例:</h2>
 * <pre>{@code
 * // 创建 ListState
 * ListState<Integer, String> state = stateFactory.listState(
 *     "order-items",
 *     IntSerializer.INSTANCE,
 *     StringSerializer.INSTANCE,
 *     1000L
 * );
 *
 * // 添加数据
 * state.add(1001, "item-A");
 * state.add(1001, "item-B");
 * state.add(1001, "item-C");
 *
 * // 查询数据
 * List<String> items = state.get(1001);  // ["item-A", "item-B", "item-C"]
 * }</pre>
 *
 * @param <K> 键的类型
 * @param <V> 值的类型
 * @see State 状态基础接口
 * @see ValueState 单值状态接口
 * @see SetState 集合状态接口
 * @see ListBulkLoader 批量加载器
 */
public interface ListState<K, V> extends State<K, V> {

    /**
     * 向指定键对应的列表中添加一个值.
     *
     * <p>新值会被追加到列表末尾,保持插入顺序。
     *
     * @param key 键
     * @param value 要添加到列表的值
     * @throws IOException 如果添加过程中发生 I/O 错误
     */
    void add(K key, V value) throws IOException;

    /**
     * 获取指定键对应的完整值列表.
     *
     * <p>返回的列表按插入顺序排列。如果键不存在,返回空列表(而非 null)。
     *
     * @param key 键
     * @return 键对应的值列表,如果键不存在则返回空列表
     * @throws IOException 如果查询过程中发生 I/O 错误
     */
    List<V> get(K key) throws IOException;

    /**
     * 创建批量加载器(BulkLoader).
     *
     * <p>批量加载器用于高效地加载大量有序数据,适合初始化场景。
     * 在批量加载模式下,可以一次性写入一个键的完整列表,避免多次追加的开销。
     *
     * <h3>使用约束:</h3>
     * <ul>
     *   <li>输入的键必须已排序
     *   <li>批量加载完成后需要调用 finish() 方法
     * </ul>
     *
     * @return 批量加载器实例
     * @see ListBulkLoader
     */
    ListBulkLoader createBulkLoader();
}
