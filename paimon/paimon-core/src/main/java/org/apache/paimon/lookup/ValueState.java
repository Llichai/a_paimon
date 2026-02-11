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

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * 单值状态接口,用于存储键值对映射关系.
 *
 * <p>ValueState 为每个键存储至多一个值,是最简单也是最常用的状态类型。
 * 该接口类似于 Flink 的 ValueState,但针对 Paimon 的 Lookup 场景进行了优化。
 *
 * <h2>主要特性:</h2>
 * <ul>
 *   <li><b>单值存储</b>: 每个键最多对应一个值,新值会覆盖旧值
 *   <li><b>可空值</b>: 支持查询不存在的键,返回 null
 *   <li><b>完整的 CRUD 操作</b>: 支持查询(get)、更新(put)、删除(delete)
 *   <li><b>批量加载</b>: 提供 BulkLoader 用于高效的初始化数据加载
 * </ul>
 *
 * <h2>使用场景:</h2>
 * <ul>
 *   <li><b>Lookup Join</b>: 存储维度表数据,通过主键快速查找对应的维度信息
 *   <li><b>去重</b>: 存储已见过的键,用于数据去重
 *   <li><b>缓存</b>: 缓存计算结果,避免重复计算
 *   <li><b>映射转换</b>: 存储键到值的映射关系,用于数据转换
 * </ul>
 *
 * <h2>典型实现:</h2>
 * <ul>
 *   <li>RocksDBValueState: 基于 RocksDB 的持久化实现,适合大状态
 *   <li>InMemoryValueState: 基于内存 HashMap 的实现,适合小状态
 * </ul>
 *
 * <h2>使用示例:</h2>
 * <pre>{@code
 * // 创建 ValueState
 * ValueState<Integer, String> state = stateFactory.valueState(
 *     "user-info",
 *     IntSerializer.INSTANCE,
 *     StringSerializer.INSTANCE,
 *     10000L  // LRU cache size
 * );
 *
 * // 写入数据
 * state.put(1, "Alice");
 * state.put(2, "Bob");
 *
 * // 查询数据
 * String user = state.get(1);  // "Alice"
 * String unknown = state.get(999);  // null
 *
 * // 删除数据
 * state.delete(1);
 * }</pre>
 *
 * @param <K> 键的类型
 * @param <V> 值的类型
 * @see State 状态基础接口
 * @see ListState 列表状态接口
 * @see SetState 集合状态接口
 * @see ValueBulkLoader 批量加载器
 */
public interface ValueState<K, V> extends State<K, V> {

    /**
     * 根据键获取对应的值.
     *
     * <p>如果键不存在,则返回 null。该方法是状态的核心查询操作。
     *
     * @param key 要查询的键
     * @return 键对应的值,如果键不存在则返回 null
     * @throws IOException 如果查询过程中发生 I/O 错误
     */
    @Nullable
    V get(K key) throws IOException;

    /**
     * 插入或更新键值对.
     *
     * <p>如果键已存在,则更新其对应的值;如果键不存在,则插入新的键值对。
     *
     * @param key 要插入或更新的键
     * @param value 要存储的值
     * @throws IOException 如果插入或更新过程中发生 I/O 错误
     */
    void put(K key, V value) throws IOException;

    /**
     * 删除指定键及其对应的值.
     *
     * <p>如果键不存在,该操作不会产生任何影响。
     *
     * @param key 要删除的键
     * @throws IOException 如果删除过程中发生 I/O 错误
     */
    void delete(K key) throws IOException;

    /**
     * 创建批量加载器(BulkLoader).
     *
     * <p>批量加载器用于高效地加载大量有序数据,适合初始化场景。
     * 例如在 Lookup Join 启动时,从维度表文件中批量加载数据到状态。
     *
     * <h3>批量加载的优势:</h3>
     * <ul>
     *   <li>比逐条 put 效率更高,特别是对于 RocksDB 等 LSM-tree 存储
     *   <li>减少写放大和内存开销
     *   <li>可以直接生成排序的 SST 文件
     * </ul>
     *
     * <h3>使用约束:</h3>
     * <ul>
     *   <li>输入的键必须已排序
     *   <li>键不能重复
     *   <li>批量加载完成后需要调用 finish() 方法
     * </ul>
     *
     * @return 批量加载器实例
     * @see ValueBulkLoader
     */
    ValueBulkLoader createBulkLoader();
}
