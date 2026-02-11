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
 * 集合状态接口,用于存储键到不重复值集合的映射关系.
 *
 * <p>SetState 为每个键维护一个去重的值集合,并按字节序返回排序后的结果。
 * 该接口支持添加和删除(撤回)操作,常用于需要维护唯一值集合的场景。
 *
 * <h2>主要特性:</h2>
 * <ul>
 *   <li><b>自动去重</b>: 同一个值不会被重复存储
 *   <li><b>有序返回</b>: 查询结果按字节数组排序,保证结果的确定性
 *   <li><b>双向操作</b>: 支持添加(add)和撤回(retract)操作
 *   <li><b>语义保证</b>: retract 必须与之前的 add 对应,类似于流式计算的撤回语义
 * </ul>
 *
 * <h2>使用场景:</h2>
 * <ul>
 *   <li><b>去重聚合</b>: 维护不重复的值集合,如统计不同用户、不同商品等
 *   <li><b>标签管理</b>: 为实体维护一组唯一的标签或属性
 *   <li><b>成员关系</b>: 维护集合成员关系,支持动态增删
 *   <li><b>撤回流处理</b>: 在流式计算中处理带有撤回消息的数据流
 * </ul>
 *
 * <h2>典型实现:</h2>
 * <ul>
 *   <li>RocksDBSetState: 基于 RocksDB 的持久化实现,使用排序的键值对存储
 *   <li>InMemorySetState: 基于内存 TreeSet 的实现,自动保持有序
 * </ul>
 *
 * <h2>与 ListState 的区别:</h2>
 * <ul>
 *   <li>SetState 保证值唯一性,ListState 允许重复
 *   <li>SetState 按字节序排序,ListState 保持插入顺序
 *   <li>SetState 支持撤回,ListState 只能追加
 * </ul>
 *
 * <h2>排序规则:</h2>
 * <p>值按序列化后的字节数组进行字典序比较。例如:
 * <ul>
 *   <li>对于整数: 1, 2, 10, 100 (按数值比较)
 *   <li>对于字符串: "a", "ab", "b" (按字典序)
 * </ul>
 *
 * <h2>使用示例:</h2>
 * <pre>{@code
 * // 创建 SetState
 * SetState<Integer, String> state = stateFactory.setState(
 *     "user-tags",
 *     IntSerializer.INSTANCE,
 *     StringSerializer.INSTANCE,
 *     1000L
 * );
 *
 * // 添加标签
 * state.add(100, "vip");
 * state.add(100, "active");
 * state.add(100, "vip");  // 重复添加,不会增加第二次
 *
 * // 查询标签(按字典序返回)
 * List<String> tags = state.get(100);  // ["active", "vip"]
 *
 * // 撤回标签
 * state.retract(100, "vip");
 * tags = state.get(100);  // ["active"]
 * }</pre>
 *
 * @param <K> 键的类型
 * @param <V> 值的类型
 * @see State 状态基础接口
 * @see ValueState 单值状态接口
 * @see ListState 列表状态接口
 */
public interface SetState<K, V> extends State<K, V> {

    /**
     * 获取指定键对应的所有值,并按字节数组排序返回.
     *
     * <p>返回的列表中的值是唯一的,并按序列化后字节数组的字典序排列。
     * 如果键不存在,返回空列表(而非 null)。
     *
     * @param key 键
     * @return 键对应的值列表(已排序且去重),如果键不存在则返回空列表
     * @throws IOException 如果查询过程中发生 I/O 错误
     */
    List<V> get(K key) throws IOException;

    /**
     * 撤回(删除)指定键对应集合中的一个值.
     *
     * <p>撤回操作用于移除之前添加的值。在流式计算场景中,该操作对应于撤回消息(retract message)。
     * 如果值不存在于集合中,该操作不会产生影响。
     *
     * <h3>语义说明:</h3>
     * <ul>
     *   <li>retract 应该与之前的 add 对应
     *   <li>多次 retract 同一个值是幂等的
     *   <li>适用于处理 CDC 数据流中的删除操作
     * </ul>
     *
     * @param key 键
     * @param value 要撤回的值
     * @throws IOException 如果撤回过程中发生 I/O 错误
     */
    void retract(K key, V value) throws IOException;

    /**
     * 向指定键对应的集合中添加一个值.
     *
     * <p>如果值已存在于集合中,该操作不会重复添加。添加的值会按字节序自动排序。
     *
     * @param key 键
     * @param value 要添加的值
     * @throws IOException 如果添加过程中发生 I/O 错误
     */
    void add(K key, V value) throws IOException;
}
