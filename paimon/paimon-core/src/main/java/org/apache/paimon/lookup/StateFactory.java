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

import org.apache.paimon.data.serializer.Serializer;

import java.io.Closeable;
import java.io.IOException;

/**
 * 状态工厂接口,用于创建各种类型的状态对象.
 *
 * <p>该工厂接口负责创建和管理不同类型的状态实例(ValueState、ListState、SetState),
 * 并提供统一的资源管理机制(通过 Closeable 接口)。
 *
 * <h2>主要职责:</h2>
 * <ul>
 *   <li>创建不同类型的状态实例(单值、列表、集合)
 *   <li>管理序列化器,将键值对转换为字节表示
 *   <li>配置 LRU 缓存大小以优化性能
 *   <li>提供批量加载优化的能力查询
 * </ul>
 *
 * <h2>使用场景:</h2>
 * <ul>
 *   <li>在 Lookup Join 中创建用于存储维度数据的状态
 *   <li>在流式计算中维护各种类型的中间状态
 *   <li>需要持久化或缓存键值对数据的场景
 * </ul>
 *
 * <h2>设计模式:</h2>
 * <ul>
 *   <li><b>抽象工厂模式</b>: 提供统一的接口创建不同类型的状态产品
 *   <li><b>资源管理</b>: 通过 Closeable 接口确保资源正确释放
 *   <li><b>策略模式</b>: 通过 preferBulkLoad() 让不同实现选择最优加载策略
 * </ul>
 *
 * <h2>典型实现:</h2>
 * <ul>
 *   <li>RocksDBStateFactory: 基于 RocksDB 的持久化状态工厂
 *   <li>InMemoryStateFactory: 基于内存的高性能状态工厂
 * </ul>
 *
 * @see State 状态接口
 * @see ValueState 单值状态
 * @see ListState 列表状态
 * @see SetState 集合状态
 */
public interface StateFactory extends Closeable {

    /**
     * 创建单值状态(ValueState)实例.
     *
     * <p>单值状态为每个键存储一个值,支持更新、删除和查询操作。
     *
     * @param <K> 键的类型
     * @param <V> 值的类型
     * @param name 状态的名称,用于标识和管理状态
     * @param keySerializer 键的序列化器,用于将键对象序列化为字节数组
     * @param valueSerializer 值的序列化器,用于将值对象序列化为字节数组
     * @param lruCacheSize LRU 缓存的大小,用于优化热点数据访问性能。值为 0 表示不使用缓存
     * @return 创建的 ValueState 实例
     * @throws IOException 如果创建状态过程中发生 I/O 错误
     */
    <K, V> ValueState<K, V> valueState(
            String name,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize)
            throws IOException;

    /**
     * 创建集合状态(SetState)实例.
     *
     * <p>集合状态为每个键存储一组不重复的值,支持添加、删除和查询操作。返回的值按字节数组排序。
     *
     * @param <K> 键的类型
     * @param <V> 值的类型
     * @param name 状态的名称,用于标识和管理状态
     * @param keySerializer 键的序列化器,用于将键对象序列化为字节数组
     * @param valueSerializer 值的序列化器,用于将值对象序列化为字节数组
     * @param lruCacheSize LRU 缓存的大小,用于优化热点数据访问性能。值为 0 表示不使用缓存
     * @return 创建的 SetState 实例
     * @throws IOException 如果创建状态过程中发生 I/O 错误
     */
    <K, V> SetState<K, V> setState(
            String name,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize)
            throws IOException;

    /**
     * 创建列表状态(ListState)实例.
     *
     * <p>列表状态为每个键存储一个有序的值列表,支持添加和查询操作。
     *
     * @param <K> 键的类型
     * @param <V> 值的类型
     * @param name 状态的名称,用于标识和管理状态
     * @param keySerializer 键的序列化器,用于将键对象序列化为字节数组
     * @param valueSerializer 值的序列化器,用于将值对象序列化为字节数组
     * @param lruCacheSize LRU 缓存的大小,用于优化热点数据访问性能。值为 0 表示不使用缓存
     * @return 创建的 ListState 实例
     * @throws IOException 如果创建状态过程中发生 I/O 错误
     */
    <K, V> ListState<K, V> listState(
            String name,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize)
            throws IOException;

    /**
     * 返回该状态工厂是否偏好使用批量加载(Bulk Load)方式.
     *
     * <p>批量加载是一种优化的数据加载方式,特别适用于大量有序数据的一次性加载场景。
     * 例如 RocksDB 的 SST 文件批量导入比逐条插入效率更高。
     *
     * <h3>批量加载的优势:</h3>
     * <ul>
     *   <li>减少写放大,提高写入效率
     *   <li>避免 LSM-tree 的多次合并操作
     *   <li>适合初始化大规模维度表数据
     * </ul>
     *
     * @return 如果偏好批量加载返回 true,否则返回 false
     */
    boolean preferBulkLoad();
}
