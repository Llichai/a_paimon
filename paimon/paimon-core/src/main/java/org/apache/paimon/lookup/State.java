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

/**
 * 状态接口,所有不同类型的状态都必须实现该接口.
 *
 * <p>该接口是 Paimon 状态管理体系的基础抽象,类似于 Flink 的状态 API 设计。它定义了状态对象需要支持的基本序列化和反序列化操作。
 *
 * <h2>主要职责:</h2>
 * <ul>
 *   <li>定义键(Key)的序列化方法,将键对象转换为字节数组
 *   <li>定义值(Value)的序列化方法,将值对象转换为字节数组
 *   <li>定义值(Value)的反序列化方法,从字节数组还原值对象
 * </ul>
 *
 * <h2>使用场景:</h2>
 * <ul>
 *   <li>作为所有状态类型(ValueState、ListState、SetState)的通用抽象
 *   <li>支持多种状态后端实现(如 RocksDB、InMemory 等)
 *   <li>在 Lookup Join 等操作中维护中间状态
 * </ul>
 *
 * <h2>设计模式:</h2>
 * <ul>
 *   <li><b>泛型设计</b>: 通过 K 和 V 泛型参数支持任意类型的键值对
 *   <li><b>序列化抽象</b>: 将序列化逻辑委托给具体实现类,保持接口简洁
 *   <li><b>分层架构</b>: 作为顶层接口,由子接口(ValueState、ListState、SetState)扩展
 * </ul>
 *
 * @param <K> 状态的键类型
 * @param <V> 状态的值类型
 * @see ValueState 单值状态接口
 * @see ListState 列表状态接口
 * @see SetState 集合状态接口
 */
public interface State<K, V> {

    /**
     * 将键对象序列化为字节数组.
     *
     * <p>该方法用于在存储键之前将其转换为字节表示形式,以便进行持久化或传输。
     *
     * @param key 要序列化的键对象
     * @return 序列化后的字节数组
     * @throws IOException 如果序列化过程发生 I/O 错误
     */
    byte[] serializeKey(K key) throws IOException;

    /**
     * 将值对象序列化为字节数组.
     *
     * <p>该方法用于在存储值之前将其转换为字节表示形式,以便进行持久化或传输。
     *
     * @param value 要序列化的值对象
     * @return 序列化后的字节数组
     * @throws IOException 如果序列化过程发生 I/O 错误
     */
    byte[] serializeValue(V value) throws IOException;

    /**
     * 从字节数组反序列化值对象.
     *
     * <p>该方法用于从存储的字节表示形式还原原始值对象。
     *
     * @param valueBytes 序列化的字节数组
     * @return 反序列化后的值对象
     * @throws IOException 如果反序列化过程发生 I/O 错误
     */
    V deserializeValue(byte[] valueBytes) throws IOException;
}
