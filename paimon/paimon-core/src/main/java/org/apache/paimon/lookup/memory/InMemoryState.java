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
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.lookup.State;

import java.io.IOException;

/**
 * 基于内存的状态抽象基类,提供序列化和反序列化的通用实现.
 *
 * <p>InMemoryState 是所有内存状态实现的基础类,提供了键值序列化的标准实现。
 * 所有数据都存储在 JVM 堆内存中,适合状态数据量较小的场景。
 *
 * <h2>主要特性:</h2>
 * <ul>
 *   <li><b>内存存储</b>: 所有数据存储在 JVM 堆中,访问速度快
 *   <li><b>序列化复用</b>: 复用序列化器和输入输出视图,减少对象创建
 *   <li><b>抽象基类</b>: 提供通用实现,由子类实现具体的状态逻辑
 * </ul>
 *
 * <h2>序列化机制:</h2>
 * <ul>
 *   <li>使用 {@link DataOutputSerializer} 进行序列化,支持高效的字节数组生成
 *   <li>使用 {@link DataInputDeserializer} 进行反序列化,支持零拷贝读取
 *   <li>初始化时预分配 32 字节缓冲区,减少小对象的内存分配
 * </ul>
 *
 * <h2>与 RocksDB 状态的对比:</h2>
 * <ul>
 *   <li><b>存储位置</b>: InMemory 在堆内存,RocksDB 在堆外 + 磁盘
 *   <li><b>容量限制</b>: InMemory 受 JVM 堆大小限制,RocksDB 几乎无限
 *   <li><b>访问速度</b>: InMemory 纳秒级,RocksDB 微秒级
 *   <li><b>持久化</b>: InMemory 不持久化,RocksDB 可持久化
 * </ul>
 *
 * <h2>典型子类:</h2>
 * <ul>
 *   <li>InMemoryValueState: 基于 HashMap 的单值状态
 *   <li>InMemoryListState: 基于 HashMap + ArrayList 的列表状态
 *   <li>InMemorySetState: 基于 HashMap + TreeSet 的集合状态
 * </ul>
 *
 * <h2>使用场景:</h2>
 * <ul>
 *   <li>状态数据量小于几百 MB
 *   <li>需要极低延迟的状态访问
 *   <li>临时状态,不需要持久化
 *   <li>测试和原型开发
 * </ul>
 *
 * @param <K> 状态的键类型
 * @param <V> 状态的值类型
 * @see InMemoryValueState 单值状态实现
 * @see InMemoryListState 列表状态实现
 * @see InMemorySetState 集合状态实现
 */
public abstract class InMemoryState<K, V> implements State<K, V> {

    /** 键的序列化器. */
    protected final Serializer<K> keySerializer;

    /** 值的序列化器. */
    protected final Serializer<V> valueSerializer;

    /** 键的序列化输出视图,复用以减少对象创建. */
    protected final DataOutputSerializer keyOutView;

    /** 值的反序列化输入视图,复用以减少对象创建. */
    protected final DataInputDeserializer valueInputView;

    /** 值的序列化输出视图,复用以减少对象创建. */
    protected final DataOutputSerializer valueOutputView;

    /**
     * 构造内存状态基类.
     *
     * <p>初始化序列化器和输入输出视图。预分配 32 字节的缓冲区用于序列化,
     * 这对于大多数基本类型(如 int、long、String)已经足够,可以避免频繁的缓冲区扩容。
     *
     * @param keySerializer 键的序列化器
     * @param valueSerializer 值的序列化器
     */
    public InMemoryState(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.keyOutView = new DataOutputSerializer(32);
        this.valueInputView = new DataInputDeserializer();
        this.valueOutputView = new DataOutputSerializer(32);
    }

    /**
     * 将键对象序列化为字节数组.
     *
     * <p>该方法会清空输出视图的缓冲区,然后使用键序列化器进行序列化,
     * 最后返回缓冲区的拷贝。返回拷贝是为了确保字节数组不会因为视图的复用而被修改。
     *
     * @param key 要序列化的键对象
     * @return 序列化后的字节数组
     * @throws IOException 如果序列化过程发生 I/O 错误
     */
    @Override
    public byte[] serializeKey(K key) throws IOException {
        keyOutView.clear();
        keySerializer.serialize(key, keyOutView);
        return keyOutView.getCopyOfBuffer();
    }

    /**
     * 将值对象序列化为字节数组.
     *
     * <p>该方法会清空输出视图的缓冲区,然后使用值序列化器进行序列化,
     * 最后返回缓冲区的拷贝。
     *
     * @param value 要序列化的值对象
     * @return 序列化后的字节数组
     * @throws IOException 如果序列化过程发生 I/O 错误
     */
    @Override
    public byte[] serializeValue(V value) throws IOException {
        valueOutputView.clear();
        valueSerializer.serialize(value, valueOutputView);
        return valueOutputView.getCopyOfBuffer();
    }

    /**
     * 从字节数组反序列化值对象.
     *
     * <p>该方法将字节数组设置到输入视图中,然后使用值序列化器进行反序列化。
     * 输入视图支持零拷贝读取,不会复制字节数组。
     *
     * @param valueBytes 序列化的字节数组
     * @return 反序列化后的值对象
     * @throws IOException 如果反序列化过程发生 I/O 错误
     */
    @Override
    public V deserializeValue(byte[] valueBytes) throws IOException {
        valueInputView.setBuffer(valueBytes);
        return valueSerializer.deserialize(valueInputView);
    }
}
