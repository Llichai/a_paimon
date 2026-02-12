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

package org.apache.paimon.data.serializer;

import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * List 序列化器 - 用于序列化 {@link List} 集合。
 *
 * <p>这个序列化器依赖于元素序列化器来处理列表中每个元素的序列化。
 * 适用于 Java 标准的 List 集合类型。
 *
 * <p>序列化格式:
 * <ul>
 *   <li>列表长度: 4 字节整数,表示列表中元素的数量
 *   <li>元素数据: 每个元素的序列化表示,按顺序连续存储
 * </ul>
 *
 * <p>设计特点:
 * <ul>
 *   <li>泛型支持: 可以序列化任意类型元素的列表
 *   <li>委托模式: 依赖元素序列化器处理具体元素
 *   <li>顺序保证: 保持列表元素的原始顺序
 *   <li>容量优化: 反序列化时预分配容量,提高性能
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>ARRAY 类型的 Java List 实现
 *   <li>需要保持元素顺序的集合
 *   <li>配置数据的列表序列化
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建字符串列表序列化器
 * ListSerializer<String> serializer =
 *     new ListSerializer<>(BinaryStringSerializer.INSTANCE);
 *
 * List<String> list = Arrays.asList("a", "b", "c");
 * byte[] bytes = serializer.serializeToBytes(list);
 * List<String> deserialized = serializer.deserializeFromBytes(bytes);
 * }</pre>
 *
 * @param <T> 列表中元素的类型
 */
public final class ListSerializer<T> implements Serializer<List<T>> {

    private static final long serialVersionUID = 1L;

    /** 列表元素的序列化器。 */
    private final Serializer<T> elementSerializer;

    /**
     * 创建使用给定元素序列化器的列表序列化器。
     *
     * @param elementSerializer 列表元素的序列化器
     */
    public ListSerializer(Serializer<T> elementSerializer) {
        this.elementSerializer = checkNotNull(elementSerializer);
    }

    // ------------------------------------------------------------------------
    //  ListSerializer 特定属性
    // ------------------------------------------------------------------------

    /**
     * 获取列表元素的序列化器。
     *
     * @return 列表元素的序列化器
     */
    public Serializer<T> getElementSerializer() {
        return elementSerializer;
    }

    // ------------------------------------------------------------------------
    //  序列化器实现
    // ------------------------------------------------------------------------

    /**
     * 复制序列化器实例。
     *
     * <p>如果元素序列化器是无状态的,返回自身;否则创建新实例。
     *
     * @return 序列化器的副本
     */
    @Override
    public Serializer<List<T>> duplicate() {
        Serializer<T> duplicateElement = elementSerializer.duplicate();
        return duplicateElement == elementSerializer
                ? this
                : new ListSerializer<>(duplicateElement);
    }

    /**
     * 复制列表。
     *
     * <p>创建新的 ArrayList 并深拷贝每个元素。
     * 使用迭代器而不是索引访问,因为不能保证列表支持 RandomAccess。
     *
     * @param from 要复制的列表
     * @return 列表的深拷贝
     */
    @Override
    public List<T> copy(List<T> from) {
        List<T> newList = new ArrayList<>(from.size());

        // 使用迭代器而不是索引访问,因为不能保证列表支持 RandomAccess
        // 在新的 JVM 上,Iterator 会被栈分配(由于逃逸分析)
        for (T element : from) {
            newList.add(elementSerializer.copy(element));
        }
        return newList;
    }

    /**
     * 将列表序列化到输出视图。
     *
     * <p>序列化过程:
     * <ol>
     *   <li>写入列表大小(4 字节整数)
     *   <li>依次序列化每个元素
     * </ol>
     *
     * @param list 要序列化的列表
     * @param target 目标输出视图
     * @throws IOException 如果写入过程中发生 I/O 错误
     */
    @Override
    public void serialize(List<T> list, DataOutputView target) throws IOException {
        final int size = list.size();
        target.writeInt(size);

        // 使用迭代器而不是索引访问,因为不能保证列表支持 RandomAccess
        // 在新的 JVM 上,Iterator 会被栈分配(由于逃逸分析)
        for (T element : list) {
            elementSerializer.serialize(element, target);
        }
    }

    /**
     * 从输入视图反序列化列表。
     *
     * <p>反序列化过程:
     * <ol>
     *   <li>读取列表大小
     *   <li>创建 ArrayList,容量设为 size + 1 以避免添加单个元素时的扩容开销
     *   <li>依次反序列化每个元素并添加到列表
     * </ol>
     *
     * @param source 源输入视图
     * @return 反序列化的列表
     * @throws IOException 如果读取过程中发生 I/O 错误
     */
    @Override
    public List<T> deserialize(DataInputView source) throws IOException {
        final int size = source.readInt();
        // 创建容量为 (size + 1) 的新列表,避免添加单个元素时的昂贵扩容
        final List<T> list = new ArrayList<>(size + 1);
        for (int i = 0; i < size; i++) {
            list.add(elementSerializer.deserialize(source));
        }
        return list;
    }

    // --------------------------------------------------------------------

    /**
     * 判断两个序列化器是否相等。
     *
     * <p>当且仅当元素序列化器相等时,两个列表序列化器才相等。
     *
     * @param obj 要比较的对象
     * @return 如果相等返回 true,否则返回 false
     */
    @Override
    public boolean equals(Object obj) {
        return obj == this
                || (obj != null
                        && obj.getClass() == getClass()
                        && elementSerializer.equals(((ListSerializer<?>) obj).elementSerializer));
    }

    /**
     * 计算序列化器的哈希码。
     *
     * <p>基于元素序列化器的哈希码。
     *
     * @return 哈希码
     */
    @Override
    public int hashCode() {
        return elementSerializer.hashCode();
    }
}
