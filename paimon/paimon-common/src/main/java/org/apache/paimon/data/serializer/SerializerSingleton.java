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

/**
 * 单例序列化器抽象类 - 无状态序列化器的基类。
 *
 * <p>这个抽象类用于实现无状态的序列化器,这些序列化器可以安全地在多个线程之间共享。
 * 由于没有内部状态,单例序列化器天然线程安全。
 *
 * <p>主要特点:
 * <ul>
 *   <li>无状态: 不维护任何可变状态,可以安全地在多线程环境中使用
 *   <li>单例模式: duplicate() 返回自身,避免不必要的对象创建
 *   <li>基于类型的相等性: 相同类型的单例序列化器被认为是相等的
 * </ul>
 *
 * <p>典型用法:
 * <pre>{@code
 * public final class IntSerializer extends SerializerSingleton<Integer> {
 *     public static final IntSerializer INSTANCE = new IntSerializer();
 *
 *     @Override
 *     public void serialize(Integer record, DataOutputView target) throws IOException {
 *         target.writeInt(record);
 *     }
 *
 *     @Override
 *     public Integer deserialize(DataInputView source) throws IOException {
 *         return source.readInt();
 *     }
 * }
 * }</pre>
 *
 * <p>适用场景:
 * <ul>
 *   <li>基本类型序列化器(IntSerializer, LongSerializer 等)
 *   <li>简单数据类型序列化器
 *   <li>不需要维护状态的序列化器
 * </ul>
 *
 * @param <T> 要序列化的对象类型
 */
public abstract class SerializerSingleton<T> implements Serializer<T> {

    private static final long serialVersionUID = 1L;

    /**
     * 返回序列化器自身。
     *
     * <p>由于单例序列化器是无状态的,不需要创建新的实例,直接返回自身即可。
     * 这样可以避免不必要的对象创建,提高性能。
     *
     * @return 序列化器自身
     */
    @Override
    public SerializerSingleton<T> duplicate() {
        return this;
    }

    /**
     * 基于类型计算哈希码。
     *
     * <p>使用类的哈希码作为序列化器的哈希码,确保相同类型的序列化器有相同的哈希值。
     *
     * @return 类的哈希码
     */
    @Override
    public int hashCode() {
        return this.getClass().hashCode();
    }

    /**
     * 基于类型判断相等性。
     *
     * <p>两个序列化器相等当且仅当它们是同一个类的实例。
     * 这确保了相同类型的单例序列化器被认为是相等的。
     *
     * @param obj 要比较的对象
     * @return 如果对象是相同类型的序列化器返回 true,否则返回 false
     */
    @Override
    public boolean equals(Object obj) {
        return obj.getClass().equals(this.getClass());
    }
}
