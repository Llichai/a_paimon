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
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * 序列化器接口 - 用于序列化内部数据结构。
 *
 * <p>这是 Paimon 序列化框架的核心接口,定义了数据序列化和反序列化的基本操作。
 *
 * <p>主要功能:
 * <ul>
 *   <li>数据复制: 创建对象的深拷贝
 *   <li>二进制序列化: 将对象序列化为字节流
 *   <li>二进制反序列化: 从字节流反序列化对象
 *   <li>字符串序列化: 将对象序列化为字符串(可选)
 *   <li>字符串反序列化: 从字符串反序列化对象(可选)
 * </ul>
 *
 * <p>设计要点:
 * <ul>
 *   <li>线程安全: 有状态的序列化器需要通过 duplicate() 创建副本以保证线程安全
 *   <li>可序列化: 实现 Serializable 接口,支持分布式传输
 *   <li>类型安全: 使用泛型保证类型安全
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>数据存储: 将内存中的数据结构序列化到文件
 *   <li>网络传输: 在网络中传输数据对象
 *   <li>缓存管理: 对象的序列化和反序列化
 *   <li>快照管理: 创建数据快照
 * </ul>
 *
 * @param <T> 要序列化的对象类型
 */
public interface Serializer<T> extends Serializable {

    /**
     * 复制序列化器实例。
     *
     * <p>如果序列化器是有状态的,则创建一个深拷贝;如果是无状态的,则可以返回自身。
     *
     * <p>需要这个方法的原因:
     * <ul>
     *   <li>序列化器可能在多个线程中使用
     *   <li>无状态序列化器天然线程安全
     *   <li>有状态序列化器可能不是线程安全的,需要为每个线程创建独立副本
     * </ul>
     *
     * @return 序列化器的副本,无状态序列化器可以返回自身
     */
    Serializer<T> duplicate();

    /**
     * 创建给定元素的深拷贝。
     *
     * <p>这个方法用于在不修改原始对象的情况下创建对象的副本。
     * 对于不可变类型(如基本类型的包装类),可以直接返回原对象。
     *
     * @param from 要复制的元素
     * @return 元素的深拷贝
     */
    T copy(T from);

    /**
     * 将给定记录序列化到目标输出视图。
     *
     * <p>这是核心的序列化方法,将内存中的对象转换为二进制格式并写入输出流。
     *
     * @param record 要序列化的记录
     * @param target 写入序列化数据的输出视图
     * @throws IOException 如果序列化过程中遇到 I/O 相关错误。
     *                     通常由输出视图抛出,可能来自底层的 I/O 通道
     */
    void serialize(T record, DataOutputView target) throws IOException;

    /**
     * 从给定的源输入视图反序列化记录。
     *
     * <p>这是核心的反序列化方法,从二进制格式恢复内存对象。
     *
     * @param source 读取数据的输入视图
     * @return 反序列化后的元素
     * @throws IOException 如果反序列化过程中遇到 I/O 相关错误。
     *                     通常由输入视图抛出,可能来自底层的 I/O 通道
     */
    T deserialize(DataInputView source) throws IOException;

    /**
     * 将给定记录序列化为字节数组。
     *
     * <p>这是一个便捷方法,内部使用 {@link #serialize(Object, DataOutputView)} 实现。
     * 适用于需要将对象完整序列化为字节数组的场景。
     *
     * @param record 要序列化的记录
     * @return 序列化后的字节数组
     * @throws IOException 如果序列化过程中发生 I/O 错误
     */
    default byte[] serializeToBytes(T record) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        serialize(record, view);
        return out.toByteArray();
    }

    /**
     * 从字节数组反序列化记录。
     *
     * <p>这是一个便捷方法,内部使用 {@link #deserialize(DataInputView)} 实现。
     * 适用于从完整的字节数组恢复对象的场景。
     *
     * @param bytes 要反序列化的字节数组
     * @return 反序列化后的元素
     * @throws IOException 如果反序列化过程中发生 I/O 错误
     */
    default T deserializeFromBytes(byte[] bytes) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(in);
        return deserialize(view);
    }

    /**
     * 将给定记录序列化为字符串。
     *
     * <p>这是一个可选的序列化方法,主要用于:
     * <ul>
     *   <li>调试和日志记录
     *   <li>配置文件的文本存储
     *   <li>简单数据类型的人类可读格式
     * </ul>
     *
     * <p>默认实现抛出 UnsupportedOperationException,子类可以根据需要覆盖。
     *
     * @param record 要序列化的记录
     * @return 序列化后的字符串
     * @throws UnsupportedOperationException 如果该序列化器不支持字符串序列化
     */
    default String serializeToString(T record) {
        throw new UnsupportedOperationException(
                String.format("serialize %s to string is unsupported", record));
    }

    /**
     * 从字符串反序列化记录。
     *
     * <p>这是一个可选的反序列化方法,与 {@link #serializeToString(Object)} 配对使用。
     *
     * <p>默认实现抛出 UnsupportedOperationException,子类可以根据需要覆盖。
     *
     * @param s 要反序列化的字符串
     * @return 反序列化后的元素
     * @throws UnsupportedOperationException 如果该序列化器不支持字符串反序列化
     */
    default T deserializeFromString(String s) {
        throw new UnsupportedOperationException(
                String.format("deserialize %s from string is unsupported", s));
    }
}
