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

import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;

import java.io.IOException;

/**
 * Variant 类型序列化器 - 用于序列化 {@link Variant} 对象。
 *
 * <p>Variant 是一种半结构化数据类型,可以存储 JSON、XML 等格式的数据。
 * 它由两部分组成:
 * <ul>
 *   <li>value: 存储实际的数据内容(编码后的字节数组)
 *   <li>metadata: 存储数据的元数据信息(如类型信息、模式等)
 * </ul>
 *
 * <p>序列化格式:
 * <ul>
 *   <li>第一部分: value 字节数组(使用 BinarySerializer 序列化)
 *   <li>第二部分: metadata 字节数组(使用 BinarySerializer 序列化)
 * </ul>
 *
 * <p>设计特点:
 * <ul>
 *   <li>双重序列化: 分别序列化 value 和 metadata
 *   <li>委托模式: 复用 BinarySerializer 的序列化逻辑
 *   <li>灵活性: 支持各种半结构化数据格式
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>VARIANT 类型字段的序列化
 *   <li>JSON 数据的存储
 *   <li>动态类型数据的处理
 *   <li>Schema evolution 场景
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * VariantSerializer serializer = VariantSerializer.INSTANCE;
 *
 * // 创建 Variant 对象
 * byte[] value = ...;      // JSON 编码的数据
 * byte[] metadata = ...;   // 类型和模式信息
 * Variant variant = new GenericVariant(value, metadata);
 *
 * // 序列化
 * byte[] bytes = serializer.serializeToBytes(variant);
 *
 * // 反序列化
 * Variant deserialized = serializer.deserializeFromBytes(bytes);
 * }</pre>
 */
public class VariantSerializer extends SerializerSingleton<Variant> {

    private static final long serialVersionUID = 1L;

    /** VariantSerializer 的共享单例实例。 */
    public static final VariantSerializer INSTANCE = new VariantSerializer();

    /**
     * 复制 Variant 对象。
     *
     * <p>创建 Variant 的深拷贝,包括 value 和 metadata 的字节数组。
     *
     * @param from 要复制的 Variant 对象
     * @return Variant 的深拷贝
     */
    @Override
    public Variant copy(Variant from) {
        return from.copy();
    }

    /**
     * 将 Variant 序列化到输出视图。
     *
     * <p>序列化过程:
     * <ol>
     *   <li>使用 BinarySerializer 序列化 value 字节数组
     *   <li>使用 BinarySerializer 序列化 metadata 字节数组
     * </ol>
     *
     * @param record 要序列化的 Variant 对象
     * @param target 目标输出视图
     * @throws IOException 如果写入过程中发生 I/O 错误
     */
    @Override
    public void serialize(Variant record, DataOutputView target) throws IOException {
        // 序列化 value 部分
        BinarySerializer.INSTANCE.serialize(record.value(), target);
        // 序列化 metadata 部分
        BinarySerializer.INSTANCE.serialize(record.metadata(), target);
    }

    /**
     * 从输入视图反序列化 Variant。
     *
     * <p>反序列化过程:
     * <ol>
     *   <li>使用 BinarySerializer 反序列化 value 字节数组
     *   <li>使用 BinarySerializer 反序列化 metadata 字节数组
     *   <li>用 value 和 metadata 构造 GenericVariant 对象
     * </ol>
     *
     * @param source 源输入视图
     * @return 反序列化的 Variant 对象
     * @throws IOException 如果读取过程中发生 I/O 错误
     */
    @Override
    public Variant deserialize(DataInputView source) throws IOException {
        // 反序列化 value 部分
        byte[] value = BinarySerializer.INSTANCE.deserialize(source);
        // 反序列化 metadata 部分
        byte[] metadata = BinarySerializer.INSTANCE.deserialize(source);
        // 构造 Variant 对象
        return new GenericVariant(value, metadata);
    }
}
