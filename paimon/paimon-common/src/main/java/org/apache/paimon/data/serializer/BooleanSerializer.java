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

/**
 * Boolean 类型序列化器 - 用于序列化 Boolean 对象(包括通过自动装箱的 boolean 基本类型)。
 *
 * <p>这是一个无状态的单例序列化器,线程安全,可以在多个线程之间共享使用。
 *
 * <p>序列化格式:
 * <ul>
 *   <li>使用 1 个字节存储 boolean 值
 *   <li>true: 0x01, false: 0x00
 * </ul>
 *
 * <p>特性:
 * <ul>
 *   <li>空间效率: 每个值占用 1 字节
 *   <li>支持字符串序列化: 可以序列化为 "true" 或 "false"
 *   <li>不可变类型: copy() 直接返回原对象
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * BooleanSerializer serializer = BooleanSerializer.INSTANCE;
 * Boolean value = true;
 *
 * // 序列化
 * byte[] bytes = serializer.serializeToBytes(value);
 *
 * // 反序列化
 * Boolean deserialized = serializer.deserializeFromBytes(bytes);
 * }</pre>
 */
public final class BooleanSerializer extends SerializerSingleton<Boolean> {

    private static final long serialVersionUID = 1L;

    /** BooleanSerializer 的共享单例实例。 */
    public static final BooleanSerializer INSTANCE = new BooleanSerializer();

    /**
     * 复制 Boolean 值。
     *
     * <p>由于 Boolean 是不可变类型,直接返回原对象即可,无需创建新对象。
     *
     * @param from 要复制的 Boolean 值
     * @return 原 Boolean 对象
     */
    @Override
    public Boolean copy(Boolean from) {
        return from;
    }

    /**
     * 将 Boolean 值序列化到输出视图。
     *
     * <p>使用 1 个字节存储 boolean 值。
     *
     * @param record 要序列化的 Boolean 值
     * @param target 目标输出视图
     * @throws IOException 如果写入过程中发生 I/O 错误
     */
    @Override
    public void serialize(Boolean record, DataOutputView target) throws IOException {
        target.writeBoolean(record);
    }

    /**
     * 从输入视图反序列化 Boolean 值。
     *
     * @param source 源输入视图
     * @return 反序列化的 Boolean 值
     * @throws IOException 如果读取过程中发生 I/O 错误
     */
    @Override
    public Boolean deserialize(DataInputView source) throws IOException {
        return source.readBoolean();
    }

    /**
     * 将 Boolean 值序列化为字符串。
     *
     * @param record 要序列化的 Boolean 值
     * @return "true" 或 "false"
     */
    @Override
    public String serializeToString(Boolean record) {
        return record.toString();
    }

    /**
     * 从字符串反序列化 Boolean 值。
     *
     * @param s 要反序列化的字符串("true" 或 "false",不区分大小写)
     * @return 反序列化的 Boolean 值
     */
    @Override
    public Boolean deserializeFromString(String s) {
        return Boolean.valueOf(s);
    }
}
