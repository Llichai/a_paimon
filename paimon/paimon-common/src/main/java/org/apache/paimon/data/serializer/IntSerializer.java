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
 * Integer 类型序列化器 - 用于序列化 Integer 对象(包括通过自动装箱的 int 基本类型)。
 *
 * <p>这是一个无状态的单例序列化器,线程安全,可以在多个线程之间共享使用。
 *
 * <p>序列化格式:
 * <ul>
 *   <li>使用 4 个字节存储 int 值
 *   <li>范围: -2,147,483,648 到 2,147,483,647
 *   <li>使用大端序(big-endian)存储
 * </ul>
 *
 * <p>特性:
 * <ul>
 *   <li>固定长度: 每个值占用 4 字节
 *   <li>支持字符串序列化: 可以序列化为数字字符串
 *   <li>不可变类型: copy() 直接返回原对象
 *   <li>高性能: 最常用的数字类型,经过高度优化
 * </ul>
 */
public final class IntSerializer extends SerializerSingleton<Integer> {

    private static final long serialVersionUID = 1L;

    /** IntSerializer 的共享单例实例。 */
    public static final IntSerializer INSTANCE = new IntSerializer();

    /**
     * 复制 Integer 值。
     *
     * <p>由于 Integer 是不可变类型,直接返回原对象即可。
     *
     * @param from 要复制的 Integer 值
     * @return 原 Integer 对象
     */
    @Override
    public Integer copy(Integer from) {
        return from;
    }

    /**
     * 将 Integer 值序列化到输出视图。
     *
     * @param record 要序列化的 Integer 值
     * @param target 目标输出视图
     * @throws IOException 如果写入过程中发生 I/O 错误
     */
    @Override
    public void serialize(Integer record, DataOutputView target) throws IOException {
        target.writeInt(record);
    }

    /**
     * 从输入视图反序列化 Integer 值。
     *
     * @param source 源输入视图
     * @return 反序列化的 Integer 值
     * @throws IOException 如果读取过程中发生 I/O 错误
     */
    @Override
    public Integer deserialize(DataInputView source) throws IOException {
        return source.readInt();
    }

    /**
     * 将 Integer 值序列化为字符串。
     *
     * @param record 要序列化的 Integer 值
     * @return Integer 的字符串表示
     */
    @Override
    public String serializeToString(Integer record) {
        return record.toString();
    }

    /**
     * 从字符串反序列化 Integer 值。
     *
     * @param s 要反序列化的字符串
     * @return 反序列化的 Integer 值
     */
    @Override
    public Integer deserializeFromString(String s) {
        return Integer.valueOf(s);
    }
}
