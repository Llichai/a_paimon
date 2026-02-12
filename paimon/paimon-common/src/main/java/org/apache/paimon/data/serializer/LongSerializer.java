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
 * Long 类型序列化器 - 用于序列化 Long 对象(包括通过自动装箱的 long 基本类型)。
 *
 * <p>这是一个无状态的单例序列化器,线程安全,可以在多个线程之间共享使用。
 *
 * <p>序列化格式:
 * <ul>
 *   <li>使用 8 个字节存储 long 值
 *   <li>范围: -9,223,372,036,854,775,808 到 9,223,372,036,854,775,807
 *   <li>使用大端序(big-endian)存储
 * </ul>
 *
 * <p>特性:
 * <ul>
 *   <li>固定长度: 每个值占用 8 字节
 *   <li>支持字符串序列化: 可以序列化为数字字符串
 *   <li>不可变类型: copy() 直接返回原对象
 *   <li>常用场景: 时间戳、ID、计数器等
 * </ul>
 */
public final class LongSerializer extends SerializerSingleton<Long> {

    private static final long serialVersionUID = 1L;

    /** LongSerializer 的共享单例实例。 */
    public static final LongSerializer INSTANCE = new LongSerializer();

    /**
     * 复制 Long 值。
     *
     * <p>由于 Long 是不可变类型,直接返回原对象即可。
     *
     * @param from 要复制的 Long 值
     * @return 原 Long 对象
     */
    @Override
    public Long copy(Long from) {
        return from;
    }

    /**
     * 将 Long 值序列化到输出视图。
     *
     * @param record 要序列化的 Long 值
     * @param target 目标输出视图
     * @throws IOException 如果写入过程中发生 I/O 错误
     */
    @Override
    public void serialize(Long record, DataOutputView target) throws IOException {
        target.writeLong(record);
    }

    /**
     * 从输入视图反序列化 Long 值。
     *
     * @param source 源输入视图
     * @return 反序列化的 Long 值
     * @throws IOException 如果读取过程中发生 I/O 错误
     */
    @Override
    public Long deserialize(DataInputView source) throws IOException {
        return source.readLong();
    }

    /**
     * 将 Long 值序列化为字符串。
     *
     * @param record 要序列化的 Long 值
     * @return Long 的字符串表示
     */
    @Override
    public String serializeToString(Long record) {
        return record.toString();
    }

    /**
     * 从字符串反序列化 Long 值。
     *
     * @param s 要反序列化的字符串
     * @return 反序列化的 Long 值
     */
    @Override
    public Long deserializeFromString(String s) {
        return Long.valueOf(s);
    }
}
