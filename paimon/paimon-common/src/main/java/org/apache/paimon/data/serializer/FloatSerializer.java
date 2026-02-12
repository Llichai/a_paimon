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
 * Float 类型序列化器 - 用于序列化 Float 对象(包括通过自动装箱的 float 基本类型)。
 *
 * <p>这是一个无状态的单例序列化器,线程安全,可以在多个线程之间共享使用。
 *
 * <p>序列化格式:
 * <ul>
 *   <li>使用 4 个字节存储 float 值
 *   <li>采用 IEEE 754 单精度浮点格式
 *   <li>范围: 约 ±3.4028235E+38
 *   <li>精度: 约 6-7 位十进制数字
 * </ul>
 *
 * <p>特性:
 * <ul>
 *   <li>固定长度: 每个值占用 4 字节
 *   <li>支持字符串序列化: 可以序列化为浮点数字符串
 *   <li>不可变类型: copy() 直接返回原对象
 *   <li>特殊值支持: NaN、正无穷、负无穷
 * </ul>
 */
public final class FloatSerializer extends SerializerSingleton<Float> {

    private static final long serialVersionUID = 1L;

    /** FloatSerializer 的共享单例实例。 */
    public static final FloatSerializer INSTANCE = new FloatSerializer();

    /**
     * 复制 Float 值。
     *
     * <p>由于 Float 是不可变类型,直接返回原对象即可。
     *
     * @param from 要复制的 Float 值
     * @return 原 Float 对象
     */
    @Override
    public Float copy(Float from) {
        return from;
    }

    /**
     * 将 Float 值序列化到输出视图。
     *
     * @param record 要序列化的 Float 值
     * @param target 目标输出视图
     * @throws IOException 如果写入过程中发生 I/O 错误
     */
    @Override
    public void serialize(Float record, DataOutputView target) throws IOException {
        target.writeFloat(record);
    }

    /**
     * 从输入视图反序列化 Float 值。
     *
     * @param source 源输入视图
     * @return 反序列化的 Float 值
     * @throws IOException 如果读取过程中发生 I/O 错误
     */
    @Override
    public Float deserialize(DataInputView source) throws IOException {
        return source.readFloat();
    }

    /**
     * 将 Float 值序列化为字符串。
     *
     * @param record 要序列化的 Float 值
     * @return Float 的字符串表示
     */
    @Override
    public String serializeToString(Float record) {
        return record.toString();
    }

    /**
     * 从字符串反序列化 Float 值。
     *
     * @param s 要反序列化的字符串
     * @return 反序列化的 Float 值
     */
    @Override
    public Float deserializeFromString(String s) {
        return Float.valueOf(s);
    }
}
