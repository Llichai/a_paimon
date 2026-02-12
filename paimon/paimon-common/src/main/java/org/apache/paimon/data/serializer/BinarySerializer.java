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

import static org.apache.paimon.utils.VarLengthIntUtils.decodeInt;
import static org.apache.paimon.utils.VarLengthIntUtils.encodeInt;

/**
 * byte[] 类型序列化器 - 用于序列化字节数组。
 *
 * <p>这是一个无状态的单例序列化器,用于处理任意长度的字节数组。
 * 字节数组是二进制数据的基本表示形式,广泛用于存储原始二进制内容。
 *
 * <p>序列化格式:
 * <ul>
 *   <li>长度前缀: 使用变长整数编码存储字节数组的长度
 *   <li>字节数据: 原始字节内容
 * </ul>
 *
 * <p>变长整数编码优势:
 * <ul>
 *   <li>短数组节省空间: 长度 < 128 的数组只需 1 字节存储长度
 *   <li>紧凑表示: 避免固定 4 字节整数的空间浪费
 *   <li>适应性: 自动适配不同长度的数组
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>BINARY 和 VARBINARY 类型的序列化
 *   <li>Blob 数据的底层序列化
 *   <li>Decimal 非紧凑格式的序列化
 *   <li>任意二进制数据的存储
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * BinarySerializer serializer = BinarySerializer.INSTANCE;
 * byte[] data = new byte[] {1, 2, 3, 4, 5};
 *
 * // 序列化
 * byte[] bytes = serializer.serializeToBytes(data);
 *
 * // 反序列化
 * byte[] deserialized = serializer.deserializeFromBytes(bytes);
 * }</pre>
 */
public final class BinarySerializer extends SerializerSingleton<byte[]> {

    private static final long serialVersionUID = 1L;

    /** BinarySerializer 的共享单例实例。 */
    public static final BinarySerializer INSTANCE = new BinarySerializer();

    /**
     * 复制字节数组。
     *
     * <p>创建字节数组的深拷贝,使用 System.arraycopy 保证高效复制。
     *
     * @param from 要复制的字节数组
     * @return 字节数组的深拷贝
     */
    @Override
    public byte[] copy(byte[] from) {
        byte[] copy = new byte[from.length];
        System.arraycopy(from, 0, copy, 0, from.length);
        return copy;
    }

    /**
     * 将字节数组序列化到输出视图。
     *
     * <p>序列化过程:
     * <ol>
     *   <li>使用变长整数编码写入数组长度
     *   <li>写入完整的字节数组内容
     * </ol>
     *
     * @param record 要序列化的字节数组
     * @param target 目标输出视图
     * @throws IOException 如果写入过程中发生 I/O 错误
     */
    @Override
    public void serialize(byte[] record, DataOutputView target) throws IOException {
        // 写入数组长度(使用变长编码)
        encodeInt(target, record.length);
        // 写入字节数组内容
        target.write(record);
    }

    /**
     * 从输入视图反序列化字节数组。
     *
     * <p>反序列化过程:
     * <ol>
     *   <li>使用变长整数解码读取数组长度
     *   <li>分配对应长度的字节数组
     *   <li>完整读取字节数组内容
     * </ol>
     *
     * @param source 源输入视图
     * @return 反序列化的字节数组
     * @throws IOException 如果读取过程中发生 I/O 错误
     */
    @Override
    public byte[] deserialize(DataInputView source) throws IOException {
        // 读取数组长度(使用变长解码)
        int len = decodeInt(source);
        // 分配字节数组并读取内容
        byte[] result = new byte[len];
        source.readFully(result);
        return result;
    }
}
