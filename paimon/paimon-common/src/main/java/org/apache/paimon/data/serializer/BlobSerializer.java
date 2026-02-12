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

import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;

import java.io.IOException;

/**
 * Blob 类型序列化器 - 用于序列化 {@link Blob} 对象。
 *
 * <p>Blob (Binary Large Object) 表示二进制大对象,用于存储大型二进制数据,
 * 如图片、音频、视频等非结构化数据。
 *
 * <p>序列化实现:
 * <ul>
 *   <li>Blob 序列化实际上委托给 {@link BinarySerializer} 处理
 *   <li>先将 Blob 转换为字节数组,然后使用 BinarySerializer 序列化
 *   <li>反序列化时,使用 BinarySerializer 读取字节数组,再包装为 BlobData
 * </ul>
 *
 * <p>设计特点:
 * <ul>
 *   <li>委托模式: 复用 BinarySerializer 的序列化逻辑
 *   <li>不可变: Blob 对象是不可变的,copy() 直接返回原对象
 *   <li>简单封装: Blob 本质上是对字节数组的简单封装
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>BLOB 类型字段的序列化
 *   <li>大型二进制数据的存储
 *   <li>文件内容的序列化
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * BlobSerializer serializer = BlobSerializer.INSTANCE;
 * Blob blob = new BlobData(new byte[] {1, 2, 3, 4, 5});
 *
 * // 序列化
 * byte[] bytes = serializer.serializeToBytes(blob);
 *
 * // 反序列化
 * Blob deserialized = serializer.deserializeFromBytes(bytes);
 * }</pre>
 */
public class BlobSerializer extends SerializerSingleton<Blob> {

    private static final long serialVersionUID = 1L;

    /** BlobSerializer 的共享单例实例。 */
    public static final BlobSerializer INSTANCE = new BlobSerializer();

    /**
     * 复制 Blob 对象。
     *
     * <p>由于 Blob 通常是不可变的,直接返回原对象即可。
     * 如果需要修改,实现类会创建新的 Blob 对象。
     *
     * @param from 要复制的 Blob 对象
     * @return 原 Blob 对象
     */
    @Override
    public Blob copy(Blob from) {
        return from;
    }

    /**
     * 将 Blob 序列化到输出视图。
     *
     * <p>序列化过程:
     * <ol>
     *   <li>调用 blob.toData() 获取字节数组
     *   <li>使用 BinarySerializer 序列化字节数组
     * </ol>
     *
     * @param blob 要序列化的 Blob 对象
     * @param target 目标输出视图
     * @throws IOException 如果写入过程中发生 I/O 错误
     */
    @Override
    public void serialize(Blob blob, DataOutputView target) throws IOException {
        BinarySerializer.INSTANCE.serialize(blob.toData(), target);
    }

    /**
     * 从输入视图反序列化 Blob。
     *
     * <p>反序列化过程:
     * <ol>
     *   <li>使用 BinarySerializer 反序列化字节数组
     *   <li>将字节数组包装为 BlobData 对象
     * </ol>
     *
     * @param source 源输入视图
     * @return 反序列化的 Blob 对象
     * @throws IOException 如果读取过程中发生 I/O 错误
     */
    @Override
    public Blob deserialize(DataInputView source) throws IOException {
        byte[] bytes = BinarySerializer.INSTANCE.deserialize(source);
        return new BlobData(bytes);
    }
}
