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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.memory.MemorySegmentUtils;

import java.io.IOException;

import static org.apache.paimon.utils.VarLengthIntUtils.decodeInt;
import static org.apache.paimon.utils.VarLengthIntUtils.encodeInt;

/**
 * BinaryString 序列化器 - 用于序列化 {@link BinaryString} 对象。
 *
 * <p>这是一个无状态的单例序列化器,专门处理 Paimon 的内部字符串表示 BinaryString。
 * BinaryString 是 UTF-8 编码的二进制字符串,存储在内存段(MemorySegment)中,具有零拷贝特性。
 *
 * <p>序列化格式:
 * <ul>
 *   <li>长度前缀: 使用变长整数编码存储字符串的字节长度
 *   <li>字符串数据: UTF-8 编码的字节数组
 * </ul>
 *
 * <p>变长整数编码优势:
 * <ul>
 *   <li>短字符串节省空间: 长度 < 128 的字符串只需 1 字节存储长度
 *   <li>紧凑表示: 避免固定长度整数的空间浪费
 * </ul>
 *
 * <p>设计特点:
 * <ul>
 *   <li>零拷贝: 直接从 MemorySegment 复制数据,避免不必要的字节数组转换
 *   <li>支持字符串序列化: 可以转换为 Java String 进行调试
 *   <li>高性能: 针对大规模字符串处理进行优化
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>表数据的字符串字段序列化
 *   <li>索引键的序列化
 *   <li>分区值的序列化
 * </ul>
 */
public final class BinaryStringSerializer extends SerializerSingleton<BinaryString> {

    private static final long serialVersionUID = 1L;

    /** BinaryStringSerializer 的共享单例实例。 */
    public static final BinaryStringSerializer INSTANCE = new BinaryStringSerializer();

    /** 私有构造函数,强制使用单例实例。 */
    private BinaryStringSerializer() {}

    /**
     * 复制 BinaryString 对象。
     *
     * <p>创建 BinaryString 的深拷贝,包括底层的字节数据。
     * 这确保修改副本不会影响原始对象。
     *
     * @param from 要复制的 BinaryString 对象
     * @return BinaryString 的深拷贝
     */
    @Override
    public BinaryString copy(BinaryString from) {
        // BinaryString 是 BinaryString 的唯一实现
        return from.copy();
    }

    /**
     * 将 BinaryString 序列化到输出视图。
     *
     * <p>序列化过程:
     * <ol>
     *   <li>使用变长整数编码写入字节长度
     *   <li>将底层 MemorySegment 中的数据拷贝到输出流
     * </ol>
     *
     * @param string 要序列化的 BinaryString 对象
     * @param target 目标输出视图
     * @throws IOException 如果写入过程中发生 I/O 错误
     */
    @Override
    public void serialize(BinaryString string, DataOutputView target) throws IOException {
        // 写入字符串的字节长度(使用变长编码)
        encodeInt(target, string.getSizeInBytes());
        // 将 MemorySegment 中的数据拷贝到输出视图
        MemorySegmentUtils.copyToView(
                string.getSegments(), string.getOffset(), string.getSizeInBytes(), target);
    }

    /**
     * 从输入视图反序列化 BinaryString。
     *
     * @param source 源输入视图
     * @return 反序列化的 BinaryString 对象
     * @throws IOException 如果读取过程中发生 I/O 错误
     */
    @Override
    public BinaryString deserialize(DataInputView source) throws IOException {
        return deserializeInternal(source);
    }

    /**
     * 内部反序列化方法,从输入视图读取 BinaryString。
     *
     * <p>反序列化过程:
     * <ol>
     *   <li>使用变长整数解码读取字节长度
     *   <li>读取指定长度的字节数组
     *   <li>从字节数组构造 BinaryString
     * </ol>
     *
     * <p>这个静态方法可以被其他类复用,避免重复代码。
     *
     * @param source 源输入视图
     * @return 反序列化的 BinaryString 对象
     * @throws IOException 如果读取过程中发生 I/O 错误
     */
    public static BinaryString deserializeInternal(DataInputView source) throws IOException {
        // 读取字符串的字节长度(使用变长解码)
        int length = decodeInt(source);
        // 读取字节数组
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        // 从字节数组构造 BinaryString
        return BinaryString.fromBytes(bytes);
    }

    /**
     * 将 BinaryString 序列化为 Java String。
     *
     * <p>这个方法用于调试和日志记录,将内部的 UTF-8 字节转换为 Java String。
     *
     * @param record 要序列化的 BinaryString 对象
     * @return Java String 表示
     */
    @Override
    public String serializeToString(BinaryString record) {
        return record.toString();
    }

    /**
     * 从 Java String 反序列化 BinaryString。
     *
     * @param s 要反序列化的字符串
     * @return BinaryString 对象
     */
    @Override
    public BinaryString deserializeFromString(String s) {
        return BinaryString.fromString(s);
    }
}
