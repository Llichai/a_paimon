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

import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryArrayWriter;
import org.apache.paimon.data.BinaryMap;
import org.apache.paimon.data.BinaryWriter;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentUtils;
import org.apache.paimon.types.DataType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 内部 Map 序列化器 - 用于序列化 {@link InternalMap} 实例。
 *
 * <p>这个序列化器处理 Map 数据结构,通过将 Map 转换为键数组和值数组的 {@link BinaryMap} 格式来实现。
 * 它是 Paimon 中 Map 类型数据的标准序列化实现。
 *
 * <p>内部表示:
 * <ul>
 *   <li>InternalMap 内部由两个 InternalArray 组成:
 *     <ul>
 *       <li>keyArray: 存储所有键的数组
 *       <li>valueArray: 存储所有值的数组
 *     </ul>
 *   <li>键和值通过数组索引一一对应
 * </ul>
 *
 * <p>序列化格式:
 * <ul>
 *   <li>Map 长度: 4 字节整数,表示 BinaryMap 的总字节数
 *   <li>BinaryMap 数据: 包含序列化的键数组和值数组
 *     <ul>
 *       <li>键数组 (BinaryArray): 所有键的二进制表示
 *       <li>值数组 (BinaryArray): 所有值的二进制表示
 *     </ul>
 * </ul>
 *
 * <p>性能特点:
 * <ul>
 *   <li>零拷贝: 对于已经是 BinaryMap 的输入,直接序列化
 *   <li>对象重用: 维护可重用的 BinaryArray 和 BinaryArrayWriter 实例
 *   <li>批量处理: 同时处理所有键值对,避免多次遍历
 *   <li>内存高效: 紧凑的二进制格式,减少存储开销
 * </ul>
 *
 * <p>重要说明:
 * <ul>
 *   <li>Map 应该是 HashMap: 将 TreeMap 的键值对插入 HashMap 时可能出现问题
 *   <li>键类型限制: 键类型必须支持比较和哈希
 *   <li>null 值支持: 值可以为 null,会在值数组中标记
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建 Map 序列化器
 * DataType keyType = new IntType();
 * DataType valueType = new VarCharType();
 * InternalMapSerializer serializer =
 *     new InternalMapSerializer(keyType, valueType);
 *
 * // 序列化 Map
 * Map<Integer, BinaryString> javaMap = new HashMap<>();
 * javaMap.put(1, BinaryString.fromString("value1"));
 * InternalMap map = new GenericMap(javaMap);
 *
 * DataOutputSerializer output = new DataOutputSerializer(128);
 * serializer.serialize(map, output);
 *
 * // 反序列化
 * DataInputDeserializer input = new DataInputDeserializer(output.getSharedBuffer());
 * BinaryMap deserialized = serializer.deserialize(input);
 *
 * // 转换为 BinaryMap
 * BinaryMap binaryMap = serializer.toBinaryMap(map);
 *
 * // 转换为 Java Map
 * Map<Object, Object> javaMapResult =
 *     InternalMapSerializer.convertToJavaMap(binaryMap, keyType, valueType);
 * }</pre>
 *
 * <p>线程安全性:
 * <ul>
 *   <li>序列化器实例不是线程安全的,因为内部维护了可重用对象
 *   <li>每个线程应该使用 {@link #duplicate()} 创建独立的序列化器实例
 * </ul>
 *
 * @see BinaryMap
 * @see InternalMap
 * @see InternalArraySerializer
 * @see InternalSerializers
 */
public class InternalMapSerializer implements Serializer<InternalMap> {

    /** 键的数据类型。 */
    private final DataType keyType;

    /** 值的数据类型。 */
    private final DataType valueType;

    /** 键的序列化器。 */
    private final Serializer keySerializer;

    /** 值的序列化器。 */
    private final Serializer valueSerializer;

    /** 键的访问器,用于高效读取键。 */
    private final InternalArray.ElementGetter keyGetter;

    /** 值的访问器,用于高效读取值。 */
    private final InternalArray.ElementGetter valueGetter;

    /** 可重用的键数组实例,避免频繁对象创建。 */
    private transient BinaryArray reuseKeyArray;

    /** 可重用的值数组实例,避免频繁对象创建。 */
    private transient BinaryArray reuseValueArray;

    /** 可重用的键数组写入器。 */
    private transient BinaryArrayWriter reuseKeyWriter;

    /** 可重用的值数组写入器。 */
    private transient BinaryArrayWriter reuseValueWriter;

    /**
     * 创建指定键值类型的 Map 序列化器。
     *
     * @param keyType 键的数据类型
     * @param valueType 值的数据类型
     */
    public InternalMapSerializer(DataType keyType, DataType valueType) {
        this(
                keyType,
                valueType,
                InternalSerializers.create(keyType),
                InternalSerializers.create(valueType));
    }

    /**
     * 内部构造函数,直接使用给定的键值序列化器。
     *
     * @param keyType 键的数据类型
     * @param valueType 值的数据类型
     * @param keySerializer 键序列化器
     * @param valueSerializer 值序列化器
     */
    private InternalMapSerializer(
            DataType keyType,
            DataType valueType,
            Serializer keySerializer,
            Serializer valueSerializer) {
        this.keyType = keyType;
        this.valueType = valueType;

        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;

        this.keyGetter = InternalArray.createElementGetter(keyType);
        this.valueGetter = InternalArray.createElementGetter(valueType);
    }

    /**
     * 复制序列化器实例。
     *
     * <p>创建新实例并复制键值序列化器,确保每个线程有独立的可重用对象。
     *
     * @return 序列化器的副本
     */
    @Override
    public Serializer<InternalMap> duplicate() {
        return new InternalMapSerializer(
                keyType, valueType, keySerializer.duplicate(), valueSerializer.duplicate());
    }

    /**
     * 深拷贝 Map。
     *
     * <p>注意: Map 应该是 HashMap。将 TreeMap 的键值对插入 HashMap 时可能出现问题。
     *
     * <p>对于 BinaryMap,直接调用其 copy() 方法;对于其他类型,先转换为 BinaryMap。
     *
     * @param from 要拷贝的 Map
     * @return Map 的深拷贝
     */
    @Override
    public InternalMap copy(InternalMap from) {
        if (from instanceof BinaryMap) {
            return ((BinaryMap) from).copy();
        } else {
            return toBinaryMap(from);
        }
    }

    /**
     * 将 Map 序列化到输出视图。
     *
     * <p>序列化过程:
     * <ol>
     *   <li>将输入 Map 转换为 BinaryMap(如果尚未是)
     *   <li>写入 BinaryMap 的总字节数(4 字节整数)
     *   <li>通过 MemorySegment 直接拷贝 BinaryMap 的完整二进制数据
     * </ol>
     *
     * @param record 要序列化的 Map
     * @param target 目标输出视图
     * @throws IOException 如果写入过程中发生 I/O 错误
     */
    @Override
    public void serialize(InternalMap record, DataOutputView target) throws IOException {
        BinaryMap binaryMap = toBinaryMap(record);
        target.writeInt(binaryMap.getSizeInBytes());
        MemorySegmentUtils.copyToView(
                binaryMap.getSegments(), binaryMap.getOffset(), binaryMap.getSizeInBytes(), target);
    }

    /**
     * 将 InternalMap 转换为 BinaryMap。
     *
     * <p>这是一个核心方法,提供了统一的二进制转换能力:
     * <ul>
     *   <li>如果已经是 BinaryMap,直接返回
     *   <li>如果键数组和值数组都是 BinaryArray,直接构造 BinaryMap
     *   <li>否则使用 BinaryArrayWriter 将键数组和值数组转换为 BinaryArray
     * </ul>
     *
     * <p>转换过程:
     * <ol>
     *   <li>提取键数组和值数组
     *   <li>创建或重用 BinaryArrayWriter
     *   <li>逐元素写入键和值:
     *     <ul>
     *       <li>null 元素: 在 null 位图中设置对应位
     *       <li>非 null 元素: 使用对应的序列化器写入数据
     *     </ul>
     *   <li>完成写入并构造 BinaryMap
     * </ol>
     *
     * @param from 要转换的 Map
     * @return BinaryMap 表示
     */
    public BinaryMap toBinaryMap(InternalMap from) {
        if (from instanceof BinaryMap) {
            return (BinaryMap) from;
        }

        InternalArray keyArray = from.keyArray();
        InternalArray valueArray = from.valueArray();
        if (keyArray instanceof BinaryArray && valueArray instanceof BinaryArray) {
            return BinaryMap.valueOf((BinaryArray) keyArray, (BinaryArray) valueArray);
        }

        int numElements = from.size();
        if (reuseKeyArray == null) {
            reuseKeyArray = new BinaryArray();
        }
        if (reuseValueArray == null) {
            reuseValueArray = new BinaryArray();
        }
        if (reuseKeyWriter == null || reuseKeyWriter.getNumElements() != numElements) {
            reuseKeyWriter =
                    new BinaryArrayWriter(
                            reuseKeyArray,
                            numElements,
                            BinaryArray.calculateFixLengthPartSize(keyType));
        } else {
            reuseKeyWriter.reset();
        }
        if (reuseValueWriter == null || reuseValueWriter.getNumElements() != numElements) {
            reuseValueWriter =
                    new BinaryArrayWriter(
                            reuseValueArray,
                            numElements,
                            BinaryArray.calculateFixLengthPartSize(valueType));
        } else {
            reuseValueWriter.reset();
        }

        for (int i = 0; i < from.size(); i++) {
            Object key = keyGetter.getElementOrNull(keyArray, i);
            Object value = valueGetter.getElementOrNull(valueArray, i);
            if (key == null) {
                reuseKeyWriter.setNullAt(i, keyType);
            } else {
                BinaryWriter.write(reuseKeyWriter, i, key, keyType, keySerializer);
            }
            if (value == null) {
                reuseValueWriter.setNullAt(i, valueType);
            } else {
                BinaryWriter.write(reuseValueWriter, i, value, valueType, valueSerializer);
            }
        }

        reuseKeyWriter.complete();
        reuseValueWriter.complete();

        return BinaryMap.valueOf(reuseKeyArray, reuseValueArray);
    }

    /**
     * 从输入视图反序列化 Map。
     *
     * <p>创建新的 BinaryMap 实例并从输入流读取数据。
     *
     * @param source 源输入视图
     * @return 反序列化的 BinaryMap
     * @throws IOException 如果读取过程中发生 I/O 错误
     */
    @Override
    public InternalMap deserialize(DataInputView source) throws IOException {
        return deserializeReuse(new BinaryMap(), source);
    }

    /**
     * 重用 BinaryMap 进行反序列化。
     *
     * <p>反序列化过程:
     * <ol>
     *   <li>读取 Map 的总字节数(4 字节整数)
     *   <li>分配字节数组并读取完整数据
     *   <li>将 BinaryMap 指向新分配的内存
     * </ol>
     *
     * @param reuse 要重用的 BinaryMap 实例
     * @param source 源输入视图
     * @return 反序列化的 BinaryMap(即传入的 reuse 对象)
     * @throws IOException 如果读取过程中发生 I/O 错误
     */
    private BinaryMap deserializeReuse(BinaryMap reuse, DataInputView source) throws IOException {
        int length = source.readInt();
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        reuse.pointTo(MemorySegment.wrap(bytes), 0, bytes.length);
        return reuse;
    }

    /**
     * 判断两个序列化器是否相等。
     *
     * <p>当且仅当键类型和值类型都相等时,两个 Map 序列化器才相等。
     *
     * @param o 要比较的对象
     * @return 如果相等返回 true,否则返回 false
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InternalMapSerializer that = (InternalMapSerializer) o;

        return keyType.equals(that.keyType) && valueType.equals(that.valueType);
    }

    /**
     * 计算序列化器的哈希码。
     *
     * <p>基于键类型和值类型的哈希码。
     *
     * @return 哈希码
     */
    @Override
    public int hashCode() {
        int result = keyType.hashCode();
        result = 31 * result + valueType.hashCode();
        return result;
    }

    /**
     * 将 {@link InternalMap} 转换为 Java {@link Map}。
     *
     * <p>这是一个工具方法,用于将 Paimon 的内部 Map 表示转换为标准 Java Map。
     * 注意:返回的 Map 中的键和值仍然是内部数据结构对象。
     *
     * <p>转换过程:
     * <ol>
     *   <li>从 InternalMap 中提取键数组和值数组
     *   <li>创建新的 HashMap
     *   <li>遍历数组,使用 ElementGetter 读取每个键值对
     *   <li>将键值对插入 HashMap
     * </ol>
     *
     * @param map 要转换的 InternalMap
     * @param keyType 键的数据类型
     * @param valueType 值的数据类型
     * @return Java Map,键和值仍然是内部数据结构对象
     */
    public static Map<Object, Object> convertToJavaMap(
            InternalMap map, DataType keyType, DataType valueType) {
        InternalArray keyArray = map.keyArray();
        InternalArray valueArray = map.valueArray();
        Map<Object, Object> javaMap = new HashMap<>();
        InternalArray.ElementGetter keyGetter = InternalArray.createElementGetter(keyType);
        InternalArray.ElementGetter valueGetter = InternalArray.createElementGetter(valueType);
        for (int i = 0; i < map.size(); i++) {
            Object key = keyGetter.getElementOrNull(keyArray, i);
            Object value = valueGetter.getElementOrNull(valueArray, i);
            javaMap.put(key, value);
        }
        return javaMap;
    }
}
