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
import org.apache.paimon.data.BinaryWriter;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentUtils;
import org.apache.paimon.types.DataType;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * 内部数组序列化器 - 用于序列化 {@link InternalArray} 实例。
 *
 * <p>这个序列化器可以处理任意类型元素的数组,通过统一将数组转换为 {@link BinaryArray} 格式来实现。
 * 它是 Paimon 中数组数据的标准序列化实现。
 *
 * <p>序列化格式:
 * <ul>
 *   <li>数组长度: 4 字节整数,表示 BinaryArray 的总字节数(不是元素数量)
 *   <li>BinaryArray 数据: 完整的 BinaryArray 二进制表示
 *     <ul>
 *       <li>固定长度部分: null 位图 + 固定大小元素区域
 *       <li>可变长度部分: 字符串、数组等可变长度数据
 *     </ul>
 * </ul>
 *
 * <p>性能特点:
 * <ul>
 *   <li>零拷贝: 对于 BinaryArray 输入,直接序列化,无需转换
 *   <li>对象重用: 维护可重用的 BinaryArray 和 BinaryArrayWriter 实例
 *   <li>批量写入: 通过 MemorySegment 实现高效的内存块传输
 *   <li>类型优化: 为原始类型数组提供快速拷贝路径
 * </ul>
 *
 * <p>支持的数组类型:
 * <ul>
 *   <li>{@link BinaryArray}: 直接序列化,性能最优
 *   <li>{@link GenericArray}: 根据元素类型选择处理方式
 *     <ul>
 *       <li>原始类型数组(boolean[], int[], long[] 等): 直接数组拷贝
 *       <li>对象数组: 逐元素深拷贝
 *     </ul>
 *   <li>其他 InternalArray 实现: 先转换为 BinaryArray 再处理
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建整数数组序列化器
 * DataType elementType = new IntType();
 * InternalArraySerializer serializer = new InternalArraySerializer(elementType);
 *
 * // 序列化数组
 * GenericArray array = new GenericArray(new int[]{1, 2, 3});
 * DataOutputSerializer output = new DataOutputSerializer(128);
 * serializer.serialize(array, output);
 *
 * // 反序列化
 * DataInputDeserializer input = new DataInputDeserializer(output.getSharedBuffer());
 * BinaryArray deserialized = serializer.deserialize(input);
 *
 * // 转换为 BinaryArray
 * BinaryArray binaryArray = serializer.toBinaryArray(array);
 * }</pre>
 *
 * <p>线程安全性:
 * <ul>
 *   <li>序列化器实例不是线程安全的,因为内部维护了可重用对象
 *   <li>每个线程应该使用 {@link #duplicate()} 创建独立的序列化器实例
 * </ul>
 *
 * @see BinaryArray
 * @see GenericArray
 * @see InternalArray
 * @see InternalSerializers
 */
public class InternalArraySerializer implements Serializer<InternalArray> {
    private static final long serialVersionUID = 1L;

    /** 数组元素的数据类型。 */
    private final DataType eleType;

    /** 数组元素的序列化器。 */
    private final Serializer<Object> eleSer;

    /** 数组元素的访问器,用于高效读取元素。 */
    private final InternalArray.ElementGetter elementGetter;

    /** 可重用的 BinaryArray 实例,避免频繁对象创建。 */
    private transient BinaryArray reuseArray;

    /** 可重用的 BinaryArrayWriter 实例,用于写入数组数据。 */
    private transient BinaryArrayWriter reuseWriter;

    /**
     * 创建指定元素类型的数组序列化器。
     *
     * @param eleType 数组元素的数据类型
     */
    public InternalArraySerializer(DataType eleType) {
        this(eleType, InternalSerializers.create(eleType));
    }

    /**
     * 内部构造函数,直接使用给定的元素序列化器。
     *
     * @param eleType 数组元素的数据类型
     * @param eleSer 元素序列化器
     */
    private InternalArraySerializer(DataType eleType, Serializer<Object> eleSer) {
        this.eleType = eleType;
        this.eleSer = eleSer;
        this.elementGetter = InternalArray.createElementGetter(eleType);
    }

    /**
     * 复制序列化器实例。
     *
     * <p>创建新实例并复制元素序列化器,确保每个线程有独立的可重用对象。
     *
     * @return 序列化器的副本
     */
    @Override
    public InternalArraySerializer duplicate() {
        return new InternalArraySerializer(eleType, eleSer.duplicate());
    }

    /**
     * 深拷贝数组。
     *
     * <p>根据数组类型选择最优的拷贝策略:
     * <ul>
     *   <li>GenericArray: 根据是否为原始类型数组选择拷贝方式
     *   <li>BinaryArray: 调用其 copy() 方法
     *   <li>其他类型: 先转换为 BinaryArray 再拷贝
     * </ul>
     *
     * @param from 要拷贝的数组
     * @return 数组的深拷贝
     */
    @Override
    public InternalArray copy(InternalArray from) {
        if (from instanceof GenericArray) {
            return copyGenericArray((GenericArray) from);
        } else if (from instanceof BinaryArray) {
            return ((BinaryArray) from).copy();
        } else {
            return toBinaryArray(from).copy();
        }
    }

    /**
     * 拷贝 GenericArray。
     *
     * <p>对于原始类型数组,使用高效的数组拷贝;对于对象数组,逐元素深拷贝。
     *
     * @param array 要拷贝的 GenericArray
     * @return GenericArray 的深拷贝
     */
    private GenericArray copyGenericArray(GenericArray array) {
        if (array.isPrimitiveArray()) {
            switch (eleType.getTypeRoot()) {
                case BOOLEAN:
                    return new GenericArray(Arrays.copyOf(array.toBooleanArray(), array.size()));
                case TINYINT:
                    return new GenericArray(Arrays.copyOf(array.toByteArray(), array.size()));
                case SMALLINT:
                    return new GenericArray(Arrays.copyOf(array.toShortArray(), array.size()));
                case INTEGER:
                    return new GenericArray(Arrays.copyOf(array.toIntArray(), array.size()));
                case BIGINT:
                    return new GenericArray(Arrays.copyOf(array.toLongArray(), array.size()));
                case FLOAT:
                    return new GenericArray(Arrays.copyOf(array.toFloatArray(), array.size()));
                case DOUBLE:
                    return new GenericArray(Arrays.copyOf(array.toDoubleArray(), array.size()));
                default:
                    throw new RuntimeException("Unknown type: " + eleType);
            }
        } else {
            Object[] objectArray = array.toObjectArray();
            Object[] newArray =
                    (Object[]) Array.newInstance(InternalRow.getDataClass(eleType), array.size());
            for (int i = 0; i < array.size(); i++) {
                if (objectArray[i] != null) {
                    newArray[i] = eleSer.copy(objectArray[i]);
                }
            }
            return new GenericArray(newArray);
        }
    }

    /**
     * 将数组序列化到输出视图。
     *
     * <p>序列化过程:
     * <ol>
     *   <li>将输入数组转换为 BinaryArray(如果尚未是)
     *   <li>写入 BinaryArray 的总字节数(4 字节整数)
     *   <li>通过 MemorySegment 直接拷贝 BinaryArray 的完整二进制数据
     * </ol>
     *
     * @param record 要序列化的数组
     * @param target 目标输出视图
     * @throws IOException 如果写入过程中发生 I/O 错误
     */
    @Override
    public void serialize(InternalArray record, DataOutputView target) throws IOException {
        BinaryArray binaryArray = toBinaryArray(record);
        target.writeInt(binaryArray.getSizeInBytes());
        MemorySegmentUtils.copyToView(
                binaryArray.getSegments(),
                binaryArray.getOffset(),
                binaryArray.getSizeInBytes(),
                target);
    }

    /**
     * 将 InternalArray 转换为 BinaryArray。
     *
     * <p>这是一个核心方法,提供了统一的二进制转换能力:
     * <ul>
     *   <li>如果已经是 BinaryArray,直接返回
     *   <li>否则使用 BinaryArrayWriter 将其转换为 BinaryArray
     * </ul>
     *
     * <p>转换过程:
     * <ol>
     *   <li>创建或重用 BinaryArrayWriter
     *   <li>逐元素写入:
     *     <ul>
     *       <li>null 元素: 在 null 位图中设置对应位
     *       <li>非 null 元素: 使用元素序列化器写入数据
     *     </ul>
     *   <li>完成写入并返回 BinaryArray
     * </ol>
     *
     * @param from 要转换的数组
     * @return BinaryArray 表示
     */
    public BinaryArray toBinaryArray(InternalArray from) {
        if (from instanceof BinaryArray) {
            return (BinaryArray) from;
        }

        int numElements = from.size();
        if (reuseArray == null) {
            reuseArray = new BinaryArray();
        }
        if (reuseWriter == null || reuseWriter.getNumElements() != numElements) {
            reuseWriter =
                    new BinaryArrayWriter(
                            reuseArray,
                            numElements,
                            BinaryArray.calculateFixLengthPartSize(eleType));
        } else {
            reuseWriter.reset();
        }

        for (int i = 0; i < numElements; i++) {
            if (from.isNullAt(i)) {
                reuseWriter.setNullAt(i, eleType);
            } else {
                BinaryWriter.write(
                        reuseWriter, i, elementGetter.getElementOrNull(from, i), eleType, eleSer);
            }
        }
        reuseWriter.complete();

        return reuseArray;
    }

    /**
     * 从输入视图反序列化数组。
     *
     * <p>创建新的 BinaryArray 实例并从输入流读取数据。
     *
     * @param source 源输入视图
     * @return 反序列化的 BinaryArray
     * @throws IOException 如果读取过程中发生 I/O 错误
     */
    @Override
    public InternalArray deserialize(DataInputView source) throws IOException {
        return deserializeReuse(new BinaryArray(), source);
    }

    /**
     * 重用 BinaryArray 进行反序列化。
     *
     * <p>反序列化过程:
     * <ol>
     *   <li>读取数组的总字节数(4 字节整数)
     *   <li>分配字节数组并读取完整数据
     *   <li>将 BinaryArray 指向新分配的内存
     * </ol>
     *
     * @param reuse 要重用的 BinaryArray 实例
     * @param source 源输入视图
     * @return 反序列化的 BinaryArray(即传入的 reuse 对象)
     * @throws IOException 如果读取过程中发生 I/O 错误
     */
    private BinaryArray deserializeReuse(BinaryArray reuse, DataInputView source)
            throws IOException {
        int length = source.readInt();
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        reuse.pointTo(MemorySegment.wrap(bytes), 0, bytes.length);
        return reuse;
    }

    /**
     * 判断两个序列化器是否相等。
     *
     * <p>当且仅当元素类型相等时,两个数组序列化器才相等。
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

        InternalArraySerializer that = (InternalArraySerializer) o;

        return eleType.equals(that.eleType);
    }

    /**
     * 计算序列化器的哈希码。
     *
     * <p>基于元素类型的哈希码。
     *
     * @return 哈希码
     */
    @Override
    public int hashCode() {
        return eleType.hashCode();
    }
}
