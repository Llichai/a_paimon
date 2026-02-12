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

package org.apache.paimon.data.columnar.writable;

import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.Dictionary;

/**
 * 可写列向量接口。
 *
 * <p>WritableColumnVector 扩展了只读的 {@link ColumnVector},提供数据写入能力。它是列式存储写入的核心接口,
 * 支持批量写入、NULL值处理、字典编码等功能。
 *
 * <h2>与Heap系列向量的区别</h2>
 * <ul>
 *   <li>Heap向量(如HeapIntVector)是只读的,而Writable向量支持写入操作
 *   <li>Heap向量数据固定,Writable向量支持动态扩容
 *   <li>Heap向量主要用于读取,Writable向量主要用于写入和构建
 *   <li>Writable向量可以转换为Heap向量用于后续读取
 * </ul>
 *
 * <h2>写入机制</h2>
 * <ul>
 *   <li>支持随机位置写入(通过rowId指定位置)
 *   <li>支持追加式写入(通过appendXxx方法)
 *   <li>追加写入时自动管理elementsAppended计数器
 *   <li>写入前需要确保容量足够(通过reserve方法)
 * </ul>
 *
 * <h2>内存管理</h2>
 * <ul>
 *   <li>通过reserve()方法预分配内存,采用2倍扩容策略
 *   <li>通过reset()方法重置向量状态,但不释放已分配内存
 *   <li>内存在向量对象生命周期内保持,避免频繁分配
 *   <li>扩容失败时抛出RuntimeException,需要调用方处理
 * </ul>
 *
 * <h2>字典编码支持</h2>
 * 支持字典编码以减少存储空间,特别适合重复值较多的场景:
 * <ul>
 *   <li>通过setDictionary()设置字典
 *   <li>通过reserveDictionaryIds()获取字典ID向量
 *   <li>实际值存储在字典中,向量中仅存储整数ID
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建可写整数向量
 * WritableIntVector vector = new HeapIntVector(100);
 *
 * // 追加数据
 * vector.appendInt(42);
 * vector.appendInt(100);
 * vector.appendNull();
 *
 * // 随机位置写入
 * vector.reserve(10);
 * vector.setInt(5, 999);
 *
 * // 批量写入
 * int[] values = {1, 2, 3, 4, 5};
 * vector.setInts(0, 5, values, 0);
 *
 * // 重置向量状态
 * vector.reset();
 * }</pre>
 *
 * <h2>线程安全性</h2>
 * WritableColumnVector <b>不是线程安全的</b>。多线程环境下需要外部同步:
 * <ul>
 *   <li>同一时刻只能有一个线程写入
 *   <li>写入过程中不能并发读取
 *   <li>建议每个线程使用独立的向量实例
 * </ul>
 *
 * @see ColumnVector 只读列向量接口
 * @see AbstractWritableVector 抽象基类实现
 * @see Dictionary 字典编码
 */
public interface WritableColumnVector extends ColumnVector {

    /**
     * 重置列向量到默认状态。
     *
     * <p>重置操作会:
     * <ul>
     *   <li>将elementsAppended计数器重置为0
     *   <li>将noNulls标志重置为true
     *   <li>将isAllNull标志重置为false
     *   <li>不会释放已分配的内存,保持当前容量
     * </ul>
     *
     * <p>重置后向量可以重新使用,避免频繁创建新对象。
     */
    void reset();

    /**
     * 在指定位置设置NULL值。
     *
     * @param rowId 行ID,范围 [0, capacity)
     */
    void setNullAt(int rowId);

    /**
     * 批量设置NULL值。
     *
     * @param rowId 起始行ID
     * @param count NULL值数量,设置范围为 [rowId, rowId + count)
     */
    void setNulls(int rowId, int count);

    /**
     * 追加一个NULL值。
     *
     * <p>该方法会:
     * <ol>
     *   <li>自动扩容确保有足够空间
     *   <li>在当前追加位置设置NULL
     *   <li>增加elementsAppended计数器
     * </ol>
     */
    default void appendNull() {
        int elementsAppended = getElementsAppended();
        reserve(elementsAppended + 1);
        setNullAt(elementsAppended);
        addElementsAppended(1);
    }

    /**
     * 用NULL值填充整个列向量。
     *
     * <p>将向量中所有位置设置为NULL,常用于初始化场景。
     */
    void fillWithNulls();

    /**
     * 设置字典编码。
     *
     * <p>设置字典后,向量中存储的是字典ID而非实际值。需要配合 {@link #reserveDictionaryIds} 使用。
     *
     * @param dictionary 字典对象,包含实际值的映射
     */
    void setDictionary(Dictionary dictionary);

    /**
     * 检查是否使用了字典编码。
     *
     * @return 如果设置了字典返回true,否则返回false
     */
    boolean hasDictionary();

    /**
     * 预留字典ID向量。
     *
     * <p>当使用字典编码时,实际值存储在字典中,向量中存储的是整数ID。此方法返回用于存储字典ID的整数向量。
     *
     * <p>注意事项:
     * <ul>
     *   <li>返回的向量容量应大于等于指定capacity
     *   <li>字典ID必须与 {@link #setDictionary} 设置的字典一致
     *   <li>不支持混合使用多个字典
     * </ul>
     *
     * @param capacity 所需容量
     * @return 字典ID向量
     */
    WritableIntVector reserveDictionaryIds(int capacity);

    /**
     * 获取已预留的字典ID向量。
     *
     * @return 字典ID向量,如果未使用字典编码则返回null
     */
    WritableIntVector getDictionaryIds();

    /**
     * 将所有值设置为NULL。
     *
     * <p>设置isAllNull标志为true,用于标识整个向量为NULL的特殊情况。
     */
    void setAllNull();

    /**
     * 检查是否所有值都是NULL。
     *
     * @return 如果所有值都是NULL返回true,否则返回false
     */
    boolean isAllNull();

    /**
     * 预留指定容量的空间。
     *
     * <p>如果请求的容量大于当前容量,会触发扩容操作。扩容策略为2倍增长,以平衡内存使用和扩容频率。
     *
     * @param capacity 需要的容量
     * @throws IllegalArgumentException 如果capacity为负数
     * @throws RuntimeException 如果内存分配失败
     */
    void reserve(int capacity);

    /**
     * 获取已追加的元素数量。
     *
     * <p>该计数器记录通过appendXxx方法追加的元素个数,也可以理解为下一个追加位置的索引。
     *
     * @return 已追加的元素数量
     */
    int getElementsAppended();

    /**
     * 增加已追加元素的计数。
     *
     * @param num 要增加的数量
     */
    void addElementsAppended(int num);

    /**
     * 预留额外的容量空间。
     *
     * <p>在当前已追加元素数的基础上,再预留指定数量的额外空间。
     *
     * @param additionalCapacity 额外需要的容量
     */
    default void reserveAdditional(int additionalCapacity) {
        reserve(getElementsAppended() + additionalCapacity);
    }
}
