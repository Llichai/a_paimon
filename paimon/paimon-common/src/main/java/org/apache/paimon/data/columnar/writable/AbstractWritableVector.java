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

import java.io.Serializable;

/**
 * 可写列向量的抽象基类。
 *
 * <p>包含所有 {@link ColumnVector} 实现的共享结构,包括NULL信息和字典编码支持。
 * 提供了NULL值管理、容量管理、字典编码等通用功能的默认实现。
 *
 * <h2>核心功能</h2>
 * <ul>
 *   <li><b>NULL值管理</b>: 通过noNulls和isAllNull标志跟踪NULL状态
 *   <li><b>容量管理</b>: 支持动态扩容,采用2倍增长策略
 *   <li><b>字典编码</b>: 支持字典压缩,减少存储空间
 *   <li><b>追加计数</b>: 通过elementsAppended跟踪已写入元素数量
 * </ul>
 *
 * <h2>NULL值处理</h2>
 * <ul>
 *   <li>noNulls=true: 表示整个向量没有NULL值(默认状态)
 *   <li>noNulls=false: 表示向量中存在NULL值
 *   <li>isAllNull=true: 表示所有值都是NULL的特殊情况
 *   <li><b>重要</b>: 如果存在NULL值,必须将noNulls设置为false
 * </ul>
 *
 * <h2>内存管理策略</h2>
 * <ul>
 *   <li>初始容量由子类在构造时指定
 *   <li>扩容时采用2倍增长策略(newCapacity = requiredCapacity * 2)
 *   <li>reset()操作不释放内存,保持当前容量以便重用
 *   <li>扩容失败时抛出RuntimeException,需要调用方处理
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 子类实现示例
 * public class HeapIntVector extends AbstractWritableVector implements WritableIntVector {
 *     private int[] vector;
 *
 *     public HeapIntVector(int capacity) {
 *         super(capacity);
 *         this.vector = new int[capacity];
 *     }
 *
 *     @Override
 *     protected void reserveInternal(int newCapacity) {
 *         int[] newVector = new int[newCapacity];
 *         System.arraycopy(vector, 0, newVector, 0, elementsAppended);
 *         vector = newVector;
 *     }
 * }
 * }</pre>
 *
 * <h2>线程安全性</h2>
 * 该类<b>不是线程安全的</b>。所有字段都没有同步保护,多线程环境下需要外部同步。
 *
 * @see WritableColumnVector 可写列向量接口
 * @see Dictionary 字典编码
 */
public abstract class AbstractWritableVector implements WritableColumnVector, Serializable {

    private static final long serialVersionUID = 1L;

    /** 如果整个列向量没有NULL值,此标志为true,否则为false。 */
    protected boolean noNulls = true;

    /** 如果所有值都是NULL,此标志为true。 */
    protected boolean isAllNull = false;

    /** 追加数据时的当前写入游标(行索引)。 */
    protected int elementsAppended;

    /** 向量的当前容量。 */
    protected int capacity;

    /**
     * 此列的字典编码。
     *
     * <p>如果不为null,将用于在get()方法中解码值。使用字典编码可以大幅减少存储空间,特别是对于重复值较多的数据。
     */
    protected Dictionary dictionary;

    /**
     * 构造指定容量的可写向量。
     *
     * @param capacity 初始容量
     */
    public AbstractWritableVector(int capacity) {
        this.capacity = capacity;
    }

    /**
     * 更新字典编码。
     *
     * @param dictionary 字典对象
     */
    @Override
    public void setDictionary(Dictionary dictionary) {
        this.dictionary = dictionary;
    }

    /**
     * 检查此列是否使用了字典编码。
     *
     * @return 如果使用了字典返回true
     */
    @Override
    public boolean hasDictionary() {
        return dictionary != null;
    }

    /**
     * 将所有值设置为NULL。
     *
     * <p>设置isAllNull标志表示整个向量为NULL的特殊情况,同时将noNulls设置为false。
     */
    @Override
    public void setAllNull() {
        isAllNull = true;
        noNulls = false;
    }

    /**
     * 检查是否所有值都是NULL。
     *
     * @return 如果所有值都是NULL返回true
     */
    @Override
    public boolean isAllNull() {
        return isAllNull;
    }

    /**
     * 获取已追加的元素数量。
     *
     * @return 已追加的元素数量
     */
    @Override
    public int getElementsAppended() {
        return elementsAppended;
    }

    /**
     * 增加已追加元素数量。
     *
     * @param num 要增加的数量
     */
    @Override
    public final void addElementsAppended(int num) {
        elementsAppended += num;
    }

    /**
     * 获取向量的当前容量。
     *
     * @return 当前容量
     */
    @Override
    public int getCapacity() {
        return this.capacity;
    }

    /**
     * 重置向量到默认状态。
     *
     * <p>为了减少复制,这里不会将容量重置为初始容量。这意味着容量将保持扩容后的大小,
     * 向量可以在不重新分配内存的情况下重用。
     */
    @Override
    public void reset() {
        // To reduce copy, Ww don't result the capacity to initial capacity here. Which means the
        // capacity will be the same as expand.
        noNulls = true;
        isAllNull = false;
        elementsAppended = 0;
    }

    /**
     * 预留指定容量的空间。
     *
     * <p>扩容策略:
     * <ul>
     *   <li>如果请求容量小于当前容量,不做任何操作
     *   <li>如果需要扩容,新容量为请求容量的2倍(不超过Integer.MAX_VALUE)
     *   <li>扩容失败时捕获OutOfMemoryError并抛出RuntimeException
     * </ul>
     *
     * @param requiredCapacity 需要的容量
     * @throws IllegalArgumentException 如果请求容量为负数
     * @throws RuntimeException 如果内存分配失败
     */
    @Override
    public void reserve(int requiredCapacity) {
        if (requiredCapacity < 0) {
            throw new IllegalArgumentException("Invalid capacity: " + requiredCapacity);
        } else if (requiredCapacity > capacity) {
            int newCapacity = (int) Math.min(Integer.MAX_VALUE, requiredCapacity * 2L);
            if (requiredCapacity <= newCapacity) {
                try {
                    reserveInternal(newCapacity);
                } catch (OutOfMemoryError outOfMemoryError) {
                    throw new RuntimeException(
                            "Failed to allocate memory for vector", outOfMemoryError);
                }
            } else {
                throw new UnsupportedOperationException(
                        "Cannot allocate :" + newCapacity + " elements");
            }
            capacity = newCapacity;
        }
    }

    /**
     * 执行实际的内存扩容操作。
     *
     * <p>子类需要实现此方法,完成底层数组的扩容和数据复制。
     *
     * @param newCapacity 新的容量大小
     */
    protected abstract void reserveInternal(int newCapacity);
}
