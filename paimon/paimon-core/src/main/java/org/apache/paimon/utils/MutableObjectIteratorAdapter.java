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

package org.apache.paimon.utils;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * 可变对象迭代器适配器
 *
 * <p>MutableObjectIteratorAdapter 将 {@link MutableObjectIterator} 包装为标准的 Java {@link Iterator}。
 *
 * <p>核心功能：
 * <ul>
 *   <li>适配器模式：将 MutableObjectIterator 适配为 Iterator 接口
 *   <li>预取机制：提前读取下一个元素以支持 hasNext() 检查
 *   <li>异常包装：将 IOException 包装为 RuntimeException
 * </ul>
 *
 * <p>关键差异处理：
 * <ul>
 *   <li>{@link MutableObjectIterator} 返回 {@code null} 表示迭代结束，而 {@link Iterator} 使用 {@link #hasNext()} 和 {@link #next()} 分离
 *   <li>{@link MutableObjectIterator} 的 {@code next()} 方法可能抛出 {@link IOException}，而标准 {@link Iterator} 不声明受检异常。此适配器将 {@link IOException} 包装为 {@link RuntimeException}
 * </ul>
 *
 * <p>预取机制：
 * <ul>
 *   <li>在 hasNext() 中预取下一个元素
 *   <li>在 next() 中返回预取的元素
 *   <li>提前读取一个元素以支持标准 Iterator 接口
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>迭代器转换：将 MutableObjectIterator 转换为标准 Iterator
 *   <li>流处理：在流式处理中使用标准 Iterator
 *   <li>兼容性：与需要标准 Iterator 的 API 兼容
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建 MutableObjectIterator
 * MutableObjectIterator<MyObject> mutableIterator = ...;
 * MyObject instance = new MyObject();
 *
 * // 包装为标准 Iterator
 * Iterator<MyObject> iterator = new MutableObjectIteratorAdapter<>(mutableIterator, instance);
 *
 * // 使用标准 Iterator 接口
 * while (iterator.hasNext()) {
 *     MyObject obj = iterator.next();
 *     // 处理对象
 * }
 * }</pre>
 *
 * @param <I> 输入元素类型（extends E）
 * @param <E> 输出元素类型
 * @see MutableObjectIterator
 * @see Iterator
 */
public class MutableObjectIteratorAdapter<I extends E, E> implements Iterator<E> {

    /** 被包装的 MutableObjectIterator */
    private final MutableObjectIterator<I> delegate;
    /** 可重用的实例（用于对象复用） */
    private final I instance;
    /** 下一个元素（预取） */
    private E nextElement;
    /** 是否有下一个元素 */
    private boolean hasNext = false;
    /** 是否已初始化 */
    private boolean initialized = false;

    /**
     * 构造可变对象迭代器适配器
     *
     * @param delegate 被包装的 MutableObjectIterator
     * @param instance 可重用的实例
     */
    public MutableObjectIteratorAdapter(MutableObjectIterator<I> delegate, I instance) {
        this.delegate = delegate;
        this.instance = instance;
    }

    @Override
    public boolean hasNext() {
        if (!initialized) {
            prefetch();
        }
        return hasNext;
    }

    @Override
    public E next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        E result = nextElement;
        prefetch();
        return result;
    }

    /**
     * 预取下一个元素
     *
     * <p>此方法提前读取一个元素以支持标准 {@link Iterator} 接口所需的 {@link #hasNext()} 检查。
     *
     * <p>将 {@link MutableObjectIterator#next} 抛出的 {@link IOException} 包装为 {@link RuntimeException}。
     */
    private void prefetch() {
        try {
            nextElement = delegate.next(instance);
            hasNext = (nextElement != null);
            initialized = true;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read next element from MutableObjectIterator", e);
        }
    }
}
