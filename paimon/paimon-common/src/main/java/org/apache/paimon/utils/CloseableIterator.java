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

import org.apache.paimon.annotation.Public;

import javax.annotation.Nonnull;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Consumer;

import static java.util.Arrays.asList;

/**
 * 可关闭的迭代器接口。
 *
 * <p>该接口表示一个同时实现 {@link Iterator} 和 {@link AutoCloseable} 的迭代器。
 * 典型的使用场景是基于本地资源(如文件、网络或数据库连接)的迭代器。
 * 客户端在使用完迭代器后必须调用 {@link #close()} 方法。
 *
 * <p>提供多种工厂方法:
 * <ul>
 *   <li>{@link #adapterForIterator} - 将普通迭代器适配为可关闭迭代器
 *   <li>{@link #fromList} - 从列表创建可关闭迭代器
 *   <li>{@link #flatten} - 展平多个迭代器
 *   <li>{@link #empty} - 返回空迭代器
 *   <li>{@link #ofElements} - 从元素数组创建迭代器
 *   <li>{@link #ofElement} - 从单个元素创建迭代器
 * </ul>
 *
 * @param <T> 迭代元素的类型
 * @since 0.4.0
 */
@Public
public interface CloseableIterator<T> extends Iterator<T>, AutoCloseable {

    /** 空的可关闭迭代器实例 */
    CloseableIterator<?> EMPTY_INSTANCE =
            CloseableIterator.adapterForIterator(Collections.emptyIterator());

    /**
     * 将普通迭代器适配为可关闭迭代器。
     *
     * <p>关闭时不执行任何操作。
     *
     * @param iterator 原始迭代器
     * @param <T> 元素类型
     * @return 可关闭迭代器
     */
    @Nonnull
    static <T> CloseableIterator<T> adapterForIterator(@Nonnull Iterator<T> iterator) {
        return adapterForIterator(iterator, () -> {});
    }

    /**
     * 将普通迭代器适配为可关闭迭代器。
     *
     * @param iterator 原始迭代器
     * @param close 关闭时执行的回调
     * @param <T> 元素类型
     * @return 可关闭迭代器
     */
    static <T> CloseableIterator<T> adapterForIterator(
            @Nonnull Iterator<T> iterator, AutoCloseable close) {
        return new IteratorAdapter<>(iterator, close);
    }

    /**
     * 从列表创建可关闭迭代器。
     *
     * <p>关闭时会对所有未消费的元素执行 closeNotConsumed 回调。
     *
     * @param list 元素列表
     * @param closeNotConsumed 对未消费元素执行的关闭回调
     * @param <T> 元素类型
     * @return 可关闭迭代器
     */
    static <T> CloseableIterator<T> fromList(List<T> list, Consumer<T> closeNotConsumed) {
        return new CloseableIterator<T>() {
            private final Deque<T> stack = new ArrayDeque<>(list);

            @Override
            public boolean hasNext() {
                return !stack.isEmpty();
            }

            @Override
            public T next() {
                return stack.poll();
            }

            @Override
            public void close() throws Exception {
                Exception exception = null;
                for (T el : stack) {
                    try {
                        closeNotConsumed.accept(el);
                    } catch (Exception e) {
                        exception = ExceptionUtils.firstOrSuppressed(e, exception);
                    }
                }
                if (exception != null) {
                    throw exception;
                }
            }
        };
    }

    /**
     * 将多个可关闭迭代器展平为一个。
     *
     * <p>按顺序遍历所有迭代器,关闭时会关闭所有传入的迭代器。
     *
     * @param iterators 多个可关闭迭代器
     * @param <T> 元素类型
     * @return 展平后的迭代器
     */
    static <T> CloseableIterator<T> flatten(CloseableIterator<T>... iterators) {
        return new CloseableIterator<T>() {
            private final Queue<CloseableIterator<T>> queue =
                    removeEmptyHead(new LinkedList<>(asList(iterators)));

            private Queue<CloseableIterator<T>> removeEmptyHead(Queue<CloseableIterator<T>> queue) {
                while (!queue.isEmpty() && !queue.peek().hasNext()) {
                    queue.poll();
                }
                return queue;
            }

            @Override
            public boolean hasNext() {
                removeEmptyHead(queue);
                return !queue.isEmpty();
            }

            @Override
            public T next() {
                removeEmptyHead(queue);
                return queue.peek().next();
            }

            @Override
            public void close() throws Exception {
                IOUtils.closeAll(iterators);
            }
        };
    }

    /**
     * 返回空的可关闭迭代器。
     *
     * @param <T> 元素类型
     * @return 空迭代器
     */
    @SuppressWarnings("unchecked")
    static <T> CloseableIterator<T> empty() {
        return (CloseableIterator<T>) EMPTY_INSTANCE;
    }

    /**
     * 从元素数组创建可关闭迭代器。
     *
     * <p>关闭时会对所有未消费的元素执行 closeNotConsumed 回调。
     *
     * @param closeNotConsumed 对未消费元素执行的关闭回调
     * @param elements 元素数组
     * @param <T> 元素类型
     * @return 可关闭迭代器
     */
    static <T> CloseableIterator<T> ofElements(Consumer<T> closeNotConsumed, T... elements) {
        return fromList(asList(elements), closeNotConsumed);
    }

    /**
     * 从单个元素创建可关闭迭代器。
     *
     * <p>如果元素未被消费,关闭时会执行 closeIfNotConsumed 回调。
     *
     * @param element 元素
     * @param closeIfNotConsumed 如果未消费则执行的关闭回调
     * @param <E> 元素类型
     * @return 可关闭迭代器
     */
    static <E> CloseableIterator<E> ofElement(E element, Consumer<E> closeIfNotConsumed) {
        return new CloseableIterator<E>() {
            private boolean hasNext = true;

            @Override
            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public E next() {
                hasNext = false;
                return element;
            }

            @Override
            public void close() {
                if (hasNext) {
                    closeIfNotConsumed.accept(element);
                }
            }
        };
    }

    /**
     * 迭代器适配器,将 {@link Iterator} 适配为 {@link CloseableIterator}。
     *
     * <p>关闭时执行指定的关闭回调。
     *
     * @param <E> 迭代元素的类型
     */
    final class IteratorAdapter<E> implements CloseableIterator<E> {

        /** 被委托的迭代器 */
        @Nonnull private final Iterator<E> delegate;

        /** 关闭时执行的回调 */
        private final AutoCloseable close;

        IteratorAdapter(@Nonnull Iterator<E> delegate, AutoCloseable close) {
            this.delegate = delegate;
            this.close = close;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public E next() {
            return delegate.next();
        }

        @Override
        public void remove() {
            delegate.remove();
        }

        @Override
        public void forEachRemaining(Consumer<? super E> action) {
            delegate.forEachRemaining(action);
        }

        @Override
        public void close() throws Exception {
            close.close();
        }
    }
}
