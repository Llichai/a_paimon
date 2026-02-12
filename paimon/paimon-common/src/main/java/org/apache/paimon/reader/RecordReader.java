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

package org.apache.paimon.reader;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.Filter;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 批量读取记录的读取器接口。
 *
 * <p>RecordReader 是 Paimon 数据读取的核心抽象,提供分批读取能力以提高性能。
 *
 * <h2>核心概念</h2>
 *
 * <ul>
 *   <li>批量读取:每次读取一批记录,而非单条记录,减少方法调用开销
 *   <li>迭代器模式:返回 {@link RecordIterator} 遍历批内的记录
 *   <li>资源管理:支持批次级别的资源释放({@link RecordIterator#releaseBatch()})
 * </ul>
 *
 * <h2>使用模式</h2>
 *
 * <pre>{@code
 * try (RecordReader<T> reader = ...) {
 *     RecordIterator<T> batch;
 *     while ((batch = reader.readBatch()) != null) {
 *         T record;
 *         while ((record = batch.next()) != null) {
 *             // 处理记录
 *         }
 *         batch.releaseBatch(); // 释放批次资源
 *     }
 * }
 * }</pre>
 *
 * <h2>数据转换</h2>
 *
 * <p>提供函数式编程风格的 API:
 *
 * <ul>
 *   <li>{@link #transform(Function)}:转换记录类型
 *   <li>{@link #filter(Filter)}:过滤记录
 *   <li>{@link #toCloseableIterator()}:转换为标准迭代器
 * </ul>
 *
 * <h2>线程安全性</h2>
 *
 * <p>实现类通常不是线程安全的,一个 reader 实例应该只由单个线程使用。
 *
 * @param <T> 记录类型
 * @since 0.4.0
 */
@Public
public interface RecordReader<T> extends Closeable {

    /**
     * 读取一批记录。
     *
     * <p>到达输入末尾时应返回 null。
     *
     * <p><strong>重要:</strong>返回的迭代器对象和其包含的对象可能会被源持有一段时间,
     * 因此不应立即被读取器重用。
     *
     * @return 记录迭代器,如果已到达末尾则返回 null
     * @throws IOException 如果发生 I/O 错误
     */
    @Nullable
    RecordIterator<T> readBatch() throws IOException;

    /**
     * 关闭读取器并释放所有资源。
     *
     * @throws IOException 如果发生 I/O 错误
     */
    @Override
    void close() throws IOException;

    /**
     * 内部迭代器接口,提供比标准 {@link Iterator} 更严格的 API。
     *
     * <p>该接口的设计目标:
     *
     * <ul>
     *   <li>支持批次级别的资源管理(releaseBatch)
     *   <li>允许抛出 IOException,适合 I/O 密集型操作
     *   <li>提供转换和过滤能力
     * </ul>
     *
     * @param <T> 记录类型
     */
    interface RecordIterator<T> {

        /**
         * 获取下一条记录。
         *
         * @return 下一条记录,如果没有更多元素则返回 null
         * @throws IOException 如果发生 I/O 错误
         */
        @Nullable
        T next() throws IOException;

        /**
         * 释放此迭代器遍历的批次。
         *
         * <p>这不会关闭读取器及其资源,只是表示该迭代器不再使用。
         * 该方法可用作钩子来回收/重用重量级对象结构。
         */
        void releaseBatch();

        /**
         * 返回对每个元素应用 {@code function} 的迭代器。
         *
         * @param function 转换函数
         * @param <R> 结果类型
         * @return 转换后的迭代器
         */
        default <R> RecordReader.RecordIterator<R> transform(Function<T, R> function) {
            RecordReader.RecordIterator<T> thisIterator = this;
            return new RecordReader.RecordIterator<R>() {
                @Nullable
                @Override
                public R next() throws IOException {
                    T next = thisIterator.next();
                    if (next == null) {
                        return null;
                    }
                    return function.apply(next);
                }

                @Override
                public void releaseBatch() {
                    thisIterator.releaseBatch();
                }
            };
        }

        /**
         * 过滤记录迭代器。
         *
         * @param filter 过滤条件
         * @return 过滤后的迭代器
         */
        default RecordIterator<T> filter(Filter<T> filter) {
            RecordIterator<T> thisIterator = this;
            return new RecordIterator<T>() {
                @Nullable
                @Override
                public T next() throws IOException {
                    while (true) {
                        T next = thisIterator.next();
                        if (next == null) {
                            return null;
                        }
                        if (filter.test(next)) {
                            return next;
                        }
                    }
                }

                @Override
                public void releaseBatch() {
                    thisIterator.releaseBatch();
                }
            };
        }
    }

    // -------------------------------------------------------------------------
    //                     工具方法
    // -------------------------------------------------------------------------

    /**
     * 对 {@link RecordReader} 中的每个剩余元素执行给定操作。
     *
     * <p>遍历所有元素直到处理完成或动作抛出异常。该方法会自动关闭读取器。
     *
     * @param action 要对每个元素执行的操作
     * @throws IOException 如果发生 I/O 错误
     */
    default void forEachRemaining(Consumer<? super T> action) throws IOException {
        RecordReader.RecordIterator<T> batch;
        T record;

        try {
            while ((batch = readBatch()) != null) {
                while ((record = batch.next()) != null) {
                    action.accept(record);
                }
                batch.releaseBatch();
            }
        } finally {
            close();
        }
    }

    /**
     * Performs the given action for each remaining element with row position in {@link
     * RecordReader} until all elements have been processed or the action throws an exception.
     */
    default void forEachRemainingWithPosition(BiConsumer<Long, ? super T> action)
            throws IOException {
        FileRecordIterator<T> batch;
        T record;

        try {
            while ((batch = (FileRecordIterator<T>) readBatch()) != null) {
                while ((record = batch.next()) != null) {
                    action.accept(batch.returnedPosition(), record);
                }
                batch.releaseBatch();
            }
        } finally {
            close();
        }
    }

    /** Returns a {@link RecordReader} that applies {@code function} to each element. */
    default <R> RecordReader<R> transform(Function<T, R> function) {
        RecordReader<T> thisReader = this;
        return new RecordReader<R>() {
            @Nullable
            @Override
            public RecordIterator<R> readBatch() throws IOException {
                RecordIterator<T> iterator = thisReader.readBatch();
                if (iterator == null) {
                    return null;
                }
                return iterator.transform(function);
            }

            @Override
            public void close() throws IOException {
                thisReader.close();
            }
        };
    }

    /** Filters a {@link RecordReader}. */
    default RecordReader<T> filter(Filter<T> filter) {
        RecordReader<T> thisReader = this;
        return new RecordReader<T>() {
            @Nullable
            @Override
            public RecordIterator<T> readBatch() throws IOException {
                RecordIterator<T> iterator = thisReader.readBatch();
                if (iterator == null) {
                    return null;
                }
                return iterator.filter(filter);
            }

            @Override
            public void close() throws IOException {
                thisReader.close();
            }
        };
    }

    /** Convert this reader to a {@link CloseableIterator}. */
    default CloseableIterator<T> toCloseableIterator() {
        return new RecordReaderIterator<>(this);
    }
}
