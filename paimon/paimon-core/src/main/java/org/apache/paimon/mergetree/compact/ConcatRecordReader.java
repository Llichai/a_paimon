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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * 串联记录读取器
 *
 * <p>将多个 {@link RecordReader} 串联起来并顺序读取。
 *
 * <p>前提条件：
 * <ul>
 *   <li>输入列表已按键和序列号排序
 *   <li>不同 RecordReader 的键范围不重叠
 * </ul>
 *
 * <p>工作原理：
 * <pre>
 * 1. 维护一个 RecordReader 队列
 * 2. 从当前 RecordReader 读取记录
 * 3. 当前 RecordReader 读完后，切换到下一个
 * 4. 所有 RecordReader 读完后返回 null
 * </pre>
 *
 * <p>使用场景：
 * <ul>
 *   <li>读取 Section 内的文件（键范围不重叠）
 *   <li>顺序读取多个已排序的文件
 * </ul>
 *
 * <p>与 {@link SortMergeReader} 的区别：
 * <ul>
 *   <li>ConcatRecordReader：顺序读取（键范围不重叠）
 *   <li>SortMergeReader：归并读取（键范围可能重叠，需要合并）
 * </ul>
 */
public class ConcatRecordReader<T> implements RecordReader<T> {

    /** RecordReader 工厂队列（延迟创建） */
    private final Queue<ReaderSupplier<T>> queue;

    /** 当前正在读取的 RecordReader */
    private RecordReader<T> current;

    /**
     * 构造串联记录读取器
     *
     * @param readerFactories RecordReader 工厂列表
     */
    protected ConcatRecordReader(List<? extends ReaderSupplier<T>> readerFactories) {
        readerFactories.forEach(
                supplier ->
                        Preconditions.checkNotNull(supplier, "Reader factory must not be null."));
        this.queue = new LinkedList<>(readerFactories);
    }

    /**
     * 创建串联记录读取器
     *
     * <p>优化：如果只有一个 reader，直接返回该 reader，避免包装开销
     *
     * @param readers RecordReader 工厂列表
     * @param <R> 记录类型
     * @return 串联记录读取器
     * @throws IOException IO 异常
     */
    public static <R> RecordReader<R> create(List<? extends ReaderSupplier<R>> readers)
            throws IOException {
        return readers.size() == 1 ? readers.get(0).get() : new ConcatRecordReader<>(readers);
    }

    /**
     * 创建串联记录读取器（两个 reader）
     *
     * @param reader1 第一个 reader 工厂
     * @param reader2 第二个 reader 工厂
     * @param <R> 记录类型
     * @return 串联记录读取器
     * @throws IOException IO 异常
     */
    public static <R> RecordReader<R> create(ReaderSupplier<R> reader1, ReaderSupplier<R> reader2)
            throws IOException {
        return create(Arrays.asList(reader1, reader2));
    }

    /**
     * 读取下一批记录
     *
     * <p>逻辑：
     * <ol>
     *   <li>如果当前 reader 有数据，返回数据
     *   <li>如果当前 reader 无数据，关闭并切换到下一个
     *   <li>如果队列为空，返回 null（全部读完）
     * </ol>
     *
     * @return 记录迭代器或 null（已读完）
     * @throws IOException IO 异常
     */
    @Nullable
    @Override
    public RecordIterator<T> readBatch() throws IOException {
        while (true) {
            if (current != null) {
                // 尝试从当前 reader 读取
                RecordIterator<T> iterator = current.readBatch();
                if (iterator != null) {
                    return iterator; // 有数据，返回
                }
                // 当前 reader 已读完，关闭并置空
                current.close();
                current = null;
            } else if (queue.size() > 0) {
                // 从队列中取下一个 reader
                current = queue.poll().get();
            } else {
                // 队列为空，全部读完
                return null;
            }
        }
    }

    /**
     * 关闭读取器
     *
     * @throws IOException IO 异常
     */
    @Override
    public void close() throws IOException {
        if (current != null) {
            current.close();
        }
    }
}
