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

import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Function;

/**
 * 支持返回记录行位置和文件路径的记录迭代器。
 *
 * <p>该接口扩展了 {@link RecordReader.RecordIterator},增加了对行位置和文件路径的跟踪能力。
 *
 * <h2>核心功能</h2>
 *
 * <ul>
 *   <li>行位置跟踪:记录当前返回记录在文件中的绝对行号
 *   <li>文件路径:提供数据所在的文件路径
 *   <li>支持转换和过滤:保留行位置和文件路径信息
 * </ul>
 *
 * <h2>使用场景</h2>
 *
 * <ul>
 *   <li>行级别的删除向量:需要记录删除行的精确位置
 *   <li>数据溯源:跟踪数据来源文件和位置
 *   <li>错误定位:在数据处理失败时快速定位问题行
 *   <li>部分读取:基于行号范围过滤数据
 * </ul>
 *
 * <h2>线程安全性</h2>
 *
 * <p>该接口的实现通常不是线程安全的,需要外部同步。
 */
public interface FileRecordIterator<T> extends RecordReader.RecordIterator<T> {

    /**
     * 获取 {@link RecordReader.RecordIterator#next} 返回记录的行位置。
     *
     * <p>行位置是从0开始的索引,表示记录在文件中的绝对行号。
     *
     * @return 行位置,范围从0到文件总行数-1
     */
    long returnedPosition();

    /**
     * 获取记录所在的文件路径。
     *
     * @return 文件路径
     */
    Path filePath();

    /**
     * 应用转换函数,同时保留行位置和文件路径信息。
     *
     * @param function 转换函数
     * @param <R> 转换后的记录类型
     * @return 转换后的文件记录迭代器
     */
    @Override
    default <R> FileRecordIterator<R> transform(Function<T, R> function) {
        FileRecordIterator<T> thisIterator = this;
        return new FileRecordIterator<R>() {
            @Override
            public long returnedPosition() {
                return thisIterator.returnedPosition();
            }

            @Override
            public Path filePath() {
                return thisIterator.filePath();
            }

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
     * 应用过滤器,同时保留行位置和文件路径信息。
     *
     * @param filter 过滤谓词
     * @return 过滤后的文件记录迭代器
     */
    @Override
    default FileRecordIterator<T> filter(Filter<T> filter) {
        FileRecordIterator<T> thisIterator = this;
        return new FileRecordIterator<T>() {
            @Override
            public long returnedPosition() {
                return thisIterator.returnedPosition();
            }

            @Override
            public Path filePath() {
                return thisIterator.filePath();
            }

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

    /**
     * 基于位图选择记录。
     *
     * <p>只返回行位置在选择位图中的记录,用于实现行级别的部分读取。
     *
     * <h2>实现逻辑</h2>
     *
     * <ul>
     *   <li>维护期望的下一个行号(nextExpected)
     *   <li>当返回的行位置与期望值匹配时,返回该记录
     *   <li>跳过不在选择位图中的记录
     * </ul>
     *
     * @param selection 行位置选择位图
     * @return 过滤后的迭代器
     */
    default FileRecordIterator<T> selection(RoaringBitmap32 selection) {
        FileRecordIterator<T> thisIterator = this;
        final Iterator<Integer> selects = selection.iterator();
        return new FileRecordIterator<T>() {
            private long nextExpected = selects.hasNext() ? selects.next() : -1;

            @Override
            public long returnedPosition() {
                return thisIterator.returnedPosition();
            }

            @Override
            public Path filePath() {
                return thisIterator.filePath();
            }

            @Nullable
            @Override
            public T next() throws IOException {
                while (true) {
                    if (nextExpected == -1) {
                        return null;
                    }
                    T next = thisIterator.next();
                    if (next == null) {
                        return null;
                    }
                    while (nextExpected != -1 && nextExpected < returnedPosition()) {
                        nextExpected = selects.hasNext() ? selects.next() : -1;
                    }
                    if (nextExpected == returnedPosition()) {
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
