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

import org.apache.paimon.utils.Filter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.function.Function;

/**
 * 支持返回记录评分的记录迭代器。
 *
 * <p>该接口扩展了 {@link RecordReader.RecordIterator},增加了对记录评分的跟踪能力。
 *
 * <h2>核心功能</h2>
 *
 * <ul>
 *   <li>评分跟踪:记录当前返回记录的评分值
 *   <li>支持转换和过滤:保留评分信息
 * </ul>
 *
 * <h2>使用场景</h2>
 *
 * <ul>
 *   <li>相似度搜索:基于向量相似度的检索
 *   <li>排序结果:根据评分排序返回记录
 *   <li>阈值过滤:只返回评分超过阈值的记录
 *   <li>Top-K查询:获取评分最高的K条记录
 * </ul>
 *
 * <h2>线程安全性</h2>
 *
 * <p>该接口的实现通常不是线程安全的,需要外部同步。
 */
public interface ScoreRecordIterator<T> extends RecordReader.RecordIterator<T> {

    /**
     * 获取最近返回记录的评分。
     *
     * @return 评分值,通常在0.0到1.0之间
     */
    float returnedScore();

    @Override
    default <R> ScoreRecordIterator<R> transform(Function<T, R> function) {
        ScoreRecordIterator<T> thisIterator = this;
        return new ScoreRecordIterator<R>() {
            @Override
            public float returnedScore() {
                return thisIterator.returnedScore();
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

    @Override
    default ScoreRecordIterator<T> filter(Filter<T> filter) {
        ScoreRecordIterator<T> thisIterator = this;
        return new ScoreRecordIterator<T>() {
            @Override
            public float returnedScore() {
                return thisIterator.returnedScore();
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
}
