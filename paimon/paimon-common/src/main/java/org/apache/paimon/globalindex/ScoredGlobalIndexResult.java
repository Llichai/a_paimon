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

package org.apache.paimon.globalindex;

import org.apache.paimon.utils.LazyField;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.util.function.Supplier;

/**
 * 向量搜索全局索引结果,支持评分索引。
 *
 * <p>该接口扩展了 {@link GlobalIndexResult},为向量搜索等需要相关性评分的场景
 * 提供支持。每个匹配的行 ID 都关联一个浮点数评分,用于表示相关性或相似度。
 *
 * <p>主要应用场景:
 * <ul>
 *   <li>向量相似度搜索 - 返回距离或相似度评分
 *   <li>全文检索 - 返回文档相关性评分
 *   <li>Top-K 查询 - 根据评分排序和筛选结果
 * </ul>
 */
public interface ScoredGlobalIndexResult extends GlobalIndexResult {

    /**
     * 返回评分获取器。
     *
     * @return 评分获取器,根据行 ID 获取对应的评分
     */
    ScoreGetter scoreGetter();

    /**
     * 与另一个结果执行 AND 运算。
     *
     * <p>默认不支持,需要由具体的全局索引实现覆盖。
     *
     * @param other 另一个索引结果
     * @return 交集结果
     * @throws UnsupportedOperationException 默认不支持
     */
    default GlobalIndexResult and(GlobalIndexResult other) {
        throw new UnsupportedOperationException("Please realize this by specified global index");
    }

    /**
     * 对所有行 ID 和评分应用偏移量。
     *
     * <p>将位图中的每个行 ID 加上偏移量,同时保持评分的映射关系。
     *
     * @param offset 偏移量
     * @return 偏移后的评分索引结果
     */
    default ScoredGlobalIndexResult offset(long offset) {
        if (offset == 0) {
            return this;
        }
        RoaringNavigableMap64 roaringNavigableMap64 = results();
        final RoaringNavigableMap64 roaringNavigableMap64Offset = new RoaringNavigableMap64();
        final ScoreGetter thisScoreGetter = scoreGetter();

        for (long rowId : roaringNavigableMap64) {
            roaringNavigableMap64Offset.add(rowId + offset);
        }

        return create(
                () -> roaringNavigableMap64Offset, rowId -> thisScoreGetter.score(rowId - offset));
    }

    /**
     * 与另一个结果执行 OR 运算。
     *
     * <p>如果另一个结果也是评分结果,则合并两个结果的行 ID 和评分。
     * 对于重复的行 ID,使用第一个结果的评分。
     *
     * @param other 另一个索引结果
     * @return 并集结果
     */
    @Override
    default GlobalIndexResult or(GlobalIndexResult other) {
        if (!(other instanceof ScoredGlobalIndexResult)) {
            return GlobalIndexResult.super.or(other);
        }
        RoaringNavigableMap64 thisRowIds = results();
        ScoreGetter thisScoreGetter = scoreGetter();

        RoaringNavigableMap64 otherRowIds = other.results();
        ScoreGetter otherScoreGetter = ((ScoredGlobalIndexResult) other).scoreGetter();

        final RoaringNavigableMap64 resultOr = RoaringNavigableMap64.or(thisRowIds, otherRowIds);
        return new ScoredGlobalIndexResult() {
            @Override
            public ScoreGetter scoreGetter() {
                return rowId -> {
                    if (thisRowIds.contains(rowId)) {
                        return thisScoreGetter.score(rowId);
                    }
                    return otherScoreGetter.score(rowId);
                };
            }

            @Override
            public RoaringNavigableMap64 results() {
                return resultOr;
            }
        };
    }

    /**
     * 从供应商和评分获取器创建新的 {@link ScoredGlobalIndexResult}。
     *
     * @param supplier 位图供应商
     * @param scoreGetter 评分获取器
     * @return 新的评分索引结果
     */
    static ScoredGlobalIndexResult create(
            Supplier<RoaringNavigableMap64> supplier, ScoreGetter scoreGetter) {
        LazyField<RoaringNavigableMap64> lazyField = new LazyField<>(supplier);
        return new ScoredGlobalIndexResult() {
            @Override
            public ScoreGetter scoreGetter() {
                return scoreGetter;
            }

            @Override
            public RoaringNavigableMap64 results() {
                return lazyField.get();
            }
        };
    }
}
