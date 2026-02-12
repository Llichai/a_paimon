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
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.util.List;
import java.util.function.Supplier;

/**
 * 全局索引结果,将行 ID 表示为压缩的位图。
 *
 * <p>该接口封装了全局索引查询的结果,使用 {@link RoaringNavigableMap64} 位图结构
 * 来高效存储和操作大量的行 ID。位图压缩可以显著减少内存占用。
 *
 * <p>主要功能:
 * <ul>
 *   <li>获取匹配的行 ID 位图
 *   <li>支持行 ID 的偏移操作
 *   <li>支持结果的逻辑运算(AND/OR)
 *   <li>支持从范围创建结果
 * </ul>
 */
public interface GlobalIndexResult {

    /**
     * 返回表示行 ID 的位图。
     *
     * @return 包含所有匹配行 ID 的位图
     */
    RoaringNavigableMap64 results();

    /**
     * 对所有行 ID 应用偏移量。
     *
     * <p>将位图中的每个行 ID 加上指定的偏移量,用于处理多个索引文件或分区的情况。
     *
     * @param startOffset 起始偏移量
     * @return 偏移后的索引结果
     */
    default GlobalIndexResult offset(long startOffset) {
        if (startOffset == 0) {
            return this;
        }
        RoaringNavigableMap64 roaringNavigableMap64 = results();
        final RoaringNavigableMap64 roaringNavigableMap64Offset = new RoaringNavigableMap64();

        for (long rowId : roaringNavigableMap64) {
            roaringNavigableMap64Offset.add(rowId + startOffset);
        }
        return create(() -> roaringNavigableMap64Offset);
    }

    /**
     * 返回此结果与另一个结果的交集。
     *
     * <p>使用原生位图 AND 运算以获得最佳性能。
     *
     * @param other 另一个索引结果
     * @return 交集结果
     */
    default GlobalIndexResult and(GlobalIndexResult other) {
        return create(() -> RoaringNavigableMap64.and(this.results(), other.results()));
    }

    /**
     * 返回此结果与另一个结果的并集。
     *
     * <p>使用原生位图 OR 运算以获得最佳性能。
     *
     * @param other 另一个索引结果
     * @return 并集结果
     */
    default GlobalIndexResult or(GlobalIndexResult other) {
        return create(() -> RoaringNavigableMap64.or(this.results(), other.results()));
    }

    /**
     * 返回一个空的 {@link GlobalIndexResult}。
     *
     * @return 空索引结果
     */
    static GlobalIndexResult createEmpty() {
        return create(RoaringNavigableMap64::new);
    }

    /**
     * 从供应商创建新的 {@link GlobalIndexResult}。
     *
     * @param supplier 位图供应商
     * @return 新的索引结果
     */
    static GlobalIndexResult create(Supplier<RoaringNavigableMap64> supplier) {
        LazyField<RoaringNavigableMap64> lazyField = new LazyField<>(supplier);
        return lazyField::get;
    }

    /**
     * 从 {@link Range} 创建新的 {@link GlobalIndexResult}。
     *
     * @param range 行 ID 范围
     * @return 包含指定范围的索引结果
     */
    static GlobalIndexResult fromRange(Range range) {
        return create(
                () -> {
                    RoaringNavigableMap64 result64 = new RoaringNavigableMap64();
                    result64.addRange(range);
                    return result64;
                });
    }

    /**
     * 从多个范围创建新的 {@link GlobalIndexResult}。
     *
     * @param ranges 行 ID 范围列表
     * @return 包含所有指定范围的索引结果
     */
    static GlobalIndexResult fromRanges(List<Range> ranges) {
        return create(
                () -> {
                    RoaringNavigableMap64 result64 = new RoaringNavigableMap64();
                    for (Range range : ranges) {
                        result64.addRange(range);
                    }
                    return result64;
                });
    }
}
