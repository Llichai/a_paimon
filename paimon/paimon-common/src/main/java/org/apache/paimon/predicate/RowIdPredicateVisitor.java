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

package org.apache.paimon.predicate;

import org.apache.paimon.utils.Range;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.table.SpecialFields.ROW_ID;

/**
 * 行ID谓词访问者。
 *
 * <p>该访问者用于从谓词中提取行ID范围列表，这些范围可以下推到 manifest 读取器和文件读取器，
 * 实现高效的随机访问。这是基于行ID的点查询和范围查询优化的关键组件。
 *
 * <h2>主要功能：</h2>
 * <ul>
 *   <li>识别涉及 {@code _ROW_ID} 字段的谓词
 *   <li>提取行ID范围并合并重叠部分
 *   <li>支持等值查询（=）和集合查询（IN）
 *   <li>支持逻辑组合（AND、OR）
 * </ul>
 *
 * <h2>返回值语义：</h2>
 * <p>返回值类型为 {@code Optional<List<Range>>}，有三种情况：
 * <ul>
 *   <li>{@code Optional.empty()}: 谓词无法转换为随机访问模式（即过滤器不可消费）
 *   <li>{@code Optional.of(emptyList)}: 没有行满足谓词（例如 {@code WHERE _ROW_ID = 3 AND _ROW_ID IN (1, 2)}）
 *   <li>{@code Optional.of(nonEmptyList)}: 提取到的行ID范围列表
 * </ul>
 *
 * <h2>使用示例：</h2>
 * <pre>{@code
 * RowIdPredicateVisitor visitor = new RowIdPredicateVisitor();
 *
 * // 示例 1：等值查询
 * // WHERE _ROW_ID = 100
 * Predicate p1 = builder.equal(rowIdIndex, 100L);
 * Optional<List<Range>> ranges1 = p1.visit(visitor);
 * // ranges1 = Optional.of([Range(100, 100)])
 *
 * // 示例 2：IN 查询
 * // WHERE _ROW_ID IN (100, 200, 300)
 * Predicate p2 = builder.in(rowIdIndex, Arrays.asList(100L, 200L, 300L));
 * Optional<List<Range>> ranges2 = p2.visit(visitor);
 * // ranges2 = Optional.of([Range(100, 100), Range(200, 200), Range(300, 300)])
 *
 * // 示例 3：AND 查询（交集）
 * // WHERE _ROW_ID IN (1, 2, 3, 4, 5) AND _ROW_ID IN (3, 4, 5, 6, 7)
 * Predicate p3 = PredicateBuilder.and(
 *     builder.in(rowIdIndex, Arrays.asList(1L, 2L, 3L, 4L, 5L)),
 *     builder.in(rowIdIndex, Arrays.asList(3L, 4L, 5L, 6L, 7L))
 * );
 * Optional<List<Range>> ranges3 = p3.visit(visitor);
 * // ranges3 = Optional.of([Range(3, 5)])  // 交集
 *
 * // 示例 4：OR 查询（并集）
 * // WHERE _ROW_ID = 100 OR _ROW_ID = 200
 * Predicate p4 = PredicateBuilder.or(
 *     builder.equal(rowIdIndex, 100L),
 *     builder.equal(rowIdIndex, 200L)
 * );
 * Optional<List<Range>> ranges4 = p4.visit(visitor);
 * // ranges4 = Optional.of([Range(100, 100), Range(200, 200)])
 *
 * // 示例 5：空交集
 * // WHERE _ROW_ID = 3 AND _ROW_ID IN (1, 2)
 * Predicate p5 = PredicateBuilder.and(
 *     builder.equal(rowIdIndex, 3L),
 *     builder.in(rowIdIndex, Arrays.asList(1L, 2L))
 * );
 * Optional<List<Range>> ranges5 = p5.visit(visitor);
 * // ranges5 = Optional.of([])  // 空列表表示无匹配行
 *
 * // 示例 6：不可消费的谓词
 * // WHERE _ROW_ID > 100
 * Predicate p6 = builder.greaterThan(rowIdIndex, 100L);
 * Optional<List<Range>> ranges6 = p6.visit(visitor);
 * // ranges6 = Optional.empty()  // 不支持范围查询
 *
 * // 示例 7：混合谓词（OR 中包含非行ID谓词）
 * // WHERE _ROW_ID = 100 OR name = 'Alice'
 * Predicate p7 = PredicateBuilder.or(
 *     builder.equal(rowIdIndex, 100L),
 *     builder.equal(nameIndex, "Alice")
 * );
 * Optional<List<Range>> ranges7 = p7.visit(visitor);
 * // ranges7 = Optional.empty()  // OR 中有不可消费的子句
 * }</pre>
 *
 * <h2>范围合并：</h2>
 * <p>返回的范围列表是经过排序和合并的，没有重叠：
 * <pre>{@code
 * // 输入：[Range(1,3), Range(5,7), Range(2,6)]
 * // 输出：[Range(1,7)]  // 合并重叠部分
 * }</pre>
 *
 * <h2>应用场景：</h2>
 * <ul>
 *   <li>主键点查询：根据行ID快速定位数据
 *   <li>批量查询：一次查询多个行ID
 *   <li>增量读取：读取指定行ID范围的数据
 *   <li>分区内随机访问：结合分区过滤使用
 * </ul>
 *
 * <h2>限制：</h2>
 * <ul>
 *   <li>只支持等值（=）和集合（IN）查询
 *   <li>不支持范围查询（>、<、>=、<=）
 *   <li>OR 中的所有子句都必须是行ID谓词
 * </ul>
 *
 * @see org.apache.paimon.table.SpecialFields#ROW_ID
 * @see org.apache.paimon.utils.Range
 */
public class RowIdPredicateVisitor implements PredicateVisitor<Optional<List<Range>>> {

    /**
     * 访问叶子谓词。
     *
     * <p>只处理 {@code _ROW_ID} 字段的等值（=）和集合（IN）查询。
     *
     * @param predicate 叶子谓词
     * @return 行ID范围列表，如果不是行ID谓词或函数不支持则返回 empty
     */
    @Override
    public Optional<List<Range>> visit(LeafPredicate predicate) {
        Optional<FieldRef> fieldRefOptional = predicate.fieldRefOptional();
        if (!fieldRefOptional.isPresent()) {
            return Optional.empty();
        }
        FieldRef fieldRef = fieldRefOptional.get();
        if (ROW_ID.name().equals(fieldRef.name())) {
            LeafFunction function = predicate.function();
            if (function instanceof Equal || function instanceof In) {
                ArrayList<Long> rowIds = new ArrayList<>();
                for (Object literal : predicate.literals()) {
                    rowIds.add((Long) literal);
                }
                // The list output by getRangesFromList is already sorted,
                // and has no overlap
                return Optional.of(Range.toRanges(rowIds));
            }
        }
        return Optional.empty();
    }

    /**
     * 访问复合谓词。
     *
     * <p>处理 AND 和 OR 逻辑组合：
     * <ul>
     *   <li>AND: 计算所有子谓词的交集
     *   <li>OR: 计算所有子谓词的并集
     * </ul>
     *
     * @param predicate 复合谓词
     * @return 合并后的行ID范围列表
     */
    @Override
    public Optional<List<Range>> visit(CompoundPredicate predicate) {
        CompoundPredicate.Function function = predicate.function();
        Optional<List<Range>> rowIds = Optional.empty();
        // `And` means we should get the intersection of all children.
        if (function instanceof And) {
            for (Predicate child : predicate.children()) {
                Optional<List<Range>> childList = child.visit(this);
                if (!childList.isPresent()) {
                    continue;
                }

                rowIds =
                        rowIds.map(ranges -> Optional.of(Range.and(ranges, childList.get())))
                                .orElse(childList);

                // shortcut for intersection
                if (rowIds.get().isEmpty()) {
                    return rowIds;
                }
            }
        } else if (function instanceof Or) {
            // `Or` means we should get the union of all children
            rowIds = Optional.of(new ArrayList<>());
            for (Predicate child : predicate.children()) {
                Optional<List<Range>> childList = child.visit(this);
                if (!childList.isPresent()) {
                    return Optional.empty();
                }

                rowIds.get().addAll(childList.get());
                rowIds = Optional.of(Range.sortAndMergeOverlap(rowIds.get(), true));
            }
        } else {
            // unexpected function type, just return empty
            return Optional.empty();
        }
        return rowIds;
    }
}
