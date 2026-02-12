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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 谓词投影转换器。
 *
 * <p>该访问者用于根据投影数组转换谓词中的字段引用。这是投影下推优化的关键组件，
 * 允许在不同的字段布局之间重映射谓词。
 *
 * <h2>主要功能：</h2>
 * <ul>
 *   <li>根据投影数组重映射字段索引
 *   <li>递归处理复合谓词（AND、OR）
 *   <li>处理包含字段转换的谓词
 *   <li>智能处理不可映射的谓词
 * </ul>
 *
 * <h2>投影数组说明：</h2>
 * <p>投影数组 {@code projection[i] = j} 表示：新布局中的字段 i 对应原布局中的字段 j。
 * <p>转换器内部维护一个反向映射 {@code reversed[j] = i}，用于快速查找原字段在新布局中的位置。
 *
 * <h2>使用示例：</h2>
 * <pre>{@code
 * // 原始表结构：[id, name, age, city]  (索引 0, 1, 2, 3)
 * // 投影后结构：[name, city]           (索引 0, 1)
 * int[] projection = {1, 3};  // 新索引0对应原索引1(name), 新索引1对应原索引3(city)
 *
 * PredicateProjectionConverter converter = new PredicateProjectionConverter(projection);
 *
 * // 示例 1：可以转换的谓词
 * // WHERE name = 'Alice' AND city = 'Beijing'
 * Predicate p1 = PredicateBuilder.and(
 *     builder.equal(1, "Alice"),   // 原索引1 (name)
 *     builder.equal(3, "Beijing")  // 原索引3 (city)
 * );
 * Optional<Predicate> converted1 = p1.visit(converter);
 * // 转换后：WHERE field_0 = 'Alice' AND field_1 = 'Beijing'
 * // 新索引：name=0, city=1
 *
 * // 示例 2：部分字段不在投影中（AND 场景）
 * // WHERE name = 'Alice' AND age > 18
 * Predicate p2 = PredicateBuilder.and(
 *     builder.equal(1, "Alice"),   // 原索引1 (name) - 在投影中
 *     builder.greaterThan(2, 18)   // 原索引2 (age) - 不在投影中
 * );
 * Optional<Predicate> converted2 = p2.visit(converter);
 * // 转换后：WHERE field_0 = 'Alice'  （age 条件被丢弃）
 *
 * // 示例 3：部分字段不在投影中（OR 场景）
 * // WHERE name = 'Alice' OR age > 18
 * Predicate p3 = PredicateBuilder.or(
 *     builder.equal(1, "Alice"),   // 原索引1 (name) - 在投影中
 *     builder.greaterThan(2, 18)   // 原索引2 (age) - 不在投影中
 * );
 * Optional<Predicate> converted3 = p3.visit(converter);
 * // 转换后：Optional.empty()  （OR 中有不可转换的子句，整个谓词失效）
 *
 * // 示例 4：所有字段都不在投影中
 * // WHERE id = 100
 * Predicate p4 = builder.equal(0, 100);  // 原索引0 (id) - 不在投影中
 * Optional<Predicate> converted4 = p4.visit(converter);
 * // 转换后：Optional.empty()
 *
 * // 示例 5：带转换的谓词
 * // WHERE YEAR(name) = 2024  (假设 name 是日期类型)
 * Transform yearTransform = new YearTransform(new FieldRef(1, "name", DataTypes.DATE()));
 * Predicate p5 = builder.equal(yearTransform, 2024);
 * Optional<Predicate> converted5 = p5.visit(converter);
 * // 转换后：WHERE YEAR(field_0) = 2024  (name 的新索引是 0)
 * }</pre>
 *
 * <h2>转换规则：</h2>
 * <ul>
 *   <li><b>叶子谓词</b>：重映射所有字段引用，如果任何字段不在投影中则返回 empty
 *   <li><b>AND 谓词</b>：尽可能转换子谓词，丢弃不可转换的子句
 *   <li><b>OR 谓词</b>：只有所有子谓词都可转换时才成功，否则返回 empty
 * </ul>
 *
 * <h2>为什么 AND 和 OR 的行为不同？</h2>
 * <ul>
 *   <li><b>AND</b>: 可以安全地丢弃不可转换的子句，因为这只会让过滤条件变宽松，
 *                   不会导致数据丢失（最多读取多余数据，可以在后续阶段再过滤）
 *   <li><b>OR</b>: 不能丢弃任何子句，因为丢弃一个子句会导致该子句匹配的数据被遗漏
 * </ul>
 *
 * <h2>应用场景：</h2>
 * <ul>
 *   <li>列裁剪：只读取需要的列时转换谓词
 *   <li>投影下推：将过滤条件下推到存储层
 *   <li>查询优化：在不同的算子间传递谓词时调整字段索引
 * </ul>
 *
 * <h2>注意事项：</h2>
 * <p>TODO: 需要与 {@link PredicateBuilder#transformFieldMapping} 合并，避免功能重复。
 *
 * @see PredicateBuilder#transformFieldMapping 类似的字段映射功能
 */
public class PredicateProjectionConverter implements PredicateVisitor<Optional<Predicate>> {

    /** 反向映射：原字段索引 → 新字段索引。 */
    private final Map<Integer, Integer> reversed;

    /**
     * 构造谓词投影转换器。
     *
     * @param projection 投影数组，projection[i] = j 表示新字段 i 对应原字段 j
     */
    public PredicateProjectionConverter(int[] projection) {
        this.reversed = new HashMap<>();
        for (int i = 0; i < projection.length; i++) {
            reversed.put(projection[i], i);
        }
    }

    /**
     * 访问叶子谓词。
     *
     * <p>重映射谓词中的所有字段引用。
     *
     * @param predicate 叶子谓词
     * @return 转换后的谓词，如果有字段不在投影中则返回 empty
     */
    @Override
    public Optional<Predicate> visit(LeafPredicate predicate) {
        List<Object> inputs = predicate.transform().inputs();
        List<Object> newInputs = new ArrayList<>(inputs.size());
        for (Object input : inputs) {
            if (input instanceof FieldRef) {
                FieldRef fieldRef = (FieldRef) input;
                Integer mappedIndex = reversed.get(fieldRef.index());
                if (mappedIndex != null) {
                    newInputs.add(new FieldRef(mappedIndex, fieldRef.name(), fieldRef.type()));
                } else {
                    return Optional.empty();
                }
            } else {
                newInputs.add(input);
            }
        }
        return Optional.of(predicate.copyWithNewInputs(newInputs));
    }

    /**
     * 访问复合谓词。
     *
     * <p>递归转换所有子谓词：
     * <ul>
     *   <li>AND: 保留所有可转换的子谓词
     *   <li>OR: 只有所有子谓词都可转换时才成功
     * </ul>
     *
     * @param predicate 复合谓词
     * @return 转换后的谓词，如果不满足转换条件则返回 empty
     */
    @Override
    public Optional<Predicate> visit(CompoundPredicate predicate) {
        List<Predicate> converted = new ArrayList<>();
        boolean isAnd = predicate.function() instanceof And;
        for (Predicate child : predicate.children()) {
            Optional<Predicate> optional = child.visit(this);
            if (optional.isPresent()) {
                converted.add(optional.get());
            } else {
                if (!isAnd) {
                    return Optional.empty();
                }
            }
        }
        return Optional.of(new CompoundPredicate(predicate.function(), converted));
    }
}
