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
import java.util.List;
import java.util.Optional;

/**
 * 谓词替换访问者接口。
 *
 * <p>这是一个基于访问者模式的谓词转换接口,用于遍历谓词树并进行替换操作。
 * 该接口继承自 {@link PredicateVisitor},返回类型为 {@link Optional}<{@link Predicate}>,
 * 表示替换可能成功也可能失败。
 *
 * <h2>主要特性</h2>
 * <ul>
 *   <li>函数式接口 - 可以使用Lambda表达式实现
 *   <li>递归遍历 - 自动处理复合谓词的所有子谓词
 *   <li>失败传播 - 任何子谓词替换失败会导致整体失败
 *   <li>不可变性 - 替换操作返回新的谓词对象,不修改原对象
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>谓词字段映射转换 - 将谓词中的字段索引映射到新的schema
 *   <li>谓词分区转换 - 将表级谓词转换为分区级谓词
 *   <li>谓词下推优化 - 将谓词转换为适合存储层的格式
 *   <li>谓词简化 - 将复杂谓词转换为简化形式
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 实现字段索引映射
 * PredicateReplaceVisitor mapper = leafPredicate -> {
 *     Optional<FieldRef> fieldRef = leafPredicate.fieldRefOptional();
 *     if (fieldRef.isPresent()) {
 *         int newIndex = indexMapping[fieldRef.get().index()];
 *         return Optional.of(leafPredicate.copyWithNewIndex(newIndex));
 *     }
 *     return Optional.empty();
 * };
 *
 * // 应用转换
 * Predicate original = builder.and(
 *     builder.equal(0, "value1"),
 *     builder.greaterThan(1, 100)
 * );
 * Optional<Predicate> converted = original.visit(mapper);
 * }</pre>
 *
 * <h2>失败处理</h2>
 * <p>当替换操作失败时,应该返回 {@link Optional#empty()}:
 * <ul>
 *   <li>字段不存在于新schema中
 *   <li>类型转换不兼容
 *   <li>谓词无法在新上下文中表示
 * </ul>
 *
 * @see PredicateVisitor
 * @see Predicate
 * @see CompoundPredicate
 * @see LeafPredicate
 */
@FunctionalInterface
public interface PredicateReplaceVisitor extends PredicateVisitor<Optional<Predicate>> {

    /**
     * 访问复合谓词并递归替换其所有子谓词。
     *
     * <p>此默认实现会:
     * <ol>
     *   <li>遍历复合谓词的所有子谓词
     *   <li>对每个子谓词调用visit方法进行替换
     *   <li>如果所有子谓词都成功替换,则创建新的复合谓词
     *   <li>如果任何子谓词替换失败,则返回 {@link Optional#empty()}
     * </ol>
     *
     * <p>替换过程中保持原有的逻辑运算符(AND/OR)不变。
     *
     * @param predicate 要访问的复合谓词
     * @return 替换后的复合谓词,如果任何子谓词替换失败则返回 {@link Optional#empty()}
     */
    @Override
    default Optional<Predicate> visit(CompoundPredicate predicate) {
        List<Predicate> converted = new ArrayList<>();
        for (Predicate child : predicate.children()) {
            Optional<Predicate> optional = child.visit(this);
            if (optional.isPresent()) {
                converted.add(optional.get());
            } else {
                return Optional.empty();
            }
        }
        return Optional.of(new CompoundPredicate(predicate.function(), converted));
    }
}
