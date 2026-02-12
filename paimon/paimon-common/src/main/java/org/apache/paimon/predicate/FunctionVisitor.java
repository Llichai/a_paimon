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

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 函数级别的谓词访问者接口。
 *
 * <p>这是一个扩展的访问者接口,提供了对具体谓词函数的细粒度访问能力。 与 {@link PredicateVisitor} 相比,它不仅访问谓词结构,还深入到具体的函数类型。
 *
 * <h2>设计目的</h2>
 * <ul>
 *   <li>类型细化: 区分不同类型的谓词函数(Equal, LessThan, In 等)
 *   <li>精确处理: 为每种函数提供专门的处理方法
 *   <li>简化转换: 便于将谓词转换为其他表示形式(SQL, 表达式树等)
 * </ul>
 *
 * <h2>方法分类</h2>
 *
 * <h3>一元函数</h3>
 * <ul>
 *   <li>{@link #visitIsNull(FieldRef)}: IS NULL
 *   <li>{@link #visitIsNotNull(FieldRef)}: IS NOT NULL
 * </ul>
 *
 * <h3>二元比较函数</h3>
 * <ul>
 *   <li>{@link #visitEqual(FieldRef, Object)}: =
 *   <li>{@link #visitNotEqual(FieldRef, Object)}: !=
 *   <li>{@link #visitLessThan(FieldRef, Object)}: <
 *   <li>{@link #visitLessOrEqual(FieldRef, Object)}: <=
 *   <li>{@link #visitGreaterThan(FieldRef, Object)}: >
 *   <li>{@link #visitGreaterOrEqual(FieldRef, Object)}: >=
 * </ul>
 *
 * <h3>字符串函数</h3>
 * <ul>
 *   <li>{@link #visitStartsWith(FieldRef, Object)}: LIKE 'abc%'
 *   <li>{@link #visitEndsWith(FieldRef, Object)}: LIKE '%abc'
 *   <li>{@link #visitContains(FieldRef, Object)}: LIKE '%abc%'
 *   <li>{@link #visitLike(FieldRef, Object)}: LIKE 'pattern'
 * </ul>
 *
 * <h3>集合函数</h3>
 * <ul>
 *   <li>{@link #visitIn(FieldRef, List)}: IN (...)
 *   <li>{@link #visitNotIn(FieldRef, List)}: NOT IN (...)
 * </ul>
 *
 * <h3>复合函数</h3>
 * <ul>
 *   <li>{@link #visitAnd(List)}: AND
 *   <li>{@link #visitOr(List)}: OR
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // SQL 生成器
 * class SqlGenerator implements FunctionVisitor<String> {
 *     @Override
 *     public String visitEqual(FieldRef fieldRef, Object literal) {
 *         return fieldRef.name() + " = " + literal;
 *     }
 *
 *     @Override
 *     public String visitLessThan(FieldRef fieldRef, Object literal) {
 *         return fieldRef.name() + " < " + literal;
 *     }
 *
 *     @Override
 *     public String visitAnd(List<String> children) {
 *         return "(" + String.join(" AND ", children) + ")";
 *     }
 *
 *     // ... 实现其他方法
 * }
 *
 * // 使用
 * Predicate p = builder.and(
 *     builder.equal("name", "John"),
 *     builder.lessThan("age", 30)
 * );
 * String sql = p.visit(new SqlGenerator());
 * // 结果: "(name = John AND age < 30)"
 * }</pre>
 *
 * <h2>默认实现</h2>
 * <ul>
 *   <li>{@link #visit(LeafPredicate)}: 分发到具体的函数方法
 *   <li>{@link #visit(CompoundPredicate)}: 递归访问子谓词
 * </ul>
 *
 * @param <T> 访问结果类型
 * @see PredicateVisitor 基础的谓词访问者
 * @see LeafFunction 叶子函数接口
 * @see CompoundPredicate.Function 复合函数接口
 */
public interface FunctionVisitor<T> extends PredicateVisitor<T> {

    /**
     * 访问叶子谓词的默认实现。
     *
     * <p>该方法将叶子谓词分发到对应的具体函数访问方法。
     *
     * @param predicate 叶子谓词
     * @return 访问结果
     */
    @Override
    default T visit(LeafPredicate predicate) {
        Optional<FieldRef> fieldRef = predicate.fieldRefOptional();
        if (!fieldRef.isPresent()) {
            return visitNonFieldLeaf(predicate);
        }
        return predicate.function().visit(this, fieldRef.get(), predicate.literals());
    }

    /**
     * 访问不包含字段引用的叶子谓词。
     *
     * @param leafPredicate 叶子谓词
     * @return 访问结果
     */
    T visitNonFieldLeaf(LeafPredicate leafPredicate);

    /**
     * 访问复合谓词的默认实现。
     *
     * <p>该方法递归访问所有子谓词,并将结果传递给对应的复合函数方法。
     *
     * @param predicate 复合谓词
     * @return 访问结果
     */
    @Override
    default T visit(CompoundPredicate predicate) {
        return predicate
                .function()
                .visit(
                        this,
                        predicate.children().stream()
                                .map(p -> p.visit(this))
                                .collect(Collectors.toList()));
    }

    // ----------------- 一元函数 ------------------------

    /** 访问 IS NOT NULL 函数 */
    T visitIsNotNull(FieldRef fieldRef);

    /** 访问 IS NULL 函数 */
    T visitIsNull(FieldRef fieldRef);

    // ----------------- 二元函数 ------------------------

    /** 访问 STARTS_WITH 函数 */
    T visitStartsWith(FieldRef fieldRef, Object literal);

    /** 访问 ENDS_WITH 函数 */
    T visitEndsWith(FieldRef fieldRef, Object literal);

    /** 访问 CONTAINS 函数 */
    T visitContains(FieldRef fieldRef, Object literal);

    /** 访问 LIKE 函数 */
    T visitLike(FieldRef fieldRef, Object literal);

    /** 访问 < 函数 */
    T visitLessThan(FieldRef fieldRef, Object literal);

    /** 访问 >= 函数 */
    T visitGreaterOrEqual(FieldRef fieldRef, Object literal);

    /** 访问 != 函数 */
    T visitNotEqual(FieldRef fieldRef, Object literal);

    /** 访问 <= 函数 */
    T visitLessOrEqual(FieldRef fieldRef, Object literal);

    /** 访问 = 函数 */
    T visitEqual(FieldRef fieldRef, Object literal);

    /** 访问 > 函数 */
    T visitGreaterThan(FieldRef fieldRef, Object literal);

    // ----------------- 其他函数 ------------------------

    /** 访问 IN 函数 */
    T visitIn(FieldRef fieldRef, List<Object> literals);

    /** 访问 NOT IN 函数 */
    T visitNotIn(FieldRef fieldRef, List<Object> literals);

    // ----------------- 复合函数 ------------------------

    /** 访问 AND 函数 */
    T visitAnd(List<T> children);

    /** 访问 OR 函数 */
    T visitOr(List<T> children);
}
