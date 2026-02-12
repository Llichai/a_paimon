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

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * 谓词访问者接口。
 *
 * <p>这是一个访问者模式的接口,用于遍历和处理 {@link Predicate} 对象树。
 *
 * <h2>访问者模式</h2>
 * <p>访问者模式允许在不修改谓词类的情况下,为谓词添加新的操作:
 * <ul>
 *   <li>分离关注点: 将谓词结构和处理逻辑分离
 *   <li>易于扩展: 添加新操作只需实现新的访问者
 *   <li>类型安全: 编译期检查而非运行时反射
 * </ul>
 *
 * <h2>主要方法</h2>
 * <ul>
 *   <li>{@link #visit(LeafPredicate)}: 访问叶子谓词
 *   <li>{@link #visit(CompoundPredicate)}: 访问复合谓词
 *   <li>{@link #collectFieldNames(Predicate)}: 工具方法,收集所有字段名
 * </ul>
 *
 * <h2>使用场景</h2>
 * <pre>{@code
 * // 实现自定义访问者
 * class MyVisitor implements PredicateVisitor<String> {
 *     @Override
 *     public String visit(LeafPredicate predicate) {
 *         return "Leaf: " + predicate.function().name();
 *     }
 *
 *     @Override
 *     public String visit(CompoundPredicate predicate) {
 *         return predicate.children().stream()
 *             .map(p -> p.visit(this))
 *             .collect(Collectors.joining(", "));
 *     }
 * }
 *
 * // 使用访问者
 * Predicate p = builder.and(
 *     builder.equal("name", "John"),
 *     builder.greaterThan("age", 18)
 * );
 * String result = p.visit(new MyVisitor());
 * }</pre>
 *
 * <h2>内置访问者</h2>
 * <ul>
 *   <li>{@link FunctionVisitor}: 函数级别的访问
 *   <li>{@link PredicateReplaceVisitor}: 谓词替换
 *   <li>{@link LeafPredicateExtractor}: 叶子谓词提取
 *   <li>{@link FieldNameCollector}: 字段名收集
 * </ul>
 *
 * @param <T> 访问结果类型
 * @see Predicate 谓词接口
 * @see LeafPredicate 叶子谓词
 * @see CompoundPredicate 复合谓词
 */
public interface PredicateVisitor<T> {

    /**
     * 访问叶子谓词。
     *
     * @param predicate 叶子谓词
     * @return 访问结果
     */
    T visit(LeafPredicate predicate);

    /**
     * 访问复合谓词。
     *
     * @param predicate 复合谓词
     * @return 访问结果
     */
    T visit(CompoundPredicate predicate);

    /**
     * 收集谓词中引用的所有字段名。
     *
     * @param predicate 谓词对象
     * @return 字段名集合
     */
    static Set<String> collectFieldNames(@Nullable Predicate predicate) {
        if (predicate == null) {
            return Collections.emptySet();
        }
        return predicate.visit(new FieldNameCollector());
    }

    /**
     * 字段名收集器。
     *
     * <p>这是一个内置的访问者实现,用于收集谓词中引用的所有字段名。
     *
     * <h3>实现细节</h3>
     * <ul>
     *   <li>叶子谓词: 从 Transform 的输入中提取 FieldRef
     *   <li>复合谓词: 递归收集所有子谓词的字段名
     *   <li>去重: 使用 HashSet 自动去重
     * </ul>
     */
    class FieldNameCollector implements PredicateVisitor<Set<String>> {

        /**
         * 从叶子谓词中提取字段名。
         *
         * @param predicate 叶子谓词
         * @return 字段名集合
         */
        @Override
        public Set<String> visit(LeafPredicate predicate) {
            Set<String> fieldNames = new HashSet<>();
            for (Object input : predicate.transform().inputs()) {
                if (input instanceof FieldRef) {
                    fieldNames.add(((FieldRef) input).name());
                }
            }
            return fieldNames;
        }

        /**
         * 递归收集复合谓词中的所有字段名。
         *
         * @param predicate 复合谓词
         * @return 字段名集合
         */
        @Override
        public Set<String> visit(CompoundPredicate predicate) {
            Set<String> fieldNames = new HashSet<>();
            for (Predicate child : predicate.children()) {
                fieldNames.addAll(child.visit(this));
            }
            return fieldNames;
        }
    }
}
