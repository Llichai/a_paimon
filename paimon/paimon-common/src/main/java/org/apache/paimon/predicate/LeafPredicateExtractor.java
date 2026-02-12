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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 叶子谓词提取器。
 *
 * <p>这是一个谓词访问者实现,用于从谓词树中提取叶子谓词,并按字段名称组织。
 * 它将复杂的谓词表达式分解为字段级别的简单谓词映射。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li>提取叶子谓词 - 从谓词树中提取所有叶子节点
 *   <li>按字段索引 - 使用字段名称作为key组织提取的谓词
 *   <li>AND谓词支持 - 仅支持AND连接的谓词,返回所有字段的谓词映射
 *   <li>OR谓词跳过 - 对于OR谓词返回空映射,因为无法按字段分离
 * </ul>
 *
 * <h2>支持的谓词结构</h2>
 * <ul>
 *   <li>单个叶子谓词 - 直接返回字段名到谓词的映射
 *   <li>AND复合谓词 - 递归提取所有子谓词,合并为一个映射
 *   <li>嵌套AND - 支持多层AND嵌套,会递归展平
 * </ul>
 *
 * <h2>不支持的谓词</h2>
 * <ul>
 *   <li>OR复合谓词 - 返回空映射,因为OR谓词不能按字段独立提取
 *   <li>无字段引用的谓词 - 抛出 {@link UnsupportedOperationException}
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 构建AND复合谓词
 * Predicate predicate = PredicateBuilder.and(
 *     builder.equal(0, "name", "Alice"),
 *     builder.greaterThan(1, "age", 18),
 *     builder.isNotNull(2, "email")
 * );
 *
 * // 提取叶子谓词
 * Map<String, LeafPredicate> predicates = predicate.visit(LeafPredicateExtractor.INSTANCE);
 * // 返回: {
 * //   "name" -> Equal(name, "Alice"),
 * //   "age" -> GreaterThan(age, 18),
 * //   "email" -> IsNotNull(email)
 * // }
 *
 * // 对于OR谓词
 * Predicate orPredicate = PredicateBuilder.or(
 *     builder.equal(0, "name", "Alice"),
 *     builder.equal(0, "name", "Bob")
 * );
 * Map<String, LeafPredicate> empty = orPredicate.visit(LeafPredicateExtractor.INSTANCE);
 * // 返回: {} (空映射,因为OR谓词不能独立提取)
 * }</pre>
 *
 * <h2>应用场景</h2>
 * <ul>
 *   <li>字段级过滤 - 提取每个字段的过滤条件用于列式存储
 *   <li>分区裁剪 - 提取分区字段的谓词用于分区过滤
 *   <li>索引选择 - 根据字段谓词选择合适的索引
 *   <li>统计信息过滤 - 使用字段级谓词过滤文件统计信息
 * </ul>
 *
 * <h2>限制</h2>
 * <ul>
 *   <li>仅适用于单字段谓词 - 每个叶子谓词只能包含一个字段引用
 *   <li>AND语义 - 只有AND连接的谓词才能正确提取
 *   <li>无重复处理 - 同一字段有多个谓词时,后者会覆盖前者
 * </ul>
 *
 * @see PredicateVisitor
 * @see LeafPredicate
 * @see CompoundPredicate
 */
public class LeafPredicateExtractor implements PredicateVisitor<Map<String, LeafPredicate>> {

    /** 单例实例。 */
    public static final LeafPredicateExtractor INSTANCE = new LeafPredicateExtractor();

    /**
     * 访问叶子谓词,提取其字段名称和谓词的映射。
     *
     * @param predicate 叶子谓词
     * @return 包含单个字段名到谓词映射的Map
     * @throws UnsupportedOperationException 如果谓词不包含字段引用
     */
    @Override
    public Map<String, LeafPredicate> visit(LeafPredicate predicate) {
        Optional<FieldRef> fieldRefOptional = predicate.fieldRefOptional();
        if (!fieldRefOptional.isPresent()) {
            throw new UnsupportedOperationException();
        }
        return Collections.singletonMap(fieldRefOptional.get().name(), predicate);
    }

    /**
     * 访问复合谓词,如果是AND谓词则递归提取所有叶子谓词。
     *
     * <p>对于AND谓词,会遍历所有子谓词并合并提取结果。
     * 对于OR谓词或其他类型,返回空映射。
     *
     * @param predicate 复合谓词
     * @return AND谓词返回所有字段的谓词映射,其他返回空映射
     */
    @Override
    public Map<String, LeafPredicate> visit(CompoundPredicate predicate) {
        if (predicate.function() instanceof And) {
            Map<String, LeafPredicate> leafPredicates = new HashMap<>();
            predicate.children().stream().map(p -> p.visit(this)).forEach(leafPredicates::putAll);
            return leafPredicates;
        }
        return Collections.emptyMap();
    }
}
