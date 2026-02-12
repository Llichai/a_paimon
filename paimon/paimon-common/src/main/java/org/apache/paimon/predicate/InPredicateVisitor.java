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
 * IN谓词访问者工具类。
 *
 * <p>用于从谓词中提取IN谓词的元素列表。
 * 该工具类可以识别由多个EQUAL谓词通过OR连接形成的模式,并将其转换为IN谓词的格式。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li>识别IN模式 - 检测OR(equal1, equal2, ...)模式
 *   <li>提取元素 - 提取所有EQUAL谓词的字面量值
 *   <li>优化机会 - 识别可以转换为IN谓词的模式
 * </ul>
 *
 * <h2>识别模式</h2>
 * <p>可以识别的谓词模式:
 * <pre>
 * OR(
 *   field = value1,
 *   field = value2,
 *   field = value3
 * )
 * </pre>
 * <p>转换为:
 * <pre>
 * field IN (value1, value2, value3)
 * </pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 构建OR谓词
 * Predicate orPredicate = PredicateBuilder.or(
 *     builder.equal(0, "status", "active"),
 *     builder.equal(0, "status", "pending"),
 *     builder.equal(0, "status", "approved")
 * );
 *
 * // 2. 提取IN元素
 * Optional<List<Object>> elements =
 *     InPredicateVisitor.extractInElements(orPredicate, "status");
 *
 * if (elements.isPresent()) {
 *     List<Object> values = elements.get();
 *     // values = ["active", "pending", "approved"]
 *
 *     // 3. 构建IN谓词
 *     Predicate inPredicate = builder.in(0, values);
 *     // 等价于: status IN ('active', 'pending', 'approved')
 * }
 *
 * // 4. 处理不匹配的情况
 * // 混合不同字段
 * Predicate mixed = PredicateBuilder.or(
 *     builder.equal(0, "status", "active"),
 *     builder.equal(1, "type", "premium")  // 不同字段
 * );
 * Optional<List<Object>> result = InPredicateVisitor.extractInElements(mixed, "status");
 * // result.isPresent() == false,因为字段不一致
 *
 * // 混合不同操作
 * Predicate mixedOp = PredicateBuilder.or(
 *     builder.equal(0, "age", 20),
 *     builder.greaterThan(0, "age", 30)  // 不是EQUAL
 * );
 * Optional<List<Object>> result2 = InPredicateVisitor.extractInElements(mixedOp, "age");
 * // result2.isPresent() == false,因为包含非EQUAL谓词
 * }</pre>
 *
 * <h2>提取条件</h2>
 * <p>只有满足以下所有条件才能成功提取:
 * <ul>
 *   <li>谓词必须是CompoundPredicate(OR谓词)
 *   <li>所有子谓词必须是LeafPredicate
 *   <li>所有子谓词的函数必须是Equal
 *   <li>所有子谓词必须引用相同的字段
 *   <li>字段名必须与参数leafName匹配
 * </ul>
 *
 * <h2>返回值说明</h2>
 * <ul>
 *   <li>{@code Optional.of(list)} - 成功提取,返回字面量值列表
 *   <li>{@code Optional.empty()} - 无法提取(不满足条件或不是CompoundPredicate)
 * </ul>
 *
 * <h2>应用场景</h2>
 * <ul>
 *   <li>谓词优化 - 将OR连接的EQUAL转换为更高效的IN
 *   <li>查询重写 - 优化查询执行计划
 *   <li>索引利用 - IN谓词可以更好地利用索引
 *   <li>谓词下推 - 简化谓词结构以便下推到存储层
 * </ul>
 *
 * <h2>性能优势</h2>
 * <p>IN谓词相比OR连接的EQUAL谓词有以下优势:
 * <ul>
 *   <li>更紧凑 - 减少谓词树的深度和节点数
 *   <li>更高效 - 可以使用HashSet等数据结构加速查找
 *   <li>更易优化 - 优化器可以更好地处理IN谓词
 * </ul>
 *
 * @see PredicateVisitor
 * @see LeafPredicateExtractor
 * @see In
 * @see Equal
 */
public class InPredicateVisitor {

    /**
     * 从OR谓词中提取IN元素列表。
     *
     * <p>尝试从复合谓词中提取指定字段的IN谓词元素。
     * 如果谓词是由多个对同一字段的EQUAL谓词通过OR连接而成,
     * 则返回所有EQUAL谓词的字面量值列表。
     *
     * @param predicate 要分析的谓词,应该是OR复合谓词
     * @param leafName 要提取的字段名称
     * @return 如果成功提取则返回包含字面量值的Optional,否则返回Optional.empty()
     */
    public static Optional<List<Object>> extractInElements(Predicate predicate, String leafName) {
        if (!(predicate instanceof CompoundPredicate)) {
            return Optional.empty();
        }

        CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
        List<Object> leafValues = new ArrayList<>();
        List<Predicate> children = compoundPredicate.children();
        for (Predicate leaf : children) {
            if (leaf instanceof LeafPredicate
                    && (((LeafPredicate) leaf).function() instanceof Equal)
                    && leaf.visit(LeafPredicateExtractor.INSTANCE).get(leafName) != null) {
                leafValues.add(((LeafPredicate) leaf).literals().get(0));
            } else {
                return Optional.empty();
            }
        }
        return Optional.of(leafValues);
    }
}
