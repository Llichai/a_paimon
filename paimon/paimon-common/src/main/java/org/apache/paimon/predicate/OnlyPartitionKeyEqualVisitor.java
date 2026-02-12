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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 分区键等值谓词访问者。
 *
 * <p>该访问者用于检查谓词是否仅包含分区键的等值比较，并且可以安全地下推到分区级别。
 * 这是分区裁剪优化的关键组件。
 *
 * <h2>主要功能：</h2>
 * <ul>
 *   <li>识别仅涉及分区键的等值谓词（field = literal）
 *   <li>提取分区键值对，用于分区过滤
 *   <li>拒绝其他类型的谓词（范围比较、LIKE、OR 等）
 * </ul>
 *
 * <h2>优化策略：</h2>
 * <p>只有同时满足以下条件的谓词才能被接受：
 * <ul>
 *   <li>谓词函数必须是 EQUAL
 *   <li>字段必须是分区键
 *   <li>如果有多个谓词，必须用 AND 连接
 * </ul>
 *
 * <h2>使用示例：</h2>
 * <pre>{@code
 * // 假设表有分区键 [dt, region]
 * List<String> partitionKeys = Arrays.asList("dt", "region");
 * OnlyPartitionKeyEqualVisitor visitor = new OnlyPartitionKeyEqualVisitor(partitionKeys);
 *
 * // 示例 1：可以下推 ✓
 * // WHERE dt = '2024-01-01' AND region = 'us'
 * Predicate p1 = PredicateBuilder.and(
 *     builder.equal(dtIndex, "2024-01-01"),
 *     builder.equal(regionIndex, "us")
 * );
 * boolean canPushDown = p1.visit(visitor);  // true
 * Map<String, String> partitions = visitor.partitions();
 * // partitions = {"dt": "2024-01-01", "region": "us"}
 *
 * // 示例 2：不能下推 ✗（包含 OR）
 * // WHERE dt = '2024-01-01' OR dt = '2024-01-02'
 * Predicate p2 = PredicateBuilder.or(...);
 * boolean canPushDown2 = p2.visit(visitor);  // false
 *
 * // 示例 3：不能下推 ✗（包含非分区键）
 * // WHERE dt = '2024-01-01' AND name = 'Alice'
 * Predicate p3 = PredicateBuilder.and(
 *     builder.equal(dtIndex, "2024-01-01"),
 *     builder.equal(nameIndex, "Alice")  // name 不是分区键
 * );
 * boolean canPushDown3 = p3.visit(visitor);  // false
 *
 * // 示例 4：不能下推 ✗（使用范围比较）
 * // WHERE dt >= '2024-01-01'
 * Predicate p4 = builder.greaterOrEqual(dtIndex, "2024-01-01");
 * boolean canPushDown4 = p4.visit(visitor);  // false
 * }</pre>
 *
 * <h2>为什么限制这么严格？</h2>
 * <p>TODO: 更多过滤器的支持需要等待 {@code BatchWriteBuilder} 支持谓词下推。
 * 目前只支持最简单的等值谓词，因为：
 * <ul>
 *   <li>等值谓词可以精确定位分区
 *   <li>范围谓词需要更复杂的分区扫描逻辑
 *   <li>OR 谓词需要合并多个分区集合
 * </ul>
 *
 * @see PartitionPredicateVisitor 更宽松的分区谓词检查
 */
public class OnlyPartitionKeyEqualVisitor implements FunctionVisitor<Boolean> {

    /** 分区键名称列表。 */
    private final List<String> partitionKeys;

    /** 收集到的分区键值对。 */
    private final Map<String, String> partitions;

    /**
     * 构造分区键等值访问者。
     *
     * @param partitionKeys 分区键名称列表
     */
    public OnlyPartitionKeyEqualVisitor(List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
        partitions = new HashMap<>();
    }

    /**
     * 获取收集到的分区键值对。
     *
     * @return 分区键值对映射
     */
    public Map<String, String> partitions() {
        return partitions;
    }

    @Override
    public Boolean visitIsNotNull(FieldRef fieldRef) {
        return false;
    }

    @Override
    public Boolean visitIsNull(FieldRef fieldRef) {
        return false;
    }

    @Override
    public Boolean visitStartsWith(FieldRef fieldRef, Object literal) {
        return false;
    }

    @Override
    public Boolean visitEndsWith(FieldRef fieldRef, Object literal) {
        return false;
    }

    @Override
    public Boolean visitContains(FieldRef fieldRef, Object literal) {
        return false;
    }

    @Override
    public Boolean visitLike(FieldRef fieldRef, Object literal) {
        return false;
    }

    @Override
    public Boolean visitLessThan(FieldRef fieldRef, Object literal) {
        return false;
    }

    @Override
    public Boolean visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return false;
    }

    @Override
    public Boolean visitNotEqual(FieldRef fieldRef, Object literal) {
        return false;
    }

    @Override
    public Boolean visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return false;
    }

    /**
     * 访问等值谓词。
     *
     * <p>只有当字段是分区键时才接受。
     *
     * @param fieldRef 字段引用
     * @param literal 字面值
     * @return 如果字段是分区键则返回 true
     */
    @Override
    public Boolean visitEqual(FieldRef fieldRef, Object literal) {
        boolean contains = partitionKeys.contains(fieldRef.name());
        if (contains) {
            partitions.put(fieldRef.name(), literal.toString());
            return true;
        }
        return false;
    }

    @Override
    public Boolean visitGreaterThan(FieldRef fieldRef, Object literal) {
        return false;
    }

    @Override
    public Boolean visitIn(FieldRef fieldRef, List<Object> literals) {
        return false;
    }

    @Override
    public Boolean visitNotIn(FieldRef fieldRef, List<Object> literals) {
        return false;
    }

    /**
     * 访问 AND 谓词。
     *
     * <p>所有子谓词都必须可以下推。
     *
     * @param children 子谓词的访问结果
     * @return 如果所有子谓词都返回 true 则返回 true
     */
    @Override
    public Boolean visitAnd(List<Boolean> children) {
        return children.stream().reduce((first, second) -> first && second).get();
    }

    /**
     * 访问 OR 谓词。
     *
     * <p>始终返回 false，因为不支持 OR 下推。
     *
     * @param children 子谓词的访问结果
     * @return 始终返回 false
     */
    @Override
    public Boolean visitOr(List<Boolean> children) {
        return false;
    }

    /**
     * 访问非字段叶子谓词（如带转换的谓词）。
     *
     * <p>始终返回 false，因为不支持转换下推。
     *
     * @param predicate 叶子谓词
     * @return 始终返回 false
     */
    @Override
    public Boolean visitNonFieldLeaf(LeafPredicate predicate) {
        return false;
    }
}
