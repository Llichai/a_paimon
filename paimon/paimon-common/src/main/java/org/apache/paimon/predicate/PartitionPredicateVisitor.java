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

/**
 * 分区谓词访问者。
 *
 * <p>该访问者用于检查谓词是否仅包含分区键相关的条件。与 {@link OnlyPartitionKeyEqualVisitor} 不同，
 * 这个访问者更加宽松，接受所有类型的谓词函数（等值、范围、LIKE 等），只要涉及的字段都是分区键。
 *
 * <h2>主要功能：</h2>
 * <ul>
 *   <li>识别所有仅涉及分区键的谓词
 *   <li>支持复杂的分区过滤条件（范围、LIKE、IN、OR 等）
 *   <li>用于分区裁剪优化
 * </ul>
 *
 * <h2>与 OnlyPartitionKeyEqualVisitor 的区别：</h2>
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>PartitionPredicateVisitor</th>
 *     <th>OnlyPartitionKeyEqualVisitor</th>
 *   </tr>
 *   <tr>
 *     <td>支持等值谓词</td>
 *     <td>✓</td>
 *     <td>✓</td>
 *   </tr>
 *   <tr>
 *     <td>支持范围谓词</td>
 *     <td>✓</td>
 *     <td>✗</td>
 *   </tr>
 *   <tr>
 *     <td>支持 LIKE 谓词</td>
 *     <td>✓</td>
 *     <td>✗</td>
 *   </tr>
 *   <tr>
 *     <td>支持 OR 谓词</td>
 *     <td>✓</td>
 *     <td>✗</td>
 *   </tr>
 *   <tr>
 *     <td>支持字段转换</td>
 *     <td>✓</td>
 *     <td>✗</td>
 *   </tr>
 *   <tr>
 *     <td>提取分区值</td>
 *     <td>✗</td>
 *     <td>✓</td>
 *   </tr>
 * </table>
 *
 * <h2>使用示例：</h2>
 * <pre>{@code
 * // 假设表有分区键 [dt, region]
 * List<String> partitionKeys = Arrays.asList("dt", "region");
 * PartitionPredicateVisitor visitor = new PartitionPredicateVisitor(partitionKeys);
 *
 * // 示例 1：纯分区谓词 ✓
 * // WHERE dt >= '2024-01-01' AND dt <= '2024-01-31'
 * Predicate p1 = PredicateBuilder.and(
 *     builder.greaterOrEqual(dtIndex, "2024-01-01"),
 *     builder.lessOrEqual(dtIndex, "2024-01-31")
 * );
 * boolean isPartitionOnly = p1.visit(visitor);  // true
 *
 * // 示例 2：纯分区谓词（带 OR）✓
 * // WHERE region = 'us' OR region = 'eu'
 * Predicate p2 = PredicateBuilder.or(
 *     builder.equal(regionIndex, "us"),
 *     builder.equal(regionIndex, "eu")
 * );
 * boolean isPartitionOnly2 = p2.visit(visitor);  // true
 *
 * // 示例 3：纯分区谓词（带 LIKE）✓
 * // WHERE region LIKE 'us%'
 * Predicate p3 = builder.like(regionIndex, "us%");
 * boolean isPartitionOnly3 = p3.visit(visitor);  // true
 *
 * // 示例 4：混合谓词 ✗
 * // WHERE dt = '2024-01-01' AND name = 'Alice'
 * Predicate p4 = PredicateBuilder.and(
 *     builder.equal(dtIndex, "2024-01-01"),
 *     builder.equal(nameIndex, "Alice")  // name 不是分区键
 * );
 * boolean isPartitionOnly4 = p4.visit(visitor);  // false
 *
 * // 示例 5：纯分区谓词（带转换）✓
 * // WHERE YEAR(dt) = 2024
 * Transform yearTransform = new YearTransform(new FieldRef(dtIndex, "dt", DataTypes.DATE()));
 * Predicate p5 = builder.equal(yearTransform, 2024);
 * boolean isPartitionOnly5 = p5.visit(visitor);  // true（如果 dt 是分区键）
 * }</pre>
 *
 * <h2>应用场景：</h2>
 * <ul>
 *   <li>分区裁剪：跳过不满足分区条件的分区
 *   <li>元数据过滤：在读取数据前过滤分区元数据
 *   <li>查询优化：判断是否可以仅扫描部分分区
 * </ul>
 *
 * @see OnlyPartitionKeyEqualVisitor 更严格的分区等值检查
 */
public class PartitionPredicateVisitor implements PredicateVisitor<Boolean> {

    /** 分区键名称列表。 */
    private final List<String> partitionKeys;

    /**
     * 构造分区谓词访问者。
     *
     * @param partitionKeys 分区键名称列表
     */
    public PartitionPredicateVisitor(List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
    }

    /**
     * 访问叶子谓词。
     *
     * <p>检查谓词中的所有字段引用是否都是分区键。
     *
     * @param predicate 叶子谓词
     * @return 如果所有字段都是分区键则返回 true
     */
    @Override
    public Boolean visit(LeafPredicate predicate) {
        Transform transform = predicate.transform();
        for (Object input : transform.inputs()) {
            if (input instanceof FieldRef) {
                if (!partitionKeys.contains(((FieldRef) input).name())) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * 访问复合谓词。
     *
     * <p>递归检查所有子谓词。
     *
     * @param predicate 复合谓词
     * @return 如果所有子谓词都是纯分区谓词则返回 true
     */
    @Override
    public Boolean visit(CompoundPredicate predicate) {
        for (Predicate child : predicate.children()) {
            Boolean matched = child.visit(this);

            if (!matched) {
                return false;
            }
        }
        return true;
    }
}
