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

import org.apache.paimon.types.DataType;

import java.util.List;

/**
 * 二元叶子函数,需要一个常量值参数,且 NULL 值返回 false。
 *
 * <p>该抽象类继承自 {@link LeafFunction},专门用于需要一个常量值的谓词操作,
 * 并且遵循"NULL 返回 false"的语义。这是 SQL 标准中大多数比较操作的语义。
 *
 * <h2>NULL 语义</h2>
 * 在 SQL 中,当比较操作的任一操作数为 NULL 时,比较结果为 UNKNOWN(在三值逻辑中),
 * 在布尔上下文中被视为 false。该类实现了这一语义:
 * <ul>
 *   <li>如果字段值为 NULL,返回 false</li>
 *   <li>如果常量值为 NULL,返回 false</li>
 *   <li>只有当两个值都不为 NULL 时,才进行实际的比较操作</li>
 * </ul>
 *
 * <h2>主要实现类</h2>
 * <ul>
 *   <li><b>比较操作</b>: {@link Equal}, {@link NotEqual}, {@link GreaterThan}, {@link LessThan},
 *       {@link GreaterOrEqual}, {@link LessOrEqual}</li>
 *   <li><b>字符串操作</b>: {@link StartsWith}, {@link EndsWith}, {@link Contains}, {@link Like}</li>
 * </ul>
 *
 * <h2>方法适配</h2>
 * 该类实现了 {@link LeafFunction} 的通用方法,并进行以下处理:
 * <ul>
 *   <li>在精确测试中:检查字段值或常量值是否为 NULL,如果是则直接返回 false</li>
 *   <li>在统计信息测试中:检查是否所有行都是 NULL 或常量值为 NULL,如果是则直接返回 false</li>
 *   <li>否则调用简化的二元方法进行实际比较</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // age > 18 谓词
 * LeafPredicate greaterThanPredicate = new LeafPredicate(
 *     GreaterThan.INSTANCE,
 *     DataTypes.INT(),
 *     0,
 *     "age",
 *     Collections.singletonList(18)  // 需要一个常量值
 * );
 *
 * // name = 'Alice' 谓词
 * LeafPredicate equalPredicate = new LeafPredicate(
 *     Equal.INSTANCE,
 *     DataTypes.STRING(),
 *     1,
 *     "name",
 *     Collections.singletonList("Alice")  // 需要一个常量值
 * );
 *
 * // 如果 age 为 NULL 或 name 为 NULL,这些谓词都会返回 false
 * }</pre>
 *
 * <h2>设计考虑</h2>
 * <ul>
 *   <li>该类简化了二元比较函数的实现,子类只需实现非 NULL 情况的比较逻辑</li>
 *   <li>NULL 检查逻辑统一处理,避免在每个子类中重复实现</li>
 *   <li>符合 SQL 标准的 NULL 语义,与主流数据库系统一致</li>
 * </ul>
 *
 * @see LeafFunction 叶子函数基类
 * @see Equal 等于比较函数
 * @see GreaterThan 大于比较函数
 * @see StartsWith 字符串前缀匹配函数
 */
public abstract class NullFalseLeafBinaryFunction extends LeafFunction {

    /** 序列化版本号 */
    private static final long serialVersionUID = 1L;

    /**
     * 基于实际字段值和常量值进行精确比较(简化的二元形式)。
     *
     * <p>该方法在确保字段值和常量值都不为 NULL 后被调用,子类只需实现非 NULL 情况的比较逻辑。
     * 例如,Equal 函数会比较两个值是否相等,GreaterThan 函数会比较字段值是否大于常量值。
     *
     * @param type 字段的数据类型
     * @param field 字段值(保证不为 null)
     * @param literal 常量值(保证不为 null)
     * @return 如果字段值满足与常量值的比较条件返回 true,否则返回 false
     */
    public abstract boolean test(DataType type, Object field, Object literal);

    /**
     * 基于统计信息和常量值进行快速过滤测试(简化的二元形式)。
     *
     * <p>该方法在确保不是所有行都为 NULL 且常量值不为 NULL 后被调用。
     * 子类利用统计信息(最小值、最大值)与常量值进行比较,判断是否可能存在满足条件的数据。
     *
     * <p>示例:
     * <ul>
     *   <li>Equal 函数:如果 literal 不在 [min, max] 范围内,返回 false</li>
     *   <li>GreaterThan 函数:如果 max <= literal,返回 false</li>
     *   <li>LessThan 函数:如果 min >= literal,返回 false</li>
     * </ul>
     *
     * @param type 字段的数据类型
     * @param rowCount 数据行总数
     * @param min 字段的最小值(可能为 null)
     * @param max 字段的最大值(可能为 null)
     * @param nullCount 字段的空值计数(可能为 null)
     * @param literal 常量值(保证不为 null)
     * @return true 表示可能包含满足条件的数据;false 表示确定不包含满足条件的数据
     */
    public abstract boolean test(
            DataType type, long rowCount, Object min, Object max, Long nullCount, Object literal);

    /**
     * 基于实际字段值和常量值列表进行精确测试(实现 LeafFunction 接口)。
     *
     * <p>该方法实现了 NULL 检查逻辑:
     * <ol>
     *   <li>如果字段值为 NULL,返回 false</li>
     *   <li>如果常量值(literals 的第一个元素)为 NULL,返回 false</li>
     *   <li>否则调用简化的二元方法 {@link #test(DataType, Object, Object)} 进行实际比较</li>
     * </ol>
     *
     * @param type 字段的数据类型
     * @param field 字段值(可能为 null)
     * @param literals 常量值列表(只使用第一个元素)
     * @return 如果字段值满足与常量值的比较条件返回 true,否则返回 false(NULL 返回 false)
     */
    @Override
    public boolean test(DataType type, Object field, List<Object> literals) {
        if (field == null || literals.get(0) == null) {
            return false;
        }
        return test(type, field, literals.get(0));
    }

    /**
     * 基于统计信息和常量值列表进行快速过滤测试(实现 LeafFunction 接口)。
     *
     * <p>该方法实现了 NULL 检查逻辑:
     * <ol>
     *   <li>如果 nullCount 不为 null:
     *       <ul>
     *         <li>如果所有行都是 NULL(rowCount == nullCount),返回 false</li>
     *         <li>如果常量值为 NULL,返回 false</li>
     *       </ul>
     *   </li>
     *   <li>否则调用简化的二元方法 {@link #test(DataType, long, Object, Object, Long, Object)} 进行实际比较</li>
     * </ol>
     *
     * @param type 字段的数据类型
     * @param rowCount 数据行总数
     * @param min 字段的最小值(可能为 null)
     * @param max 字段的最大值(可能为 null)
     * @param nullCount 字段的空值计数(可能为 null)
     * @param literals 常量值列表(只使用第一个元素)
     * @return true 表示可能包含满足条件的数据;false 表示确定不包含满足条件的数据
     */
    @Override
    public boolean test(
            DataType type,
            long rowCount,
            Object min,
            Object max,
            Long nullCount,
            List<Object> literals) {
        if (nullCount != null) {
            if (rowCount == nullCount || literals.get(0) == null) {
                return false;
            }
        }
        return test(type, rowCount, min, max, nullCount, literals.get(0));
    }
}
