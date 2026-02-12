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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;

import java.util.List;
import java.util.Optional;

import static org.apache.paimon.predicate.CompareUtils.compareLiteral;

/**
 * 等于(Equal)比较函数,用于判断字段值是否等于常量值。
 *
 * <p>该类实现了 SQL 中的等于比较操作(=),例如: {@code age = 18}, {@code name = 'Alice'}
 *
 * <h2>比较逻辑</h2>
 * <ul>
 *   <li><b>精确测试</b>: 使用类型感知的比较器判断字段值与常量值是否相等</li>
 *   <li><b>统计信息测试</b>: 如果常量值在 [min, max] 范围内,返回 true(可能存在相等的值);
 *       否则返回 false(确定不存在相等的值)</li>
 * </ul>
 *
 * <h2>NULL 语义</h2>
 * 该类继承自 {@link NullFalseLeafBinaryFunction},遵循 SQL 的 NULL 语义:
 * <ul>
 *   <li>{@code NULL = NULL} 返回 false (不是 true)</li>
 *   <li>{@code NULL = 'value'} 返回 false</li>
 *   <li>{@code 'value' = NULL} 返回 false</li>
 * </ul>
 *
 * <h2>谓词否定</h2>
 * Equal 的否定是 {@link NotEqual}:
 * <ul>
 *   <li>NOT (age = 18) 等价于 age != 18</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // age = 18
 * LeafPredicate predicate = new LeafPredicate(
 *     Equal.INSTANCE,
 *     DataTypes.INT(),
 *     0, "age",
 *     Collections.singletonList(18)
 * );
 *
 * // 统计信息过滤示例:
 * // 如果 min=10, max=20, 则 age=18 可能存在,返回 true
 * // 如果 min=20, max=30, 则 age=18 不可能存在,返回 false
 * }</pre>
 *
 * <h2>设计考虑</h2>
 * <ul>
 *   <li>该类是无状态的单例,通过 {@link #INSTANCE} 访问</li>
 *   <li>使用 {@link CompareUtils#compareLiteral} 进行类型感知的比较,支持所有 Paimon 数据类型</li>
 *   <li>统计信息测试采用保守策略,只有在确定不存在满足条件的数据时才返回 false</li>
 * </ul>
 *
 * @see NotEqual 不等于比较函数
 * @see CompareUtils 类型感知的比较工具
 * @see NullFalseLeafBinaryFunction 二元比较函数基类
 */
public class Equal extends NullFalseLeafBinaryFunction {

    /** JSON 序列化时的函数名称 */
    public static final String NAME = "EQUAL";

    /** 单例实例 */
    public static final Equal INSTANCE = new Equal();

    /** 私有构造函数,强制使用单例 */
    @JsonCreator
    private Equal() {}

    /**
     * 基于实际字段值进行精确的等于比较。
     *
     * <p>使用类型感知的比较器判断字段值是否等于常量值。
     * 该方法在 NULL 检查通过后被调用,因此 field 和 literal 都不为 null。
     *
     * @param type 字段的数据类型
     * @param field 字段值(保证不为 null)
     * @param literal 常量值(保证不为 null)
     * @return 如果字段值等于常量值返回 true,否则返回 false
     */
    @Override
    public boolean test(DataType type, Object field, Object literal) {
        return compareLiteral(type, literal, field) == 0;
    }

    /**
     * 基于统计信息进行快速过滤测试。
     *
     * <p>该方法判断常量值是否在字段的值域范围内:
     * <ul>
     *   <li>如果 literal >= min 且 literal <= max,返回 true(可能存在等于 literal 的值)</li>
     *   <li>如果 literal < min 或 literal > max,返回 false(确定不存在等于 literal 的值)</li>
     * </ul>
     *
     * <p>示例:
     * <ul>
     *   <li>age = 18, min=10, max=20: 18 在 [10,20] 范围内,返回 true</li>
     *   <li>age = 25, min=10, max=20: 25 > 20,返回 false</li>
     * </ul>
     *
     * @param type 字段的数据类型
     * @param rowCount 数据行总数(未使用)
     * @param min 字段的最小值
     * @param max 字段的最大值
     * @param nullCount 字段的空值计数(未使用)
     * @param literal 常量值
     * @return true 表示可能存在等于常量值的数据;false 表示确定不存在等于常量值的数据
     */
    @Override
    public boolean test(
            DataType type, long rowCount, Object min, Object max, Long nullCount, Object literal) {
        return compareLiteral(type, literal, min) >= 0 && compareLiteral(type, literal, max) <= 0;
    }

    /**
     * 返回该函数的否定形式。
     *
     * @return NotEqual 函数
     */
    @Override
    public Optional<LeafFunction> negate() {
        return Optional.of(NotEqual.INSTANCE);
    }

    /**
     * 访问者模式的接受方法。
     *
     * @param visitor 函数访问者
     * @param fieldRef 字段引用
     * @param literals 常量值列表
     * @param <T> 访问结果的类型
     * @return 访问者处理的结果
     */
    @Override
    public <T> T visit(FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals) {
        return visitor.visitEqual(fieldRef, literals.get(0));
    }
}
