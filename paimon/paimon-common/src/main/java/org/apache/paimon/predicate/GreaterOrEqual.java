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
 * 大于等于(Greater Or Equal)比较函数,用于判断字段值是否大于或等于常量值。
 *
 * <p>该类实现了 SQL 中的大于等于比较操作(>=),例如: {@code age >= 18}, {@code score >= 60.0}
 *
 * <h2>比较逻辑</h2>
 * <ul>
 *   <li><b>精确测试</b>: 使用类型感知的比较器判断字段值是否大于或等于常量值</li>
 *   <li><b>统计信息测试</b>: 如果 max >= literal,返回 true(可能存在 >= literal 的值);
 *       否则返回 false(所有值都 < literal)</li>
 * </ul>
 *
 * <h2>NULL 语义</h2>
 * 该类继承自 {@link NullFalseLeafBinaryFunction},遵循 SQL 的 NULL 语义:
 * <ul>
 *   <li>{@code NULL >= value} 返回 false</li>
 *   <li>{@code value >= NULL} 返回 false</li>
 * </ul>
 *
 * <h2>谓词否定</h2>
 * GreaterOrEqual 的否定是 {@link LessThan}:
 * <ul>
 *   <li>NOT (age >= 18) 等价于 age < 18</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // age >= 18
 * LeafPredicate predicate = new LeafPredicate(
 *     GreaterOrEqual.INSTANCE,
 *     DataTypes.INT(),
 *     0, "age",
 *     Collections.singletonList(18)
 * );
 *
 * // 统计信息过滤示例:
 * // 如果 max=25, 则 age>=18 可能存在,返回 true
 * // 如果 max=15, 则 age>=18 不可能存在,返回 false
 * // 如果 max=18, 则 age>=18 可能存在(可能所有值都是18),返回 true
 * }</pre>
 *
 * <h2>设计考虑</h2>
 * <ul>
 *   <li>该类是无状态的单例,通过 {@link #INSTANCE} 访问</li>
 *   <li>统计信息测试只需检查 max 值,如果 max < literal,则确定不存在满足条件的数据</li>
 *   <li>与 {@link GreaterThan} 的区别在于包含等于的情况</li>
 * </ul>
 *
 * @see LessOrEqual 小于等于比较函数
 * @see GreaterThan 大于比较函数
 * @see LessThan 小于比较函数(GreaterOrEqual 的否定形式)
 * @see CompareUtils 类型感知的比较工具
 */
public class GreaterOrEqual extends NullFalseLeafBinaryFunction {

    /** JSON 序列化时的函数名称 */
    public static final String NAME = "GREATER_OR_EQUAL";

    /** 单例实例 */
    public static final GreaterOrEqual INSTANCE = new GreaterOrEqual();

    /** 私有构造函数,强制使用单例 */
    @JsonCreator
    private GreaterOrEqual() {}

    /**
     * 基于实际字段值进行精确的大于等于比较。
     *
     * <p>使用类型感知的比较器判断字段值是否大于或等于常量值。
     * 该方法在 NULL 检查通过后被调用,因此 field 和 literal 都不为 null。
     *
     * @param type 字段的数据类型
     * @param field 字段值(保证不为 null)
     * @param literal 常量值(保证不为 null)
     * @return 如果字段值大于或等于常量值返回 true,否则返回 false
     */
    @Override
    public boolean test(DataType type, Object field, Object literal) {
        return compareLiteral(type, literal, field) <= 0;
    }

    /**
     * 基于统计信息进行快速过滤测试。
     *
     * <p>该方法判断是否可能存在大于或等于常量值的数据:
     * <ul>
     *   <li>如果 max >= literal,返回 true(可能存在 >= literal 的值)</li>
     *   <li>如果 max < literal,返回 false(所有值都 < literal,不存在 >= literal 的值)</li>
     * </ul>
     *
     * <p>示例:
     * <ul>
     *   <li>age >= 18, max=25: 25 >= 18,返回 true</li>
     *   <li>age >= 18, max=15: 15 < 18,返回 false</li>
     *   <li>age >= 18, max=18: 18 >= 18,返回 true (与 GreaterThan 的区别)</li>
     * </ul>
     *
     * @param type 字段的数据类型
     * @param rowCount 数据行总数(未使用)
     * @param min 字段的最小值(未使用)
     * @param max 字段的最大值
     * @param nullCount 字段的空值计数(未使用)
     * @param literal 常量值
     * @return true 表示可能存在大于或等于常量值的数据;false 表示确定不存在大于或等于常量值的数据
     */
    @Override
    public boolean test(
            DataType type, long rowCount, Object min, Object max, Long nullCount, Object literal) {
        return compareLiteral(type, literal, max) <= 0;
    }

    /**
     * 返回该函数的否定形式。
     *
     * @return LessThan 函数
     */
    @Override
    public Optional<LeafFunction> negate() {
        return Optional.of(LessThan.INSTANCE);
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
        return visitor.visitGreaterOrEqual(fieldRef, literals.get(0));
    }
}
