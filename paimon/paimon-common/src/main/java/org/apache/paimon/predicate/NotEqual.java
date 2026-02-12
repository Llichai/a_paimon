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
 * 不等于(Not Equal)比较函数,用于判断字段值是否不等于常量值。
 *
 * <p>该类实现了 SQL 中的不等于比较操作(!=, <>),例如: {@code age != 18}, {@code name <> 'Alice'}
 *
 * <h2>比较逻辑</h2>
 * <ul>
 *   <li><b>精确测试</b>: 使用类型感知的比较器判断字段值与常量值是否不相等</li>
 *   <li><b>统计信息测试</b>: 如果常量值不等于 min 或不等于 max,返回 true(可能存在不等于的值);
 *       只有当 min = max = literal 时才返回 false(所有值都等于 literal)</li>
 * </ul>
 *
 * <h2>NULL 语义</h2>
 * 该类继承自 {@link NullFalseLeafBinaryFunction},遵循 SQL 的 NULL 语义:
 * <ul>
 *   <li>{@code NULL != NULL} 返回 false (不是 true)</li>
 *   <li>{@code NULL != 'value'} 返回 false</li>
 *   <li>{@code 'value' != NULL} 返回 false</li>
 * </ul>
 *
 * <h2>谓词否定</h2>
 * NotEqual 的否定是 {@link Equal}:
 * <ul>
 *   <li>NOT (age != 18) 等价于 age = 18</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // status != 'DELETED'
 * LeafPredicate predicate = new LeafPredicate(
 *     NotEqual.INSTANCE,
 *     DataTypes.STRING(),
 *     0, "status",
 *     Collections.singletonList("DELETED")
 * );
 *
 * // 统计信息过滤示例:
 * // 如果 min='ACTIVE', max='PENDING', 则 status!='DELETED' 可能存在,返回 true
 * // 如果 min='DELETED', max='DELETED', 则 status!='DELETED' 不可能存在,返回 false
 * }</pre>
 *
 * <h2>设计考虑</h2>
 * <ul>
 *   <li>该类是无状态的单例,通过 {@link #INSTANCE} 访问</li>
 *   <li>统计信息测试采用保守策略:只有在确定所有值都等于 literal 时才返回 false</li>
 *   <li>如果 min != literal 或 max != literal,说明至少存在一些不等于 literal 的值</li>
 * </ul>
 *
 * @see Equal 等于比较函数
 * @see CompareUtils 类型感知的比较工具
 * @see NullFalseLeafBinaryFunction 二元比较函数基类
 */
public class NotEqual extends NullFalseLeafBinaryFunction {

    /** JSON 序列化时的函数名称 */
    public static final String NAME = "NOT_EQUAL";

    /** 单例实例 */
    public static final NotEqual INSTANCE = new NotEqual();

    /** 私有构造函数,强制使用单例 */
    @JsonCreator
    private NotEqual() {}

    /**
     * 基于实际字段值进行精确的不等于比较。
     *
     * <p>使用类型感知的比较器判断字段值是否不等于常量值。
     * 该方法在 NULL 检查通过后被调用,因此 field 和 literal 都不为 null。
     *
     * @param type 字段的数据类型
     * @param field 字段值(保证不为 null)
     * @param literal 常量值(保证不为 null)
     * @return 如果字段值不等于常量值返回 true,否则返回 false
     */
    @Override
    public boolean test(DataType type, Object field, Object literal) {
        return compareLiteral(type, literal, field) != 0;
    }

    /**
     * 基于统计信息进行快速过滤测试。
     *
     * <p>该方法判断是否可能存在不等于常量值的数据:
     * <ul>
     *   <li>如果 literal != min 或 literal != max,返回 true(至少存在一些不等于 literal 的值)</li>
     *   <li>如果 literal == min 且 literal == max,返回 false(所有值都等于 literal,不存在不等于的值)</li>
     * </ul>
     *
     * <p>示例:
     * <ul>
     *   <li>status != 'DELETED', min='ACTIVE', max='PENDING': 'DELETED' != 'ACTIVE',返回 true</li>
     *   <li>status != 'ACTIVE', min='ACTIVE', max='ACTIVE': 所有值都是 'ACTIVE',返回 false</li>
     * </ul>
     *
     * @param type 字段的数据类型
     * @param rowCount 数据行总数(未使用)
     * @param min 字段的最小值
     * @param max 字段的最大值
     * @param nullCount 字段的空值计数(未使用)
     * @param literal 常量值
     * @return true 表示可能存在不等于常量值的数据;false 表示确定不存在不等于常量值的数据
     */
    @Override
    public boolean test(
            DataType type, long rowCount, Object min, Object max, Long nullCount, Object literal) {
        return compareLiteral(type, literal, min) != 0 || compareLiteral(type, literal, max) != 0;
    }

    /**
     * 返回该函数的否定形式。
     *
     * @return Equal 函数
     */
    @Override
    public Optional<LeafFunction> negate() {
        return Optional.of(Equal.INSTANCE);
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
        return visitor.visitNotEqual(fieldRef, literals.get(0));
    }
}
