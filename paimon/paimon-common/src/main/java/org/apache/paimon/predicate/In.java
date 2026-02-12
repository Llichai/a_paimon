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
 * In 集合成员判断谓词函数。
 *
 * <p>这是一个 {@link LeafFunction},用于评估字段值是否在指定的值集合中,等价于 SQL 中的 IN 操作。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li>集合成员测试: 检查字段值是否在给定的字面量列表中
 *   <li>NULL 处理: field 或 literal 为 NULL 时返回 false(遵循 SQL 三值逻辑)
 *   <li>统计过滤: 基于 min/max 统计信息进行过滤优化
 *   <li>支持取反: 可以转换为 {@link NotIn}
 * </ul>
 *
 * <h2>使用场景</h2>
 * <pre>{@code
 * // SQL: SELECT * FROM table WHERE status IN ('ACTIVE', 'PENDING', 'RUNNING')
 * PredicateBuilder builder = new PredicateBuilder(rowType);
 * List<Object> values = Arrays.asList(
 *     BinaryString.fromString("ACTIVE"),
 *     BinaryString.fromString("PENDING"),
 *     BinaryString.fromString("RUNNING")
 * );
 * Predicate p = builder.in(statusIdx, values);
 *
 * // SQL: SELECT * FROM table WHERE age IN (18, 25, 30, 35)
 * List<Object> ages = Arrays.asList(18, 25, 30, 35);
 * Predicate p = builder.in(ageIdx, ages);
 * }</pre>
 *
 * <h2>SQL 三值逻辑</h2>
 * <p>IN 操作的 NULL 处理遵循 SQL 标准:
 * <ul>
 *   <li>{@code NULL IN (1, 2, 3)} -> false
 *   <li>{@code 1 IN (1, NULL, 3)} -> true (找到匹配值)
 *   <li>{@code 2 IN (1, NULL, 3)} -> false (NULL 字面量被忽略)
 *   <li>{@code NULL IN (NULL)} -> false
 * </ul>
 *
 * <h2>性能优化</h2>
 * <ul>
 *   <li>统计过滤: 利用 min/max 统计信息快速排除不可能的情况
 *   <li>NULL 统计: 如果所有值都是 NULL,直接返回 false
 *   <li>范围检查: 只有字面量在 [min, max] 范围内时才可能匹配
 *   <li>短路求值: 找到匹配值立即返回 true
 * </ul>
 *
 * <h2>与 OR 的关系</h2>
 * <p>在 {@link PredicateBuilder} 中,少量字面量的 IN 会被展开为 OR:
 * <pre>{@code
 * // 少于等于 20 个字面量时
 * IN (1, 2, 3) -> (field = 1) OR (field = 2) OR (field = 3)
 *
 * // 超过 20 个字面量时使用 IN 函数
 * IN (1, 2, ..., 50) -> IN function
 * }</pre>
 *
 * <h2>实现细节</h2>
 * <ul>
 *   <li>单例模式: 使用 {@link #INSTANCE} 访问
 *   <li>可序列化: serialVersionUID = 1L
 *   <li>支持取反: {@link #negate()} 返回 {@link NotIn#INSTANCE}
 *   <li>访问者模式: 支持通过 {@link FunctionVisitor} 遍历
 * </ul>
 *
 * @see NotIn IN 的取反操作
 * @see Equal 单个值相等判断
 * @see Or 逻辑 OR 操作
 * @see CompareUtils 值比较工具
 */
public class In extends LeafFunction {

    private static final long serialVersionUID = 1L;

    /** 函数名称常量。 */
    public static final String NAME = "IN";

    /** 单例实例。 */
    public static final In INSTANCE = new In();

    /** 私有构造函数,强制使用单例。 */
    @JsonCreator
    private In() {}

    /**
     * 测试字段值是否在字面量列表中。
     *
     * <p>NULL 处理: field 为 NULL 或 literal 为 NULL 时都不匹配。
     *
     * @param type 字段的数据类型
     * @param field 要测试的字段值
     * @param literals 字面量列表
     * @return 如果字段值在列表中返回 true,否则返回 false
     */
    @Override
    public boolean test(DataType type, Object field, List<Object> literals) {
        if (field == null) {
            return false;
        }
        for (Object literal : literals) {
            if (literal != null && compareLiteral(type, literal, field) == 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * 基于统计信息测试是否可能存在匹配的值。
     *
     * <p>优化策略:
     * <ul>
     *   <li>全 NULL: 如果所有值都是 NULL,返回 false
     *   <li>范围检查: 只有字面量在 [min, max] 范围内才可能匹配
     * </ul>
     *
     * @param type 字段的数据类型
     * @param rowCount 行数
     * @param min 最小值
     * @param max 最大值
     * @param nullCount NULL 值数量
     * @param literals 字面量列表
     * @return 如果可能存在匹配返回 true,否则返回 false
     */
    @Override
    public boolean test(
            DataType type,
            long rowCount,
            Object min,
            Object max,
            Long nullCount,
            List<Object> literals) {
        if (nullCount != null && rowCount == nullCount) {
            return false;
        }
        for (Object literal : literals) {
            if (literal != null
                    && compareLiteral(type, literal, min) >= 0
                    && compareLiteral(type, literal, max) <= 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * 对函数取反。
     *
     * @return 返回 {@link NotIn#INSTANCE}
     */
    @Override
    public Optional<LeafFunction> negate() {
        return Optional.of(NotIn.INSTANCE);
    }

    /**
     * 接受访问者模式的访问。
     *
     * @param visitor 函数访问者
     * @param fieldRef 字段引用
     * @param literals 字面量列表
     * @param <T> 访问结果类型
     * @return 访问结果
     */
    @Override
    public <T> T visit(FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals) {
        return visitor.visitIn(fieldRef, literals);
    }
}
