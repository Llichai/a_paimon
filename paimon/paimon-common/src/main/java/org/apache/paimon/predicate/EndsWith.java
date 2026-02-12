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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.types.DataType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;

import java.util.List;
import java.util.Optional;

/**
 * EndsWith 字符串结尾匹配谓词函数。
 *
 * <p>这是一个 {@link NullFalseLeafBinaryFunction},用于评估字符串的结尾匹配操作,等价于 SQL 中的 LIKE 模式:
 * <ul>
 *   <li>{@code field LIKE '%abc'} - 以固定字符串结尾
 *   <li>{@code field LIKE '_abc'} - 单字符通配符 + 固定结尾
 * </ul>
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li>字符串后缀匹配: 检查字段值是否以指定模式结尾
 *   <li>LIKE 优化: 作为 LIKE '%pattern' 的优化实现
 *   <li>NULL 安全: 遇到 NULL 值时返回 false
 *   <li>统计过滤: 基于统计信息的过滤(当前实现总是返回 true)
 * </ul>
 *
 * <h2>使用场景</h2>
 * <pre>{@code
 * // SQL: SELECT * FROM table WHERE name LIKE '%@gmail.com'
 * // 等价于: endsWith(name, '@gmail.com')
 *
 * // SQL: SELECT * FROM table WHERE file_path LIKE '%/data.txt'
 * // 等价于: endsWith(file_path, '/data.txt')
 * }</pre>
 *
 * <h2>性能优化</h2>
 * <ul>
 *   <li>直接使用 BinaryString.endsWith() 方法进行匹配
 *   <li>避免正则表达式的开销
 *   <li>单例模式减少对象创建
 * </ul>
 *
 * <h2>实现细节</h2>
 * <ul>
 *   <li>单例实例: 使用 {@link #INSTANCE} 访问
 *   <li>不支持取反: {@link #negate()} 返回 empty
 *   <li>访问者模式: 支持通过 {@link FunctionVisitor} 遍历
 * </ul>
 *
 * @see StartsWith 字符串开头匹配
 * @see Contains 字符串包含匹配
 * @see Like 完整的 LIKE 模式匹配
 * @see LikeOptimization LIKE 模式的自动优化
 */
public class EndsWith extends NullFalseLeafBinaryFunction {

    /** 函数名称常量。 */
    public static final String NAME = "ENDS_WITH";

    /** 单例实例。 */
    public static final EndsWith INSTANCE = new EndsWith();

    /** 私有构造函数,强制使用单例。 */
    @JsonCreator
    private EndsWith() {}

    /**
     * 测试字段值是否以指定模式结尾。
     *
     * @param type 字段的数据类型
     * @param field 要测试的字段值(BinaryString)
     * @param patternLiteral 结尾模式字面量(BinaryString)
     * @return 如果字段值以模式结尾返回 true,否则返回 false
     */
    @Override
    public boolean test(DataType type, Object field, Object patternLiteral) {
        BinaryString fieldString = (BinaryString) field;
        return fieldString.endsWith((BinaryString) patternLiteral);
    }

    /**
     * 基于统计信息测试是否可能存在匹配的值。
     *
     * <p>注意: 当前实现总是返回 true,因为仅基于 min/max 统计信息 无法确定是否存在以特定模式结尾的字符串。
     *
     * @param type 字段的数据类型
     * @param rowCount 行数
     * @param min 最小值
     * @param max 最大值
     * @param nullCount NULL 值数量
     * @param patternLiteral 结尾模式字面量
     * @return 总是返回 true(保守估计)
     */
    @Override
    public boolean test(
            DataType type,
            long rowCount,
            Object min,
            Object max,
            Long nullCount,
            Object patternLiteral) {
        return true;
    }

    /**
     * 尝试对函数取反。
     *
     * @return 返回 empty,因为不支持取反操作
     */
    @Override
    public Optional<LeafFunction> negate() {
        return Optional.empty();
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
        return visitor.visitEndsWith(fieldRef, literals.get(0));
    }
}
