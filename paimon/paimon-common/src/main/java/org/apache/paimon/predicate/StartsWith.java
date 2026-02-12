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
 * 字符串前缀匹配谓词函数,实现 LIKE 'prefix%' 模式匹配。
 *
 * <p>该类继承自 {@link NullFalseLeafBinaryFunction},用于判断字符串字段是否以指定的前缀开始。
 * 这是 SQL LIKE 操作符的一种优化形式,适用于 'abc%' 或 'abc_' 这样的简单前缀模式。
 *
 * <h2>功能特点</h2>
 * <ul>
 *   <li><b>前缀匹配</b>:检查字符串是否以指定的前缀开始</li>
 *   <li><b>NULL 语义</b>:字段值或模式为 NULL 时返回 false</li>
 *   <li><b>统计信息优化</b>:利用字符串的 Min/Max 值进行范围过滤</li>
 *   <li><b>前缀索引友好</b>:该谓词可以有效利用前缀索引</li>
 * </ul>
 *
 * <h2>NULL 处理</h2>
 * 遵循 SQL 标准的 NULL 语义:
 * <ul>
 *   <li>如果字段值为 NULL,返回 false</li>
 *   <li>如果模式值为 NULL,返回 false</li>
 *   <li>只有当两者都不为 NULL 时,才进行实际的前缀匹配</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // name LIKE 'Alice%'
 * Predicate startsWithPredicate = new LeafPredicate(
 *     StartsWith.INSTANCE,
 *     DataTypes.STRING(),
 *     0,
 *     "name",
 *     Collections.singletonList(BinaryString.fromString("Alice"))
 * );
 *
 * // 测试数据:
 * // "Alice" -> true (以 "Alice" 开头)
 * // "Alice Smith" -> true (以 "Alice" 开头)
 * // "Bob" -> false (不以 "Alice" 开头)
 * // null -> false (NULL 返回 false)
 * }</pre>
 *
 * <h2>统计信息过滤</h2>
 * 该谓词利用字符串的 Min/Max 统计信息进行范围过滤:
 * <ul>
 *   <li><b>最小值匹配</b>:如果 min 以 pattern 开头,或者 min <= pattern,可能包含匹配数据</li>
 *   <li><b>最大值匹配</b>:如果 max 以 pattern 开头,或者 max >= pattern,可能包含匹配数据</li>
 *   <li><b>范围检查</b>:只有当 min 和 max 都满足条件时,才认为可能包含匹配数据</li>
 * </ul>
 *
 * <h3>统计信息过滤示例</h3>
 * <pre>{@code
 * // 假设某个数据块的统计信息:
 * // name: min='Alice', max='Bob', nullCount=0
 *
 * // 查询 1: name LIKE 'Charlie%'
 * // min='Alice' < 'Charlie' 且不以 'Charlie' 开头 -> 可能包含
 * // max='Bob' < 'Charlie' 且不以 'Charlie' 开头 -> 不包含
 * // 结果: false,跳过该数据块
 *
 * // 查询 2: name LIKE 'A%'
 * // min='Alice' 以 'A' 开头 -> 可能包含
 * // max='Bob' >= 'A' -> 可能包含
 * // 结果: true,需要读取该数据块
 *
 * // 查询 3: name LIKE 'Al%'
 * // min='Alice' 以 'Al' 开头 -> 可能包含
 * // max='Bob' >= 'Al' -> 可能包含
 * // 结果: true,需要读取该数据块
 * }</pre>
 *
 * <h2>索引利用</h2>
 * StartsWith 谓词可以有效利用以下索引:
 * <ul>
 *   <li><b>前缀索引</b>:B-Tree 索引可以高效处理前缀查询</li>
 *   <li><b>倒排索引</b>:某些倒排索引支持前缀搜索</li>
 *   <li><b>字典压缩</b>:字典编码的字符串可以快速进行前缀匹配</li>
 * </ul>
 *
 * <h2>与 LIKE 的关系</h2>
 * StartsWith 是 {@link Like} 操作符的一种优化形式,由 {@link LikeOptimization} 将简单的
 * LIKE 'prefix%' 模式优化为 StartsWith 谓词:
 * <pre>{@code
 * // 原始 LIKE 查询:
 * // name LIKE 'Alice%'
 *
 * // 优化后:
 * // name STARTS_WITH 'Alice'
 * }</pre>
 *
 * <h2>不可否定</h2>
 * StartsWith 谓词不支持否定操作,{@link #negate()} 返回 {@code Optional.empty()}。
 * 如果需要否定 StartsWith,需要使用其他方式表达,如:
 * <pre>{@code
 * // NOT (name LIKE 'Alice%')
 * // 无法简单地转换为其他谓词,需要保持原样或使用通用的 NOT 包装
 * }</pre>
 *
 * <h2>性能考虑</h2>
 * <ul>
 *   <li><b>字符串比较</b>:使用 BinaryString 的高效前缀匹配方法</li>
 *   <li><b>统计信息过滤</b>:通过 Min/Max 值快速排除不匹配的数据块</li>
 *   <li><b>索引友好</b>:可以充分利用前缀索引加速查询</li>
 * </ul>
 *
 * @see NullFalseLeafBinaryFunction 二元叶子函数基类
 * @see EndsWith 字符串后缀匹配
 * @see Contains 字符串包含匹配
 * @see Like 通用 LIKE 模式匹配
 * @see LikeOptimization LIKE 模式优化器
 */
public class StartsWith extends NullFalseLeafBinaryFunction {

    /** JSON 序列化时的函数名称 */
    public static final String NAME = "STARTS_WITH";

    /** StartsWith 函数的单例实例 */
    public static final StartsWith INSTANCE = new StartsWith();

    /**
     * 私有构造函数,用于 JSON 反序列化。
     */
    @JsonCreator
    private StartsWith() {}

    /**
     * 基于实际字段值进行前缀匹配测试。
     *
     * <p>检查字段值是否以指定的模式前缀开始。
     *
     * @param type 字段的数据类型
     * @param field 字段值(BinaryString 类型)
     * @param patternLiteral 前缀模式(BinaryString 类型)
     * @return 如果字段值以模式前缀开始返回 true,否则返回 false
     */
    @Override
    public boolean test(DataType type, Object field, Object patternLiteral) {
        BinaryString fieldString = (BinaryString) field;
        return fieldString.startsWith((BinaryString) patternLiteral);
    }

    /**
     * 基于统计信息进行快速过滤测试。
     *
     * <p>该方法利用字符串的 Min/Max 值判断数据块是否可能包含以指定前缀开始的字符串。
     *
     * <p>过滤逻辑:
     * <ul>
     *   <li>如果 min 以 pattern 开头,或者 min <= pattern,可能包含匹配数据</li>
     *   <li>如果 max 以 pattern 开头,或者 max >= pattern,可能包含匹配数据</li>
     *   <li>只有当两个条件都满足时,才认为可能包含匹配数据</li>
     * </ul>
     *
     * @param type 字段的数据类型
     * @param rowCount 数据行总数
     * @param min 字段的最小值
     * @param max 字段的最大值
     * @param nullCount 字段的空值计数
     * @param patternLiteral 前缀模式
     * @return true 表示可能包含匹配数据;false 表示确定不包含匹配数据
     */
    @Override
    public boolean test(
            DataType type,
            long rowCount,
            Object min,
            Object max,
            Long nullCount,
            Object patternLiteral) {
        BinaryString minStr = (BinaryString) min;
        BinaryString maxStr = (BinaryString) max;
        BinaryString pattern = (BinaryString) patternLiteral;
        return (minStr.startsWith(pattern) || minStr.compareTo(pattern) <= 0)
                && (maxStr.startsWith(pattern) || maxStr.compareTo(pattern) >= 0);
    }

    /**
     * StartsWith 谓词不支持否定。
     *
     * @return Optional.empty()
     */
    @Override
    public Optional<LeafFunction> negate() {
        return Optional.empty();
    }

    /**
     * 访问者模式的接受方法。
     *
     * @param visitor 函数访问者
     * @param fieldRef 字段引用
     * @param literals 常量值列表
     * @param <T> 访问结果的类型
     * @return 访问者处理 StartsWith 的结果
     */
    @Override
    public <T> T visit(FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals) {
        return visitor.visitStartsWith(fieldRef, literals.get(0));
    }
}
