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
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Pair;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Like 模式匹配谓词函数。
 *
 * <p>这是一个 {@link NullFalseLeafBinaryFunction},用于实现 SQL 标准的 LIKE 模式匹配操作。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li>LIKE 模式匹配: 支持 % 和 _ 通配符
 *   <li>自动优化: 简单模式自动转换为更高效的操作
 *   <li>正则表达式转换: 将 SQL LIKE 模式转换为 Java 正则表达式
 *   <li>结果缓存: 使用软引用缓存编译后的模式
 * </ul>
 *
 * <h2>LIKE 模式语法</h2>
 * <ul>
 *   <li>% - 匹配任意数量的字符(包括零个)
 *   <li>_ - 匹配单个字符
 *   <li>\ - 转义字符(可自定义)
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 简单模式(会被自动优化)
 * // "abc%" -> StartsWith
 * // "%abc" -> EndsWith
 * // "%abc%" -> Contains
 * // "abc" -> Equal
 *
 * // 复杂模式(使用正则表达式)
 * // "a%b%c" -> 正则匹配
 * // "_bc" -> 正则匹配
 * // "a_c" -> 正则匹配
 *
 * // SQL 示例
 * SELECT * FROM table WHERE name LIKE 'John%';  // 以 John 开头
 * SELECT * FROM table WHERE name LIKE '%son';   // 以 son 结尾
 * SELECT * FROM table WHERE name LIKE '%oh%';   // 包含 oh
 * SELECT * FROM table WHERE name LIKE 'J_hn';   // J 和 hn 之间有一个字符
 * }</pre>
 *
 * <h2>性能优化策略</h2>
 * <ul>
 *   <li>自动优化: {@link LikeOptimization} 将简单模式转换为专用函数
 *   <li>模式缓存: 使用 Caffeine 缓存编译后的过滤器,避免重复编译
 *   <li>软引用: 缓存使用软引用,在内存压力下自动清理
 *   <li>同步执行器: 缓存操作在当前线程执行,避免线程切换开销
 * </ul>
 *
 * <h2>正则表达式转换规则</h2>
 * <pre>
 * SQL LIKE    Java Regex
 * %           (?s:.*)      匹配任意字符(包括换行符)
 * _           .            匹配单个字符
 * [           \[           转义特殊字符
 * \           \\           转义字符本身
 * </pre>
 *
 * <h2>实现细节</h2>
 * <ul>
 *   <li>单例模式: 使用 {@link #INSTANCE} 访问
 *   <li>缓存实现: Caffeine 缓存,软引用,线程安全
 *   <li>不支持取反: {@link #negate()} 返回 empty
 *   <li>统计过滤: 基于统计信息的过滤(当前总是返回 true)
 * </ul>
 *
 * @see LikeOptimization LIKE 模式自动优化
 * @see StartsWith 以指定字符串开头
 * @see EndsWith 以指定字符串结尾
 * @see Contains 包含指定字符串
 * @see Equal 完全相等
 */
public class Like extends NullFalseLeafBinaryFunction {

    /** 函数名称常量。 */
    public static final String NAME = "LIKE";

    /** 单例实例。 */
    public static final Like INSTANCE = new Like();

    /**
     * 模式缓存,用于避免重复编译正则表达式。
     *
     * <p>特性:
     * <ul>
     *   <li>软引用: 在内存压力下自动清理
     *   <li>同步执行: 在当前线程执行,避免线程切换
     *   <li>线程安全: Caffeine 提供并发安全保证
     * </ul>
     */
    private static final Cache<BinaryString, Filter<BinaryString>> CACHE =
            Caffeine.newBuilder().softValues().executor(Runnable::run).build();

    /** 私有构造函数,强制使用单例。 */
    @JsonCreator
    private Like() {}

    /**
     * 测试字段值是否匹配 LIKE 模式。
     *
     * <p>实现流程:
     * <ol>
     *   <li>NULL 检查: field 为 null 返回 false
     *   <li>缓存查找: 尝试从缓存获取编译后的过滤器
     *   <li>创建过滤器: 缓存未命中时创建新过滤器并缓存
     *   <li>执行匹配: 使用过滤器测试字段值
     * </ol>
     *
     * @param type 字段的数据类型
     * @param field 要测试的字段值(BinaryString)
     * @param patternLiteral LIKE 模式字面量(BinaryString)
     * @return 如果字段值匹配模式返回 true,否则返回 false
     */
    @Override
    public boolean test(DataType type, Object field, Object patternLiteral) {
        if (field == null) {
            return false;
        }

        BinaryString pattern = (BinaryString) patternLiteral;
        Filter<BinaryString> filter = CACHE.getIfPresent(pattern);
        if (filter == null) {
            filter = createFunc(type, patternLiteral);
            CACHE.put(pattern, filter);
        }
        return filter.test((BinaryString) field);
    }

    /**
     * 创建 LIKE 模式的过滤器函数。
     *
     * <p>优化策略:
     * <ol>
     *   <li>尝试优化: 使用 {@link LikeOptimization} 尝试转换为简单操作
     *   <li>使用优化: 如果可以优化,使用专用函数(StartsWith/EndsWith/Contains/Equal)
     *   <li>正则表达式: 无法优化时,编译为正则表达式
     * </ol>
     *
     * @param type 字段的数据类型
     * @param patternLiteral LIKE 模式字面量
     * @return 过滤器函数
     */
    private Filter<BinaryString> createFunc(DataType type, Object patternLiteral) {
        Optional<Pair<NullFalseLeafBinaryFunction, Object>> optimized =
                LikeOptimization.tryOptimize(patternLiteral);
        if (optimized.isPresent()) {
            NullFalseLeafBinaryFunction func = optimized.get().getKey();
            Object literal = optimized.get().getValue();
            return field -> func.test(type, field, literal);
        }
        // TODO optimize for chain checkers when there is no '_'
        // TODO for example: "abc%def%","%abc%def","%abc%def%","abc%def"
        String regex = sqlToRegexLike(patternLiteral.toString(), null);
        Pattern pattern = Pattern.compile(regex);
        return input -> pattern.matcher(input.toString()).matches();
    }

    /**
     * 将 SQL LIKE 模式转换为 Java 正则表达式(带自定义转义字符)。
     *
     * @param sqlPattern SQL LIKE 模式
     * @param escapeStr 自定义转义字符串(null 表示使用默认的 '\')
     * @return Java 正则表达式字符串
     * @throws RuntimeException 如果转义字符无效
     */
    private static String sqlToRegexLike(String sqlPattern, @Nullable CharSequence escapeStr) {
        char escapeChar;
        if (escapeStr != null) {
            if (escapeStr.length() != 1) {
                throw invalidEscapeCharacter(escapeStr.toString());
            }

            escapeChar = escapeStr.charAt(0);
        } else {
            escapeChar = '\\';
        }

        return sqlToRegexLike(sqlPattern, escapeChar);
    }

    /**
     * 将 SQL LIKE 模式转换为 Java 正则表达式。
     *
     * <p>转换规则:
     * <ul>
     *   <li>% -> (?s:.*) 匹配任意字符序列(包括换行符)
     *   <li>_ -> . 匹配单个字符
     *   <li>正则特殊字符 -> 转义 (如 []()|^-+*?{}$\.)
     *   <li>转义字符 -> 处理转义序列
     * </ul>
     *
     * @param sqlPattern SQL LIKE 模式
     * @param escapeChar 转义字符
     * @return Java 正则表达式字符串
     * @throws RuntimeException 如果转义序列无效
     */
    private static String sqlToRegexLike(String sqlPattern, char escapeChar) {
        int len = sqlPattern.length();
        StringBuilder javaPattern = new StringBuilder(len + len);

        for (int i = 0; i < len; ++i) {
            char c = sqlPattern.charAt(i);
            if ("[]()|^-+*?{}$\\.".indexOf(c) >= 0) {
                javaPattern.append('\\');
            }

            if (c == escapeChar) {
                if (i == sqlPattern.length() - 1) {
                    throw invalidEscapeSequence(sqlPattern, i);
                }

                char nextChar = sqlPattern.charAt(i + 1);
                if (nextChar != '_' && nextChar != '%' && nextChar != escapeChar) {
                    throw invalidEscapeSequence(sqlPattern, i);
                }

                javaPattern.append(nextChar);
                ++i;
            } else if (c == '_') {
                javaPattern.append('.');
            } else if (c == '%') {
                javaPattern.append("(?s:.*)");
            } else {
                javaPattern.append(c);
            }
        }

        return javaPattern.toString();
    }

    /**
     * 创建无效转义字符异常。
     *
     * @param s 转义字符串
     * @return RuntimeException
     */
    private static RuntimeException invalidEscapeCharacter(String s) {
        return new RuntimeException("Invalid escape character '" + s + "'");
    }

    /**
     * 创建无效转义序列异常。
     *
     * @param s 模式字符串
     * @param i 错误位置
     * @return RuntimeException
     */
    private static RuntimeException invalidEscapeSequence(String s, int i) {
        return new RuntimeException("Invalid escape sequence '" + s + "', " + i);
    }

    /**
     * 基于统计信息测试是否可能存在匹配的值。
     *
     * <p>注意: 当前实现总是返回 true,因为 LIKE 模式可以非常复杂, 仅基于 min/max 统计信息无法准确判断。
     *
     * @param type 字段的数据类型
     * @param rowCount 行数
     * @param min 最小值
     * @param max 最大值
     * @param nullCount NULL 值数量
     * @param patternLiteral LIKE 模式字面量
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
        return visitor.visitLike(fieldRef, literals.get(0));
    }
}
