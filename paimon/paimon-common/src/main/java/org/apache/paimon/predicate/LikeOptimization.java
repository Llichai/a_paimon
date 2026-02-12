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
import org.apache.paimon.utils.Pair;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.paimon.data.BinaryString.fromString;

/**
 * LIKE 模式优化器。
 *
 * <p>该类负责将简单的 SQL LIKE 模式转换为更高效的专用操作,避免正则表达式的性能开销。
 *
 * <h2>优化策略</h2>
 * <p>LIKE 模式可以被优化为以下几种情况:
 * <table border="1">
 *   <tr>
 *     <th>LIKE 模式</th>
 *     <th>优化为</th>
 *     <th>示例</th>
 *   </tr>
 *   <tr>
 *     <td>无通配符</td>
 *     <td>{@link Equal}</td>
 *     <td>"abc" -> Equal("abc")</td>
 *   </tr>
 *   <tr>
 *     <td>前缀匹配</td>
 *     <td>{@link StartsWith}</td>
 *     <td>"abc%" -> StartsWith("abc")</td>
 *   </tr>
 *   <tr>
 *     <td>后缀匹配</td>
 *     <td>{@link EndsWith}</td>
 *     <td>"%abc" -> EndsWith("abc")</td>
 *   </tr>
 *   <tr>
 *     <td>子串匹配</td>
 *     <td>{@link Contains}</td>
 *     <td>"%abc%" -> Contains("abc")</td>
 *   </tr>
 * </table>
 *
 * <h2>优化条件</h2>
 * <ul>
 *   <li>不包含 '_' 通配符: 只有 '%' 的模式才能被优化
 *   <li>简单模式: 只有符合特定格式的模式才能被优化
 *   <li>非空模式: 模式不能为 null
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 在 PredicateBuilder 中的使用
 * public Predicate like(int idx, Object patternLiteral) {
 *     // 尝试优化
 *     Pair<NullFalseLeafBinaryFunction, Object> optimized =
 *         LikeOptimization.tryOptimize(patternLiteral)
 *             .orElse(Pair.of(Like.INSTANCE, patternLiteral));
 *     return leaf(optimized.getKey(), idx, optimized.getValue());
 * }
 *
 * // 优化示例
 * tryOptimize("abc%")    // -> (StartsWith, "abc")
 * tryOptimize("%abc")    // -> (EndsWith, "abc")
 * tryOptimize("%abc%")   // -> (Contains, "abc")
 * tryOptimize("abc")     // -> (Equal, "abc")
 * tryOptimize("a%b")     // -> empty (无法优化)
 * tryOptimize("a_c")     // -> empty (包含 '_')
 * }</pre>
 *
 * <h2>性能影响</h2>
 * <ul>
 *   <li>避免正则表达式: 专用函数比正则表达式快 10-100 倍
 *   <li>字符串直接比较: 使用 BinaryString 的原生方法
 *   <li>减少内存分配: 避免创建 Pattern 对象
 *   <li>缓存友好: 简化的操作更容易被缓存
 * </ul>
 *
 * <h2>实现细节</h2>
 * <ul>
 *   <li>正则匹配器: 使用预编译的 Pattern 来识别可优化的模式
 *   <li>模式优先级: 按照 无通配符 -> 前缀 -> 后缀 -> 子串 的顺序尝试匹配
 *   <li>线程安全: 所有方法都是静态的,Pattern 是线程安全的
 * </ul>
 *
 * @see Like LIKE 模式匹配的完整实现
 * @see StartsWith 字符串开头匹配
 * @see EndsWith 字符串结尾匹配
 * @see Contains 字符串包含匹配
 * @see Equal 完全相等匹配
 */
public class LikeOptimization {

    /** 匹配前缀模式: "abc%" */
    private static final Pattern BEGIN_PATTERN = Pattern.compile("([^%]+)%");

    /** 匹配后缀模式: "%abc" */
    private static final Pattern END_PATTERN = Pattern.compile("%([^%]+)");

    /** 匹配子串模式: "%abc%" */
    private static final Pattern MIDDLE_PATTERN = Pattern.compile("%([^%]+)%");

    /** 匹配完全相等模式: "abc" (无通配符) */
    private static final Pattern NONE_PATTERN = Pattern.compile("[^%]+");

    /**
     * 尝试将 LIKE 模式优化为更高效的操作。
     *
     * <p>优化流程:
     * <ol>
     *   <li>NULL 检查: 模式不能为 null
     *   <li>下划线检查: 包含 '_' 通配符无法优化
     *   <li>按优先级匹配: 无通配符 -> 前缀 -> 后缀 -> 子串
     *   <li>返回优化结果: 包含优化后的函数和提取的字面量
     * </ol>
     *
     * @param patternLiteral LIKE 模式字面量
     * @return 优化结果,包含 (函数, 字面量) 对;如果无法优化则返回 empty
     * @throws IllegalArgumentException 如果模式为 null
     */
    public static Optional<Pair<NullFalseLeafBinaryFunction, Object>> tryOptimize(
            Object patternLiteral) {
        if (patternLiteral == null) {
            throw new IllegalArgumentException("Pattern can not be null.");
        }

        String pattern = patternLiteral.toString();
        if (pattern.contains("_")) {
            return Optional.empty();
        }

        Matcher noneMatcher = NONE_PATTERN.matcher(pattern);
        Matcher beginMatcher = BEGIN_PATTERN.matcher(pattern);
        Matcher endMatcher = END_PATTERN.matcher(pattern);
        Matcher middleMatcher = MIDDLE_PATTERN.matcher(pattern);

        if (noneMatcher.matches()) {
            // "abc" -> Equal("abc")
            BinaryString equals = fromString(pattern);
            return Optional.of(Pair.of(Equal.INSTANCE, equals));
        } else if (beginMatcher.matches()) {
            // "abc%" -> StartsWith("abc")
            BinaryString begin = fromString(beginMatcher.group(1));
            return Optional.of(Pair.of(StartsWith.INSTANCE, begin));
        } else if (endMatcher.matches()) {
            // "%abc" -> EndsWith("abc")
            BinaryString end = fromString(endMatcher.group(1));
            return Optional.of(Pair.of(EndsWith.INSTANCE, end));
        } else if (middleMatcher.matches()) {
            // "%abc%" -> Contains("abc")
            BinaryString middle = fromString(middleMatcher.group(1));
            return Optional.of(Pair.of(Contains.INSTANCE, middle));
        } else {
            // 无法优化,需要使用正则表达式
            return Optional.empty();
        }
    }
}
