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

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonValue;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * 谓词树的非叶子节点,表示由多个子谓词组成的复合谓词。
 *
 * <p>复合谓词是复合模式(Composite Pattern)中的组合节点,它的计算结果依赖于其子谓词的结果。
 * 复合谓词通过逻辑运算符(AND、OR)将多个谓词组合成更复杂的过滤条件。
 *
 * <h2>组成部分</h2>
 * 一个复合谓词由两部分组成:
 * <ul>
 *   <li><b>Function</b>: 逻辑运算函数,定义如何组合子谓词的结果
 *       <br>例如: And(所有子谓词都为 true 时返回 true)、Or(任一子谓词为 true 时返回 true)
 *   </li>
 *   <li><b>Children</b>: 子谓词列表,可以是叶子谓词或其他复合谓词
 *       <br>例如: [age > 18, status = 'ACTIVE'] 或 [(age > 18), (name = 'Alice' OR role = 'Admin')]
 *   </li>
 * </ul>
 *
 * <h2>复合谓词示例</h2>
 * <pre>{@code
 * // age > 18 AND status = 'ACTIVE'
 * new CompoundPredicate(
 *     And.INSTANCE,
 *     Arrays.asList(
 *         new LeafPredicate(GreaterThan.INSTANCE, ageType, 0, "age", Collections.singletonList(18)),
 *         new LeafPredicate(Equal.INSTANCE, stringType, 1, "status", Collections.singletonList("ACTIVE"))
 *     )
 * )
 *
 * // (age > 18 AND age < 60) OR role = 'Admin'
 * new CompoundPredicate(
 *     Or.INSTANCE,
 *     Arrays.asList(
 *         new CompoundPredicate(
 *             And.INSTANCE,
 *             Arrays.asList(
 *                 new LeafPredicate(GreaterThan.INSTANCE, ageType, 0, "age", Collections.singletonList(18)),
 *                 new LeafPredicate(LessThan.INSTANCE, ageType, 0, "age", Collections.singletonList(60))
 *             )
 *         ),
 *         new LeafPredicate(Equal.INSTANCE, stringType, 2, "role", Collections.singletonList("Admin"))
 *     )
 * )
 * }</pre>
 *
 * <h2>短路求值</h2>
 * 复合谓词支持短路求值优化:
 * <ul>
 *   <li>AND 运算:如果某个子谓词为 false,立即返回 false,不再计算后续子谓词</li>
 *   <li>OR 运算:如果某个子谓词为 true,立即返回 true,不再计算后续子谓词</li>
 * </ul>
 *
 * <h2>统计信息过滤</h2>
 * 复合谓词的统计信息测试同样支持短路求值:
 * <ul>
 *   <li>AND 运算:如果某个子谓词的统计信息测试返回 false(确定不包含满足条件的数据),
 *       整个 AND 谓词也返回 false</li>
 *   <li>OR 运算:只有当所有子谓词的统计信息测试都返回 false 时,整个 OR 谓词才返回 false</li>
 * </ul>
 *
 * <h2>谓词否定</h2>
 * 复合谓词支持否定操作,遵循德摩根定律(De Morgan's Law):
 * <ul>
 *   <li>NOT (A AND B) = (NOT A) OR (NOT B)</li>
 *   <li>NOT (A OR B) = (NOT A) AND (NOT B)</li>
 * </ul>
 * 如果任何子谓词无法否定,则整个复合谓词也无法否定。
 *
 * <h2>序列化支持</h2>
 * 该类使用 Jackson 注解支持 JSON 序列化和反序列化。Function 字段使用自定义的序列化逻辑,
 * 将函数对象序列化为简单的字符串名称(如 "And"、"Or")。
 *
 * @see LeafPredicate 叶子谓词
 * @see CompoundPredicate.Function 复合谓词函数
 * @see And AND 逻辑运算
 * @see Or OR 逻辑运算
 */
public class CompoundPredicate implements Predicate {

    /** JSON 序列化时的类型名称 */
    public static final String NAME = "COMPOUND";

    /** JSON 字段名:逻辑运算函数 */
    private static final String FIELD_FUNCTION = "function";

    /** JSON 字段名:子谓词列表 */
    private static final String FIELD_CHILDREN = "children";

    /** 逻辑运算函数,定义如何组合子谓词的结果 */
    @JsonProperty(FIELD_FUNCTION)
    private final Function function;

    /** 子谓词列表,可以是叶子谓词或其他复合谓词 */
    @JsonProperty(FIELD_CHILDREN)
    private final List<Predicate> children;

    /**
     * 构造复合谓词。
     *
     * @param function 逻辑运算函数
     * @param children 子谓词列表
     */
    @JsonCreator
    public CompoundPredicate(
            @JsonProperty(FIELD_FUNCTION) Function function,
            @JsonProperty(FIELD_CHILDREN) List<Predicate> children) {
        this.function = function;
        this.children = children;
    }

    /**
     * 获取逻辑运算函数。
     *
     * @return 逻辑运算函数
     */
    public Function function() {
        return function;
    }

    /**
     * 获取子谓词列表。
     *
     * @return 子谓词列表
     */
    public List<Predicate> children() {
        return children;
    }

    /**
     * 基于具体的数据行进行精确测试。
     *
     * <p>该方法将测试委托给 function,由 function 决定如何组合子谓词的测试结果。
     * 例如,AND 函数会要求所有子谓词都返回 true,而 OR 函数只要求任一子谓词返回 true。
     *
     * @param row 待测试的数据行
     * @return 根据逻辑运算规则组合子谓词的结果
     */
    @Override
    public boolean test(InternalRow row) {
        return function.test(row, children);
    }

    /**
     * 基于统计信息进行快速过滤测试。
     *
     * <p>该方法将统计信息测试委托给 function,由 function 决定如何组合子谓词的统计信息测试结果。
     * 例如:
     * <ul>
     *   <li>AND 函数:如果任一子谓词的统计信息测试返回 false,立即返回 false</li>
     *   <li>OR 函数:只有当所有子谓词的统计信息测试都返回 false 时才返回 false</li>
     * </ul>
     *
     * @param rowCount 数据行总数
     * @param minValues 每个字段的最小值
     * @param maxValues 每个字段的最大值
     * @param nullCounts 每个字段的空值计数
     * @return 根据逻辑运算规则组合子谓词的统计信息测试结果
     */
    @Override
    public boolean test(
            long rowCount, InternalRow minValues, InternalRow maxValues, InternalArray nullCounts) {
        return function.test(rowCount, minValues, maxValues, nullCounts, children);
    }

    /**
     * 返回该谓词的否定形式(如果可能)。
     *
     * <p>该方法将否定操作委托给 function,由 function 应用德摩根定律进行否定。
     * 例如:
     * <ul>
     *   <li>NOT (A AND B) = (NOT A) OR (NOT B)</li>
     *   <li>NOT (A OR B) = (NOT A) AND (NOT B)</li>
     * </ul>
     *
     * <p>如果任何子谓词无法否定,则整个复合谓词也无法否定,返回 Optional.empty()。
     *
     * @return 包含否定谓词的 Optional,如果无法否定则返回 Optional.empty()
     */
    @Override
    public Optional<Predicate> negate() {
        return function.negate(children);
    }

    /**
     * 访问者模式的接受方法。
     *
     * @param visitor 谓词访问者
     * @param <T> 访问结果的类型
     * @return 访问者处理的结果
     */
    @Override
    public <T> T visit(PredicateVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /**
     * 比较两个复合谓词是否相等。
     *
     * @param o 待比较的对象
     * @return 如果两个谓词的 function 和 children 都相等返回 true,否则返回 false
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CompoundPredicate)) {
            return false;
        }
        CompoundPredicate that = (CompoundPredicate) o;
        return Objects.equals(function, that.function) && Objects.equals(children, that.children);
    }

    /**
     * 计算复合谓词的哈希码。
     *
     * @return 基于 function 和 children 计算的哈希码
     */
    @Override
    public int hashCode() {
        return Objects.hash(function, children);
    }

    /**
     * 返回复合谓词的字符串表示。
     *
     * <p>格式: function([child1, child2, ...])
     * <br>例如: And([GreaterThan(age, 18), Equal(status, ACTIVE)])
     *
     * @return 谓词的字符串表示
     */
    @Override
    public String toString() {
        return function + "(" + children + ")";
    }

    /**
     * 复合谓词函数,定义如何组合多个子谓词的结果。
     *
     * <p>该抽象类是策略模式(Strategy Pattern)的应用,不同的实现类(如 And、Or)
     * 定义了不同的逻辑运算规则。
     *
     * <h2>主要实现</h2>
     * <ul>
     *   <li>{@link And}: AND 逻辑,所有子谓词都为 true 时返回 true,支持短路求值</li>
     *   <li>{@link Or}: OR 逻辑,任一子谓词为 true 时返回 true,支持短路求值</li>
     * </ul>
     *
     * <h2>序列化支持</h2>
     * 该类使用自定义的 JSON 序列化逻辑:
     * <ul>
     *   <li>序列化:将函数对象转换为简单的字符串名称(如 "And"、"Or")</li>
     *   <li>反序列化:从字符串名称恢复为函数单例对象</li>
     * </ul>
     *
     * <h2>设计考虑</h2>
     * 所有函数实现类应该是无状态的单例,以减少内存开销。相同类型的函数实例应该是相等的,
     * 因此 equals 和 hashCode 方法基于类型而不是实例进行比较。
     */
    public abstract static class Function implements Serializable {
        /**
         * 从 JSON 字符串名称反序列化为函数对象。
         *
         * @param name 函数名称(如 "And"、"Or")
         * @return 对应的函数单例对象
         * @throws IOException 如果函数名称未知
         */
        @JsonCreator
        public static Function fromJson(String name) throws IOException {
            switch (name) {
                case And.NAME:
                    return And.INSTANCE;
                case Or.NAME:
                    return Or.INSTANCE;
                default:
                    throw new IllegalArgumentException(
                            "Could not resolve compound predicate function '" + name + "'");
            }
        }

        /**
         * 将函数对象序列化为 JSON 字符串名称。
         *
         * @return 函数名称(如 "And"、"Or")
         * @throws IllegalArgumentException 如果函数类型未知
         */
        @JsonValue
        public String toJson() {
            if (this instanceof And) {
                return And.NAME;
            } else if (this instanceof Or) {
                return Or.NAME;
            } else {
                throw new IllegalArgumentException(
                        "Unknown compound predicate function class for JSON serialization: "
                                + getClass());
            }
        }

        /**
         * 基于具体的数据行测试复合谓词。
         *
         * <p>该方法定义如何组合子谓词的测试结果。实现类应该支持短路求值以提高性能。
         *
         * @param row 待测试的数据行
         * @param children 子谓词列表
         * @return 组合后的测试结果
         */
        public abstract boolean test(InternalRow row, List<Predicate> children);

        /**
         * 基于统计信息测试复合谓词。
         *
         * <p>该方法定义如何组合子谓词的统计信息测试结果。实现类应该支持短路求值以提高性能。
         *
         * @param rowCount 数据行总数
         * @param minValues 每个字段的最小值
         * @param maxValues 每个字段的最大值
         * @param nullCounts 每个字段的空值计数
         * @param children 子谓词列表
         * @return 组合后的统计信息测试结果
         */
        public abstract boolean test(
                long rowCount,
                InternalRow minValues,
                InternalRow maxValues,
                InternalArray nullCounts,
                List<Predicate> children);

        /**
         * 否定复合谓词。
         *
         * <p>该方法应用德摩根定律对复合谓词进行否定:
         * <ul>
         *   <li>NOT (A AND B) = (NOT A) OR (NOT B)</li>
         *   <li>NOT (A OR B) = (NOT A) AND (NOT B)</li>
         * </ul>
         *
         * @param children 子谓词列表
         * @return 包含否定谓词的 Optional,如果任何子谓词无法否定则返回 Optional.empty()
         */
        public abstract Optional<Predicate> negate(List<Predicate> children);

        /**
         * 访问者模式的接受方法。
         *
         * @param visitor 函数访问者
         * @param children 处理后的子元素列表
         * @param <T> 访问结果的类型
         * @return 访问者处理的结果
         */
        public abstract <T> T visit(FunctionVisitor<T> visitor, List<T> children);

        /**
         * 计算函数的哈希码。
         *
         * <p>基于类名计算哈希码,确保相同类型的函数实例具有相同的哈希码。
         *
         * @return 基于类名的哈希码
         */
        @Override
        public int hashCode() {
            return this.getClass().getName().hashCode();
        }

        /**
         * 比较两个函数是否相等。
         *
         * <p>基于类型进行比较,相同类型的函数实例被视为相等。
         *
         * @param o 待比较的对象
         * @return 如果两个函数类型相同返回 true,否则返回 false
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return o != null && getClass() == o.getClass();
        }

        /**
         * 返回函数的字符串表示。
         *
         * @return 函数的简单类名
         */
        @Override
        public String toString() {
            return getClass().getSimpleName();
        }
    }
}
