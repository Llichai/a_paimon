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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * AND 逻辑运算函数,实现复合谓词的"与"操作。
 *
 * <p>该类是 {@link CompoundPredicate.Function} 的实现,用于组合多个子谓词,
 * 只有当所有子谓词都返回 true 时,AND 运算才返回 true。
 *
 * <h2>短路求值优化</h2>
 * AND 运算实现了短路求值(Short-Circuit Evaluation)优化:
 * <ul>
 *   <li><b>精确测试</b>:从左到右依次测试每个子谓词,一旦发现某个子谓词返回 false,
 *       立即返回 false,不再测试后续子谓词</li>
 *   <li><b>统计信息测试</b>:从左到右依次测试每个子谓词的统计信息,一旦发现某个子谓词
 *       的统计信息测试返回 false(确定不包含满足条件的数据),立即返回 false</li>
 * </ul>
 *
 * <p>短路求值可以显著提高性能,特别是当:
 * <ul>
 *   <li>子谓词按选择性从高到低排序时(选择性高的谓词更容易返回 false)</li>
 *   <li>某些子谓词的计算成本较高时(将低成本谓词放在前面)</li>
 * </ul>
 *
 * <h2>真值表</h2>
 * <pre>
 * A     | B     | A AND B
 * ------|-------|--------
 * true  | true  | true
 * true  | false | false
 * false | true  | false
 * false | false | false
 * </pre>
 *
 * <h2>德摩根定律</h2>
 * AND 谓词的否定遵循德摩根定律:
 * <pre>
 * NOT (A AND B) = (NOT A) OR (NOT B)
 * </pre>
 * 例如:
 * <pre>{@code
 * // 原谓词: (age > 18) AND (status = 'ACTIVE')
 * // 否定后: (age <= 18) OR (status != 'ACTIVE')
 * }</pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // age > 18 AND status = 'ACTIVE'
 * Predicate andPredicate = new CompoundPredicate(
 *     And.INSTANCE,
 *     Arrays.asList(
 *         greaterThan("age", 18),
 *         equal("status", "ACTIVE")
 *     )
 * );
 *
 * // 嵌套 AND: (age > 18 AND age < 60) AND (status = 'ACTIVE')
 * Predicate nestedAnd = new CompoundPredicate(
 *     And.INSTANCE,
 *     Arrays.asList(
 *         new CompoundPredicate(
 *             And.INSTANCE,
 *             Arrays.asList(greaterThan("age", 18), lessThan("age", 60))
 *         ),
 *         equal("status", "ACTIVE")
 *     )
 * );
 * }</pre>
 *
 * <h2>统计信息过滤示例</h2>
 * <pre>{@code
 * // 假设某个数据块的统计信息:
 * // age: min=25, max=45, nullCount=0
 * // status: min='ACTIVE', max='INACTIVE', nullCount=0
 *
 * // 查询: age > 50 AND status = 'ACTIVE'
 * // 第一个子谓词 (age > 50) 的统计信息测试:
 * //   max(age)=45 < 50,返回 false
 * // AND 短路,直接返回 false,跳过该数据块
 *
 * // 查询: age > 20 AND status = 'PENDING'
 * // 第一个子谓词 (age > 20) 的统计信息测试:
 * //   max(age)=45 > 20,返回 true,继续测试第二个子谓词
 * // 第二个子谓词 (status = 'PENDING') 的统计信息测试:
 * //   'PENDING' 不在 ['ACTIVE', 'INACTIVE'] 范围内,返回 false
 * // AND 返回 false,跳过该数据块
 * }</pre>
 *
 * <h2>设计模式</h2>
 * <ul>
 *   <li><b>单例模式</b>:使用 INSTANCE 单例,避免重复创建相同的函数对象</li>
 *   <li><b>策略模式</b>:作为 CompoundPredicate.Function 的一个策略实现</li>
 * </ul>
 *
 * @see CompoundPredicate 复合谓词
 * @see Or OR 逻辑运算
 * @see CompoundPredicate.Function 复合谓词函数基类
 */
public class And extends CompoundPredicate.Function {

    /** 序列化版本号 */
    private static final long serialVersionUID = 1L;

    /** JSON 序列化时的函数名称 */
    public static final String NAME = "AND";

    /** AND 函数的单例实例,避免重复创建对象 */
    public static final And INSTANCE = new And();

    /**
     * 私有构造函数,用于 JSON 反序列化。
     *
     * <p>该构造函数标注了 @JsonCreator,允许 Jackson 在反序列化时创建实例。
     * 由于 AND 函数是无状态的,因此使用单例模式,不应该通过 new 创建新实例。
     */
    @JsonCreator
    private And() {}

    /**
     * 基于具体数据行进行精确测试。
     *
     * <p>该方法实现 AND 逻辑的精确测试:遍历所有子谓词,只有当所有子谓词都返回 true 时,
     * AND 运算才返回 true。
     *
     * <h3>短路求值优化</h3>
     * 该方法实现了短路求值:从左到右依次测试每个子谓词,一旦发现某个子谓词返回 false,
     * 立即返回 false,不再测试后续子谓词。这可以显著提高性能,特别是当:
     * <ul>
     *   <li>子谓词按选择性从高到低排序时</li>
     *   <li>某些子谓词的计算成本较高时</li>
     * </ul>
     *
     * <h3>示例</h3>
     * <pre>{@code
     * // 假设有数据行: {age: 25, status: 'ACTIVE', role: 'USER'}
     * // 查询: age > 18 AND status = 'ACTIVE' AND role = 'ADMIN'
     *
     * // 测试过程:
     * // 1. 测试 age > 18: 25 > 18 = true,继续测试
     * // 2. 测试 status = 'ACTIVE': 'ACTIVE' = 'ACTIVE' = true,继续测试
     * // 3. 测试 role = 'ADMIN': 'USER' = 'ADMIN' = false,短路,返回 false
     * // 结果: false
     * }</pre>
     *
     * @param row 待测试的数据行
     * @param children 子谓词列表
     * @return 所有子谓词都返回 true 时返回 true,否则返回 false
     */
    @Override
    public boolean test(InternalRow row, List<Predicate> children) {
        for (Predicate child : children) {
            if (!child.test(row)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 基于统计信息进行快速过滤测试。
     *
     * <p>该方法实现 AND 逻辑的统计信息测试:遍历所有子谓词,只有当所有子谓词的统计信息测试
     * 都返回 true 时,AND 运算才返回 true。
     *
     * <h3>短路求值优化</h3>
     * 该方法实现了短路求值:从左到右依次测试每个子谓词的统计信息,一旦发现某个子谓词的
     * 统计信息测试返回 false(确定不包含满足条件的数据),立即返回 false,不再测试后续子谓词。
     *
     * <h3>过滤语义</h3>
     * <ul>
     *   <li><b>返回 true</b>:表示该数据块可能包含满足 AND 条件的数据,需要读取并精确过滤</li>
     *   <li><b>返回 false</b>:表示该数据块确定不包含满足 AND 条件的数据,可以安全跳过</li>
     * </ul>
     *
     * <h3>示例</h3>
     * <pre>{@code
     * // 假设某个数据块的统计信息:
     * // age: min=25, max=45, nullCount=0, rowCount=100
     * // status: min='ACTIVE', max='INACTIVE', nullCount=5, rowCount=100
     *
     * // 查询 1: age > 50 AND status = 'ACTIVE'
     * // 测试过程:
     * // 1. 测试 age > 50 的统计信息:
     * //    max(age)=45 <= 50,返回 false(确定没有 age > 50 的数据)
     * // 2. 短路,直接返回 false,跳过该数据块
     *
     * // 查询 2: age > 20 AND status = 'PENDING'
     * // 测试过程:
     * // 1. 测试 age > 20 的统计信息:
     * //    max(age)=45 > 20,返回 true(可能有 age > 20 的数据)
     * // 2. 测试 status = 'PENDING' 的统计信息:
     * //    'PENDING' 不在 [ACTIVE, INACTIVE] 范围内,返回 false
     * // 3. 返回 false,跳过该数据块
     *
     * // 查询 3: age > 20 AND status = 'ACTIVE'
     * // 测试过程:
     * // 1. 测试 age > 20 的统计信息:
     * //    max(age)=45 > 20,返回 true
     * // 2. 测试 status = 'ACTIVE' 的统计信息:
     * //    'ACTIVE' 在 [ACTIVE, INACTIVE] 范围内,返回 true
     * // 3. 返回 true,需要读取该数据块并精确过滤
     * }</pre>
     *
     * @param rowCount 数据行总数
     * @param minValues 每个字段的最小值行
     * @param maxValues 每个字段的最大值行
     * @param nullCounts 每个字段的空值计数数组
     * @param children 子谓词列表
     * @return true 表示可能包含满足条件的数据;false 表示确定不包含满足条件的数据
     */
    @Override
    public boolean test(
            long rowCount,
            InternalRow minValues,
            InternalRow maxValues,
            InternalArray nullCounts,
            List<Predicate> children) {
        for (Predicate child : children) {
            if (!child.test(rowCount, minValues, maxValues, nullCounts)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 否定 AND 谓词,应用德摩根定律。
     *
     * <p>该方法实现德摩根定律(De Morgan's Law):
     * <pre>
     * NOT (A AND B) = (NOT A) OR (NOT B)
     * </pre>
     *
     * <p>否定过程:
     * <ol>
     *   <li>遍历所有子谓词,对每个子谓词进行否定</li>
     *   <li>如果所有子谓词都能成功否定,将否定后的子谓词用 OR 连接</li>
     *   <li>如果任何子谓词无法否定,返回 Optional.empty()</li>
     * </ol>
     *
     * <h3>示例</h3>
     * <pre>{@code
     * // 原谓词: (age > 18) AND (status = 'ACTIVE')
     * // 否定步骤:
     * // 1. 否定 age > 18 得到 age <= 18
     * // 2. 否定 status = 'ACTIVE' 得到 status != 'ACTIVE'
     * // 3. 用 OR 连接: (age <= 18) OR (status != 'ACTIVE')
     *
     * // 嵌套 AND: (age > 18 AND age < 60) AND role = 'ADMIN'
     * // 否定步骤:
     * // 1. 否定 (age > 18 AND age < 60):
     * //    = (age <= 18) OR (age >= 60)
     * // 2. 否定 role = 'ADMIN':
     * //    = role != 'ADMIN'
     * // 3. 用 OR 连接:
     * //    = (age <= 18 OR age >= 60) OR (role != 'ADMIN')
     * //    = (age <= 18) OR (age >= 60) OR (role != 'ADMIN')
     * }</pre>
     *
     * @param children 子谓词列表
     * @return 包含否定后谓词(OR 连接)的 Optional,如果任何子谓词无法否定则返回 Optional.empty()
     */
    @Override
    public Optional<Predicate> negate(List<Predicate> children) {
        List<Predicate> negatedChildren = new ArrayList<>();
        for (Predicate child : children) {
            Optional<Predicate> negatedChild = child.negate();
            if (negatedChild.isPresent()) {
                negatedChildren.add(negatedChild.get());
            } else {
                return Optional.empty();
            }
        }
        return Optional.of(new CompoundPredicate(Or.INSTANCE, negatedChildren));
    }

    /**
     * 访问者模式的接受方法。
     *
     * <p>该方法将 AND 运算的处理委托给访问者,允许访问者以自定义的方式处理 AND 逻辑。
     *
     * @param visitor 函数访问者
     * @param children 处理后的子元素列表
     * @param <T> 访问结果的类型
     * @return 访问者处理 AND 运算的结果
     */
    @Override
    public <T> T visit(FunctionVisitor<T> visitor, List<T> children) {
        return visitor.visitAnd(children);
    }
}
