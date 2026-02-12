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
 * OR 逻辑运算函数,实现复合谓词的"或"操作。
 *
 * <p>该类是 {@link CompoundPredicate.Function} 的实现,用于组合多个子谓词,
 * 只要任一子谓词返回 true,OR 运算就返回 true。
 *
 * <h2>短路求值优化</h2>
 * OR 运算实现了短路求值(Short-Circuit Evaluation)优化:
 * <ul>
 *   <li><b>精确测试</b>:从左到右依次测试每个子谓词,一旦发现某个子谓词返回 true,
 *       立即返回 true,不再测试后续子谓词</li>
 *   <li><b>统计信息测试</b>:从左到右依次测试每个子谓词的统计信息,只有当所有子谓词
 *       的统计信息测试都返回 false(确定不包含满足条件的数据)时,才返回 false</li>
 * </ul>
 *
 * <p>短路求值可以显著提高性能,特别是当:
 * <ul>
 *   <li>子谓词按选择性从低到高排序时(选择性低的谓词更容易返回 true)</li>
 *   <li>某些子谓词的计算成本较高时(将低成本谓词放在前面)</li>
 * </ul>
 *
 * <h2>真值表</h2>
 * <pre>
 * A     | B     | A OR B
 * ------|-------|--------
 * true  | true  | true
 * true  | false | true
 * false | true  | true
 * false | false | false
 * </pre>
 *
 * <h2>德摩根定律</h2>
 * OR 谓词的否定遵循德摩根定律:
 * <pre>
 * NOT (A OR B) = (NOT A) AND (NOT B)
 * </pre>
 * 例如:
 * <pre>{@code
 * // 原谓词: (age < 18) OR (age > 60)
 * // 否定后: (age >= 18) AND (age <= 60)
 * }</pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // age < 18 OR age > 60
 * Predicate orPredicate = new CompoundPredicate(
 *     Or.INSTANCE,
 *     Arrays.asList(
 *         lessThan("age", 18),
 *         greaterThan("age", 60)
 *     )
 * );
 *
 * // 嵌套 OR: (age < 18 OR age > 60) OR (status = 'VIP')
 * Predicate nestedOr = new CompoundPredicate(
 *     Or.INSTANCE,
 *     Arrays.asList(
 *         new CompoundPredicate(
 *             Or.INSTANCE,
 *             Arrays.asList(lessThan("age", 18), greaterThan("age", 60))
 *         ),
 *         equal("status", "VIP")
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
 * // 查询 1: age < 20 OR age > 50
 * // 第一个子谓词 (age < 20) 的统计信息测试:
 * //   min(age)=25 >= 20,返回 false
 * // 第二个子谓词 (age > 50) 的统计信息测试:
 * //   max(age)=45 <= 50,返回 false
 * // 所有子谓词都返回 false,OR 返回 false,跳过该数据块
 *
 * // 查询 2: age < 30 OR status = 'VIP'
 * // 第一个子谓词 (age < 30) 的统计信息测试:
 * //   min(age)=25 < 30,返回 true(可能有 age < 30 的数据)
 * // OR 短路,直接返回 true,需要读取该数据块
 * }</pre>
 *
 * <h2>设计模式</h2>
 * <ul>
 *   <li><b>单例模式</b>:使用 INSTANCE 单例,避免重复创建相同的函数对象</li>
 *   <li><b>策略模式</b>:作为 CompoundPredicate.Function 的一个策略实现</li>
 * </ul>
 *
 * @see CompoundPredicate 复合谓词
 * @see And AND 逻辑运算
 * @see CompoundPredicate.Function 复合谓词函数基类
 */
public class Or extends CompoundPredicate.Function {

    /** 序列化版本号 */
    private static final long serialVersionUID = 1L;

    /** JSON 序列化时的函数名称 */
    public static final String NAME = "OR";

    /** OR 函数的单例实例,避免重复创建对象 */
    public static final Or INSTANCE = new Or();

    /**
     * 私有构造函数,用于 JSON 反序列化。
     *
     * <p>该构造函数标注了 @JsonCreator,允许 Jackson 在反序列化时创建实例。
     * 由于 OR 函数是无状态的,因此使用单例模式,不应该通过 new 创建新实例。
     */
    @JsonCreator
    private Or() {}

    /**
     * 基于具体数据行进行精确测试。
     *
     * <p>该方法实现 OR 逻辑的精确测试:遍历所有子谓词,只要任一子谓词返回 true,
     * OR 运算就返回 true。
     *
     * <h3>短路求值优化</h3>
     * 该方法实现了短路求值:从左到右依次测试每个子谓词,一旦发现某个子谓词返回 true,
     * 立即返回 true,不再测试后续子谓词。
     *
     * <h3>示例</h3>
     * <pre>{@code
     * // 假设有数据行: {age: 25, status: 'INACTIVE', role: 'USER'}
     * // 查询: age < 18 OR status = 'ACTIVE' OR role = 'ADMIN'
     *
     * // 测试过程:
     * // 1. 测试 age < 18: 25 < 18 = false,继续测试
     * // 2. 测试 status = 'ACTIVE': 'INACTIVE' = 'ACTIVE' = false,继续测试
     * // 3. 测试 role = 'ADMIN': 'USER' = 'ADMIN' = false
     * // 结果: false
     *
     * // 假设有数据行: {age: 15, status: 'INACTIVE', role: 'USER'}
     * // 查询: age < 18 OR status = 'ACTIVE' OR role = 'ADMIN'
     *
     * // 测试过程:
     * // 1. 测试 age < 18: 15 < 18 = true,短路,返回 true
     * // 结果: true(不再测试后续子谓词)
     * }</pre>
     *
     * @param row 待测试的数据行
     * @param children 子谓词列表
     * @return 任一子谓词返回 true 时返回 true,所有子谓词都返回 false 时返回 false
     */
    @Override
    public boolean test(InternalRow row, List<Predicate> children) {
        for (Predicate child : children) {
            if (child.test(row)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 基于统计信息进行快速过滤测试。
     *
     * <p>该方法实现 OR 逻辑的统计信息测试:遍历所有子谓词,只要任一子谓词的统计信息测试
     * 返回 true,OR 运算就返回 true。
     *
     * <h3>短路求值优化</h3>
     * 该方法实现了短路求值:从左到右依次测试每个子谓词的统计信息,一旦发现某个子谓词的
     * 统计信息测试返回 true(可能包含满足条件的数据),立即返回 true,不再测试后续子谓词。
     *
     * <h3>过滤语义</h3>
     * <ul>
     *   <li><b>返回 true</b>:表示该数据块可能包含满足 OR 条件的数据,需要读取并精确过滤</li>
     *   <li><b>返回 false</b>:表示该数据块确定不包含满足 OR 条件的数据,可以安全跳过
     *       (所有子谓词的统计信息测试都返回 false)</li>
     * </ul>
     *
     * <h3>示例</h3>
     * <pre>{@code
     * // 假设某个数据块的统计信息:
     * // age: min=25, max=45, nullCount=0, rowCount=100
     * // status: min='ACTIVE', max='INACTIVE', nullCount=5, rowCount=100
     *
     * // 查询 1: age < 20 OR age > 50
     * // 测试过程:
     * // 1. 测试 age < 20 的统计信息:
     * //    min(age)=25 >= 20,返回 false
     * // 2. 测试 age > 50 的统计信息:
     * //    max(age)=45 <= 50,返回 false
     * // 3. 所有子谓词都返回 false,OR 返回 false,跳过该数据块
     *
     * // 查询 2: age < 30 OR status = 'VIP'
     * // 测试过程:
     * // 1. 测试 age < 30 的统计信息:
     * //    min(age)=25 < 30,返回 true(可能有 age < 30 的数据)
     * // 2. 短路,直接返回 true,需要读取该数据块
     *
     * // 查询 3: age < 20 OR status = 'ACTIVE'
     * // 测试过程:
     * // 1. 测试 age < 20 的统计信息:
     * //    min(age)=25 >= 20,返回 false
     * // 2. 测试 status = 'ACTIVE' 的统计信息:
     * //    'ACTIVE' 在 [ACTIVE, INACTIVE] 范围内,返回 true
     * // 3. 返回 true,需要读取该数据块
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
            if (child.test(rowCount, minValues, maxValues, nullCounts)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 否定 OR 谓词,应用德摩根定律。
     *
     * <p>该方法实现德摩根定律(De Morgan's Law):
     * <pre>
     * NOT (A OR B) = (NOT A) AND (NOT B)
     * </pre>
     *
     * <p>否定过程:
     * <ol>
     *   <li>遍历所有子谓词,对每个子谓词进行否定</li>
     *   <li>如果所有子谓词都能成功否定,将否定后的子谓词用 AND 连接</li>
     *   <li>如果任何子谓词无法否定,返回 Optional.empty()</li>
     * </ol>
     *
     * <h3>示例</h3>
     * <pre>{@code
     * // 原谓词: (age < 18) OR (age > 60)
     * // 否定步骤:
     * // 1. 否定 age < 18 得到 age >= 18
     * // 2. 否定 age > 60 得到 age <= 60
     * // 3. 用 AND 连接: (age >= 18) AND (age <= 60)
     *
     * // 嵌套 OR: (age < 18 OR age > 60) OR role = 'VIP'
     * // 否定步骤:
     * // 1. 否定 (age < 18 OR age > 60):
     * //    = (age >= 18) AND (age <= 60)
     * // 2. 否定 role = 'VIP':
     * //    = role != 'VIP'
     * // 3. 用 AND 连接:
     * //    = (age >= 18 AND age <= 60) AND (role != 'VIP')
     * //    = (age >= 18) AND (age <= 60) AND (role != 'VIP')
     * }</pre>
     *
     * @param children 子谓词列表
     * @return 包含否定后谓词(AND 连接)的 Optional,如果任何子谓词无法否定则返回 Optional.empty()
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
        return Optional.of(new CompoundPredicate(And.INSTANCE, negatedChildren));
    }

    /**
     * 访问者模式的接受方法。
     *
     * <p>该方法将 OR 运算的处理委托给访问者,允许访问者以自定义的方式处理 OR 逻辑。
     *
     * @param visitor 函数访问者
     * @param children 处理后的子元素列表
     * @param <T> 访问结果的类型
     * @return 访问者处理 OR 运算的结果
     */
    @Override
    public <T> T visit(FunctionVisitor<T> visitor, List<T> children) {
        return visitor.visitOr(children);
    }
}
