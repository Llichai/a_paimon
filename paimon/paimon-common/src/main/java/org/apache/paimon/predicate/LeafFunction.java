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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonValue;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 * 叶子谓词函数,用于测试字段与常量值的关系。
 *
 * <p>该抽象类是策略模式(Strategy Pattern)的应用,定义了叶子谓词的比较逻辑接口。
 * 不同的实现类提供了不同的比较操作,如相等、大于、小于、包含、为空等。
 *
 * <h2>主要实现类</h2>
 * <ul>
 *   <li><b>比较操作</b>: {@link Equal}, {@link NotEqual}, {@link GreaterThan}, {@link LessThan},
 *       {@link GreaterOrEqual}, {@link LessOrEqual}</li>
 *   <li><b>NULL 检查</b>: {@link IsNull}, {@link IsNotNull}</li>
 *   <li><b>字符串操作</b>: {@link StartsWith}, {@link EndsWith}, {@link Contains}, {@link Like}</li>
 *   <li><b>集合操作</b>: {@link In}, {@link NotIn}</li>
 * </ul>
 *
 * <h2>测试方法</h2>
 * 该类提供两种测试方法:
 * <ul>
 *   <li><b>精确测试</b>: {@link #test(DataType, Object, List)} - 基于实际字段值进行测试</li>
 *   <li><b>统计信息测试</b>: {@link #test(DataType, long, Object, Object, Long, List)} -
 *       基于字段的统计信息(最小值、最大值、空值计数)进行快速过滤</li>
 * </ul>
 *
 * <h2>序列化支持</h2>
 * 该类使用自定义的 JSON 序列化逻辑:
 * <ul>
 *   <li>序列化:将函数对象转换为简单的字符串名称(如 "Equal"、"GreaterThan")</li>
 *   <li>反序列化:从字符串名称恢复为函数单例对象</li>
 * </ul>
 *
 * <h2>设计考虑</h2>
 * <ul>
 *   <li>所有函数实现类应该是无状态的单例,以减少内存开销</li>
 *   <li>相同类型的函数实例应该是相等的,因此 equals 和 hashCode 方法基于类型而不是实例进行比较</li>
 *   <li>大多数函数支持否定操作,通过 {@link #negate()} 方法返回对应的否定函数</li>
 * </ul>
 *
 * <h2>子类继承</h2>
 * 该类有两个主要的抽象子类:
 * <ul>
 *   <li>{@link LeafUnaryFunction}: 一元函数,不需要常量值(如 IS NULL)</li>
 *   <li>{@link NullFalseLeafBinaryFunction}: 二元函数,需要一个常量值,NULL 值返回 false</li>
 * </ul>
 *
 * @see LeafPredicate 使用该函数的叶子谓词
 * @see LeafUnaryFunction 一元叶子函数
 * @see NullFalseLeafBinaryFunction 二元叶子函数(NULL 返回 false)
 */
public abstract class LeafFunction implements Serializable {

    /**
     * 从 JSON 字符串名称反序列化为函数对象。
     *
     * @param name 函数名称(如 "Equal"、"GreaterThan"、"IsNull" 等)
     * @return 对应的函数单例对象
     * @throws IOException 如果函数名称未知
     */
    @JsonCreator
    public static LeafFunction fromJson(String name) throws IOException {
        switch (name) {
            case Equal.NAME:
                return Equal.INSTANCE;
            case NotEqual.NAME:
                return NotEqual.INSTANCE;
            case LessThan.NAME:
                return LessThan.INSTANCE;
            case LessOrEqual.NAME:
                return LessOrEqual.INSTANCE;
            case GreaterThan.NAME:
                return GreaterThan.INSTANCE;
            case GreaterOrEqual.NAME:
                return GreaterOrEqual.INSTANCE;
            case IsNull.NAME:
                return IsNull.INSTANCE;
            case IsNotNull.NAME:
                return IsNotNull.INSTANCE;
            case StartsWith.NAME:
                return StartsWith.INSTANCE;
            case EndsWith.NAME:
                return EndsWith.INSTANCE;
            case Contains.NAME:
                return Contains.INSTANCE;
            case Like.NAME:
                return Like.INSTANCE;
            case In.NAME:
                return In.INSTANCE;
            case NotIn.NAME:
                return NotIn.INSTANCE;
            default:
                throw new IllegalArgumentException(
                        "Could not resolve leaf function '" + name + "'");
        }
    }

    /**
     * 将函数对象序列化为 JSON 字符串名称。
     *
     * @return 函数名称(如 "Equal"、"GreaterThan"、"IsNull" 等)
     * @throws IllegalArgumentException 如果函数类型未知
     */
    @JsonValue
    public String toJson() {
        if (this instanceof Equal) {
            return Equal.NAME;
        } else if (this instanceof NotEqual) {
            return NotEqual.NAME;
        } else if (this instanceof LessThan) {
            return LessThan.NAME;
        } else if (this instanceof LessOrEqual) {
            return LessOrEqual.NAME;
        } else if (this instanceof GreaterThan) {
            return GreaterThan.NAME;
        } else if (this instanceof GreaterOrEqual) {
            return GreaterOrEqual.NAME;
        } else if (this instanceof IsNull) {
            return IsNull.NAME;
        } else if (this instanceof IsNotNull) {
            return IsNotNull.NAME;
        } else if (this instanceof StartsWith) {
            return StartsWith.NAME;
        } else if (this instanceof EndsWith) {
            return EndsWith.NAME;
        } else if (this instanceof Contains) {
            return Contains.NAME;
        } else if (this instanceof Like) {
            return Like.NAME;
        } else if (this instanceof In) {
            return In.NAME;
        } else if (this instanceof NotIn) {
            return NotIn.NAME;
        } else {
            throw new IllegalArgumentException(
                    "Unknown leaf function class for JSON serialization: " + getClass());
        }
    }

    /**
     * 基于实际字段值进行精确测试。
     *
     * <p>该方法将字段值与常量值列表进行比较,返回比较结果。
     * 不同的函数实现类提供不同的比较逻辑。
     *
     * @param type 字段的数据类型
     * @param field 字段值(可能为 null)
     * @param literals 用于比较的常量值列表
     * @return 如果字段值满足比较条件返回 true,否则返回 false
     */
    public abstract boolean test(DataType type, Object field, List<Object> literals);

    /**
     * 基于统计信息进行快速过滤测试。
     *
     * <p>该方法利用字段的统计信息(最小值、最大值、空值计数)来判断是否可能存在满足条件的数据,
     * 从而实现数据文件或数据块的快速跳过。
     *
     * <p>测试逻辑示例:
     * <ul>
     *   <li>对于 "age > 18": 如果 max <= 18,返回 false(确定不包含满足条件的数据)</li>
     *   <li>对于 "age < 60": 如果 min >= 60,返回 false(确定不包含满足条件的数据)</li>
     *   <li>对于 "name = 'Alice'": 如果 min 和 max 的字典序范围不包含 'Alice',返回 false</li>
     *   <li>对于 "id IS NOT NULL": 如果 nullCount == rowCount,返回 false(所有值都是 NULL)</li>
     * </ul>
     *
     * <p>注意:该方法返回 true 只表示"可能"存在满足条件的数据(可能存在假阳性),
     * 需要进一步进行精确测试。返回 false 表示确定不包含满足条件的数据。
     *
     * @param type 字段的数据类型
     * @param rowCount 数据行总数
     * @param min 字段的最小值(可能为 null,表示统计信息不完整)
     * @param max 字段的最大值(可能为 null,表示统计信息不完整)
     * @param nullCount 字段的空值计数(可能为 null,表示统计信息不完整)
     * @param literals 用于比较的常量值列表
     * @return true 表示可能包含满足条件的数据;false 表示确定不包含满足条件的数据
     */
    public abstract boolean test(
            DataType type,
            long rowCount,
            Object min,
            Object max,
            Long nullCount,
            List<Object> literals);

    /**
     * 返回该函数的否定形式(如果可能)。
     *
     * <p>大多数比较函数支持否定操作,例如:
     * <ul>
     *   <li>Equal 的否定是 NotEqual</li>
     *   <li>GreaterThan 的否定是 LessOrEqual</li>
     *   <li>IsNull 的否定是 IsNotNull</li>
     *   <li>In 的否定是 NotIn</li>
     * </ul>
     *
     * <p>某些函数可能无法否定,此时返回 Optional.empty()。
     *
     * @return 包含否定函数的 Optional,如果无法否定则返回 Optional.empty()
     */
    public abstract Optional<LeafFunction> negate();

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
     * 访问者模式的接受方法。
     *
     * @param visitor 函数访问者
     * @param fieldRef 字段引用
     * @param literals 常量值列表
     * @param <T> 访问结果的类型
     * @return 访问者处理的结果
     */
    public abstract <T> T visit(
            FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals);

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
