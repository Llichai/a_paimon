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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;
import java.util.Optional;

/**
 * 谓词接口,用于数据过滤和筛选。
 *
 * <p>谓词是一个返回布尔值的表达式,用于测试数据行是否满足特定条件。Paimon 的谓词系统支持两种测试方式:
 * 1. 基于实际数据行的精确测试
 * 2. 基于统计信息(最小值、最大值、空值计数)的快速过滤
 *
 * <h2>设计模式</h2>
 * 该接口采用组合模式(Composite Pattern)设计:
 * <ul>
 *   <li>{@link LeafPredicate} - 叶子节点,表示基本的比较操作,如 field = value</li>
 *   <li>{@link CompoundPredicate} - 复合节点,表示逻辑运算,如 AND、OR</li>
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>查询过滤:在读取数据时应用 WHERE 条件</li>
 *   <li>文件跳过:基于文件统计信息跳过不满足条件的数据文件</li>
 *   <li>分区裁剪:根据分区键的谓词条件裁剪不相关的分区</li>
 *   <li>数据删除:标识需要删除的数据行</li>
 * </ul>
 *
 * <h2>统计信息过滤</h2>
 * 谓词支持基于统计信息的快速过滤,可以在不读取实际数据的情况下判断一个数据文件或数据块是否可能包含满足条件的记录。
 * 这种机制可以显著提升查询性能:
 * <ul>
 *   <li>如果返回 false:该文件/块确定不包含满足条件的数据,可以安全跳过</li>
 *   <li>如果返回 true:该文件/块可能包含满足条件的数据,需要读取并进行精确测试</li>
 * </ul>
 *
 * <h2>序列化支持</h2>
 * 该接口使用 Jackson 注解支持 JSON 序列化和反序列化,便于谓词在分布式环境中传递和存储。
 *
 * @see PredicateBuilder 用于构建谓词的工具类
 * @see LeafPredicate 叶子谓词实现
 * @see CompoundPredicate 复合谓词实现
 * @since 0.4.0
 */
@Public
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = Predicate.FIELD_KIND)
@JsonSubTypes({
    @JsonSubTypes.Type(value = LeafPredicate.class, name = LeafPredicate.NAME),
    @JsonSubTypes.Type(value = CompoundPredicate.class, name = CompoundPredicate.NAME)
})
public interface Predicate extends Serializable {

    /** JSON 序列化时用于标识谓词类型的字段名 */
    String FIELD_KIND = "kind";

    /**
     * 基于具体的数据行进行精确测试。
     *
     * <p>该方法用于在实际读取数据后进行精确的条件判断,是谓词过滤的核心方法。
     * 例如,对于谓词 "age > 18",该方法会读取 row 中的 age 字段值并与 18 进行比较。
     *
     * @param row 待测试的数据行
     * @return 如果数据行满足谓词条件返回 true,否则返回 false
     */
    boolean test(InternalRow row);

    /**
     * 基于统计信息进行快速过滤测试。
     *
     * <p>该方法利用数据文件或数据块的统计信息(最小值、最大值、空值计数)来判断是否可能存在满足条件的数据,
     * 从而实现文件级或块级的数据跳过,避免不必要的数据读取。
     *
     * <p>测试逻辑示例:
     * <ul>
     *   <li>对于 "age > 18",如果 maxValues 中 age 的值 <= 18,则确定该文件不包含满足条件的数据,返回 false</li>
     *   <li>对于 "name = 'Alice'",如果 minValues 和 maxValues 的字典序范围不包含 'Alice',返回 false</li>
     *   <li>对于 "id IS NOT NULL",如果 nullCount 等于 rowCount,说明所有值都是 NULL,返回 false</li>
     * </ul>
     *
     * <p>注意:该方法返回 true 只表示"可能"存在满足条件的数据,可能存在假阳性(false positive),
     * 需要进一步使用 {@link #test(InternalRow)} 进行精确判断。
     *
     * @param rowCount 数据行总数
     * @param minValues 每个字段的最小值
     * @param maxValues 每个字段的最大值
     * @param nullCounts 每个字段的空值计数
     * @return true 表示可能包含满足条件的数据(可能存在假阳性);false 表示确定不包含满足条件的数据
     */
    boolean test(
            long rowCount, InternalRow minValues, InternalRow maxValues, InternalArray nullCounts);

    /**
     * 返回该谓词的否定形式(如果可能)。
     *
     * <p>谓词否定用于优化查询和支持 NOT 操作。例如:
     * <ul>
     *   <li>"age > 18" 的否定是 "age <= 18"</li>
     *   <li>"name = 'Alice'" 的否定是 "name != 'Alice'"</li>
     *   <li>"a AND b" 的否定是 "NOT a OR NOT b" (德摩根定律)</li>
     * </ul>
     *
     * <p>某些谓词可能无法否定(例如包含函数转换的谓词),此时返回 Optional.empty()。
     *
     * @return 包含否定谓词的 Optional,如果无法否定则返回 Optional.empty()
     */
    Optional<Predicate> negate();

    /**
     * 访问者模式的接受方法,用于遍历和处理谓词树。
     *
     * <p>访问者模式允许在不修改谓词类的情况下定义新的操作,常用于:
     * <ul>
     *   <li>谓词转换和重写</li>
     *   <li>提取特定字段引用</li>
     *   <li>谓词优化和简化</li>
     *   <li>谓词到其他表达式的转换</li>
     * </ul>
     *
     * @param visitor 谓词访问者
     * @param <T> 访问结果的类型
     * @return 访问者处理的结果
     */
    <T> T visit(PredicateVisitor<T> visitor);
}
