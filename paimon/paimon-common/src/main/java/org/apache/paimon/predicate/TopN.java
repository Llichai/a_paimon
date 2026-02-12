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

import org.apache.paimon.predicate.SortValue.NullOrdering;
import org.apache.paimon.predicate.SortValue.SortDirection;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.ListUtils.isNullOrEmpty;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * TopN谓词。
 *
 * <p>表示一个TopN操作,用于限制查询结果的数量并按指定顺序排序。
 * TopN是一种特殊的谓词,结合了排序(ORDER BY)和限制(LIMIT)的功能。
 *
 * <h2>主要组成</h2>
 * <ul>
 *   <li>排序规则 - 一个或多个{@link SortValue}定义排序字段和顺序
 *   <li>限制数量 - 要返回的最大记录数
 *   <li>序列化支持 - 实现Serializable,可以跨网络传输
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 单字段排序的TopN
 * FieldRef ageField = new FieldRef(0, "age", DataTypes.INT());
 * TopN topN = new TopN(
 *     ageField,
 *     SortDirection.DESCENDING,  // 降序
 *     NullOrdering.NULLS_LAST,   // NULL值排在最后
 *     10                          // 限制10条
 * );
 * // SQL: SELECT * FROM table ORDER BY age DESC NULLS LAST LIMIT 10
 *
 * // 2. 多字段排序的TopN
 * FieldRef scoreField = new FieldRef(1, "score", DataTypes.DOUBLE());
 * FieldRef nameField = new FieldRef(2, "name", DataTypes.STRING());
 *
 * List<SortValue> orders = Arrays.asList(
 *     new SortValue(scoreField, SortDirection.DESCENDING, NullOrdering.NULLS_LAST),
 *     new SortValue(nameField, SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
 * );
 * TopN multiSortTopN = new TopN(orders, 20);
 * // SQL: SELECT * FROM table
 * //      ORDER BY score DESC NULLS LAST, name ASC NULLS FIRST
 *      LIMIT 20
 *
 * // 3. 获取前3名得分最高的记录
 * TopN top3 = new TopN(
 *     scoreField,
 *     SortDirection.DESCENDING,
 *     NullOrdering.NULLS_LAST,
 *     3
 * );
 * // SQL: SELECT * FROM table ORDER BY score DESC LIMIT 3
 * }</pre>
 *
 * <h2>SQL对应关系</h2>
 * <pre>
 * SQL: SELECT * FROM table
 *      ORDER BY col1 DESC NULLS LAST, col2 ASC NULLS FIRST
 *      LIMIT n
 *
 * Paimon: new TopN(
 *     Arrays.asList(
 *         new SortValue(field1, DESCENDING, NULLS_LAST),
 *         new SortValue(field2, ASCENDING, NULLS_FIRST)
 *     ),
 *     n
 * )
 * </pre>
 *
 * <h2>应用场景</h2>
 * <ul>
 *   <li>分页查询 - 获取排序后的前N条记录
 *   <li>排行榜 - 查询得分最高的前N名
 *   <li>最新记录 - 按时间戳降序获取最新的N条记录
 *   <li>结果采样 - 限制大型查询的返回结果数量
 * </ul>
 *
 * <h2>优化机会</h2>
 * <ul>
 *   <li>文件级过滤 - 可以基于文件统计信息跳过整个文件
 *   <li>早期终止 - 一旦收集到足够的记录就可以停止扫描
 *   <li>堆排序 - 使用优先队列保持TopN记录,避免全量排序
 *   <li>索引利用 - 如果存在匹配的排序索引,可以直接使用
 * </ul>
 *
 * <h2>与LIMIT的区别</h2>
 * <ul>
 *   <li>TopN - 包含排序逻辑,保证结果的顺序性
 *   <li>LIMIT - 仅限制数量,不保证顺序
 *   <li>TopN更适合 - 需要"最大/最小的N个"场景
 * </ul>
 *
 * <h2>限制</h2>
 * <ul>
 *   <li>非空orders - 至少需要一个排序字段
 *   <li>正数limit - 限制数量必须大于0
 *   <li>内存消耗 - 需要在内存中维护TopN记录
 * </ul>
 *
 * @see SortValue
 * @see SortValue.SortDirection
 * @see SortValue.NullOrdering
 */
public class TopN implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 排序规则列表,定义排序的字段、方向和NULL值处理。 */
    private final List<SortValue> orders;

    /** 限制返回的记录数量。 */
    private final int limit;

    /**
     * 构造TopN谓词。
     *
     * @param orders 排序规则列表,不能为空
     * @param limit 限制的记录数量,必须大于0
     * @throws IllegalArgumentException 如果orders为null或空
     */
    public TopN(List<SortValue> orders, int limit) {
        checkArgument(!isNullOrEmpty(orders), "orders should not be null or empty");
        this.orders = orders;
        this.limit = limit;
    }

    /**
     * 构造单字段排序的TopN谓词。
     *
     * <p>这是一个便捷构造方法,用于最常见的单字段排序场景。
     *
     * @param ref 排序字段引用
     * @param direction 排序方向(升序或降序)
     * @param nullOrdering NULL值排序方式(靠前或靠后)
     * @param limit 限制的记录数量
     */
    public TopN(FieldRef ref, SortDirection direction, NullOrdering nullOrdering, int limit) {
        SortValue order = new SortValue(ref, direction, nullOrdering);
        this.orders = Collections.singletonList(order);
        this.limit = limit;
    }

    /**
     * 获取排序规则列表。
     *
     * @return 排序规则列表
     */
    public List<SortValue> orders() {
        return orders;
    }

    /**
     * 获取限制的记录数量。
     *
     * @return 限制数量
     */
    public int limit() {
        return limit;
    }

    /**
     * 返回TopN的字符串表示。
     *
     * <p>格式: Sort(field1 ASC NULLS FIRST, field2 DESC NULLS LAST), Limit(n)
     *
     * @return 字符串表示
     */
    @Override
    public String toString() {
        String sort = orders.stream().map(SortValue::toString).collect(Collectors.joining(", "));
        return String.format("Sort(%s), Limit(%s)", sort, limit);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopN topN = (TopN) o;
        return limit == topN.limit && Objects.equals(orders, topN.orders);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orders, limit);
    }
}
