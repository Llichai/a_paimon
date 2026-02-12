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

import org.apache.paimon.utils.Preconditions;

import java.io.Serializable;
import java.util.Objects;

/**
 * 排序值定义。
 *
 * <p>表示一个排序字段的完整定义,包括字段引用、排序方向和NULL值处理方式。
 * 这是构成{@link TopN}谓词的基本组件。
 *
 * <h2>主要属性</h2>
 * <ul>
 *   <li>field - 排序字段的引用
 *   <li>direction - 排序方向(升序或降序)
 *   <li>nullOrdering - NULL值的排序位置(靠前或靠后)
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 创建升序排序(NULL值在前)
 * FieldRef nameField = new FieldRef(0, "name", DataTypes.STRING());
 * SortValue ascSort = new SortValue(
 *     nameField,
 *     SortDirection.ASCENDING,
 *     NullOrdering.NULLS_FIRST
 * );
 * // SQL: ORDER BY name ASC NULLS FIRST
 *
 * // 2. 创建降序排序(NULL值在后)
 * FieldRef scoreField = new FieldRef(1, "score", DataTypes.DOUBLE());
 * SortValue descSort = new SortValue(
 *     scoreField,
 *     SortDirection.DESCENDING,
 *     NullOrdering.NULLS_LAST
 * );
 * // SQL: ORDER BY score DESC NULLS LAST
 *
 * // 3. 用于TopN
 * TopN topN = new TopN(
 *     Arrays.asList(descSort, ascSort),
 *     10
 * );
 * // SQL: SELECT * FROM table
 * //      ORDER BY score DESC NULLS LAST, name ASC NULLS FIRST
 * //      LIMIT 10
 *
 * // 4. 多字段排序
 * List<SortValue> sortOrders = Arrays.asList(
 *     new SortValue(
 *         new FieldRef(0, "category", DataTypes.STRING()),
 *         SortDirection.ASCENDING,
 *         NullOrdering.NULLS_LAST
 *     ),
 *     new SortValue(
 *         new FieldRef(1, "price", DataTypes.DECIMAL(10, 2)),
 *         SortDirection.DESCENDING,
 *         NullOrdering.NULLS_LAST
 *     )
 * );
 * // SQL: ORDER BY category ASC NULLS LAST, price DESC NULLS LAST
 * }</pre>
 *
 * <h2>排序方向枚举</h2>
 * <ul>
 *   <li>{@link SortDirection#ASCENDING} - 升序(从小到大)
 *   <li>{@link SortDirection#DESCENDING} - 降序(从大到小)
 * </ul>
 *
 * <h2>NULL值排序枚举</h2>
 * <ul>
 *   <li>{@link NullOrdering#NULLS_FIRST} - NULL值排在最前面
 *   <li>{@link NullOrdering#NULLS_LAST} - NULL值排在最后面
 * </ul>
 *
 * <h2>NULL值排序的重要性</h2>
 * <p>不同数据库对NULL值的默认排序行为不同:
 * <ul>
 *   <li>PostgreSQL - 默认NULLS LAST(升序时)
 *   <li>MySQL - 默认NULL值最小
 *   <li>Oracle - 默认NULL值最大
 * </ul>
 * <p>明确指定NullOrdering可以保证跨数据库的一致性。
 *
 * <h2>应用场景</h2>
 * <ul>
 *   <li>TopN查询 - 定义TopN的排序规则
 *   <li>排序读取 - 指定数据读取的排序顺序
 *   <li>排序索引 - 定义排序索引的结构
 *   <li>分组排序 - 在分组内进行排序
 * </ul>
 *
 * <h2>字符串表示</h2>
 * <p>格式: "field_name direction null_ordering"
 * <p>示例: "score DESC NULLS LAST"
 *
 * @see TopN
 * @see FieldRef
 */
public class SortValue implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 排序字段引用。 */
    private final FieldRef field;

    /** 排序方向。 */
    private final SortDirection direction;

    /** NULL值排序方式。 */
    private final NullOrdering nullOrdering;

    /**
     * 构造排序值定义。
     *
     * @param field 排序字段引用,不能为null
     * @param direction 排序方向,不能为null
     * @param nullOrdering NULL值排序方式,不能为null
     * @throws NullPointerException 如果任何参数为null
     */
    public SortValue(FieldRef field, SortDirection direction, NullOrdering nullOrdering) {
        this.field = Preconditions.checkNotNull(field);
        this.direction = Preconditions.checkNotNull(direction);
        this.nullOrdering = Preconditions.checkNotNull(nullOrdering);
    }

    /**
     * 获取排序字段引用。
     *
     * @return 字段引用
     */
    public FieldRef field() {
        return field;
    }

    /**
     * 获取排序方向。
     *
     * @return 排序方向(升序或降序)
     */
    public SortDirection direction() {
        return direction;
    }

    /**
     * 获取NULL值排序方式。
     *
     * @return NULL值排序方式(靠前或靠后)
     */
    public NullOrdering nullOrdering() {
        return nullOrdering;
    }

    /**
     * 返回排序值的字符串表示。
     *
     * <p>格式: "field_name direction null_ordering"
     *
     * @return 字符串表示
     */
    @Override
    public String toString() {
        return String.format(
                "%s %s %s", field.name(), direction.toString(), nullOrdering.toString());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SortValue sortValue = (SortValue) o;
        return Objects.equals(field, sortValue.field)
                && direction == sortValue.direction
                && nullOrdering == sortValue.nullOrdering;
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, direction, nullOrdering);
    }

    /**
     * NULL值排序枚举。
     *
     * <p>定义NULL值在排序结果中的位置。
     */
    public enum NullOrdering {
        /** NULL值排在最前面。 */
        NULLS_FIRST("NULLS FIRST"),

        /** NULL值排在最后面。 */
        NULLS_LAST("NULLS LAST");

        private final String name;

        NullOrdering(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * 排序方向枚举。
     *
     * <p>定义排序的顺序(升序或降序)。
     */
    public enum SortDirection {
        /** 升序排序(从小到大)。 */
        ASCENDING("ASC"),

        /** 降序排序(从大到小)。 */
        DESCENDING("DESC");

        private final String name;

        SortDirection(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
