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

package org.apache.paimon.types;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

/**
 * 多重集(Multiset)类型的数据类型定义。
 *
 * <p>也称为袋(bag)类型。与集合(set)不同,它允许同一子类型的元素出现多次。
 * 每个唯一值(包括 {@code NULL})都映射到某个重数(multiplicity)。对元素类型没有限制;
 * 用户有责任确保唯一性。
 *
 * <p>可以通过一个 Map 来进行转换,该 Map 将每个值分配给一个整数重数({@code Map<T, Integer>})。
 *
 * <h2>核心特性</h2>
 * <ul>
 *   <li>重复元素 - 允许相同元素出现多次
 *   <li>计数维护 - 记录每个唯一值的出现次数
 *   <li>等价表示 - 可以用 Map&lt;T, Integer&gt; 表示
 *   <li>支持 NULL - 可以包含 NULL 值并记录其重数
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建整数多重集类型
 * MultisetType intMultiset = new MultisetType(new IntType());
 *
 * // 创建不可空的字符串多重集类型
 * MultisetType stringMultiset = new MultisetType(false, new VarCharType());
 *
 * // 等价的 Map 表示: MAP<STRING, INT>
 * MapType equivalentMap = new MapType(new VarCharType(), new IntType());
 * }</pre>
 *
 * <h2>与其他集合类型的区别</h2>
 * <ul>
 *   <li>Set - 不允许重复元素
 *   <li>Multiset - 允许重复元素并计数
 *   <li>List - 有序集合,允许重复
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public class MultisetType extends DataType {

    private static final long serialVersionUID = 1L;

    /** SQL 字符串格式模板。 */
    public static final String FORMAT = "MULTISET<%s>";

    /** 多重集元素的数据类型。 */
    private final DataType elementType;

    /**
     * 创建多重集类型。
     *
     * @param isNullable 多重集本身是否可为 NULL
     * @param elementType 元素的数据类型,不能为 null
     */
    public MultisetType(boolean isNullable, DataType elementType) {
        super(isNullable, DataTypeRoot.MULTISET);
        this.elementType =
                Preconditions.checkNotNull(elementType, "Element type must not be null.");
    }

    /**
     * 创建可空的多重集类型。
     *
     * @param elementType 元素的数据类型
     */
    public MultisetType(DataType elementType) {
        this(true, elementType);
    }

    /**
     * 获取多重集元素的数据类型。
     *
     * @return 元素类型
     */
    public DataType getElementType() {
        return elementType;
    }

    /**
     * 返回此类型的默认大小(字节)。
     *
     * <p>多重集的默认大小等于元素类型的默认大小加上 4 字节(用于存储计数)。
     *
     * @return 元素类型的默认大小 + 4
     */
    @Override
    public int defaultSize() {
        return elementType.defaultSize() + 4;
    }

    /**
     * 创建具有指定可空性的多重集类型副本。
     *
     * @param isNullable 新实例是否可为 NULL
     * @return 新的多重集类型实例
     */
    @Override
    public DataType copy(boolean isNullable) {
        return new MultisetType(isNullable, elementType);
    }

    /**
     * 生成 SQL 字符串表示。
     *
     * <p>格式: MULTISET&lt;elementType&gt; [NOT NULL]
     *
     * @return SQL 字符串表示
     */
    @Override
    public String asSQLString() {
        return withNullability(FORMAT, elementType.asSQLString());
    }

    /**
     * 将多重集类型序列化为 JSON 格式。
     *
     * <p>JSON 格式:
     * <pre>{@code
     * {
     *   "type": "MULTISET" 或 "MULTISET NOT NULL",
     *   "element": <元素类型的JSON表示>
     * }
     * }</pre>
     *
     * @param generator JSON 生成器
     * @throws IOException 如果序列化失败
     */
    @Override
    public void serializeJson(JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeStringField("type", isNullable() ? "MULTISET" : "MULTISET NOT NULL");
        generator.writeFieldName("element");
        elementType.serializeJson(generator);
        generator.writeEndObject();
    }

    /**
     * 判断是否与另一个对象相等。
     *
     * <p>两个多重集类型相等的条件:
     * <ul>
     *   <li>都是 MultisetType 实例
     *   <li>可空性相同
     *   <li>元素类型相等
     * </ul>
     *
     * @param o 要比较的对象
     * @return 如果相等返回 true
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        MultisetType that = (MultisetType) o;
        return elementType.equals(that.elementType);
    }

    /**
     * 计算哈希码。
     *
     * @return 基于可空性和元素类型的哈希码
     */
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), elementType);
    }

    /**
     * 接受访问者访问。
     *
     * @param visitor 数据类型访问者
     * @param <R> 返回类型
     * @return 访问结果
     */
    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    /**
     * 收集此类型及其嵌套类型中的所有字段 ID。
     *
     * @param fieldIds 用于收集字段 ID 的集合
     */
    @Override
    public void collectFieldIds(Set<Integer> fieldIds) {
        elementType.collectFieldIds(fieldIds);
    }
}
