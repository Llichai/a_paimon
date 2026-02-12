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
 * 数组类型的数据类型定义。
 *
 * <p>表示具有相同子类型的元素数组。与 SQL 标准相比,数组的最大基数不能指定,
 * 但固定为 {@link Integer#MAX_VALUE}。此外,任何有效类型都可以作为子类型。
 *
 * <h2>核心特性</h2>
 * <ul>
 *   <li>支持嵌套结构 - 元素可以是任何数据类型,包括复杂类型
 *   <li>固定最大长度 - 数组长度上限为 Integer.MAX_VALUE
 *   <li>类型一致性 - 数组中所有元素必须是相同类型
 *   <li>可空性控制 - 可以指定数组本身是否可为 NULL
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建整数数组类型
 * ArrayType intArray = new ArrayType(new IntType());
 *
 * // 创建字符串数组类型(不可空)
 * ArrayType stringArray = new ArrayType(false, new VarCharType());
 *
 * // 创建嵌套数组类型 ARRAY<ARRAY<INT>>
 * ArrayType nestedArray = new ArrayType(new ArrayType(new IntType()));
 * }</pre>
 *
 * @since 0.4.0
 */
@Public
public final class ArrayType extends DataType {

    private static final long serialVersionUID = 1L;

    /** SQL 字符串格式模板。 */
    public static final String FORMAT = "ARRAY<%s>";

    /** 数组元素的数据类型。 */
    private final DataType elementType;

    /**
     * 创建数组类型。
     *
     * @param isNullable 数组本身是否可为 NULL
     * @param elementType 元素的数据类型,不能为 null
     */
    public ArrayType(boolean isNullable, DataType elementType) {
        super(isNullable, DataTypeRoot.ARRAY);
        this.elementType =
                Preconditions.checkNotNull(elementType, "Element type must not be null.");
    }

    /**
     * 创建可空的数组类型。
     *
     * @param elementType 元素的数据类型
     */
    public ArrayType(DataType elementType) {
        this(true, elementType);
    }

    /**
     * 获取数组元素的数据类型。
     *
     * @return 元素类型
     */
    public DataType getElementType() {
        return elementType;
    }

    /**
     * 创建具有新元素类型的数组类型。
     *
     * @param newElementType 新的元素类型
     * @return 新的数组类型实例
     */
    public DataType newElementType(DataType newElementType) {
        return new ArrayType(isNullable(), newElementType);
    }

    /**
     * 返回此类型的默认大小(字节)。
     *
     * <p>数组的默认大小等于其元素类型的默认大小。
     *
     * @return 元素类型的默认大小
     */
    @Override
    public int defaultSize() {
        return elementType.defaultSize();
    }

    /**
     * 创建具有指定可空性的数组类型副本。
     *
     * <p>同时递归复制元素类型。
     *
     * @param isNullable 新实例是否可为 NULL
     * @return 新的数组类型实例
     */
    @Override
    public DataType copy(boolean isNullable) {
        return new ArrayType(isNullable, elementType.copy());
    }

    /**
     * 生成 SQL 字符串表示。
     *
     * <p>格式: ARRAY&lt;elementType&gt; [NOT NULL]
     *
     * @return SQL 字符串表示
     */
    @Override
    public String asSQLString() {
        return withNullability(FORMAT, elementType.asSQLString());
    }

    /**
     * 将数组类型序列化为 JSON 格式。
     *
     * <p>JSON 格式:
     * <pre>{@code
     * {
     *   "type": "ARRAY" 或 "ARRAY NOT NULL",
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
        generator.writeStringField("type", isNullable() ? "ARRAY" : "ARRAY NOT NULL");
        generator.writeFieldName("element");
        elementType.serializeJson(generator);
        generator.writeEndObject();
    }

    /**
     * 判断是否与另一个对象相等。
     *
     * <p>两个数组类型相等的条件:
     * <ul>
     *   <li>都是 ArrayType 实例
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
        ArrayType arrayType = (ArrayType) o;
        return elementType.equals(arrayType.elementType);
    }

    /**
     * 判断是否与另一个数据类型相等(忽略字段 ID)。
     *
     * <p>用于比较类型结构是否相同,但不考虑字段 ID。
     *
     * @param o 要比较的数据类型
     * @return 如果相等返回 true
     */
    @Override
    public boolean equalsIgnoreFieldId(DataType o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        ArrayType arrayType = (ArrayType) o;
        return elementType.equalsIgnoreFieldId(arrayType.elementType);
    }

    /**
     * 判断此类型是否是从给定类型裁剪而来。
     *
     * <p>用于判断类型是否是列裁剪的结果。
     *
     * @param o 原始类型
     * @return 如果是裁剪结果返回 true
     */
    @Override
    public boolean isPrunedFrom(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        ArrayType arrayType = (ArrayType) o;
        return elementType.isPrunedFrom(arrayType.elementType);
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
