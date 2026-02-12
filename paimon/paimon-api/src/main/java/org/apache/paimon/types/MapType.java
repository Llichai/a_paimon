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
 * Map 类型的数据类型定义。
 *
 * <p>表示关联数组,将键(包括 {@code NULL})映射到值(包括 {@code NULL})。
 * Map 不能包含重复的键;每个键最多可以映射到一个值。对键的类型没有限制;
 * 用户有责任确保键的唯一性。Map 类型是对 SQL 标准的扩展。
 *
 * <h2>核心特性</h2>
 * <ul>
 *   <li>键值对存储 - 维护键到值的映射关系
 *   <li>键唯一性 - 每个键只能出现一次
 *   <li>类型灵活性 - 键和值可以是任何数据类型,包括复杂类型
 *   <li>支持 NULL - 键和值都可以为 NULL(如果类型允许)
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建字符串到整数的 Map 类型
 * MapType stringIntMap = new MapType(new VarCharType(), new IntType());
 *
 * // 创建不可空的 Map 类型
 * MapType nonNullMap = new MapType(false, new IntType(), new VarCharType());
 *
 * // 创建嵌套 Map 类型 MAP<STRING, MAP<INT, DOUBLE>>
 * MapType nestedMap = new MapType(
 *     new VarCharType(),
 *     new MapType(new IntType(), new DoubleType())
 * );
 * }</pre>
 *
 * @since 0.4.0
 */
@Public
public class MapType extends DataType {

    private static final long serialVersionUID = 1L;

    /** SQL 字符串格式模板。 */
    public static final String FORMAT = "MAP<%s, %s>";

    /** Map 键的数据类型。 */
    private final DataType keyType;

    /** Map 值的数据类型。 */
    private final DataType valueType;

    /**
     * 创建 Map 类型。
     *
     * @param isNullable Map 本身是否可为 NULL
     * @param keyType 键的数据类型,不能为 null
     * @param valueType 值的数据类型,不能为 null
     */
    public MapType(boolean isNullable, DataType keyType, DataType valueType) {
        super(isNullable, DataTypeRoot.MAP);
        this.keyType = Preconditions.checkNotNull(keyType, "Key type must not be null.");
        this.valueType = Preconditions.checkNotNull(valueType, "Value type must not be null.");
    }

    /**
     * 创建可空的 Map 类型。
     *
     * @param keyType 键的数据类型
     * @param valueType 值的数据类型
     */
    public MapType(DataType keyType, DataType valueType) {
        this(true, keyType, valueType);
    }

    /**
     * 获取 Map 键的数据类型。
     *
     * @return 键类型
     */
    public DataType getKeyType() {
        return keyType;
    }

    /**
     * 获取 Map 值的数据类型。
     *
     * @return 值类型
     */
    public DataType getValueType() {
        return valueType;
    }

    /**
     * 返回此类型的默认大小(字节)。
     *
     * <p>Map 的默认大小等于键类型和值类型的默认大小之和。
     *
     * @return 键类型和值类型的默认大小之和
     */
    @Override
    public int defaultSize() {
        return keyType.defaultSize() + valueType.defaultSize();
    }

    /**
     * 创建具有指定可空性的 Map 类型副本。
     *
     * <p>同时递归复制键类型和值类型。
     *
     * @param isNullable 新实例是否可为 NULL
     * @return 新的 Map 类型实例
     */
    @Override
    public DataType copy(boolean isNullable) {
        return new MapType(isNullable, keyType.copy(), valueType.copy());
    }

    /**
     * 创建具有新键值类型的 Map 类型。
     *
     * @param newKeyType 新的键类型
     * @param newValueType 新的值类型
     * @return 新的 Map 类型实例
     */
    public DataType newKeyValueType(DataType newKeyType, DataType newValueType) {
        return new MapType(isNullable(), newKeyType, newValueType);
    }

    /**
     * 生成 SQL 字符串表示。
     *
     * <p>格式: MAP&lt;keyType, valueType&gt; [NOT NULL]
     *
     * @return SQL 字符串表示
     */
    @Override
    public String asSQLString() {
        return withNullability(FORMAT, keyType.asSQLString(), valueType.asSQLString());
    }

    /**
     * 将 Map 类型序列化为 JSON 格式。
     *
     * <p>JSON 格式:
     * <pre>{@code
     * {
     *   "type": "MAP" 或 "MAP NOT NULL",
     *   "key": <键类型的JSON表示>,
     *   "value": <值类型的JSON表示>
     * }
     * }</pre>
     *
     * @param generator JSON 生成器
     * @throws IOException 如果序列化失败
     */
    @Override
    public void serializeJson(JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeStringField("type", isNullable() ? "MAP" : "MAP NOT NULL");
        generator.writeFieldName("key");
        keyType.serializeJson(generator);
        generator.writeFieldName("value");
        valueType.serializeJson(generator);
        generator.writeEndObject();
    }

    /**
     * 判断是否与另一个对象相等。
     *
     * <p>两个 Map 类型相等的条件:
     * <ul>
     *   <li>都是 MapType 实例
     *   <li>可空性相同
     *   <li>键类型相等
     *   <li>值类型相等
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
        MapType mapType = (MapType) o;
        return keyType.equals(mapType.keyType) && valueType.equals(mapType.valueType);
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
        MapType mapType = (MapType) o;
        return keyType.equalsIgnoreFieldId(mapType.keyType)
                && valueType.equalsIgnoreFieldId(mapType.valueType);
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
        MapType mapType = (MapType) o;
        return keyType.isPrunedFrom(mapType.keyType) && valueType.isPrunedFrom(mapType.valueType);
    }

    /**
     * 计算哈希码。
     *
     * @return 基于可空性、键类型和值类型的哈希码
     */
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), keyType, valueType);
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
        keyType.collectFieldIds(fieldIds);
        valueType.collectFieldIds(fieldIds);
    }
}
