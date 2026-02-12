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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

/**
 * 字段引用类。
 *
 * <p>表示对输入数据中某个字段的引用,包含字段的索引、名称和数据类型。
 * 这是谓词系统中最基础的组件之一,用于在谓词表达式中引用表的字段。
 *
 * <h2>主要属性</h2>
 * <ul>
 *   <li>index - 字段在schema中的位置索引(从0开始)
 *   <li>name - 字段名称
 *   <li>type - 字段的数据类型
 * </ul>
 *
 * <h2>序列化支持</h2>
 * <p>该类支持JSON序列化,使用Jackson注解:
 * <ul>
 *   <li>{@code @JsonCreator} - 标记反序列化构造函数
 *   <li>{@code @JsonProperty} - 标记JSON属性映射
 *   <li>实现 {@link Serializable} - 支持Java序列化
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 从RowType创建字段引用
 * RowType rowType = RowType.of(
 *     new DataType[] {DataTypes.STRING(), DataTypes.INT(), DataTypes.BOOLEAN()},
 *     new String[] {"name", "age", "active"}
 * );
 *
 * // 创建字段引用
 * FieldRef nameRef = new FieldRef(0, "name", DataTypes.STRING());
 * FieldRef ageRef = new FieldRef(1, "age", DataTypes.INT());
 * FieldRef activeRef = new FieldRef(2, "active", DataTypes.BOOLEAN());
 *
 * // 在谓词中使用
 * Predicate predicate = new LeafPredicate(
 *     Equal.INSTANCE,
 *     nameRef.type(),
 *     nameRef.index(),
 *     nameRef.name(),
 *     Collections.singletonList("Alice")
 * );
 *
 * // 在Transform中使用
 * Transform fieldTransform = new FieldTransform(nameRef);
 * Object value = fieldTransform.transform(row); // 提取row中的name字段
 * }</pre>
 *
 * <h2>应用场景</h2>
 * <ul>
 *   <li>谓词构建 - 在叶子谓词中引用字段
 *   <li>字段转换 - 在Transform中引用源字段
 *   <li>投影操作 - 指定需要读取的字段
 *   <li>字段映射 - 在schema演化时映射字段
 * </ul>
 *
 * <h2>相等性</h2>
 * <p>两个FieldRef相等当且仅当:
 * <ul>
 *   <li>索引相同
 *   <li>名称相同
 *   <li>类型相同
 * </ul>
 *
 * <h2>字符串表示</h2>
 * <p>{@link #toString()} 方法仅返回字段名称,用于日志和调试输出。
 *
 * @see LeafPredicate
 * @see Transform
 * @see FieldTransform
 */
public class FieldRef implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String FIELD_INDEX = "index";
    private static final String FIELD_NAME = "name";
    private static final String FIELD_TYPE = "type";

    /** 字段在schema中的索引位置。 */
    private final int index;

    /** 字段名称。 */
    private final String name;

    /** 字段的数据类型。 */
    private final DataType type;

    /**
     * 构造字段引用。
     *
     * @param index 字段在schema中的索引(从0开始)
     * @param name 字段名称
     * @param type 字段的数据类型
     */
    @JsonCreator
    public FieldRef(
            @JsonProperty(FIELD_INDEX) int index,
            @JsonProperty(FIELD_NAME) String name,
            @JsonProperty(FIELD_TYPE) DataType type) {
        this.index = index;
        this.name = name;
        this.type = type;
    }

    /**
     * 获取字段索引。
     *
     * @return 字段在schema中的索引
     */
    @JsonProperty(FIELD_INDEX)
    public int index() {
        return index;
    }

    /**
     * 获取字段名称。
     *
     * @return 字段名称
     */
    @JsonProperty(FIELD_NAME)
    public String name() {
        return name;
    }

    /**
     * 获取字段类型。
     *
     * @return 字段的数据类型
     */
    @JsonProperty(FIELD_TYPE)
    public DataType type() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FieldRef fieldRef = (FieldRef) o;
        return index == fieldRef.index
                && Objects.equals(name, fieldRef.name)
                && Objects.equals(type, fieldRef.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, name, type);
    }

    @Override
    public String toString() {
        return name;
    }
}
