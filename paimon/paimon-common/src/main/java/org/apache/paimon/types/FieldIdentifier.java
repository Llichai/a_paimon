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

import java.util.Objects;

/**
 * 字段标识符类。
 *
 * <p>用于表示字段的唯一性标识。该类通过字段的名称、类型和描述信息来唯一确定一个字段。
 *
 * <p>主要用途：
 * <ul>
 *   <li>在 Schema 演化过程中识别字段的唯一性
 *   <li>判断两个字段是否为同一个字段
 *   <li>作为字段比较和查找的基准
 * </ul>
 *
 * <p>设计要点：
 * <ul>
 *   <li>不可变对象：所有字段都是 final 的，保证线程安全
 *   <li>基于值的相等性：通过 equals 和 hashCode 实现基于内容的比较
 *   <li>完整性：包含字段的所有关键属性（名称、类型、描述）
 * </ul>
 */
public class FieldIdentifier {
    /** 字段名称 */
    private final String name;

    /** 字段数据类型 */
    private final DataType type;

    /** 字段描述信息 */
    private final String description;

    /**
     * 从 DataField 构造 FieldIdentifier。
     *
     * @param dataField 数据字段，包含字段的名称、类型和描述信息
     */
    public FieldIdentifier(DataField dataField) {
        this.name = dataField.name();
        this.type = dataField.type();
        this.description = dataField.description();
    }

    /**
     * 判断两个 FieldIdentifier 是否相等。
     *
     * <p>相等条件：名称、类型和描述信息都相同。
     *
     * @param o 待比较的对象
     * @return 如果两个字段标识符相等则返回 true，否则返回 false
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FieldIdentifier field = (FieldIdentifier) o;
        return Objects.equals(name, field.name)
                && Objects.equals(type, field.type)
                && Objects.equals(description, field.description);
    }

    /**
     * 计算 FieldIdentifier 的哈希码。
     *
     * <p>基于名称、类型和描述信息计算哈希值，确保相等的对象有相同的哈希码。
     *
     * @return 对象的哈希码
     */
    @Override
    public int hashCode() {
        return Objects.hash(name, type, description);
    }
}
