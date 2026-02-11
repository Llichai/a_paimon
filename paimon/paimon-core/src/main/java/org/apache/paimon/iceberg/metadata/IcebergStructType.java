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

package org.apache.paimon.iceberg.metadata;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

/**
 * Iceberg 中的结构体类型类。
 *
 * <p>表示 Iceberg 的 STRUCT 类型,对应 Paimon 的 {@link org.apache.paimon.types.RowType}。
 * 包含一个字段列表,每个字段都是 {@link IcebergDataField}。
 *
 * <p>用途:
 * <ul>
 *   <li>表示嵌套的结构体类型字段
 *   <li>表示表的顶层 Schema 结构
 * </ul>
 *
 * <p>参考: <a href="https://iceberg.apache.org/spec/#schemas">Iceberg 规范</a>
 *
 * @see IcebergDataField 字段定义类
 * @see org.apache.paimon.types.RowType Paimon 行类型
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IcebergStructType {

    private static final String FIELD_TYPE = "type";
    private static final String FIELD_FIELDS = "fields";

    @JsonProperty(FIELD_TYPE)
    private final String type;

    @JsonProperty(FIELD_FIELDS)
    private final List<IcebergDataField> fields;

    public IcebergStructType(List<IcebergDataField> fields) {
        this("struct", fields);
    }

    @JsonCreator
    public IcebergStructType(
            @JsonProperty(FIELD_TYPE) String type,
            @JsonProperty(FIELD_FIELDS) List<IcebergDataField> fields) {
        this.type = type;
        this.fields = fields;
    }

    @JsonGetter(FIELD_TYPE)
    public String type() {
        return type;
    }

    @JsonGetter(FIELD_FIELDS)
    public List<IcebergDataField> fields() {
        return fields;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, fields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergStructType)) {
            return false;
        }

        IcebergStructType that = (IcebergStructType) o;
        return Objects.equals(type, that.type) && Objects.equals(fields, that.fields);
    }
}
