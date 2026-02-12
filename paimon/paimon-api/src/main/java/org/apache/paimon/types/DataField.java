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
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

import static org.apache.paimon.utils.EncodingUtils.escapeIdentifier;
import static org.apache.paimon.utils.EncodingUtils.escapeSingleQuotes;

/**
 * 定义行类型(Row Type)的字段。
 *
 * <p>DataField 是构成 {@link RowType} 的基本单元,表示表或结构体中的一个命名字段。
 * 每个字段包含以下核心信息:
 * <ul>
 *     <li><b>字段 ID</b>: 全局唯一的字段标识符,用于 Schema 演化和列裁剪</li>
 *     <li><b>字段名称</b>: 字段的名称,在同一个 RowType 中必须唯一</li>
 *     <li><b>字段类型</b>: 字段的数据类型</li>
 *     <li><b>描述信息</b>: 可选的字段注释,用于文档和元数据</li>
 *     <li><b>默认值</b>: 可选的默认值表达式</li>
 * </ul>
 *
 * <p>字段 ID 的作用:
 * <ul>
 *     <li><b>Schema 演化</b>: 在 Schema 变更时跟踪字段的对应关系,即使字段名称或顺序发生变化</li>
 *     <li><b>列裁剪</b>: 通过字段 ID 精确定位需要读取的列,提高查询性能</li>
 *     <li><b>数据重组</b>: 在字段顺序调整时正确映射数据</li>
 *     <li><b>版本兼容</b>: 支持不同 Schema 版本的数据文件兼容读取</li>
 * </ul>
 *
 * <p>设计特点:
 * <ul>
 *     <li><b>不可变性</b>: 字段实例是不可变的,修改操作返回新实例</li>
 *     <li><b>可序列化</b>: 支持序列化,可在分布式环境中传输</li>
 *     <li><b>流式 API</b>: 提供 newXxx() 方法用于创建修改后的字段副本</li>
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建一个简单字段
 * DataField field1 = new DataField(0, "id", DataTypes.INT());
 *
 * // 创建带描述的字段
 * DataField field2 = new DataField(1, "name", DataTypes.STRING(), "用户名称");
 *
 * // 创建带默认值的字段
 * DataField field3 = new DataField(2, "age", DataTypes.INT(), "年龄", "18");
 *
 * // 修改字段属性(返回新实例)
 * DataField field4 = field1.newType(DataTypes.BIGINT());
 * }</pre>
 *
 * @see RowType 行类型,由多个 DataField 组成
 * @see DataType 数据类型基类
 * @since 0.4.0
 */
@Public
public final class DataField implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 字段的全局唯一标识符,用于 Schema 演化 */
    private final int id;

    /** 字段名称,在同一个 RowType 中必须唯一 */
    private final String name;

    /** 字段的数据类型 */
    private final DataType type;

    /** 可选的字段描述信息(注释) */
    private final @Nullable String description;

    /** 可选的默认值表达式 */
    private final @Nullable String defaultValue;

    /**
     * 创建一个数据字段,不包含描述和默认值。
     *
     * @param id 字段的全局唯一标识符
     * @param name 字段名称
     * @param dataType 字段的数据类型
     */
    public DataField(int id, String name, DataType dataType) {
        this(id, name, dataType, null, null);
    }

    /**
     * 创建一个数据字段,包含描述但不包含默认值。
     *
     * @param id 字段的全局唯一标识符
     * @param name 字段名称
     * @param dataType 字段的数据类型
     * @param description 字段的描述信息(注释),可为 null
     */
    public DataField(int id, String name, DataType dataType, @Nullable String description) {
        this(id, name, dataType, description, null);
    }

    /**
     * 创建一个完整的数据字段。
     *
     * @param id 字段的全局唯一标识符
     * @param name 字段名称
     * @param type 字段的数据类型
     * @param description 字段的描述信息(注释),可为 null
     * @param defaultValue 字段的默认值表达式,可为 null
     */
    public DataField(
            int id,
            String name,
            DataType type,
            @Nullable String description,
            @Nullable String defaultValue) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.description = description;
        this.defaultValue = defaultValue;
    }

    /**
     * 获取字段的唯一标识符。
     *
     * @return 字段 ID
     */
    public int id() {
        return id;
    }

    /**
     * 获取字段名称。
     *
     * @return 字段名称
     */
    public String name() {
        return name;
    }

    /**
     * 获取字段的数据类型。
     *
     * @return 字段类型
     */
    public DataType type() {
        return type;
    }

    /**
     * 创建一个具有新 ID 的字段副本。
     *
     * <p>用于 Schema 演化时重新分配字段 ID。
     *
     * @param newId 新的字段 ID
     * @return 具有新 ID 的字段副本
     */
    public DataField newId(int newId) {
        return new DataField(newId, name, type, description, defaultValue);
    }

    /**
     * 创建一个具有新名称的字段副本。
     *
     * <p>用于字段重命名操作。
     *
     * @param newName 新的字段名称
     * @return 具有新名称的字段副本
     */
    public DataField newName(String newName) {
        return new DataField(id, newName, type, description, defaultValue);
    }

    /**
     * 创建一个具有新类型的字段副本。
     *
     * <p>用于字段类型变更操作,如类型升级(INT -> BIGINT)或类型转换。
     *
     * @param newType 新的字段类型
     * @return 具有新类型的字段副本
     */
    public DataField newType(DataType newType) {
        return new DataField(id, name, newType, description, defaultValue);
    }

    /**
     * 创建一个具有新描述的字段副本。
     *
     * <p>用于更新字段的注释信息。
     *
     * @param newDescription 新的字段描述
     * @return 具有新描述的字段副本
     */
    public DataField newDescription(String newDescription) {
        return new DataField(id, name, type, newDescription, defaultValue);
    }

    /**
     * 创建一个具有新默认值的字段副本。
     *
     * <p>用于更新字段的默认值表达式。
     *
     * @param newDefaultValue 新的默认值表达式
     * @return 具有新默认值的字段副本
     */
    public DataField newDefaultValue(String newDefaultValue) {
        return new DataField(id, name, type, description, newDefaultValue);
    }

    /**
     * 获取字段的描述信息。
     *
     * @return 字段描述,如果未设置则返回 null
     */
    @Nullable
    public String description() {
        return description;
    }

    /**
     * 获取字段的默认值表达式。
     *
     * @return 默认值表达式,如果未设置则返回 null
     */
    @Nullable
    public String defaultValue() {
        return defaultValue;
    }

    /**
     * 创建该字段的深拷贝,保持原有的可空性。
     *
     * <p>递归复制字段类型,确保嵌套类型也被正确复制。
     *
     * @return 字段的深拷贝
     */
    public DataField copy() {
        return new DataField(id, name, type.copy(), description, defaultValue);
    }

    /**
     * 创建该字段的深拷贝,可以指定不同的可空性。
     *
     * <p>用于修改字段类型的可空性,例如将可空字段改为 NOT NULL。
     *
     * @param isNullable 目标可空性
     * @return 具有指定可空性的字段深拷贝
     */
    public DataField copy(boolean isNullable) {
        return new DataField(id, name, type.copy(isNullable), description, defaultValue);
    }

    /**
     * 返回该字段的 SQL 标准字符串表示形式。
     *
     * <p>格式: {@code name type [COMMENT 'description'] [DEFAULT defaultValue]}
     *
     * <p>示例输出:
     * <pre>
     * id INT NOT NULL
     * name VARCHAR(100) COMMENT '用户名称'
     * age INT DEFAULT 18
     * create_time TIMESTAMP COMMENT '创建时间' DEFAULT CURRENT_TIMESTAMP
     * </pre>
     *
     * @return SQL 格式的字段定义字符串
     */
    public String asSQLString() {
        StringBuilder sb = new StringBuilder();
        sb.append(escapeIdentifier(name)).append(" ").append(type.asSQLString());
        if (StringUtils.isNotEmpty(description)) {
            sb.append(" COMMENT '").append(escapeSingleQuotes(description)).append("'");
        }
        if (defaultValue != null) {
            sb.append(" DEFAULT ").append(defaultValue);
        }
        return sb.toString();
    }

    /**
     * 将该字段序列化为 JSON 格式。
     *
     * <p>生成的 JSON 结构包含字段的所有属性:
     * <pre>{@code
     * {
     *   "id": 0,
     *   "name": "username",
     *   "type": "VARCHAR(100)",
     *   "description": "用户名称",  // 如果有
     *   "defaultValue": "'guest'"   // 如果有
     * }
     * }</pre>
     *
     * @param generator JSON 生成器
     * @throws IOException 如果序列化过程中发生 I/O 错误
     */
    public void serializeJson(JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeNumberField("id", id());
        generator.writeStringField("name", name());
        generator.writeFieldName("type");
        type.serializeJson(generator);
        if (description() != null) {
            generator.writeStringField("description", description());
        }
        if (defaultValue() != null) {
            generator.writeStringField("defaultValue", defaultValue());
        }
        generator.writeEndObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataField field = (DataField) o;
        return Objects.equals(id, field.id)
                && Objects.equals(name, field.name)
                && Objects.equals(type, field.type)
                && Objects.equals(description, field.description)
                && Objects.equals(defaultValue, field.defaultValue);
    }

    /**
     * 比较两个字段是否相等,忽略字段 ID。
     *
     * <p>在某些场景下(如 Schema 比较、兼容性检查),我们只关心字段的语义信息
     * (名称、类型、描述、默认值),而不关心字段 ID。字段 ID 是内部标识符,
     * 在不同的 Schema 版本中可能不同,即使字段在语义上是相同的。
     *
     * <p>此方法递归比较嵌套类型,也会忽略嵌套字段的 ID。
     *
     * @param other 要比较的目标字段
     * @return 如果忽略字段 ID 后两个字段相等则返回 true,否则返回 false
     */
    public boolean equalsIgnoreFieldId(DataField other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        return Objects.equals(name, other.name)
                && type.equalsIgnoreFieldId(other.type)
                && Objects.equals(description, other.description)
                && Objects.equals(defaultValue, other.defaultValue);
    }

    /**
     * 判断当前字段是否是目标字段经过裁剪后的结果,或者两者完全相同。
     *
     * <p>裁剪(Pruning)主要针对复杂类型(如 ROW)中的嵌套字段。例如,
     * 从一个包含 10 个嵌套字段的 ROW 类型字段中只选择 3 个嵌套字段,
     * 结果字段就是原字段的裁剪版本。
     *
     * <p>此方法会递归检查嵌套类型的裁剪关系。
     *
     * @param other 目标字段(可能是被裁剪前的字段)
     * @return 如果当前字段是目标字段的裁剪版本或两者相同则返回 true,否则返回 false
     */
    public boolean isPrunedFrom(DataField other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        return Objects.equals(id, other.id)
                && Objects.equals(name, other.name)
                && type.isPrunedFrom(other.type)
                && Objects.equals(description, other.description)
                && Objects.equals(defaultValue, other.defaultValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, type, description, defaultValue);
    }

    @Override
    public String toString() {
        return asSQLString();
    }

    /**
     * 比较两个字段是否相等,忽略字段 ID。
     *
     * <p>这是一个静态工具方法,用于方便地比较两个可能为 null 的字段。
     * 相比实例方法 {@link #equalsIgnoreFieldId(DataField)},此方法可以处理
     * dataField1 或 dataField2 为 null 的情况。
     *
     * <p>比较逻辑:
     * <ul>
     *     <li>如果两个字段引用相同,返回 true</li>
     *     <li>如果任一字段为 null 而另一个不为 null,返回 false</li>
     *     <li>如果两个字段都不为 null,比较名称、类型、描述和默认值(忽略 ID)</li>
     * </ul>
     *
     * <p>使用场景:
     * <ul>
     *     <li>Schema 演化: 判断两个 Schema 版本中的同名字段是否语义相同</li>
     *     <li>Schema 合并: 合并多个 Schema 时去重</li>
     *     <li>兼容性检查: 验证新旧 Schema 的兼容性</li>
     * </ul>
     *
     * @param dataField1 第一个字段,可为 null
     * @param dataField2 第二个字段,可为 null
     * @return 如果忽略字段 ID 后两个字段相等则返回 true,否则返回 false
     */
    public static boolean dataFieldEqualsIgnoreId(DataField dataField1, DataField dataField2) {
        if (dataField1 == dataField2) {
            return true;
        } else if (dataField1 != null && dataField2 != null) {
            return Objects.equals(dataField1.name(), dataField2.name())
                    && Objects.equals(dataField1.type(), dataField2.type())
                    && Objects.equals(dataField1.description(), dataField2.description())
                    && Objects.equals(dataField1.defaultValue(), dataField2.defaultValue());
        } else {
            return false;
        }
    }
}
