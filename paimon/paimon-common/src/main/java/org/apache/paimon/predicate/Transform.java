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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;
import java.util.List;

/**
 * 转换函数接口。
 *
 * <p>表示对数据行进行转换的函数,可以从行中提取字段、进行类型转换或执行字符串操作。
 * Transform是谓词系统中的重要组件,允许谓词作用于计算字段而不仅仅是原始字段。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li>字段提取 - 从InternalRow中提取指定字段的值
 *   <li>类型转换 - 将字段值转换为其他类型
 *   <li>字符串操作 - 对字符串进行大小写转换、拼接、截取等操作
 *   <li>表达式计算 - 支持基于字段的简单表达式
 * </ul>
 *
 * <h2>内置实现</h2>
 * <ul>
 *   <li>{@link FieldTransform} - 字段引用,直接提取字段值
 *   <li>{@link CastTransform} - 类型转换
 *   <li>{@link ConcatTransform} - 字符串拼接
 *   <li>{@link ConcatWsTransform} - 使用分隔符的字符串拼接
 *   <li>{@link UpperTransform} - 转换为大写
 *   <li>{@link LowerTransform} - 转换为小写
 *   <li>{@link SubstringTransform} - 字符串截取
 * </ul>
 *
 * <h2>JSON序列化</h2>
 * <p>使用Jackson的多态序列化支持:
 * <ul>
 *   <li>{@code @JsonTypeInfo} - 在JSON中包含类型信息
 *   <li>{@code @JsonSubTypes} - 注册所有子类型
 *   <li>类型标识符存储在"name"属性中
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 字段引用转换
 * FieldRef nameField = new FieldRef(0, "name", DataTypes.STRING());
 * Transform fieldTransform = new FieldTransform(nameField);
 *
 * // 在谓词中使用
 * Predicate predicate = builder.equal(fieldTransform, "Alice");
 * // SQL: WHERE name = 'Alice'
 *
 * // 2. 字符串转大写
 * Transform upperTransform = new UpperTransform(
 *     Collections.singletonList(nameField)
 * );
 * Predicate upperPredicate = builder.equal(upperTransform, "ALICE");
 * // SQL: WHERE UPPER(name) = 'ALICE'
 *
 * // 3. 字符串拼接
 * FieldRef firstName = new FieldRef(0, "first_name", DataTypes.STRING());
 * FieldRef lastName = new FieldRef(1, "last_name", DataTypes.STRING());
 * Transform concatTransform = new ConcatTransform(
 *     Arrays.asList(firstName, lastName)
 * );
 * Predicate concatPredicate = builder.equal(concatTransform, "JohnDoe");
 * // SQL: WHERE CONCAT(first_name, last_name) = 'JohnDoe'
 *
 * // 4. 类型转换
 * FieldRef ageField = new FieldRef(2, "age", DataTypes.INT());
 * Transform castTransform = new CastTransform(ageField, DataTypes.BIGINT());
 * // 将INT字段转换为BIGINT
 *
 * // 5. 执行转换
 * InternalRow row = ...; // 包含数据的行
 * Object result = fieldTransform.transform(row); // 执行转换获取结果
 * }</pre>
 *
 * <h2>应用场景</h2>
 * <ul>
 *   <li>计算列谓词 - 对计算字段应用过滤条件
 *   <li>大小写不敏感匹配 - 使用UPPER/LOWER转换实现
 *   <li>组合字段过滤 - 对多个字段的组合值进行过滤
 *   <li>类型自适应 - 在谓词评估前进行必要的类型转换
 * </ul>
 *
 * <h2>不可变性</h2>
 * <p>Transform对象应该是不可变的。当需要修改输入时,
 * 使用 {@link #copyWithNewInputs(List)} 创建新实例。
 *
 * @see FieldTransform
 * @see CastTransform
 * @see StringTransform
 * @see LeafPredicate
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = Transform.FIELD_NAME)
@JsonSubTypes({
    @JsonSubTypes.Type(value = FieldTransform.class, name = FieldTransform.NAME),
    @JsonSubTypes.Type(value = CastTransform.class, name = CastTransform.NAME),
    @JsonSubTypes.Type(value = ConcatTransform.class, name = ConcatTransform.NAME),
    @JsonSubTypes.Type(value = ConcatWsTransform.class, name = ConcatWsTransform.NAME),
    @JsonSubTypes.Type(value = UpperTransform.class, name = UpperTransform.NAME),
    @JsonSubTypes.Type(value = LowerTransform.class, name = LowerTransform.NAME)
})
public interface Transform extends Serializable {

    /** JSON序列化时的类型字段名称。 */
    String FIELD_NAME = "name";

    /**
     * 获取转换函数的名称。
     *
     * <p>名称用于标识转换类型,在JSON序列化时作为类型标识符。
     *
     * @return 转换函数名称,如"FIELD_REF"、"UPPER"、"CONCAT"等
     */
    String name();

    /**
     * 获取转换函数的输入参数。
     *
     * <p>输入可以是:
     * <ul>
     *   <li>{@link FieldRef} - 字段引用
     *   <li>字面量值 - 用于常量参数
     *   <li>其他Transform - 支持嵌套转换
     * </ul>
     *
     * @return 输入参数列表
     */
    List<Object> inputs();

    /**
     * 获取转换函数的输出类型。
     *
     * <p>输出类型由转换函数的语义决定:
     * <ul>
     *   <li>FieldTransform - 返回字段类型
     *   <li>CastTransform - 返回目标类型
     *   <li>UpperTransform/LowerTransform - 返回STRING类型
     *   <li>ConcatTransform - 返回STRING类型
     * </ul>
     *
     * @return 转换结果的数据类型
     */
    DataType outputType();

    /**
     * 对指定的行执行转换。
     *
     * <p>从输入行中提取字段值,执行转换操作,返回转换结果。
     *
     * @param row 输入数据行
     * @return 转换后的值,可能为null
     */
    Object transform(InternalRow row);

    /**
     * 使用新的输入参数创建Transform的副本。
     *
     * <p>此方法用于实现Transform的不可变性。当需要修改输入时,
     * 创建一个新的Transform实例而不是修改现有实例。
     *
     * @param inputs 新的输入参数列表
     * @return 具有新输入的Transform实例
     */
    Transform copyWithNewInputs(List<Object> inputs);
}
