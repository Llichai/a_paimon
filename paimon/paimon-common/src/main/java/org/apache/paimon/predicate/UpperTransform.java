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

import org.apache.paimon.data.BinaryString;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 大写转换。
 *
 * <p>将输入字符串转换为大写形式的Transform实现。
 * 这是 {@link StringTransform} 的一个具体实现,对应SQL中的UPPER函数。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li>字符串转大写 - 将所有字符转换为大写形式
 *   <li>空值安全 - 输入为null时返回null
 *   <li>Unicode支持 - 正确处理Unicode字符
 *   <li>不可变性 - 不修改原字符串,返回新字符串
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 基本用法
 * FieldRef nameField = new FieldRef(0, "name", DataTypes.STRING());
 * Transform upperTransform = new UpperTransform(
 *     Collections.singletonList(nameField)
 * );
 *
 * // WHERE UPPER(name) = 'ALICE'
 * Predicate predicate = builder.equal(upperTransform, "ALICE");
 *
 * // 2. 大小写不敏感匹配
 * // WHERE UPPER(name) = UPPER('alice')
 * // 实际比较时都转换为大写,实现不区分大小写的匹配
 * Predicate caseInsensitive = builder.equal(upperTransform, "ALICE");
 *
 * // 3. 组合使用
 * // WHERE UPPER(name) LIKE 'A%'
 * Predicate startsWithA = builder.startsWith(upperTransform, "A");
 *
 * // 4. 执行转换
 * InternalRow row = ...; // name = "alice"
 * Object result = upperTransform.transform(row);
 * // result = "ALICE"
 * }</pre>
 *
 * <h2>SQL对应关系</h2>
 * <pre>
 * SQL: WHERE UPPER(column_name) = 'VALUE'
 * Paimon: builder.equal(
 *     new UpperTransform(Collections.singletonList(fieldRef)),
 *     "VALUE"
 * )
 * </pre>
 *
 * <h2>应用场景</h2>
 * <ul>
 *   <li>大小写不敏感查询 - 实现不区分大小写的字符串比较
 *   <li>标准化过滤 - 将数据标准化为大写后进行过滤
 *   <li>字符串匹配 - 配合LIKE谓词进行模式匹配
 *   <li>数据清洗 - 在谓词中进行数据标准化
 * </ul>
 *
 * <h2>性能考虑</h2>
 * <ul>
 *   <li>字符串创建 - 每次转换都会创建新的字符串对象
 *   <li>无法使用索引 - 在UPPER(column)上的谓词通常无法使用列索引
 *   <li>CPU开销 - 大写转换需要遍历字符串的所有字符
 * </ul>
 *
 * <h2>限制</h2>
 * <ul>
 *   <li>单输入 - 只接受一个输入参数
 *   <li>字符串类型 - 输入必须是字符串类型
 *   <li>无Locale - 使用默认Locale进行大小写转换
 * </ul>
 *
 * @see LowerTransform
 * @see StringTransform
 * @see Transform
 */
public class UpperTransform extends StringTransform {

    private static final long serialVersionUID = 1L;

    /** Transform名称,用于JSON序列化时的类型标识。 */
    public static final String NAME = "UPPER";

    /**
     * 构造大写转换。
     *
     * @param inputs 输入参数列表,必须包含恰好一个字符串字段引用或字符串常量
     */
    @JsonCreator
    public UpperTransform(
            @JsonProperty(StringTransform.FIELD_INPUTS)
                    @JsonDeserialize(contentUsing = StringTransform.InputDeserializer.class)
                    List<Object> inputs) {
        super(inputs);
        checkArgument(inputs.size() == 1);
    }

    @Override
    public String name() {
        return NAME;
    }

    /**
     * 执行大写转换。
     *
     * @param inputs 输入字符串列表,包含一个字符串
     * @return 转换为大写的字符串,如果输入为null则返回null
     */
    @Override
    public BinaryString transform(List<BinaryString> inputs) {
        BinaryString string = inputs.get(0);
        if (string == null) {
            return null;
        }
        return string.toUpperCase();
    }

    /**
     * 使用新的输入创建UpperTransform的副本。
     *
     * @param inputs 新的输入列表
     * @return 新的UpperTransform实例
     */
    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        return new UpperTransform(inputs);
    }
}
