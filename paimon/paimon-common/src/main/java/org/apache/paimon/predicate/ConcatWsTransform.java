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
 * 带分隔符拼接 Transform。
 *
 * <p>该 Transform 使用指定的分隔符连接多个字符串字段，类似于 SQL 的 {@code CONCAT_WS} 函数。
 *
 * <h2>主要功能：</h2>
 * <ul>
 *   <li>使用自定义分隔符连接字符串
 *   <li>自动跳过 NULL 值
 *   <li>支持字段引用和字面量混合
 *   <li>至少需要 2 个输入（分隔符 + 1个字符串）
 * </ul>
 *
 * <h2>与 ConcatTransform 的区别：</h2>
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>CONCAT_WS</th>
 *     <th>CONCAT</th>
 *   </tr>
 *   <tr>
 *     <td>分隔符</td>
 *     <td>有（第一个参数）</td>
 *     <td>无</td>
 *   </tr>
 *   <tr>
 *     <td>NULL 处理</td>
 *     <td>跳过 NULL</td>
 *     <td>NULL → 空字符串</td>
 *   </tr>
 *   <tr>
 *     <td>最少参数</td>
 *     <td>2（分隔符 + 1个字符串）</td>
 *     <td>1</td>
 *   </tr>
 * </table>
 *
 * <h2>使用示例：</h2>
 * <pre>{@code
 * // 示例 1：基本用法
 * // CONCAT_WS('-', first_name, last_name)
 * List<Object> inputs1 = Arrays.asList(
 *     BinaryString.fromString("-"),                          // 分隔符
 *     new FieldRef(0, "first_name", DataTypes.STRING()),     // 字段1
 *     new FieldRef(1, "last_name", DataTypes.STRING())       // 字段2
 * );
 * ConcatWsTransform transform1 = new ConcatWsTransform(inputs1);
 * // 输入：first_name='John', last_name='Doe'
 * // 输出：'John-Doe'
 *
 * // 示例 2：混合字面量
 * // CONCAT_WS(', ', city, 'China')
 * List<Object> inputs2 = Arrays.asList(
 *     BinaryString.fromString(", "),                      // 分隔符
 *     new FieldRef(2, "city", DataTypes.STRING()),        // 字段
 *     BinaryString.fromString("China")                    // 字面量
 * );
 * ConcatWsTransform transform2 = new ConcatWsTransform(inputs2);
 * // 输入：city='Beijing'
 * // 输出：'Beijing, China'
 *
 * // 示例 3：NULL 值处理
 * // CONCAT_WS('-', field1, field2, field3)
 * // 输入：field1='A', field2=NULL, field3='C'
 * // 输出：'A-C'  （跳过 NULL）
 *
 * // 示例 4：在谓词中使用
 * // WHERE CONCAT_WS(' ', first_name, last_name) = 'John Doe'
 * PredicateBuilder builder = new PredicateBuilder(rowType);
 * List<Object> inputs = Arrays.asList(
 *     BinaryString.fromString(" "),
 *     new FieldRef(0, "first_name", DataTypes.STRING()),
 *     new FieldRef(1, "last_name", DataTypes.STRING())
 * );
 * Transform concatWs = new ConcatWsTransform(inputs);
 * Predicate predicate = builder.equal(concatWs, "John Doe");
 *
 * // 示例 5：动态分隔符（从字段读取）
 * // CONCAT_WS(separator_field, field1, field2)
 * List<Object> inputs5 = Arrays.asList(
 *     new FieldRef(0, "separator", DataTypes.STRING()),  // 分隔符也可以是字段
 *     new FieldRef(1, "field1", DataTypes.STRING()),
 *     new FieldRef(2, "field2", DataTypes.STRING())
 * );
 * ConcatWsTransform transform5 = new ConcatWsTransform(inputs5);
 * }</pre>
 *
 * <h2>输入要求：</h2>
 * <ul>
 *   <li>至少 2 个输入（分隔符 + 1个待拼接字符串）
 *   <li>第一个输入是分隔符
 *   <li>所有输入必须是字符串类型（STRING、CHAR、VARCHAR）
 *   <li>输入可以是 {@link FieldRef} 或 {@link BinaryString} 字面量
 * </ul>
 *
 * <h2>分隔符说明：</h2>
 * <ul>
 *   <li>分隔符在运行时从第一个输入获取
 *   <li>可以是字段引用（动态分隔符）或字面量（固定分隔符）
 *   <li>分隔符可以是空字符串
 * </ul>
 *
 * <h2>NULL 值处理：</h2>
 * <ul>
 *   <li>如果分隔符为 NULL，结果为 NULL
 *   <li>字符串字段的 NULL 值会被跳过，不参与拼接
 *   <li>如果所有字符串字段都是 NULL，结果为空字符串
 * </ul>
 *
 * @see ConcatTransform 无分隔符的字符串拼接
 * @see StringTransform 字符串 Transform 的基类
 */
public class ConcatWsTransform extends StringTransform {

    private static final long serialVersionUID = 1L;

    /** Transform 名称常量。 */
    public static final String NAME = "CONCAT_WS";

    /**
     * 构造带分隔符拼接 Transform。
     *
     * @param inputs 输入列表（至少2个：分隔符 + 字符串）
     * @throws IllegalArgumentException 如果输入少于2个
     */
    @JsonCreator
    public ConcatWsTransform(
            @JsonProperty(StringTransform.FIELD_INPUTS)
                    @JsonDeserialize(contentUsing = StringTransform.InputDeserializer.class)
                    List<Object> inputs) {
        super(inputs);
        checkArgument(inputs.size() >= 2);
    }

    @Override
    public String name() {
        return NAME;
    }

    /**
     * 使用分隔符连接字符串。
     *
     * @param inputs 输入字符串列表（第一个是分隔符）
     * @return 拼接后的字符串
     */
    @Override
    public BinaryString transform(List<BinaryString> inputs) {
        BinaryString separator = inputs.get(0);
        return BinaryString.concatWs(separator, inputs.subList(1, inputs.size()));
    }

    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        return new ConcatWsTransform(inputs);
    }
}
