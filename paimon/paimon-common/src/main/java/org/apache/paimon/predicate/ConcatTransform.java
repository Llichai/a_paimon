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

/**
 * 字符串拼接转换。
 *
 * <p>将多个输入字符串拼接为一个字符串的Transform实现。
 * 这是 {@link StringTransform} 的一个具体实现,对应SQL中的CONCAT函数。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li>多字符串拼接 - 将多个字符串按顺序连接
 *   <li>空值处理 - 如果任一输入为null,结果为null
 *   <li>可变参数 - 支持任意数量的输入字符串
 *   <li>零拷贝优化 - 使用高效的字符串拼接实现
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 拼接两个字段
 * FieldRef firstName = new FieldRef(0, "first_name", DataTypes.STRING());
 * FieldRef lastName = new FieldRef(1, "last_name", DataTypes.STRING());
 *
 * Transform concatTransform = new ConcatTransform(
 *     Arrays.asList(firstName, lastName)
 * );
 *
 * // WHERE CONCAT(first_name, last_name) = 'JohnDoe'
 * Predicate predicate = builder.equal(concatTransform, "JohnDoe");
 *
 * // 2. 拼接三个字段
 * FieldRef middleName = new FieldRef(2, "middle_name", DataTypes.STRING());
 * Transform fullNameTransform = new ConcatTransform(
 *     Arrays.asList(firstName, middleName, lastName)
 * );
 *
 * // WHERE CONCAT(first_name, middle_name, last_name) = 'JohnMDoe'
 * Predicate fullNamePredicate = builder.equal(fullNameTransform, "JohnMDoe");
 *
 * // 3. 字段与常量拼接
 * BinaryString prefix = BinaryString.fromString("Mr.");
 * Transform titleTransform = new ConcatTransform(
 *     Arrays.asList(prefix, firstName, lastName)
 * );
 * // 结果: "Mr.JohnDoe"
 *
 * // 4. 执行转换
 * InternalRow row = ...; // first_name="John", last_name="Doe"
 * Object result = concatTransform.transform(row);
 * // result = "JohnDoe"
 * }</pre>
 *
 * <h2>SQL对应关系</h2>
 * <pre>
 * SQL: WHERE CONCAT(col1, col2, col3) = 'value'
 * Paimon: builder.equal(
 *     new ConcatTransform(Arrays.asList(field1, field2, field3)),
 *     "value"
 * )
 * </pre>
 *
 * <h2>应用场景</h2>
 * <ul>
 *   <li>全名匹配 - 拼接姓和名进行匹配
 *   <li>复合键查询 - 将多个字段组合成复合键进行过滤
 *   <li>地址拼接 - 拼接街道、城市、省份等字段
 *   <li>URL构建 - 拼接协议、域名、路径等组件
 * </ul>
 *
 * <h2>与ConcatWsTransform的区别</h2>
 * <ul>
 *   <li>无分隔符 - ConcatTransform直接拼接,无分隔符
 *   <li>ConcatWsTransform - 使用指定的分隔符连接字符串
 *   <li>空值处理 - CONCAT遇到null返回null,CONCAT_WS可以跳过null
 * </ul>
 *
 * <h2>空值处理</h2>
 * <p>如果任何输入字符串为null,整个拼接结果为null。
 * 这与SQL标准的CONCAT函数行为一致。
 *
 * <pre>{@code
 * // 示例:空值处理
 * inputs = ["John", null, "Doe"]
 * result = null  // 任一输入为null,结果为null
 * }</pre>
 *
 * <h2>性能考虑</h2>
 * <ul>
 *   <li>内存分配 - 拼接会创建新的字符串对象
 *   <li>拷贝开销 - 需要将所有输入字符串的内容拷贝到新字符串
 *   <li>输入数量 - 输入越多,拷贝开销越大
 * </ul>
 *
 * @see ConcatWsTransform
 * @see StringTransform
 * @see Transform
 */
public class ConcatTransform extends StringTransform {

    private static final long serialVersionUID = 1L;

    /** Transform名称,用于JSON序列化时的类型标识。 */
    public static final String NAME = "CONCAT";

    /**
     * 构造字符串拼接转换。
     *
     * @param inputs 输入参数列表,可以包含任意数量的字符串字段引用或字符串常量
     */
    @JsonCreator
    public ConcatTransform(
            @JsonProperty(StringTransform.FIELD_INPUTS)
                    @JsonDeserialize(contentUsing = StringTransform.InputDeserializer.class)
                    List<Object> inputs) {
        super(inputs);
    }

    @Override
    public String name() {
        return NAME;
    }

    /**
     * 执行字符串拼接。
     *
     * <p>将所有输入字符串按顺序拼接为一个字符串。
     * 使用 {@link BinaryString#concat(List)} 实现高效拼接。
     *
     * @param inputs 输入字符串列表
     * @return 拼接后的字符串,如果任一输入为null则返回null
     */
    @Override
    public BinaryString transform(List<BinaryString> inputs) {
        return BinaryString.concat(inputs);
    }

    /**
     * 使用新的输入创建ConcatTransform的副本。
     *
     * @param inputs 新的输入列表
     * @return 新的ConcatTransform实例
     */
    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        return new ConcatTransform(inputs);
    }
}
