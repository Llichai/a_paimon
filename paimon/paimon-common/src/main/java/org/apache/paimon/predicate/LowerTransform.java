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
 * 小写转换。
 *
 * <p>将输入字符串转换为小写形式的Transform实现。
 * 这是 {@link StringTransform} 的一个具体实现,对应SQL中的LOWER函数。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li>字符串转小写 - 将所有字符转换为小写形式
 *   <li>空值安全 - 输入为null时返回null
 *   <li>Unicode支持 - 正确处理Unicode字符
 *   <li>不可变性 - 不修改原字符串,返回新字符串
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 基本用法
 * FieldRef nameField = new FieldRef(0, "name", DataTypes.STRING());
 * Transform lowerTransform = new LowerTransform(
 *     Collections.singletonList(nameField)
 * );
 *
 * // WHERE LOWER(name) = 'alice'
 * Predicate predicate = builder.equal(lowerTransform, "alice");
 *
 * // 2. 大小写不敏感匹配
 * // WHERE LOWER(name) = LOWER('Alice')
 * // 实际比较时都转换为小写,实现不区分大小写的匹配
 * Predicate caseInsensitive = builder.equal(lowerTransform, "alice");
 *
 * // 3. 组合使用
 * // WHERE LOWER(email) LIKE '%@example.com'
 * Predicate emailFilter = builder.endsWith(lowerTransform, "@example.com");
 *
 * // 4. 执行转换
 * InternalRow row = ...; // name = "ALICE"
 * Object result = lowerTransform.transform(row);
 * // result = "alice"
 * }</pre>
 *
 * <h2>SQL对应关系</h2>
 * <pre>
 * SQL: WHERE LOWER(column_name) = 'value'
 * Paimon: builder.equal(
 *     new LowerTransform(Collections.singletonList(fieldRef)),
 *     "value"
 * )
 * </pre>
 *
 * <h2>应用场景</h2>
 * <ul>
 *   <li>大小写不敏感查询 - 实现不区分大小写的字符串比较
 *   <li>Email/用户名匹配 - 邮箱和用户名通常不区分大小写
 *   <li>标准化过滤 - 将数据标准化为小写后进行过滤
 *   <li>模式匹配 - 配合LIKE谓词进行大小写不敏感的模式匹配
 * </ul>
 *
 * <h2>与UpperTransform的对比</h2>
 * <ul>
 *   <li>功能对称 - LOWER和UPPER功能完全对称
 *   <li>可互换 - 在大小写不敏感匹配中,两者可互换使用
 *   <li>约定优先 - 选择LOWER还是UPPER取决于数据约定
 * </ul>
 *
 * <h2>性能考虑</h2>
 * <ul>
 *   <li>字符串创建 - 每次转换都会创建新的字符串对象
 *   <li>无法使用索引 - 在LOWER(column)上的谓词通常无法使用列索引
 *   <li>CPU开销 - 小写转换需要遍历字符串的所有字符
 * </ul>
 *
 * <h2>限制</h2>
 * <ul>
 *   <li>单输入 - 只接受一个输入参数
 *   <li>字符串类型 - 输入必须是字符串类型
 *   <li>无Locale - 使用默认Locale进行大小写转换
 * </ul>
 *
 * @see UpperTransform
 * @see StringTransform
 * @see Transform
 */
public class LowerTransform extends StringTransform {

    private static final long serialVersionUID = 1L;

    /** Transform名称,用于JSON序列化时的类型标识。 */
    public static final String NAME = "LOWER";

    /**
     * 构造小写转换。
     *
     * @param inputs 输入参数列表,必须包含恰好一个字符串字段引用或字符串常量
     */
    @JsonCreator
    public LowerTransform(
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
     * 执行小写转换。
     *
     * @param inputs 输入字符串列表,包含一个字符串
     * @return 转换为小写的字符串,如果输入为null则返回null
     */
    @Override
    public BinaryString transform(List<BinaryString> inputs) {
        BinaryString string = inputs.get(0);
        if (string == null) {
            return null;
        }
        return string.toLowerCase();
    }

    /**
     * 使用新的输入创建LowerTransform的副本。
     *
     * @param inputs 新的输入列表
     * @return 新的LowerTransform实例
     */
    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        return new LowerTransform(inputs);
    }
}
