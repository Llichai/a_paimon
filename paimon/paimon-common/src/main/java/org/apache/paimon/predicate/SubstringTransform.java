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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import java.util.List;
import java.util.Objects;

import static org.apache.paimon.types.DataTypeFamily.CHARACTER_STRING;
import static org.apache.paimon.types.DataTypeFamily.INTEGER_NUMERIC;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 字符串截取转换。
 *
 * <p>从字符串中提取子串的Transform实现,对应SQL中的SUBSTRING函数。
 * 支持指定起始位置和可选的长度参数。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li>位置截取 - 从指定位置开始提取子串
 *   <li>长度控制 - 可选的长度参数控制提取的字符数
 *   <li>边界处理 - 正确处理越界情况
 *   <li>空值安全 - 源字符串为null时返回null
 * </ul>
 *
 * <h2>参数说明</h2>
 * <ul>
 *   <li>第1个参数(必需) - 源字符串(FieldRef或BinaryString常量)
 *   <li>第2个参数(必需) - 起始位置(从1开始,FieldRef或整数常量)
 *   <li>第3个参数(可选) - 提取长度(FieldRef或整数常量)
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 从位置3开始提取到末尾
 * FieldRef emailField = new FieldRef(0, "email", DataTypes.STRING());
 * Transform substringTransform = new SubstringTransform(
 *     Arrays.asList(emailField, 3)  // SUBSTRING(email, 3)
 * );
 *
 * // WHERE SUBSTRING(email, 3) = 'hn@example.com'
 * Predicate predicate = builder.equal(substringTransform, "hn@example.com");
 * // email="john@example.com" -> "hn@example.com"
 *
 * // 2. 提取指定长度的子串
 * Transform substringLengthTransform = new SubstringTransform(
 *     Arrays.asList(emailField, 1, 4)  // SUBSTRING(email, 1, 4)
 * );
 *
 * // WHERE SUBSTRING(email, 1, 4) = 'john'
 * Predicate lengthPredicate = builder.equal(substringLengthTransform, "john");
 * // email="john@example.com" -> "john"
 *
 * // 3. 提取域名部分(使用字段作为位置)
 * FieldRef atPosition = new FieldRef(1, "at_pos", DataTypes.INT());
 * Transform domainTransform = new SubstringTransform(
 *     Arrays.asList(emailField, atPosition)
 * );
 * // at_pos=5 时,email="john@example.com" -> "@example.com"
 *
 * // 4. 执行转换
 * InternalRow row = ...; // email = "john@example.com"
 * Object result = substringLengthTransform.transform(row);
 * // result = "john"
 * }</pre>
 *
 * <h2>SQL对应关系</h2>
 * <pre>
 * -- 两参数形式
 * SQL: WHERE SUBSTRING(column, start_pos) = 'value'
 * Paimon: builder.equal(
 *     new SubstringTransform(Arrays.asList(fieldRef, start)),
 *     "value"
 * )
 *
 * -- 三参数形式
 * SQL: WHERE SUBSTRING(column, start_pos, length) = 'value'
 * Paimon: builder.equal(
 *     new SubstringTransform(Arrays.asList(fieldRef, start, length)),
 *     "value"
 * )
 * </pre>
 *
 * <h2>索引说明</h2>
 * <p>起始位置使用基于1的索引(与SQL标准一致):
 * <ul>
 *   <li>位置1 - 字符串的第一个字符
 *   <li>位置2 - 字符串的第二个字符
 *   <li>依此类推
 * </ul>
 *
 * <h2>边界处理</h2>
 * <ul>
 *   <li>起始位置超过字符串长度 - 返回空字符串
 *   <li>长度超过剩余字符 - 截取到字符串末尾
 *   <li>负数位置或长度 - 行为未定义(会抛出异常)
 * </ul>
 *
 * <h2>应用场景</h2>
 * <ul>
 *   <li>Email域名提取 - 提取@符号后的域名部分
 *   <li>前缀匹配 - 提取固定长度的前缀进行匹配
 *   <li>编码提取 - 从复合编码中提取特定部分
 *   <li>URL解析 - 提取URL的特定部分
 * </ul>
 *
 * <h2>性能考虑</h2>
 * <ul>
 *   <li>字符串创建 - 每次截取都会创建新字符串对象
 *   <li>Unicode处理 - 正确处理多字节字符可能有额外开销
 *   <li>动态参数 - 使用FieldRef作为位置/长度会有额外的字段访问开销
 * </ul>
 *
 * <h2>限制</h2>
 * <ul>
 *   <li>参数数量 - 必须是2个或3个参数
 *   <li>类型约束 - 源必须是字符串,位置和长度必须是整数
 *   <li>不支持负数索引 - 不像某些语言支持从末尾开始的负数索引
 * </ul>
 *
 * @see StringTransform
 * @see Transform
 */
public class SubstringTransform implements Transform {

    private static final long serialVersionUID = 1L;

    /** Transform名称,用于JSON序列化时的类型标识。 */
    public static final String NAME = "SUBSTRING";

    /** 输入参数列表。 */
    private final List<Object> inputs;

    /**
     * 构造字符串截取转换。
     *
     * @param inputs 输入参数列表,包含2个或3个元素:
     *               <ul>
     *                 <li>inputs[0] - 源字符串(FieldRef或BinaryString)
     *                 <li>inputs[1] - 起始位置(FieldRef或Integer,从1开始)
     *                 <li>inputs[2] - 可选的长度(FieldRef或Integer)
     *               </ul>
     * @throws IllegalArgumentException 如果inputs大小不是2或3
     */
    public SubstringTransform(List<Object> inputs) {
        checkArgument(inputs.size() == 2 || inputs.size() == 3);
        this.inputs = inputs;
    }

    @Override
    public String name() {
        return NAME;
    }

    /**
     * 从数据行中执行字符串截取。
     *
     * <p>实现步骤:
     * <ol>
     *   <li>从输入中提取源字符串
     *   <li>提取起始位置(将基于1的索引转换为基于0的索引)
     *   <li>如果提供了长度参数,计算结束位置
     *   <li>执行子串提取
     * </ol>
     *
     * @param row 输入数据行
     * @return 提取的子串,源为null时返回null,起始位置超出范围时返回空字符串
     */
    @Override
    public final Object transform(InternalRow row) {
        Object source = inputs.get(0);
        BinaryString sourceString = null;
        if (source instanceof FieldRef) {
            FieldRef sourceFieldRef = (FieldRef) source;
            checkArgument(sourceFieldRef.type().is(CHARACTER_STRING));
            sourceString = row.isNullAt(0) ? null : row.getString(sourceFieldRef.index());
        } else {
            sourceString = (BinaryString) inputs.get(0);
        }
        if (sourceString == null) {
            return sourceString;
        }

        String sourceJavaString = sourceString.toString();
        Object begin = inputs.get(1);
        int beginIndex;
        if (begin instanceof FieldRef) {
            FieldRef beginRef = (FieldRef) begin;
            checkArgument(beginRef.type().is(INTEGER_NUMERIC));
            beginIndex = row.getInt(beginRef.index());
        } else {
            beginIndex = Integer.parseInt(inputs.get(1).toString());
        }
        if (beginIndex > sourceJavaString.length()) {
            return BinaryString.EMPTY_UTF8;
        }

        int endIndex = sourceJavaString.length();
        if (inputs.size() == 3) {
            Object end = inputs.get(2);
            if (end instanceof FieldRef) {
                FieldRef endRef = (FieldRef) inputs.get(2);
                checkArgument(endRef.type().is(INTEGER_NUMERIC));
                endIndex = beginIndex + row.getInt(endRef.index()) - 1;
            } else {
                endIndex = beginIndex + Integer.parseInt(inputs.get(2).toString()) - 1;
            }
        }
        endIndex = Math.min(endIndex, sourceJavaString.length());
        beginIndex--;
        checkArgument(beginIndex < endIndex);

        return BinaryString.fromString(sourceJavaString.substring(beginIndex, endIndex));
    }

    /**
     * 使用新的输入创建SubstringTransform的副本。
     *
     * @param inputs 新的输入列表
     * @return 新的SubstringTransform实例
     */
    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        return new SubstringTransform(inputs);
    }

    @Override
    public final List<Object> inputs() {
        return inputs;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubstringTransform that = (SubstringTransform) o;
        return Objects.equals(inputs, that.inputs);
    }

    /**
     * 获取输出类型。
     *
     * <p>SUBSTRING函数总是返回STRING类型。
     *
     * @return STRING类型
     */
    @Override
    public DataType outputType() {
        return DataTypes.STRING();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(inputs);
    }
}
