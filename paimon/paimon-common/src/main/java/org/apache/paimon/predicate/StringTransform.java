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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.ObjectCodec;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.paimon.types.DataTypeFamily.CHARACTER_STRING;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 字符串 Transform 基类。
 *
 * <p>该抽象类为所有字符串输入和字符串输出的 Transform 提供了统一的基础设施。
 * 子类只需要实现具体的字符串转换逻辑。
 *
 * <h2>主要功能：</h2>
 * <ul>
 *   <li>统一处理字符串输入（字段引用和字面量）
 *   <li>提供 JSON 序列化/反序列化支持
 *   <li>类型验证（确保所有输入都是字符串类型）
 *   <li>运行时值提取和转换
 * </ul>
 *
 * <h2>子类实现模式：</h2>
 * <p>子类需要：
 * <ol>
 *   <li>继承 StringTransform
 *   <li>提供构造函数（通常接受 List<Object> inputs）
 *   <li>实现 {@link #transform(List)} 方法进行实际的字符串转换
 *   <li>实现 {@link #copyWithNewInputs(List)} 方法用于克隆
 *   <li>实现 {@link #name()} 方法返回 Transform 名称
 * </ol>
 *
 * <h2>使用示例：</h2>
 * <pre>{@code
 * // 自定义字符串 Transform
 * public class UpperCaseTransform extends StringTransform {
 *     public UpperCaseTransform(List<Object> inputs) {
 *         super(inputs);  // 基类会验证类型
 *     }
 *
 *     @Override
 *     public String name() {
 *         return "UPPER";
 *     }
 *
 *     @Override
 *     protected BinaryString transform(List<BinaryString> inputs) {
 *         // inputs[0] 已经是 BinaryString 类型
 *         return BinaryString.fromString(inputs.get(0).toString().toUpperCase());
 *     }
 *
 *     @Override
 *     public Transform copyWithNewInputs(List<Object> inputs) {
 *         return new UpperCaseTransform(inputs);
 *     }
 * }
 *
 * // 使用自定义 Transform
 * List<Object> inputs = Arrays.asList(
 *     new FieldRef(0, "name", DataTypes.STRING())
 * );
 * Transform upper = new UpperCaseTransform(inputs);
 *
 * // 在谓词中使用
 * PredicateBuilder builder = new PredicateBuilder(rowType);
 * Predicate predicate = builder.equal(upper, "JOHN");
 * // WHERE UPPER(name) = 'JOHN'
 * }</pre>
 *
 * <h2>输入类型：</h2>
 * <p>输入列表中的元素可以是：
 * <ul>
 *   <li>{@link FieldRef}: 字段引用，类型必须是 CHARACTER_STRING 家族
 *   <li>{@link BinaryString}: 字符串字面量
 *   <li>{@code null}: 允许 null 值（子类需要正确处理）
 * </ul>
 *
 * <h2>类型验证：</h2>
 * <p>构造函数会验证所有输入：
 * <pre>{@code
 * // 合法输入
 * List<Object> valid = Arrays.asList(
 *     new FieldRef(0, "str1", DataTypes.STRING()),
 *     new FieldRef(1, "str2", DataTypes.VARCHAR(100)),
 *     BinaryString.fromString("literal"),
 *     null  // 允许
 * );
 *
 * // 非法输入（会抛出异常）
 * List<Object> invalid = Arrays.asList(
 *     new FieldRef(0, "num", DataTypes.INT())  // 不是字符串类型
 * );
 * }</pre>
 *
 * <h2>JSON 序列化：</h2>
 * <p>JSON 序列化时，{@link BinaryString} 会被转换为普通字符串：
 * <pre>{@code
 * // 内存中的输入
 * List<Object> inputs = Arrays.asList(
 *     new FieldRef(0, "name", DataTypes.STRING()),
 *     BinaryString.fromString("suffix")
 * );
 *
 * // JSON 表示
 * {
 *   "inputs": [
 *     {"index": 0, "name": "name", "type": "STRING"},
 *     "suffix"  // BinaryString 序列化为字符串
 *   ]
 * }
 *
 * // 反序列化时自动转回 BinaryString
 * }</pre>
 *
 * <h2>运行时执行：</h2>
 * <p>{@link #transform(InternalRow)} 方法会：
 * <ol>
 *   <li>从行中提取字段值或使用字面量
 *   <li>将所有值转换为 {@link BinaryString}
 *   <li>调用子类的 {@link #transform(List)} 方法
 *   <li>返回转换结果
 * </ol>
 *
 * <h2>内置子类：</h2>
 * <ul>
 *   <li>{@link ConcatTransform}: 字符串拼接（无分隔符）
 *   <li>{@link ConcatWsTransform}: 字符串拼接（带分隔符）
 *   <li>{@link SubstringTransform}: 字符串截取
 *   <li>{@link TrimTransform}: 去除空格
 * </ul>
 *
 * @see Transform Transform 接口
 * @see FieldRef 字段引用
 * @see BinaryString 内部字符串表示
 */
public abstract class StringTransform implements Transform {

    private static final long serialVersionUID = 1L;

    /** JSON 字段名：输入列表。 */
    static final String FIELD_INPUTS = "inputs";

    /** 输入列表（FieldRef 或 BinaryString）。 */
    private final List<Object> inputs;

    /**
     * 构造字符串 Transform。
     *
     * <p>会验证所有输入都是字符串类型。
     *
     * @param inputs 输入列表
     * @throws IllegalArgumentException 如果输入类型不正确
     */
    public StringTransform(List<Object> inputs) {
        this.inputs = inputs;
        for (Object input : inputs) {
            if (input == null) {
                continue;
            }
            if (input instanceof FieldRef) {
                FieldRef ref = (FieldRef) input;
                checkArgument(ref.type().is(CHARACTER_STRING));
            } else {
                checkArgument(input instanceof BinaryString);
            }
        }
    }

    /**
     * JSON 输入反序列化器。
     *
     * <p>支持两种输入格式：
     * <ul>
     *   <li>字符串：转换为 {@link BinaryString}
     *   <li>对象：转换为 {@link FieldRef}
     * </ul>
     */
    public static class InputDeserializer extends JsonDeserializer<Object> implements Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public Object deserialize(JsonParser parser, DeserializationContext context)
                throws java.io.IOException {
            ObjectCodec codec = parser.getCodec();
            JsonNode node = codec.readTree(parser);

            if (node == null || node.isNull()) {
                return null;
            }

            if (node.isTextual()) {
                return BinaryString.fromString(node.asText());
            }

            if (node.isObject()) {
                try {
                    return codec.treeToValue(node, FieldRef.class);
                } catch (Exception e) {
                    context.reportInputMismatch(
                            Object.class,
                            "Failed to deserialize StringTransform input as FieldRef: %s",
                            node.toString());
                }
            }

            context.reportInputMismatch(
                    Object.class, "Unsupported StringTransform input JSON: %s", node.toString());
            return null;
        }
    }

    /**
     * 获取输入列表（用于内部使用）。
     *
     * @return 输入列表
     */
    @Override
    @JsonIgnore
    public final List<Object> inputs() {
        return inputs;
    }

    /**
     * 获取输入列表（用于 JSON 序列化）。
     *
     * <p>将 {@link BinaryString} 转换为普通字符串。
     *
     * @return 可序列化的输入列表
     */
    @JsonGetter(FIELD_INPUTS)
    public final List<Object> inputsForJson() {
        List<Object> serialized = new ArrayList<>(inputs.size());
        for (Object input : inputs) {
            if (input instanceof BinaryString) {
                serialized.add(input.toString());
            } else {
                serialized.add(input);
            }
        }
        return serialized;
    }

    /**
     * 输出类型始终是 STRING。
     *
     * @return STRING 类型
     */
    @Override
    public final DataType outputType() {
        return DataTypes.STRING();
    }

    /**
     * 对行数据执行字符串转换。
     *
     * <p>从行中提取所有字段值，转换为 {@link BinaryString}，然后调用子类的转换方法。
     *
     * @param row 输入行
     * @return 转换后的字符串
     */
    @Override
    public final Object transform(InternalRow row) {
        List<BinaryString> strings = new ArrayList<>(inputs.size());
        for (Object input : inputs) {
            if (input instanceof FieldRef) {
                FieldRef ref = (FieldRef) input;
                int i = ref.index();
                strings.add(row.isNullAt(i) ? null : row.getString(i));
            } else {
                strings.add((BinaryString) input);
            }
        }
        return transform(strings);
    }

    /**
     * 子类实现：对字符串列表执行转换。
     *
     * @param inputs 输入字符串列表（可能包含 null）
     * @return 转换后的字符串
     */
    protected abstract BinaryString transform(List<BinaryString> inputs);

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StringTransform that = (StringTransform) o;
        return Objects.equals(inputs, that.inputs);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(inputs);
    }

    @Override
    public String toString() {
        List<String> inputs =
                this.inputs.stream().map(Object::toString).collect(Collectors.toList());
        return name() + "(" + String.join(", ", inputs) + ')';
    }
}
