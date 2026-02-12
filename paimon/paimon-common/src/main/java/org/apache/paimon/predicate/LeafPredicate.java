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

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.ListSerializer;
import org.apache.paimon.data.serializer.NullableSerializer;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.paimon.utils.InternalRowUtils.get;

/**
 * 谓词树的叶子节点,表示对字段进行的基本比较操作。
 *
 * <p>叶子谓词是谓词表达式的最基本单元,将数据行中的字段(可选地经过转换)与常量值进行比较。
 * 它是复合模式(Composite Pattern)中的叶子节点,不包含子谓词。
 *
 * <h2>组成部分</h2>
 * 一个叶子谓词由三部分组成:
 * <ul>
 *   <li><b>Transform</b>: 字段引用或字段转换,定义要测试的字段或表达式
 *       <br>例如: FieldRef(age) 或 CastTransform(age, STRING)
 *   </li>
 *   <li><b>Function</b>: 比较函数,定义比较操作的类型
 *       <br>例如: Equal、GreaterThan、IsNull、In 等
 *   </li>
 *   <li><b>Literals</b>: 常量值列表,用于与字段值进行比较
 *       <br>例如: [18]、['Alice', 'Bob']
 *   </li>
 * </ul>
 *
 * <h2>谓词示例</h2>
 * <pre>{@code
 * // age > 18
 * new LeafPredicate(
 *     new FieldTransform(new FieldRef(0, "age", DataTypes.INT())),
 *     GreaterThan.INSTANCE,
 *     Collections.singletonList(18)
 * )
 *
 * // LOWER(name) = 'alice'
 * new LeafPredicate(
 *     new LowerTransform(new FieldRef(1, "name", DataTypes.STRING())),
 *     Equal.INSTANCE,
 *     Collections.singletonList("alice")
 * )
 *
 * // status IN ('ACTIVE', 'PENDING')
 * new LeafPredicate(
 *     new FieldTransform(new FieldRef(2, "status", DataTypes.STRING())),
 *     In.INSTANCE,
 *     Arrays.asList("ACTIVE", "PENDING")
 * )
 *
 * // email IS NOT NULL
 * new LeafPredicate(
 *     new FieldTransform(new FieldRef(3, "email", DataTypes.STRING())),
 *     IsNotNull.INSTANCE,
 *     Collections.emptyList()
 * )
 * }</pre>
 *
 * <h2>序列化支持</h2>
 * 该类实现了自定义序列化逻辑:
 * <ul>
 *   <li>使用 Jackson 注解支持 JSON 序列化</li>
 *   <li>使用 Java 序列化机制处理 literals 字段(因为它是 transient 的)</li>
 *   <li>literals 使用类型感知的序列化器,确保正确处理各种数据类型</li>
 * </ul>
 *
 * <h2>统计信息过滤</h2>
 * 叶子谓词支持基于统计信息的快速过滤,前提是:
 * <ul>
 *   <li>Transform 是简单的 FieldTransform(不包含复杂转换)</li>
 *   <li>统计信息(min/max/nullCount)可用</li>
 * </ul>
 * 如果包含复杂转换(如 LOWER、SUBSTRING 等),则统计信息测试会保守地返回 true,
 * 表示需要读取实际数据进行精确测试。
 *
 * <h2>谓词否定</h2>
 * 叶子谓词支持否定操作,但仅当:
 * <ul>
 *   <li>Transform 是简单的 FieldTransform</li>
 *   <li>Function 支持否定(实现了 negate() 方法)</li>
 * </ul>
 * 例如:
 * <ul>
 *   <li>"age > 18" 可以否定为 "age <= 18"</li>
 *   <li>"LOWER(name) = 'alice'" 无法否定(因为包含转换)</li>
 * </ul>
 *
 * @see Transform 字段引用和转换
 * @see LeafFunction 叶子谓词函数
 * @see CompoundPredicate 复合谓词
 * @see PredicateBuilder 用于构建谓词的工具类
 */
public class LeafPredicate implements Predicate {

    /** 序列化版本号 */
    private static final long serialVersionUID = 3L;

    /** JSON 序列化时的类型名称 */
    public static final String NAME = "LEAF";

    /** JSON 字段名:字段转换 */
    public static final String FIELD_TRANSFORM = "transform";

    /** JSON 字段名:比较函数 */
    public static final String FIELD_FUNCTION = "function";

    /** JSON 字段名:常量值列表 */
    public static final String FIELD_LITERALS = "literals";

    /** 字段引用或字段转换,定义要测试的字段或表达式 */
    @JsonProperty(FIELD_TRANSFORM)
    private final Transform transform;

    /** 比较函数,定义比较操作的类型 */
    @JsonProperty(FIELD_FUNCTION)
    private final LeafFunction function;

    /**
     * 常量值列表,用于与字段值进行比较。
     *
     * <p>该字段标记为 transient,因为需要使用自定义序列化逻辑来正确处理各种数据类型。
     * 在 writeObject 和 readObject 方法中使用类型感知的序列化器进行序列化和反序列化。
     */
    private transient List<Object> literals;

    /**
     * 构造叶子谓词。
     *
     * @param function 比较函数
     * @param type 字段数据类型
     * @param fieldIndex 字段在行中的索引位置
     * @param fieldName 字段名称
     * @param literals 用于比较的常量值列表
     */
    public LeafPredicate(
            LeafFunction function,
            DataType type,
            int fieldIndex,
            String fieldName,
            List<Object> literals) {
        this(new FieldTransform(new FieldRef(fieldIndex, fieldName, type)), function, literals);
    }

    /**
     * 构造叶子谓词(通用形式,支持字段转换)。
     *
     * @param transform 字段引用或字段转换
     * @param function 比较函数
     * @param literals 用于比较的常量值列表
     */
    public LeafPredicate(Transform transform, LeafFunction function, List<Object> literals) {
        this.transform = transform;
        this.function = function;
        this.literals = literals;
    }

    /**
     * 静态工厂方法,创建叶子谓词。
     *
     * @param transform 字段引用或字段转换
     * @param function 比较函数
     * @param literals 用于比较的常量值列表
     * @return 新的叶子谓词实例
     */
    public static LeafPredicate of(
            Transform transform, LeafFunction function, List<Object> literals) {
        return new LeafPredicate(transform, function, literals);
    }

    /**
     * JSON 反序列化时使用的构造方法。
     *
     * <p>该方法会将 JSON 中的 literals 转换为 Paimon 内部使用的数据类型格式。
     *
     * @param transform 字段引用或字段转换
     * @param function 比较函数
     * @param literals JSON 格式的常量值列表
     * @return 新的叶子谓词实例
     */
    @JsonCreator
    protected static LeafPredicate fromJson(
            @JsonProperty(FIELD_TRANSFORM) Transform transform,
            @JsonProperty(FIELD_FUNCTION) LeafFunction function,
            @JsonProperty(FIELD_LITERALS) List<Object> literals) {
        List<Object> convertedLiterals = deserializeLiterals(transform.outputType(), literals);
        return new LeafPredicate(transform, function, convertedLiterals);
    }

    /**
     * 复制谓词并使用新的输入参数(用于谓词转换)。
     *
     * @param newInputs 新的输入参数列表
     * @return 新的叶子谓词实例
     */
    public LeafPredicate copyWithNewInputs(List<Object> newInputs) {
        return new LeafPredicate(transform.copyWithNewInputs(newInputs), function, literals);
    }

    /**
     * 获取字段转换。
     *
     * @return 字段引用或字段转换
     */
    @JsonGetter(FIELD_TRANSFORM)
    public Transform transform() {
        return transform;
    }

    /**
     * 获取比较函数。
     *
     * @return 比较函数
     */
    @JsonGetter(FIELD_FUNCTION)
    public LeafFunction function() {
        return function;
    }

    /**
     * 获取用于 JSON 序列化的常量值列表。
     *
     * <p>该方法将内部数据格式转换为 JSON 友好的格式。
     *
     * @return JSON 格式的常量值列表
     */
    @JsonGetter(FIELD_LITERALS)
    public List<Object> literalsForJson() {
        return serializeLiterals(transform.outputType(), literals);
    }

    /**
     * 获取谓词中涉及的所有字段名称。
     *
     * <p>该方法遍历 transform 的所有输入,提取其中的字段引用名称。
     * 用于分析谓词的字段依赖关系。
     *
     * @return 字段名称列表
     */
    public List<String> fieldNames() {
        List<String> names = new ArrayList<>();
        for (Object input : transform.inputs()) {
            if (input instanceof FieldRef) {
                names.add(((FieldRef) input).name());
            }
        }
        return names;
    }

    /**
     * 获取字段引用(如果 transform 是简单的字段引用)。
     *
     * <p>只有当 transform 是 FieldTransform 时才返回字段引用,
     * 如果是复杂转换(如 LOWER、CAST 等)则返回 Optional.empty()。
     *
     * @return 包含字段引用的 Optional,如果不是简单字段引用则返回 Optional.empty()
     */
    public Optional<FieldRef> fieldRefOptional() {
        if (transform instanceof FieldTransform) {
            return Optional.of(((FieldTransform) transform).fieldRef());
        }
        return Optional.empty();
    }

    /**
     * 获取常量值列表。
     *
     * @return 用于比较的常量值列表
     */
    public List<Object> literals() {
        return literals;
    }

    /**
     * 基于具体的数据行进行精确测试。
     *
     * <p>该方法执行以下步骤:
     * <ol>
     *   <li>使用 transform 从数据行中提取或计算字段值</li>
     *   <li>使用 function 将字段值与 literals 进行比较</li>
     *   <li>返回比较结果</li>
     * </ol>
     *
     * <p>示例:
     * <ul>
     *   <li>对于 "age > 18": transform 提取 age 字段,function 比较 age > 18</li>
     *   <li>对于 "LOWER(name) = 'alice'": transform 提取 name 并转小写,function 比较是否等于 'alice'</li>
     * </ul>
     *
     * @param row 待测试的数据行
     * @return 如果数据行满足谓词条件返回 true,否则返回 false
     */
    @Override
    public boolean test(InternalRow row) {
        Object value = transform.transform(row);
        return function.test(transform.outputType(), value, literals);
    }

    /**
     * 基于统计信息进行快速过滤测试。
     *
     * <p>该方法首先检查 transform 是否为简单的字段引用:
     * <ul>
     *   <li>如果不是简单字段引用(包含复杂转换),则保守地返回 true,表示需要读取数据进行精确测试</li>
     *   <li>如果是简单字段引用,则使用字段的统计信息进行快速判断</li>
     * </ul>
     *
     * <p>对于简单字段引用,该方法会:
     * <ol>
     *   <li>提取字段的最小值、最大值和空值计数</li>
     *   <li>检查是否所有值都是 NULL</li>
     *   <li>如果统计信息不完整(min 或 max 为 null),保守地返回 true</li>
     *   <li>否则调用 function 的统计信息测试方法</li>
     * </ol>
     *
     * <p>示例:
     * <ul>
     *   <li>对于 "age > 18": 如果 maxValues[age] <= 18,返回 false(确定不包含满足条件的数据)</li>
     *   <li>对于 "status IS NOT NULL": 如果 nullCount[status] == rowCount,返回 false(所有值都是 NULL)</li>
     * </ul>
     *
     * @param rowCount 数据行总数
     * @param minValues 每个字段的最小值
     * @param maxValues 每个字段的最大值
     * @param nullCounts 每个字段的空值计数
     * @return true 表示可能包含满足条件的数据;false 表示确定不包含满足条件的数据
     */
    @Override
    public boolean test(
            long rowCount, InternalRow minValues, InternalRow maxValues, InternalArray nullCounts) {
        Optional<FieldRef> fieldRefOptional = fieldRefOptional();
        if (!fieldRefOptional.isPresent()) {
            // 包含复杂转换,无法基于统计信息判断,保守地返回 true
            return true;
        }
        FieldRef fieldRef = fieldRefOptional.get();
        int index = fieldRef.index();
        DataType type = fieldRef.type();

        Object min = get(minValues, index, type);
        Object max = get(maxValues, index, type);
        Long nullCount = nullCounts.isNullAt(index) ? null : nullCounts.getLong(index);
        if (nullCount == null || rowCount != nullCount) {
            // 不是所有值都是 NULL
            // 但是 min 或 max 为 null(统计信息不完整)
            // 或者统计信息未知
            if (min == null || max == null) {
                return true;
            }
        }
        return function.test(type, rowCount, min, max, nullCount, literals);
    }

    /**
     * 返回该谓词的否定形式(如果可能)。
     *
     * <p>该方法首先检查 transform 是否为简单的字段引用:
     * <ul>
     *   <li>如果不是简单字段引用(包含复杂转换),则无法否定,返回 Optional.empty()</li>
     *   <li>如果是简单字段引用,则尝试否定 function</li>
     * </ul>
     *
     * <p>示例:
     * <ul>
     *   <li>"age > 18" 可以否定为 "age <= 18"</li>
     *   <li>"name = 'Alice'" 可以否定为 "name != 'Alice'"</li>
     *   <li>"LOWER(name) = 'alice'" 无法否定(包含转换),返回 Optional.empty()</li>
     * </ul>
     *
     * @return 包含否定谓词的 Optional,如果无法否定则返回 Optional.empty()
     */
    @Override
    public Optional<Predicate> negate() {
        Optional<FieldRef> fieldRefOptional = fieldRefOptional();
        if (!fieldRefOptional.isPresent()) {
            return Optional.empty();
        }
        FieldRef fieldRef = fieldRefOptional.get();
        return function.negate()
                .map(
                        negate ->
                                new LeafPredicate(
                                        negate,
                                        fieldRef.type(),
                                        fieldRef.index(),
                                        fieldRef.name(),
                                        literals));
    }

    /**
     * 访问者模式的接受方法。
     *
     * @param visitor 谓词访问者
     * @param <T> 访问结果的类型
     * @return 访问者处理的结果
     */
    @Override
    public <T> T visit(PredicateVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /**
     * 比较两个叶子谓词是否相等。
     *
     * @param o 待比较的对象
     * @return 如果两个谓词的 transform、function 和 literals 都相等返回 true,否则返回 false
     */
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LeafPredicate that = (LeafPredicate) o;
        return Objects.equals(transform, that.transform)
                && Objects.equals(function, that.function)
                && Objects.equals(literals, that.literals);
    }

    /**
     * 计算叶子谓词的哈希码。
     *
     * @return 基于 transform、function 和 literals 计算的哈希码
     */
    @Override
    public int hashCode() {
        return Objects.hash(transform, function, literals);
    }

    /**
     * 返回叶子谓词的字符串表示。
     *
     * <p>格式:
     * <ul>
     *   <li>无常量值: function(transform)</li>
     *   <li>单个常量值: function(transform, literal)</li>
     *   <li>多个常量值: function(transform, [literal1, literal2, ...])</li>
     * </ul>
     *
     * <p>示例:
     * <ul>
     *   <li>IsNull(age)</li>
     *   <li>Equal(name, 'Alice')</li>
     *   <li>In(status, [ACTIVE, PENDING])</li>
     * </ul>
     *
     * @return 谓词的字符串表示
     */
    @Override
    public String toString() {
        String literalsStr;
        int literalsSize = literals == null ? 0 : literals.size();
        if (literalsSize == 0) {
            literalsStr = "";
        } else if (literalsSize == 1) {
            literalsStr = Objects.toString(literals.get(0));
        } else {
            literalsStr = StringUtils.truncatedString(literals, "[", ", ", "]");
        }
        return literalsStr.isEmpty()
                ? function + "(" + transform + ")"
                : function + "(" + transform + ", " + literalsStr + ")";
    }

    /**
     * 创建用于序列化 literals 的序列化器。
     *
     * <p>该序列化器根据 transform 的输出类型创建,确保正确处理各种数据类型。
     *
     * @return literals 的序列化器
     */
    private ListSerializer<Object> literalsSerializer() {
        return new ListSerializer<>(
                NullableSerializer.wrapIfNullIsNotSupported(
                        InternalSerializers.create(transform.outputType())));
    }

    /**
     * 自定义 Java 序列化的写入方法。
     *
     * <p>由于 literals 字段是 transient 的,需要手动序列化。
     * 该方法首先调用默认序列化,然后使用类型感知的序列化器序列化 literals。
     *
     * @param out 对象输出流
     * @throws IOException 如果序列化失败
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        literalsSerializer().serialize(literals, new DataOutputViewStreamWrapper(out));
    }

    /**
     * 自定义 Java 序列化的读取方法。
     *
     * <p>该方法首先调用默认反序列化,然后使用类型感知的序列化器反序列化 literals。
     *
     * @param in 对象输入流
     * @throws IOException 如果反序列化失败
     * @throws ClassNotFoundException 如果找不到类
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        literals = literalsSerializer().deserialize(new DataInputViewStreamWrapper(in));
    }

    // ====================== 废弃的方法 ===============================
    // ================ 将在下一个版本中移除 ========================

    /**
     * 获取字段类型。
     *
     * @deprecated 请使用 {@link #fieldRefOptional()} 代替
     * @return 字段数据类型
     */
    @Deprecated
    public DataType type() {
        return fieldRef().type();
    }

    /**
     * 获取字段索引。
     *
     * @deprecated 请使用 {@link #fieldRefOptional()} 代替
     * @return 字段在行中的索引位置
     */
    @Deprecated
    public int index() {
        return fieldRef().index();
    }

    /**
     * 获取字段名称。
     *
     * @deprecated 请使用 {@link #fieldRefOptional()} 代替
     * @return 字段名称
     */
    @Deprecated
    public String fieldName() {
        return fieldRef().name();
    }

    /**
     * 获取字段引用。
     *
     * @deprecated 请使用 {@link #fieldRefOptional()} 代替
     * @return 字段引用
     */
    @Deprecated
    public FieldRef fieldRef() {
        //noinspection OptionalGetWithoutIsPresent
        return fieldRefOptional().get();
    }

    /**
     * 复制谓词并使用新的字段索引。
     *
     * @deprecated 请使用 {@link #fieldRefOptional()} 代替
     * @param fieldIndex 新的字段索引
     * @return 新的叶子谓词实例
     */
    @Deprecated
    public LeafPredicate copyWithNewIndex(int fieldIndex) {
        return new LeafPredicate(function, type(), fieldIndex, fieldName(), literals);
    }

    /**
     * 将内部数据格式的 literals 序列化为 JSON 友好的格式。
     *
     * <p>该方法将 Paimon 内部使用的数据类型(如 BinaryString)转换为标准 Java 类型(如 String)。
     *
     * @param type 数据类型
     * @param literals 内部格式的常量值列表
     * @return JSON 友好格式的常量值列表
     */
    protected static List<Object> serializeLiterals(DataType type, List<Object> literals) {
        if (literals == null) {
            return null;
        }
        List<Object> serialized = new ArrayList<>(literals.size());
        for (Object lit : literals) {
            serialized.add(PredicateBuilder.convertToJavaObject(type, lit));
        }
        return serialized;
    }

    /**
     * 将 JSON 格式的 literals 反序列化为内部数据格式。
     *
     * <p>该方法将标准 Java 类型(如 String)转换为 Paimon 内部使用的数据类型(如 BinaryString)。
     *
     * @param type 数据类型
     * @param literals JSON 格式的常量值列表
     * @return 内部格式的常量值列表
     */
    protected static List<Object> deserializeLiterals(DataType type, List<Object> literals) {
        if (literals == null) {
            return null;
        }
        List<Object> converted = new ArrayList<>(literals.size());
        for (Object literal : literals) {
            if (literal instanceof DataType) {
                converted.add(literal);
                continue;
            }
            converted.add(PredicateBuilder.convertJavaObject(type, literal));
        }
        return converted;
    }
}
