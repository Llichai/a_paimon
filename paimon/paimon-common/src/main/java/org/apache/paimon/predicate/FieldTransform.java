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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.utils.InternalRowUtils.get;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 字段提取转换。
 *
 * <p>这是最基础的Transform实现,用于从数据行中提取指定字段的值。
 * 它直接引用原始字段,不进行任何计算或转换。
 *
 * <h2>主要特性</h2>
 * <ul>
 *   <li>零开销 - 直接提取字段值,无额外计算
 *   <li>类型保持 - 输出类型与字段类型相同
 *   <li>空值处理 - 正确处理null值
 *   <li>序列化支持 - 支持JSON序列化和Java序列化
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 创建字段引用
 * FieldRef nameField = new FieldRef(0, "name", DataTypes.STRING());
 * FieldRef ageField = new FieldRef(1, "age", DataTypes.INT());
 *
 * // 2. 创建字段转换
 * Transform nameTransform = new FieldTransform(nameField);
 * Transform ageTransform = new FieldTransform(ageField);
 *
 * // 3. 在谓词中使用
 * PredicateBuilder builder = new PredicateBuilder(rowType);
 *
 * // WHERE name = 'Alice'
 * Predicate namePredicate = builder.equal(nameTransform, "Alice");
 *
 * // WHERE age > 18
 * Predicate agePredicate = builder.greaterThan(ageTransform, 18);
 *
 * // WHERE name IS NOT NULL AND age > 18
 * Predicate combined = PredicateBuilder.and(
 *     builder.isNotNull(nameTransform),
 *     agePredicate
 * );
 *
 * // 4. 执行转换
 * InternalRow row = ...; // 包含数据的行
 * Object name = nameTransform.transform(row); // 提取name字段
 * Object age = ageTransform.transform(row); // 提取age字段
 * }</pre>
 *
 * <h2>与直接字段引用的区别</h2>
 * <p>FieldTransform与LeafPredicate中直接使用字段索引的区别:
 * <ul>
 *   <li>复用性 - Transform可以在多个谓词中复用
 *   <li>组合性 - 可以作为其他Transform的输入(如UPPER(name))
 *   <li>一致性 - 与其他Transform使用相同的接口
 * </ul>
 *
 * <h2>应用场景</h2>
 * <ul>
 *   <li>基本谓词 - 构建简单的字段比较谓词
 *   <li>组合转换 - 作为复杂Transform的基础组件
 *   <li>投影操作 - 指定需要读取的字段
 *   <li>字段映射 - 在schema演化时映射字段
 * </ul>
 *
 * <h2>JSON序列化格式</h2>
 * <pre>{@code
 * {
 *   "name": "FIELD_REF",
 *   "fieldRef": {
 *     "index": 0,
 *     "name": "user_name",
 *     "type": "STRING"
 *   }
 * }
 * }</pre>
 *
 * @see Transform
 * @see FieldRef
 * @see LeafPredicate
 */
public class FieldTransform implements Transform {

    private static final long serialVersionUID = 1L;

    /** Transform名称,用于JSON序列化时的类型标识。 */
    public static final String NAME = "FIELD_REF";

    /** 引用的字段。 */
    private final FieldRef fieldRef;

    /** JSON序列化时的字段名。 */
    public static final String FIELD_FIELD_REF = "fieldRef";

    /**
     * 构造字段转换。
     *
     * @param fieldRef 要提取的字段引用
     */
    @JsonCreator
    public FieldTransform(@JsonProperty(FIELD_FIELD_REF) FieldRef fieldRef) {
        this.fieldRef = fieldRef;
    }

    @Override
    public String name() {
        return NAME;
    }

    /**
     * 获取字段引用。
     *
     * @return 字段引用对象
     */
    @JsonProperty(FIELD_FIELD_REF)
    public FieldRef fieldRef() {
        return fieldRef;
    }

    /**
     * 获取输入参数列表。
     *
     * <p>对于FieldTransform,输入是单个FieldRef对象。
     *
     * @return 包含一个FieldRef的列表
     */
    @Override
    @JsonIgnore
    public List<Object> inputs() {
        return Collections.singletonList(fieldRef);
    }

    /**
     * 获取输出类型。
     *
     * <p>FieldTransform的输出类型与字段类型相同。
     *
     * @return 字段的数据类型
     */
    @Override
    public DataType outputType() {
        return fieldRef.type();
    }

    /**
     * 从数据行中提取字段值。
     *
     * <p>根据字段类型调用相应的getter方法提取值。
     *
     * @param row 输入数据行
     * @return 字段值,可能为null
     */
    @Override
    public Object transform(InternalRow row) {
        return get(row, fieldRef.index(), fieldRef.type());
    }

    /**
     * 使用新的输入创建FieldTransform的副本。
     *
     * @param inputs 新的输入列表,必须包含恰好一个FieldRef
     * @return 新的FieldTransform实例
     * @throws IllegalArgumentException 如果inputs大小不为1
     */
    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        checkArgument(inputs.size() == 1);
        return new FieldTransform((FieldRef) inputs.get(0));
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FieldTransform that = (FieldTransform) o;
        return Objects.equals(fieldRef, that.fieldRef);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(fieldRef);
    }

    /**
     * 返回字段名称作为字符串表示。
     *
     * @return 字段名称
     */
    @Override
    public String toString() {
        return fieldRef.name();
    }
}
