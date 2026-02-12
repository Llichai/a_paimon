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

import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.paimon.utils.InternalRowUtils.get;

/**
 * 类型转换 Transform。
 *
 * <p>该 Transform 用于将字段值从一种类型转换为另一种类型。基于 Paimon 的类型转换框架
 * {@link CastExecutors} 实现，支持大部分常见的类型转换。
 *
 * <h2>主要功能：</h2>
 * <ul>
 *   <li>字段类型转换（如 INT → BIGINT, STRING → DATE）
 *   <li>自动验证转换可行性
 *   <li>运行时高效转换
 *   <li>序列化支持（JSON）
 * </ul>
 *
 * <h2>使用示例：</h2>
 * <pre>{@code
 * // 示例 1：整数类型扩展
 * FieldRef intField = new FieldRef(0, "id", DataTypes.INT());
 * CastTransform toLong = new CastTransform(intField, DataTypes.BIGINT());
 * // CAST(id AS BIGINT)
 *
 * // 示例 2：字符串转日期
 * FieldRef dateStr = new FieldRef(1, "date_str", DataTypes.STRING());
 * CastTransform toDate = new CastTransform(dateStr, DataTypes.DATE());
 * // CAST(date_str AS DATE)
 *
 * // 示例 3：数值精度调整
 * FieldRef decimalField = new FieldRef(2, "amount", DataTypes.DECIMAL(10, 2));
 * CastTransform toHigherPrecision = new CastTransform(
 *     decimalField,
 *     DataTypes.DECIMAL(20, 4)
 * );
 * // CAST(amount AS DECIMAL(20, 4))
 *
 * // 示例 4：在谓词中使用
 * // WHERE CAST(id AS BIGINT) = 1000000000
 * PredicateBuilder builder = new PredicateBuilder(rowType);
 * Predicate predicate = builder.equal(toLong, 1000000000L);
 *
 * // 示例 5：安全创建（检查转换可行性）
 * FieldRef field = new FieldRef(3, "value", DataTypes.STRING());
 * Optional<Transform> castOpt = CastTransform.tryCreate(field, DataTypes.INT());
 * if (castOpt.isPresent()) {
 *     // 转换是可行的
 *     Transform cast = castOpt.get();
 * } else {
 *     // 转换不可行（例如 STRING → ARRAY）
 * }
 * }</pre>
 *
 * <h2>支持的类型转换：</h2>
 * <p>具体支持的转换取决于 {@link CastExecutors}，常见的包括：
 * <ul>
 *   <li>数值类型之间的转换（可能有精度损失）
 *   <li>字符串与其他类型的转换
 *   <li>时间类型之间的转换
 *   <li>精度调整（DECIMAL）
 * </ul>
 *
 * <h2>特殊情况：</h2>
 * <ul>
 *   <li>如果源类型和目标类型相同，{@link #tryCreate} 会返回 {@link FieldTransform} 而非 CastTransform
 *   <li>如果转换不可行，构造函数会抛出 {@link IllegalArgumentException}
 *   <li>转换执行器在反序列化时会被重新初始化
 * </ul>
 *
 * <h2>序列化：</h2>
 * <p>支持 JSON 序列化和 Java 序列化。由于 {@link CastExecutor} 可能不可序列化，
 * 在反序列化时会根据类型信息重新创建执行器。
 *
 * @see CastExecutors 类型转换执行器工厂
 * @see FieldTransform 无转换的字段引用
 */
public class CastTransform implements Transform {

    private static final long serialVersionUID = 1L;

    /** Transform 名称常量。 */
    public static final String NAME = "CAST";

    /** 源字段引用。 */
    private final FieldRef fieldRef;

    /** 目标类型。 */
    private final DataType type;

    /** 类型转换执行器（transient，需要在反序列化时重建）。 */
    private transient CastExecutor<Object, Object> cast;

    /** JSON 字段名：字段引用。 */
    public static final String FIELD_FIELD_REF = "fieldRef";

    /** JSON 字段名：目标类型。 */
    public static final String FIELD_TYPE = "type";

    /**
     * 构造类型转换 Transform。
     *
     * @param fieldRef 源字段引用
     * @param type 目标类型
     * @throws IllegalArgumentException 如果类型转换不可行
     */
    @JsonCreator
    public CastTransform(
            @JsonProperty(FIELD_FIELD_REF) FieldRef fieldRef,
            @JsonProperty(FIELD_TYPE) DataType type) {
        this.fieldRef = fieldRef;
        this.type = type;
        @SuppressWarnings("unchecked")
        CastExecutor<Object, Object> resolved =
                (CastExecutor<Object, Object>) CastExecutors.resolve(fieldRef.type(), type);
        if (resolved == null) {
            throw new IllegalArgumentException("Cannot create CastTransform");
        }
        this.cast = resolved;
    }

    /**
     * 内部构造函数（带执行器）。
     *
     * @param fieldRef 源字段引用
     * @param type 目标类型
     * @param cast 类型转换执行器
     */
    private CastTransform(FieldRef fieldRef, DataType type, CastExecutor<Object, Object> cast) {
        this.fieldRef = fieldRef;
        this.type = type;
        this.cast = cast;
    }

    /**
     * 尝试创建类型转换 Transform。
     *
     * <p>如果源类型和目标类型相同，返回 {@link FieldTransform}。
     * <p>如果转换不可行，返回 {@code Optional.empty()}。
     *
     * @param fieldRef 源字段引用
     * @param type 目标类型
     * @return 转换 Transform，如果不可行则返回 empty
     */
    public static Optional<Transform> tryCreate(FieldRef fieldRef, DataType type) {
        if (fieldRef.type().equals(type)) {
            return Optional.of(new FieldTransform(fieldRef));
        }

        @SuppressWarnings("unchecked")
        CastExecutor<Object, Object> cast =
                (CastExecutor<Object, Object>) CastExecutors.resolve(fieldRef.type(), type);
        if (cast == null) {
            return Optional.empty();
        } else {
            return Optional.of(new CastTransform(fieldRef, type, cast));
        }
    }

    /**
     * 获取源字段引用。
     *
     * @return 字段引用
     */
    @JsonGetter(FIELD_FIELD_REF)
    public FieldRef fieldRef() {
        return fieldRef;
    }

    /**
     * 获取目标类型。
     *
     * @return 目标类型
     */
    @JsonGetter(FIELD_TYPE)
    public DataType type() {
        return type;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public List<Object> inputs() {
        return Collections.singletonList(fieldRef);
    }

    @Override
    public DataType outputType() {
        return type;
    }

    /**
     * 对行数据执行类型转换。
     *
     * @param row 输入行
     * @return 转换后的值
     */
    @Override
    public Object transform(InternalRow row) {
        return cast.cast(get(row, fieldRef.index(), fieldRef.type()));
    }

    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        assert inputs.size() == 1;
        return new CastTransform((FieldRef) inputs.get(0), type, cast);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CastTransform that = (CastTransform) o;
        return Objects.equals(fieldRef, that.fieldRef) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldRef, type);
    }

    @Override
    public String toString() {
        return "CAST( " + fieldRef + " AS " + type + ")";
    }

    /**
     * 自定义反序列化逻辑。
     *
     * <p>重新创建类型转换执行器。
     */
    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        @SuppressWarnings("unchecked")
        CastExecutor<Object, Object> resolved =
                (CastExecutor<Object, Object>) CastExecutors.resolve(fieldRef.type(), type);
        this.cast = resolved;
    }
}
