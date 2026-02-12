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

package org.apache.paimon.data;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * {@link RowType} 的内部数据结构实现。
 *
 * <p>{@link GenericRow} 是 {@link InternalRow} 的通用实现,底层由 Java {@link Object} 数组支持。
 * {@link GenericRow} 可以包含任意数量的不同类型字段。行中的字段可以通过位置(从0开始)访问,
 * 既可以使用通用的 {@link #getField(int)} 方法,也可以使用类型特定的 getter 方法(如 {@link #getInt(int)})。
 * 字段可以通过通用的 {@link #setField(int, Object)} 方法更新。
 *
 * <p>数据结构特点:
 * <ul>
 *   <li>灵活性: 基于对象数组,可以存储任意类型的字段</li>
 *   <li>可变性: 支持字段的动态更新</li>
 *   <li>空值支持: 字段值可以为 null 表示空值</li>
 *   <li>性能权衡: 相比 {@link BinaryRow},序列化和访问性能较低,但使用更灵活</li>
 * </ul>
 *
 * <p>注意:此数据结构的所有字段必须是内部数据结构。关于内部数据结构的更多信息,
 * 请参阅 {@link InternalRow} 的文档。
 *
 * <p>{@link GenericRow} 中的字段可以为 null 以表示空值。这与 {@link BinaryRow}
 * 使用位图标记空值的方式不同。
 *
 * <p>使用场景:
 * <ul>
 *   <li>测试和原型开发</li>
 *   <li>小数据集处理</li>
 *   <li>需要频繁修改字段的场景</li>
 *   <li>不需要高性能序列化的场景</li>
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public final class GenericRow implements InternalRow, Serializable {

    private static final long serialVersionUID = 1L;

    /** 存储实际内部格式值的数组。每个元素必须是内部数据结构或基本类型的包装类。 */
    private final Object[] fields;

    /** 此行在变更日志中描述的变更类型。 */
    private RowKind kind;

    /**
     * 创建具有给定变更类型和字段数量的 {@link GenericRow} 实例。
     *
     * <p>初始时,所有字段都设置为 null。
     *
     * <p>注意:行的所有字段必须是内部数据结构。
     *
     * @param kind 此行在变更日志中描述的变更类型
     * @param arity 字段数量
     */
    public GenericRow(RowKind kind, int arity) {
        this.fields = new Object[arity];
        this.kind = kind;
    }

    /**
     * 创建具有给定字段数量的 {@link GenericRow} 实例。
     *
     * <p>初始时,所有字段都设置为 null。默认情况下,此行描述变更日志中的 {@link RowKind#INSERT} 操作。
     *
     * <p>注意:行的所有字段必须是内部数据结构。
     *
     * @param arity 字段数量
     */
    public GenericRow(int arity) {
        this.fields = new Object[arity];
        this.kind = RowKind.INSERT; // INSERT as default
    }

    /**
     * 设置给定位置的字段值。
     *
     * <p>注意:给定的字段值必须是内部数据结构。否则 {@link GenericRow} 将损坏,
     * 在处理时可能抛出异常。关于内部数据结构的更多信息,请参阅 {@link InternalRow}。
     *
     * <p>字段值可以为 null 以表示空值。
     *
     * @param pos 字段位置(从0开始)
     * @param value 要设置的字段值(必须是内部数据结构)
     */
    public void setField(int pos, Object value) {
        this.fields[pos] = value;
    }

    /**
     * 返回给定位置的字段值。
     *
     * <p>注意:返回的值是内部数据结构。关于内部数据结构的更多信息,请参阅 {@link InternalRow}。
     *
     * <p>返回的字段值可以为 null 以表示空值。
     *
     * @param pos 字段位置(从0开始)
     * @return 字段值(内部数据结构或 null)
     */
    public Object getField(int pos) {
        return this.fields[pos];
    }

    @Override
    public int getFieldCount() {
        return fields.length;
    }

    @Override
    public RowKind getRowKind() {
        return kind;
    }

    @Override
    public void setRowKind(RowKind kind) {
        checkNotNull(kind);
        this.kind = kind;
    }

    @Override
    public boolean isNullAt(int pos) {
        return this.fields[pos] == null;
    }

    @Override
    public boolean getBoolean(int pos) {
        return (boolean) this.fields[pos];
    }

    @Override
    public byte getByte(int pos) {
        return (byte) this.fields[pos];
    }

    @Override
    public short getShort(int pos) {
        return (short) this.fields[pos];
    }

    @Override
    public int getInt(int pos) {
        return (int) this.fields[pos];
    }

    @Override
    public long getLong(int pos) {
        return (long) this.fields[pos];
    }

    @Override
    public float getFloat(int pos) {
        return (float) this.fields[pos];
    }

    @Override
    public double getDouble(int pos) {
        return (double) this.fields[pos];
    }

    @Override
    public BinaryString getString(int pos) {
        return (BinaryString) this.fields[pos];
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return (Decimal) this.fields[pos];
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return (Timestamp) this.fields[pos];
    }

    @Override
    public byte[] getBinary(int pos) {
        return (byte[]) this.fields[pos];
    }

    @Override
    public Variant getVariant(int pos) {
        return (Variant) this.fields[pos];
    }

    @Override
    public Blob getBlob(int pos) {
        return (Blob) this.fields[pos];
    }

    @Override
    public InternalArray getArray(int pos) {
        return (InternalArray) this.fields[pos];
    }

    @Override
    public InternalMap getMap(int pos) {
        return (InternalMap) this.fields[pos];
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return (InternalRow) this.fields[pos];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GenericRow)) {
            return false;
        }
        GenericRow that = (GenericRow) o;
        return kind == that.kind && Arrays.deepEquals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(kind);
        result = 31 * result + Arrays.deepHashCode(fields);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(kind.shortString()).append("(");
        for (int i = 0; i < fields.length; i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append(arrayAwareToString(fields[i]));
        }
        sb.append(")");
        return sb.toString();
    }

    private static String arrayAwareToString(Object o) {
        final String arrayString = Arrays.deepToString(new Object[] {o});
        return arrayString.substring(1, arrayString.length() - 1);
    }

    // ----------------------------------------------------------------------------------------
    // 工具方法
    // ----------------------------------------------------------------------------------------

    /**
     * 使用给定的字段值创建 {@link GenericRow} 实例。
     *
     * <p>默认情况下,此行描述变更日志中的 {@link RowKind#INSERT} 操作。
     *
     * <p>注意:行的所有字段必须是内部数据结构。
     *
     * <p>使用示例:
     * <pre>{@code
     * GenericRow row = GenericRow.of(
     *     BinaryString.fromString("hello"),
     *     42,
     *     true
     * );
     * }</pre>
     *
     * @param values 字段值数组(每个值必须是内部数据结构)
     * @return 新创建的 GenericRow 实例
     */
    public static GenericRow of(Object... values) {
        GenericRow row = new GenericRow(values.length);

        for (int i = 0; i < values.length; ++i) {
            row.setField(i, values[i]);
        }

        return row;
    }

    /**
     * 使用给定的变更类型和字段值创建 {@link GenericRow} 实例。
     *
     * <p>注意:行的所有字段必须是内部数据结构。
     *
     * <p>使用示例:
     * <pre>{@code
     * GenericRow row = GenericRow.ofKind(
     *     RowKind.UPDATE_AFTER,
     *     BinaryString.fromString("world"),
     *     100
     * );
     * }</pre>
     *
     * @param kind 变更类型
     * @param values 字段值数组(每个值必须是内部数据结构)
     * @return 新创建的 GenericRow 实例
     */
    public static GenericRow ofKind(RowKind kind, Object... values) {
        GenericRow row = new GenericRow(kind, values.length);

        for (int i = 0; i < values.length; ++i) {
            row.setField(i, values[i]);
        }

        return row;
    }
}
