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

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * {@link InternalRow} 的实现,由两个连接的 {@link InternalRow} 支持。
 *
 * <p>此实现是可变的,允许在热点代码路径中进行高性能的变更。
 *
 * <p><b>设计目的:</b>
 * JoinedRow 主要用于 Join 操作和需要合并两个行的场景。它提供了一个逻辑视图,
 * 将两个独立的行连接成一个单一的行,而无需物理复制数据。
 *
 * <p><b>字段布局:</b>
 * 逻辑上,JoinedRow 的字段由 row1 和 row2 的字段连接而成:
 * <pre>
 * row1 字段: [f0, f1, ..., f(n-1)]
 * row2 字段: [f0, f1, ..., f(m-1)]
 *            ↓ 连接后 ↓
 * JoinedRow:  [f0, f1, ..., f(n-1), f(n), f(n+1), ..., f(n+m-1)]
 *             └─────── row1 ────────┘ └────────── row2 ──────────┘
 * </pre>
 *
 * <p><b>性能优势:</b>
 * <ul>
 *   <li>零拷贝: 不复制底层行数据,仅持有引用</li>
 *   <li>可重用性: 支持原地替换底层行,避免对象创建</li>
 *   <li>延迟绑定: 可以先创建 JoinedRow,后续再设置底层行</li>
 * </ul>
 *
 * <p><b>字段访问规则:</b>
 * <ul>
 *   <li>位置 < row1.getFieldCount(): 从 row1 访问</li>
 *   <li>位置 >= row1.getFieldCount(): 从 row2 访问(位置需减去 row1 的字段数)</li>
 * </ul>
 *
 * <p><b>可变性:</b>
 * JoinedRow 是可变的,可以通过 {@link #replace(InternalRow, InternalRow)} 方法
 * 原地替换底层行。这对于在循环中重用对象非常有用,可以显著减少 GC 压力。
 *
 * <p>使用示例:
 * <pre>{@code
 * JoinedRow joinedRow = new JoinedRow();
 * for (InternalRow left : leftRows) {
 *     for (InternalRow right : rightRows) {
 *         joinedRow.replace(left, right);  // 重用同一个 JoinedRow 对象
 *         // 处理 joinedRow...
 *     }
 * }
 * }</pre>
 *
 * <p>使用场景:
 * <ul>
 *   <li>Join 操作:连接左表和右表的行</li>
 *   <li>宽表构建:将多个窄表合并为宽表</li>
 *   <li>字段扩展:在原有行基础上追加额外字段</li>
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public class JoinedRow implements InternalRow {

    /** 此行的变更类型,默认为 INSERT。 */
    private RowKind rowKind = RowKind.INSERT;
    /** 第一个底层行。 */
    private InternalRow row1;
    /** 第二个底层行。 */
    private InternalRow row2;

    /**
     * 创建一个类型为 {@link RowKind#INSERT} 的新 {@link JoinedRow},但没有底层行。
     *
     * <p>注意:必须确保在从此 {@link JoinedRow} 访问数据之前,
     * 将底层行设置为非 {@code null} 值。
     */
    public JoinedRow() {}

    /**
     * 创建一个由 row1 和 row2 支持的,类型为 {@link RowKind#INSERT} 的新 {@link JoinedRow}。
     *
     * <p>注意:必须确保在从此 {@link JoinedRow} 访问数据之前,
     * 将底层行设置为非 {@code null} 值。
     *
     * @param row1 第一个行
     * @param row2 第二个行
     */
    public JoinedRow(@Nullable InternalRow row1, @Nullable InternalRow row2) {
        this(RowKind.INSERT, row1, row2);
    }

    /**
     * 创建一个由 row1 和 row2 支持的,具有指定 RowKind 的新 {@link JoinedRow}。
     *
     * <p>注意:必须确保在从此 {@link JoinedRow} 访问数据之前,
     * 将底层行设置为非 {@code null} 值。
     *
     * @param rowKind 行的变更类型
     * @param row1 第一个行
     * @param row2 第二个行
     */
    public JoinedRow(RowKind rowKind, @Nullable InternalRow row1, @Nullable InternalRow row2) {
        this.rowKind = rowKind;
        this.row1 = row1;
        this.row2 = row2;
    }

    /**
     * 创建一个连接两个行的 JoinedRow 的静态工厂方法。
     *
     * @param row1 第一个行
     * @param row2 第二个行
     * @return 新创建的 JoinedRow
     */
    public static JoinedRow join(InternalRow row1, InternalRow row2) {
        return new JoinedRow(row1, row2);
    }

    /**
     * 替换支持此 {@link JoinedRow} 的底层 {@link InternalRow}。
     *
     * <p>此方法原地替换底层行,不返回新对象。这样做是出于性能考虑,
     * 可以在循环中重用同一个 JoinedRow 对象,避免频繁创建对象。
     *
     * @param row1 新的第一个行
     * @param row2 新的第二个行
     * @return 此 JoinedRow 实例本身(用于链式调用)
     */
    public JoinedRow replace(InternalRow row1, InternalRow row2) {
        this.row1 = row1;
        this.row2 = row2;
        return this;
    }

    /**
     * 返回第一个底层行。
     *
     * @return 第一个行
     */
    public InternalRow row1() {
        return row1;
    }

    /**
     * 返回第二个底层行。
     *
     * @return 第二个行
     */
    public InternalRow row2() {
        return row2;
    }

    // ---------------------------------------------------------------------------------------------

    @Override
    public int getFieldCount() {
        return row1.getFieldCount() + row2.getFieldCount();
    }

    @Override
    public RowKind getRowKind() {
        return rowKind;
    }

    @Override
    public void setRowKind(RowKind kind) {
        this.rowKind = kind;
    }

    @Override
    public boolean isNullAt(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.isNullAt(pos);
        } else {
            return row2.isNullAt(pos - row1.getFieldCount());
        }
    }

    @Override
    public boolean getBoolean(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.getBoolean(pos);
        } else {
            return row2.getBoolean(pos - row1.getFieldCount());
        }
    }

    @Override
    public byte getByte(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.getByte(pos);
        } else {
            return row2.getByte(pos - row1.getFieldCount());
        }
    }

    @Override
    public short getShort(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.getShort(pos);
        } else {
            return row2.getShort(pos - row1.getFieldCount());
        }
    }

    @Override
    public int getInt(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.getInt(pos);
        } else {
            return row2.getInt(pos - row1.getFieldCount());
        }
    }

    @Override
    public long getLong(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.getLong(pos);
        } else {
            return row2.getLong(pos - row1.getFieldCount());
        }
    }

    @Override
    public float getFloat(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.getFloat(pos);
        } else {
            return row2.getFloat(pos - row1.getFieldCount());
        }
    }

    @Override
    public double getDouble(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.getDouble(pos);
        } else {
            return row2.getDouble(pos - row1.getFieldCount());
        }
    }

    @Override
    public BinaryString getString(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.getString(pos);
        } else {
            return row2.getString(pos - row1.getFieldCount());
        }
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        if (pos < row1.getFieldCount()) {
            return row1.getDecimal(pos, precision, scale);
        } else {
            return row2.getDecimal(pos - row1.getFieldCount(), precision, scale);
        }
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        if (pos < row1.getFieldCount()) {
            return row1.getTimestamp(pos, precision);
        } else {
            return row2.getTimestamp(pos - row1.getFieldCount(), precision);
        }
    }

    @Override
    public byte[] getBinary(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.getBinary(pos);
        } else {
            return row2.getBinary(pos - row1.getFieldCount());
        }
    }

    @Override
    public Variant getVariant(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.getVariant(pos);
        } else {
            return row2.getVariant(pos - row1.getFieldCount());
        }
    }

    @Override
    public Blob getBlob(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.getBlob(pos);
        } else {
            return row2.getBlob(pos - row1.getFieldCount());
        }
    }

    @Override
    public InternalArray getArray(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.getArray(pos);
        } else {
            return row2.getArray(pos - row1.getFieldCount());
        }
    }

    @Override
    public InternalMap getMap(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.getMap(pos);
        } else {
            return row2.getMap(pos - row1.getFieldCount());
        }
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        if (pos < row1.getFieldCount()) {
            return row1.getRow(pos, numFields);
        } else {
            return row2.getRow(pos - row1.getFieldCount(), numFields);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JoinedRow that = (JoinedRow) o;
        return Objects.equals(rowKind, that.rowKind)
                && Objects.equals(this.row1, that.row1)
                && Objects.equals(this.row2, that.row2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowKind, row1, row2);
    }

    @Override
    public String toString() {
        return rowKind.shortString() + "{" + "row1=" + row1 + ", row2=" + row2 + '}';
    }
}
