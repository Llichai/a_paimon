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

package org.apache.paimon.utils;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.RowKind;

/**
 * 偏移行包装器（零拷贝实现）
 *
 * <p>OffsetRow 是 {@link InternalRow} 的包装实现，用于在不复制数据的情况下，
 * 访问原始行的一个子集字段（从指定偏移位置开始）。
 *
 * <p>核心功能：
 * <ul>
 *   <li>零拷贝：不复制原始行的数据，只是重定向字段访问
 *   <li>字段偏移：将访问位置 pos 映射为 offset + pos
 *   <li>行重用：支持替换底层行对象，实现对象重用
 * </ul>
 *
 * <p>工作原理：
 * <pre>
 * 原始行:  [field0, field1, field2, field3, field4, field5]
 *                           ↑ offset=2
 * OffsetRow (offset=2, arity=3):
 *   访问 pos=0 → 实际访问 offset+0=2 (field2)
 *   访问 pos=1 → 实际访问 offset+1=3 (field3)
 *   访问 pos=2 → 实际访问 offset+2=4 (field4)
 * </pre>
 *
 * <p>使用场景：
 * <ul>
 *   <li>投影下推：读取行的部分字段时避免数据复制
 *   <li>Schema 映射：将宽表的部分列映射为窄表
 *   <li>性能优化：在迭代过程中重用 OffsetRow 对象
 * </ul>
 *
 * <p>与 PartialRow 的区别：
 * <ul>
 *   <li>OffsetRow：从指定偏移位置开始的连续字段（如 field[2..4]）
 *   <li>PartialRow：从位置 0 开始的前 N 个字段（如 field[0..2]）
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 原始行有 6 个字段：[id, name, age, city, phone, email]
 * InternalRow originalRow = ...;
 *
 * // 创建 OffsetRow，从位置 2 开始，包含 3 个字段（age, city, phone）
 * OffsetRow offsetRow = new OffsetRow(3, 2);
 * offsetRow.replace(originalRow);
 *
 * // 访问字段
 * int age = offsetRow.getInt(0);      // 实际访问 originalRow.getInt(2)
 * String city = offsetRow.getString(1).toString(); // 实际访问 originalRow.getString(3)
 * String phone = offsetRow.getString(2).toString(); // 实际访问 originalRow.getString(4)
 *
 * // 重用对象
 * InternalRow nextRow = ...;
 * offsetRow.replace(nextRow);  // 不需要重新创建 OffsetRow
 * }</pre>
 *
 * @see PartialRow
 * @see InternalRow
 */
public class OffsetRow implements InternalRow {

    /** 字段数量（对外暴露的字段数） */
    private final int arity;

    /** 字段偏移量（在原始行中的起始位置） */
    private final int offset;

    /** 被包装的原始行 */
    private InternalRow row;

    /**
     * 构造偏移行包装器
     *
     * @param arity 字段数量
     * @param offset 字段偏移量
     */
    public OffsetRow(int arity, int offset) {
        this.arity = arity;
        this.offset = offset;
    }

    /**
     * 获取原始行对象
     *
     * @return 被包装的原始行
     */
    public InternalRow getOriginalRow() {
        return row;
    }

    /**
     * 替换底层行对象
     *
     * <p>用于对象重用，避免频繁创建 OffsetRow 实例。
     *
     * @param row 新的原始行
     * @return 当前 OffsetRow 实例（支持链式调用）
     */
    public OffsetRow replace(InternalRow row) {
        this.row = row;
        return this;
    }

    @Override
    public int getFieldCount() {
        return arity;
    }

    @Override
    public RowKind getRowKind() {
        return row.getRowKind();
    }

    @Override
    public void setRowKind(RowKind kind) {
        row.setRowKind(kind);
    }

    @Override
    public boolean isNullAt(int pos) {
        return row.isNullAt(offset + pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return row.getBoolean(offset + pos);
    }

    @Override
    public byte getByte(int pos) {
        return row.getByte(offset + pos);
    }

    @Override
    public short getShort(int pos) {
        return row.getShort(offset + pos);
    }

    @Override
    public int getInt(int pos) {
        return row.getInt(offset + pos);
    }

    @Override
    public long getLong(int pos) {
        return row.getLong(offset + pos);
    }

    @Override
    public float getFloat(int pos) {
        return row.getFloat(offset + pos);
    }

    @Override
    public double getDouble(int pos) {
        return row.getDouble(offset + pos);
    }

    @Override
    public BinaryString getString(int pos) {
        return row.getString(offset + pos);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return row.getDecimal(offset + pos, precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return row.getTimestamp(offset + pos, precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        return row.getBinary(offset + pos);
    }

    @Override
    public Variant getVariant(int pos) {
        return row.getVariant(offset + pos);
    }

    @Override
    public Blob getBlob(int pos) {
        return row.getBlob(offset + pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        return row.getArray(offset + pos);
    }

    @Override
    public InternalMap getMap(int pos) {
        return row.getMap(offset + pos);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return row.getRow(offset + pos, numFields);
    }
}
