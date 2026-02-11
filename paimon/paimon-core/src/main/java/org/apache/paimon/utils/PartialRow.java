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
 * 部分行包装器（零拷贝实现）
 *
 * <p>PartialRow 是 {@link InternalRow} 的包装实现，用于在不复制数据的情况下，
 * 访问原始行的前 N 个字段。
 *
 * <p>核心功能：
 * <ul>
 *   <li>零拷贝：不复制原始行的数据，只是限制字段数量
 *   <li>字段截断：只暴露前 arity 个字段，隐藏其余字段
 *   <li>行重用：支持替换底层行对象，实现对象重用
 * </ul>
 *
 * <p>工作原理：
 * <pre>
 * 原始行:  [field0, field1, field2, field3, field4, field5]
 * PartialRow (arity=3):
 *   访问 pos=0 → 实际访问 field0
 *   访问 pos=1 → 实际访问 field1
 *   访问 pos=2 → 实际访问 field2
 *   （field3, field4, field5 被隐藏）
 * </pre>
 *
 * <p>使用场景：
 * <ul>
 *   <li>Schema 演化：新 Schema 比旧 Schema 多字段，读取旧数据时只访问前 N 个字段
 *   <li>投影下推：只需要表的部分列（前 N 列）时避免数据复制
 *   <li>性能优化：在迭代过程中重用 PartialRow 对象
 * </ul>
 *
 * <p>与 OffsetRow 的区别：
 * <ul>
 *   <li>PartialRow：从位置 0 开始的前 N 个字段（如 field[0..2]）
 *   <li>OffsetRow：从指定偏移位置开始的连续字段（如 field[2..4]）
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 原始行有 6 个字段：[id, name, age, city, phone, email]
 * InternalRow originalRow = ...;
 *
 * // 创建 PartialRow，只暴露前 3 个字段（id, name, age）
 * PartialRow partialRow = new PartialRow(3);
 * partialRow.replace(originalRow);
 *
 * // 访问字段
 * long id = partialRow.getLong(0);         // 访问 originalRow.getLong(0)
 * String name = partialRow.getString(1).toString(); // 访问 originalRow.getString(1)
 * int age = partialRow.getInt(2);          // 访问 originalRow.getInt(2)
 *
 * // 字段数量
 * int count = partialRow.getFieldCount();  // 返回 3（而非原始行的 6）
 *
 * // 重用对象
 * InternalRow nextRow = ...;
 * partialRow.replace(nextRow);  // 不需要重新创建 PartialRow
 * }</pre>
 *
 * @see OffsetRow
 * @see InternalRow
 */
public class PartialRow implements InternalRow {

    /** 字段数量（对外暴露的字段数） */
    private final int arity;

    /** 被包装的原始行 */
    private InternalRow row;

    /**
     * 构造部分行包装器
     *
     * @param arity 字段数量（要暴露的字段数）
     */
    public PartialRow(int arity) {
        this.arity = arity;
    }

    /**
     * 构造部分行包装器并初始化原始行
     *
     * @param arity 字段数量
     * @param row 原始行对象
     */
    public PartialRow(int arity, InternalRow row) {
        this.arity = arity;
        this.row = row;
    }

    /**
     * 替换底层行对象
     *
     * <p>用于对象重用，避免频繁创建 PartialRow 实例。
     *
     * @param row 新的原始行
     * @return 当前 PartialRow 实例（支持链式调用）
     */
    public PartialRow replace(InternalRow row) {
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
        return row.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return row.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return row.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return row.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return row.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return row.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return row.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return row.getDouble(pos);
    }

    @Override
    public BinaryString getString(int pos) {
        return row.getString(pos);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return row.getDecimal(pos, precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return row.getTimestamp(pos, precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        return row.getBinary(pos);
    }

    @Override
    public Variant getVariant(int pos) {
        return row.getVariant(pos);
    }

    @Override
    public Blob getBlob(int pos) {
        return row.getBlob(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        return row.getArray(pos);
    }

    @Override
    public InternalMap getMap(int pos) {
        return row.getMap(pos);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return row.getRow(pos, numFields);
    }
}
