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

package org.apache.paimon.data.columnar;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.DataSetters;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.RowKind;

import java.io.Serializable;

/**
 * 列式行,用于访问向量化列数据的行视图。
 *
 * <p>这是 {@link VectorizedColumnBatch} 中的行视图,提供了一种以行为单位访问列式存储数据的方式。
 * 本质上是一个指向列批次中特定行的游标,通过委托给底层的列向量来获取字段值。
 *
 * <h2>设计模式</h2>
 * <ul>
 *   <li><b>外观模式(Facade Pattern):</b> 为列式存储提供行式访问接口
 *   <li><b>游标模式(Cursor Pattern):</b> 通过 rowId 指向列批次中的当前行
 *   <li><b>委托模式(Delegation Pattern):</b> 所有数据访问委托给底层的 VectorizedColumnBatch
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>从列式存储读取数据后,需要以行为单位处理
 *   <li>迭代列批次中的所有行(配合 {@link ColumnarRowIterator} 使用)
 *   <li>将列式数据转换为行式处理逻辑所需的格式
 * </ul>
 *
 * <h2>工作原理</h2>
 * <pre>
 * VectorizedColumnBatch (列式存储):
 *   col0: [v0, v1, v2, v3]
 *   col1: [v0, v1, v2, v3]
 *   col2: [v0, v1, v2, v3]
 *
 * ColumnarRow (行视图, rowId=1):
 *   row1: [col0[1], col1[1], col2[1]]  => [v1, v1, v1]
 * </pre>
 *
 * <h2>特性</h2>
 * <ul>
 *   <li><b>只读性:</b> 此类是只读的,所有 setter 方法都会抛出 UnsupportedOperationException
 *   <li><b>可重用性:</b> 通过 {@link #setRowId(int)} 可以复用同一个对象访问不同行
 *   <li><b>轻量级:</b> 不复制数据,只维护对列批次的引用和行索引
 *   <li><b>可序列化:</b> 支持序列化,可用于分布式计算框架
 * </ul>
 *
 * <p><b>注意:</b> 此类不支持 {@link #equals(Object)} 和 {@link #hashCode()},
 * 因为比较和哈希操作应该针对实际的字段值进行,而不是行视图对象本身。
 *
 * @see VectorizedColumnBatch 列批次
 * @see ColumnarRowIterator 列式行迭代器
 * @see ColumnarArray 列式数组视图
 * @see InternalRow 内部行接口
 */
public final class ColumnarRow implements InternalRow, DataSetters, Serializable {

    private static final long serialVersionUID = 1L;

    private RowKind rowKind = RowKind.INSERT;
    private VectorizedColumnBatch vectorizedColumnBatch;
    private int rowId;

    public ColumnarRow() {}

    public ColumnarRow(VectorizedColumnBatch vectorizedColumnBatch) {
        this(vectorizedColumnBatch, 0);
    }

    public ColumnarRow(VectorizedColumnBatch vectorizedColumnBatch, int rowId) {
        this.vectorizedColumnBatch = vectorizedColumnBatch;
        this.rowId = rowId;
    }

    public void setVectorizedColumnBatch(VectorizedColumnBatch vectorizedColumnBatch) {
        this.vectorizedColumnBatch = vectorizedColumnBatch;
        this.rowId = 0;
    }

    public VectorizedColumnBatch batch() {
        return vectorizedColumnBatch;
    }

    public void setRowId(int rowId) {
        this.rowId = rowId;
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
    public int getFieldCount() {
        return vectorizedColumnBatch.getArity();
    }

    @Override
    public boolean isNullAt(int pos) {
        return vectorizedColumnBatch.isNullAt(rowId, pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return vectorizedColumnBatch.getBoolean(rowId, pos);
    }

    @Override
    public byte getByte(int pos) {
        return vectorizedColumnBatch.getByte(rowId, pos);
    }

    @Override
    public short getShort(int pos) {
        return vectorizedColumnBatch.getShort(rowId, pos);
    }

    @Override
    public int getInt(int pos) {
        return vectorizedColumnBatch.getInt(rowId, pos);
    }

    @Override
    public long getLong(int pos) {
        return vectorizedColumnBatch.getLong(rowId, pos);
    }

    @Override
    public float getFloat(int pos) {
        return vectorizedColumnBatch.getFloat(rowId, pos);
    }

    @Override
    public double getDouble(int pos) {
        return vectorizedColumnBatch.getDouble(rowId, pos);
    }

    @Override
    public BinaryString getString(int pos) {
        return vectorizedColumnBatch.getString(rowId, pos);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return vectorizedColumnBatch.getDecimal(rowId, pos, precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return vectorizedColumnBatch.getTimestamp(rowId, pos, precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        return vectorizedColumnBatch.getBinary(rowId, pos);
    }

    @Override
    public Variant getVariant(int pos) {
        return vectorizedColumnBatch.getVariant(rowId, pos);
    }

    @Override
    public Blob getBlob(int pos) {
        return new BlobData(getBinary(pos));
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return vectorizedColumnBatch.getRow(rowId, pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        return vectorizedColumnBatch.getArray(rowId, pos);
    }

    @Override
    public InternalMap getMap(int pos) {
        return vectorizedColumnBatch.getMap(rowId, pos);
    }

    @Override
    public void setNullAt(int pos) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setBoolean(int pos, boolean value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setByte(int pos, byte value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setShort(int pos, short value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setInt(int pos, int value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setLong(int pos, long value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setFloat(int pos, float value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setDouble(int pos, double value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setDecimal(int pos, Decimal value, int precision) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setTimestamp(int pos, Timestamp value, int precision) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException(
                "ColumnarRowData do not support equals, please compare fields one by one!");
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException(
                "ColumnarRowData do not support hashCode, please hash fields one by one!");
    }

    public ColumnarRow copy(ColumnVector[] vectors) {
        VectorizedColumnBatch vectorizedColumnBatchCopy = vectorizedColumnBatch.copy(vectors);
        ColumnarRow columnarRow = new ColumnarRow(vectorizedColumnBatchCopy, rowId);
        columnarRow.setRowKind(rowKind);
        return columnarRow;
    }
}
