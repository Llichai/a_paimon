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
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.Variant;

import java.io.Serializable;
import java.util.Arrays;

/**
 * 列式数组,用于访问向量化列数据的数组视图。
 *
 * <p>此类提供了对列向量中一段连续数据的数组视图,用于表示 ARRAY 类型的数据。
 * 与 {@link ColumnarRow} 类似,它通过委托给底层列向量来访问元素,而不复制数据。
 *
 * <h2>设计模式</h2>
 * <ul>
 *   <li><b>外观模式:</b> 为列向量的一段数据提供数组访问接口
 *   <li><b>切片视图:</b> 通过 offset 和 numElements 定义列向量的一个切片
 *   <li><b>委托模式:</b> 所有数据访问委托给底层的 ColumnVector
 * </ul>
 *
 * <h2>数据表示</h2>
 * <pre>
 * 列向量:  [e0, e1, e2, e3, e4, e5, e6, e7]
 *             ↑           ↑
 *          offset=2    numElements=3
 *
 * ColumnarArray 视图: [e2, e3, e4]
 * </pre>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>表示嵌套的 ARRAY 类型字段
 *   <li>从 ArrayColumnVector 中获取数组元素
 *   <li>处理变长数组数据
 * </ul>
 *
 * <h2>特性</h2>
 * <ul>
 *   <li><b>只读性:</b> 所有 setter 方法都会抛出 UnsupportedOperationException
 *   <li><b>零拷贝:</b> 不复制数据,直接引用底层列向量
 *   <li><b>类型化访问:</b> 支持各种基本类型和复杂类型的访问方法
 *   <li><b>可序列化:</b> 支持序列化传输
 * </ul>
 *
 * <p><b>注意:</b> 不支持 equals() 方法,需要逐字段比较数组元素。
 *
 * @see InternalArray 内部数组接口
 * @see ArrayColumnVector 数组列向量接口
 * @see ColumnarRow 列式行视图
 */
public final class ColumnarArray implements InternalArray, DataSetters, Serializable {

    private static final long serialVersionUID = 1L;

    private final ColumnVector data;
    private final int offset;
    private final int numElements;

    public ColumnarArray(ColumnVector data, int offset, int numElements) {
        this.data = data;
        this.offset = offset;
        this.numElements = numElements;
    }

    @Override
    public int size() {
        return numElements;
    }

    @Override
    public boolean isNullAt(int pos) {
        return data.isNullAt(offset + pos);
    }

    @Override
    public void setNullAt(int pos) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public boolean getBoolean(int pos) {
        return ((BooleanColumnVector) data).getBoolean(offset + pos);
    }

    @Override
    public byte getByte(int pos) {
        return ((ByteColumnVector) data).getByte(offset + pos);
    }

    @Override
    public short getShort(int pos) {
        return ((ShortColumnVector) data).getShort(offset + pos);
    }

    @Override
    public int getInt(int pos) {
        return ((IntColumnVector) data).getInt(offset + pos);
    }

    @Override
    public long getLong(int pos) {
        return ((LongColumnVector) data).getLong(offset + pos);
    }

    @Override
    public float getFloat(int pos) {
        return ((FloatColumnVector) data).getFloat(offset + pos);
    }

    @Override
    public double getDouble(int pos) {
        return ((DoubleColumnVector) data).getDouble(offset + pos);
    }

    @Override
    public BinaryString getString(int pos) {
        BytesColumnVector.Bytes byteArray = getByteArray(pos);
        return BinaryString.fromBytes(byteArray.data, byteArray.offset, byteArray.len);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return ((DecimalColumnVector) data).getDecimal(offset + pos, precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return ((TimestampColumnVector) data).getTimestamp(offset + pos, precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        BytesColumnVector.Bytes byteArray = getByteArray(pos);
        if (byteArray.len == byteArray.data.length) {
            return byteArray.data;
        } else {
            return Arrays.copyOfRange(
                    byteArray.data, byteArray.offset, byteArray.offset + byteArray.len);
        }
    }

    @Override
    public Variant getVariant(int pos) {
        InternalRow row = getRow(pos, 2);
        byte[] value = row.getBinary(0);
        byte[] metadata = row.getBinary(1);
        return new GenericVariant(value, metadata);
    }

    @Override
    public Blob getBlob(int pos) {
        return new BlobData(getBinary(pos));
    }

    @Override
    public InternalArray getArray(int pos) {
        return ((ArrayColumnVector) data).getArray(offset + pos);
    }

    @Override
    public InternalMap getMap(int pos) {
        return ((MapColumnVector) data).getMap(offset + pos);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return ((RowColumnVector) data).getRow(offset + pos);
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
    public boolean[] toBooleanArray() {
        boolean[] res = new boolean[numElements];
        for (int i = 0; i < numElements; i++) {
            res[i] = getBoolean(i);
        }
        return res;
    }

    @Override
    public byte[] toByteArray() {
        byte[] res = new byte[numElements];
        for (int i = 0; i < numElements; i++) {
            res[i] = getByte(i);
        }
        return res;
    }

    @Override
    public short[] toShortArray() {
        short[] res = new short[numElements];
        for (int i = 0; i < numElements; i++) {
            res[i] = getShort(i);
        }
        return res;
    }

    @Override
    public int[] toIntArray() {
        int[] res = new int[numElements];
        for (int i = 0; i < numElements; i++) {
            res[i] = getInt(i);
        }
        return res;
    }

    @Override
    public long[] toLongArray() {
        long[] res = new long[numElements];
        for (int i = 0; i < numElements; i++) {
            res[i] = getLong(i);
        }
        return res;
    }

    @Override
    public float[] toFloatArray() {
        float[] res = new float[numElements];
        for (int i = 0; i < numElements; i++) {
            res[i] = getFloat(i);
        }
        return res;
    }

    @Override
    public double[] toDoubleArray() {
        double[] res = new double[numElements];
        for (int i = 0; i < numElements; i++) {
            res[i] = getDouble(i);
        }
        return res;
    }

    private BytesColumnVector.Bytes getByteArray(int pos) {
        return ((BytesColumnVector) data).getBytes(offset + pos);
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException(
                "ColumnarArray do not support equals, please compare fields one by one!");
    }
}
