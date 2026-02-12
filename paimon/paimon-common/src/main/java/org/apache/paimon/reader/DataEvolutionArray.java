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

package org.apache.paimon.reader;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;

/**
 * 由多个数组组合而成的数据演化数组。
 *
 * <p>该类是 {@link InternalArray} 的实现,将来自不同模式版本的多个数组组合成一个统一的数组视图。
 *
 * <h2>设计背景</h2>
 *
 * <p>在表模式演化过程中,嵌套数组类型的字段可能来自不同的文件和模式版本。
 * DataEvolutionArray 通过映射关系将这些部分数组组合为完整的数组。
 *
 * <h2>核心概念</h2>
 *
 * <ul>
 *   <li><b>rowOffsets</b>:长度为输出元素数,指示每个元素来自哪个内部数组(数组索引)
 *   <li><b>fieldOffsets</b>:长度为输出元素数,指示每个元素在源数组中的偏移量
 *   <li><b>rows</b>:内部数组列表,存储来自不同模式版本的多个数组
 * </ul>
 *
 * <h2>工作原理</h2>
 *
 * <p>与 {@link DataEvolutionRow} 类似,但操作对象是数组元素而非行字段:
 *
 * <ul>
 *   <li>通过 rowOffsets[i] 确定第i个元素来自哪个内部数组
 *   <li>通过 fieldOffsets[i] 确定在该数组中的位置
 *   <li>当 rowOffsets[i] < 0 时,表示该元素不存在,返回 null
 * </ul>
 *
 * <h2>空值处理</h2>
 *
 * <p>当 rowOffsets[pos] < 0 时,{@link #isNullAt(int)} 返回 true,表示该位置的元素不存在。
 *
 * <h2>性能优化</h2>
 *
 * <ul>
 *   <li>零拷贝:不复制数据,只维护引用
 *   <li>延迟计算:仅在访问元素时才进行映射
 *   <li>数组访问:使用数组而非Map,提高查找效率
 * </ul>
 *
 * <h2>线程安全性</h2>
 *
 * <p>该类不是线程安全的,需要外部同步。
 *
 * @see DataEvolutionRow
 */
public class DataEvolutionArray implements InternalArray {

    private final InternalArray[] rows;
    private final int[] rowOffsets;
    private final int[] fieldOffsets;

    public DataEvolutionArray(int rowNumber, int[] rowOffsets, int[] fieldOffsets) {
        this.rows = new InternalArray[rowNumber];
        this.rowOffsets = rowOffsets;
        this.fieldOffsets = fieldOffsets;
    }

    public void setRow(int pos, InternalArray row) {
        if (pos >= rows.length) {
            throw new IndexOutOfBoundsException(
                    "Position " + pos + " is out of bounds for rows size " + rows.length);
        } else {
            rows[pos] = row;
        }
    }

    public void setRows(InternalArray[] rows) {
        if (rows.length != this.rows.length) {
            throw new IllegalArgumentException(
                    "The length of input rows "
                            + rows.length
                            + " is not equal to the expected length "
                            + this.rows.length);
        }
        for (int i = 0; i < rows.length; i++) {
            setRow(i, rows[i]);
        }
    }

    private InternalArray chooseArray(int pos) {
        return rows[(rowOffsets[pos])];
    }

    private int offsetInRow(int pos) {
        return fieldOffsets[pos];
    }

    @Override
    public boolean isNullAt(int pos) {
        if (rowOffsets[pos] < 0) {
            return true;
        }
        return chooseArray(pos).isNullAt(offsetInRow(pos));
    }

    @Override
    public boolean getBoolean(int pos) {
        return chooseArray(pos).getBoolean(offsetInRow(pos));
    }

    @Override
    public byte getByte(int pos) {
        return chooseArray(pos).getByte(offsetInRow(pos));
    }

    @Override
    public short getShort(int pos) {
        return chooseArray(pos).getShort(offsetInRow(pos));
    }

    @Override
    public int getInt(int pos) {
        return chooseArray(pos).getInt(offsetInRow(pos));
    }

    @Override
    public long getLong(int pos) {
        return chooseArray(pos).getLong(offsetInRow(pos));
    }

    @Override
    public float getFloat(int pos) {
        return chooseArray(pos).getFloat(offsetInRow(pos));
    }

    @Override
    public double getDouble(int pos) {
        return chooseArray(pos).getDouble(offsetInRow(pos));
    }

    @Override
    public BinaryString getString(int pos) {
        return chooseArray(pos).getString(offsetInRow(pos));
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return chooseArray(pos).getDecimal(offsetInRow(pos), precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return chooseArray(pos).getTimestamp(offsetInRow(pos), precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        return chooseArray(pos).getBinary(offsetInRow(pos));
    }

    @Override
    public Variant getVariant(int pos) {
        return chooseArray(pos).getVariant(offsetInRow(pos));
    }

    @Override
    public Blob getBlob(int pos) {
        return chooseArray(pos).getBlob(offsetInRow(pos));
    }

    @Override
    public InternalArray getArray(int pos) {
        return chooseArray(pos).getArray(offsetInRow(pos));
    }

    @Override
    public InternalMap getMap(int pos) {
        return chooseArray(pos).getMap(offsetInRow(pos));
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return chooseArray(pos).getRow(offsetInRow(pos), numFields);
    }

    @Override
    public int size() {
        return rowOffsets.length;
    }

    @Override
    public boolean[] toBooleanArray() {
        boolean[] result = new boolean[rowOffsets.length];
        for (int i = 0; i < rowOffsets.length; i++) {
            result[i] = getBoolean(i);
        }
        return result;
    }

    @Override
    public byte[] toByteArray() {
        byte[] result = new byte[rowOffsets.length];
        for (int i = 0; i < rowOffsets.length; i++) {
            result[i] = getByte(i);
        }
        return result;
    }

    @Override
    public short[] toShortArray() {
        short[] result = new short[rowOffsets.length];
        for (int i = 0; i < rowOffsets.length; i++) {
            result[i] = getShort(i);
        }
        return result;
    }

    @Override
    public int[] toIntArray() {
        int[] result = new int[rowOffsets.length];
        for (int i = 0; i < rowOffsets.length; i++) {
            result[i] = getInt(i);
        }
        return result;
    }

    @Override
    public long[] toLongArray() {
        long[] result = new long[rowOffsets.length];
        for (int i = 0; i < rowOffsets.length; i++) {
            result[i] = getLong(i);
        }
        return result;
    }

    @Override
    public float[] toFloatArray() {
        float[] result = new float[rowOffsets.length];
        for (int i = 0; i < rowOffsets.length; i++) {
            result[i] = getFloat(i);
        }
        return result;
    }

    @Override
    public double[] toDoubleArray() {
        double[] result = new double[rowOffsets.length];
        for (int i = 0; i < rowOffsets.length; i++) {
            result[i] = getDouble(i);
        }
        return result;
    }
}
