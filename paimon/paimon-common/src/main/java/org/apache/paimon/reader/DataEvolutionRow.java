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
import org.apache.paimon.types.RowKind;

/**
 * 由多个行组合而成的数据演化行。
 *
 * <p>该类是模式演化(Schema Evolution)的核心实现,将来自不同模式版本的多个内部行组合成一个统一的行视图。
 *
 * <h2>设计背景</h2>
 *
 * <p>在表模式演化过程中,旧文件和新文件的列结构可能不同:
 *
 * <ul>
 *   <li>旧文件缺少新增的列
 *   <li>新文件可能删除了某些列
 *   <li>列的顺序可能发生变化
 * </ul>
 *
 * <p>DataEvolutionRow 通过映射关系将多个部分行组合为完整的行。
 *
 * <h2>核心概念</h2>
 *
 * <ul>
 *   <li><b>rowOffsets</b>:长度为输出字段数,指示每个字段来自哪个内部行(行索引)
 *   <li><b>fieldOffsets</b>:长度为输出字段数,指示每个字段在源行中的偏移量
 *   <li><b>rows</b>:内部行数组,存储来自不同模式版本的多个行
 * </ul>
 *
 * <h2>工作原理</h2>
 *
 * <p>假设输出模式有6个字段:[int, int, string, int, string, int],包含3个内部行:
 *
 * <ul>
 *   <li>reader0(模式V1):[int, string] - 包含列0和列2
 *   <li>reader1(模式V2):[int, int] - 包含列3和列5
 *   <li>reader2(模式V3):[string, string] - 包含列1和列4
 * </ul>
 *
 * <p>映射关系:
 *
 * <ul>
 *   <li>rowOffsets = {0, 2, 0, 1, 2, 1} - 指示字段来源的行号
 *   <li>fieldOffsets = {0, 0, 1, 0, 1, 1} - 指示字段在源行中的位置
 * </ul>
 *
 * <p>读取字段3时:rowOffsets[3]=1, fieldOffsets[3]=0,表示从 rows[1] 的第0个字段读取。
 *
 * <h2>空值处理</h2>
 *
 * <p>当 rowOffsets[i] < 0 时,表示该字段不存在(新增列在旧数据中),返回 null。
 *
 * <h2>性能优化</h2>
 *
 * <ul>
 *   <li>零拷贝:不复制数据,只维护引用
 *   <li>延迟计算:仅在访问字段时才进行映射
 *   <li>数组访问:使用数组而非Map,提高查找效率
 * </ul>
 *
 * <h2>线程安全性</h2>
 *
 * <p>该类不是线程安全的,需要外部同步。
 *
 * @see DataEvolutionFileReader
 * @see DataEvolutionIterator
 */
public class DataEvolutionRow implements InternalRow {

    private final InternalRow[] rows;
    private final int[] rowOffsets;
    private final int[] fieldOffsets;
    private RowKind rowKind;

    public DataEvolutionRow(int rowNumber, int[] rowOffsets, int[] fieldOffsets) {
        this.rows = new InternalRow[rowNumber];
        this.rowOffsets = rowOffsets;
        this.fieldOffsets = fieldOffsets;
    }

    public int rowNumber() {
        return rows.length;
    }

    public void setRow(int pos, InternalRow row) {
        if (pos >= rows.length) {
            throw new IndexOutOfBoundsException(
                    "Position " + pos + " is out of bounds for rows size " + rows.length);
        } else {
            if (rowKind == null) {
                this.rowKind = row.getRowKind();
            }
            rows[pos] = row;
        }
    }

    public void setRows(InternalRow[] rows) {
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

    private InternalRow chooseRow(int pos) {
        return rows[(rowOffsets[pos])];
    }

    private int offsetInRow(int pos) {
        return fieldOffsets[pos];
    }

    @Override
    public int getFieldCount() {
        return fieldOffsets.length;
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
        if (rowOffsets[pos] < 0) {
            return true;
        }
        return chooseRow(pos).isNullAt(offsetInRow(pos));
    }

    @Override
    public boolean getBoolean(int pos) {
        return chooseRow(pos).getBoolean(offsetInRow(pos));
    }

    @Override
    public byte getByte(int pos) {
        return chooseRow(pos).getByte(offsetInRow(pos));
    }

    @Override
    public short getShort(int pos) {
        return chooseRow(pos).getShort(offsetInRow(pos));
    }

    @Override
    public int getInt(int pos) {
        return chooseRow(pos).getInt(offsetInRow(pos));
    }

    @Override
    public long getLong(int pos) {
        return chooseRow(pos).getLong(offsetInRow(pos));
    }

    @Override
    public float getFloat(int pos) {
        return chooseRow(pos).getFloat(offsetInRow(pos));
    }

    @Override
    public double getDouble(int pos) {
        return chooseRow(pos).getDouble(offsetInRow(pos));
    }

    @Override
    public BinaryString getString(int pos) {
        return chooseRow(pos).getString(offsetInRow(pos));
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return chooseRow(pos).getDecimal(offsetInRow(pos), precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return chooseRow(pos).getTimestamp(offsetInRow(pos), precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        return chooseRow(pos).getBinary(offsetInRow(pos));
    }

    @Override
    public Variant getVariant(int pos) {
        return chooseRow(pos).getVariant(offsetInRow(pos));
    }

    @Override
    public Blob getBlob(int pos) {
        return chooseRow(pos).getBlob(offsetInRow(pos));
    }

    @Override
    public InternalArray getArray(int pos) {
        return chooseRow(pos).getArray(offsetInRow(pos));
    }

    @Override
    public InternalMap getMap(int pos) {
        return chooseRow(pos).getMap(offsetInRow(pos));
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return chooseRow(pos).getRow(offsetInRow(pos), numFields);
    }
}
