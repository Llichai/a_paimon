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
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.columnar.BytesColumnVector.Bytes;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.Variant;

import java.io.Serializable;

/**
 * 向量化列批次,将一批行数据组织为列向量的集合,是查询执行的基本单元。
 *
 * <p>列批次是列式存储的核心数据结构,将数据按列组织可以最小化每行的处理成本,
 * 并支持向量化执行引擎进行批量处理和 SIMD 优化。
 *
 * <h2>设计思想</h2>
 * <ul>
 *   <li>借鉴 Apache Hive 的 VectorizedRowBatch 设计
 *   <li>将行式数据转换为列式表示,提高缓存局部性和处理效率
 *   <li>作为向量化执行引擎的数据载体
 * </ul>
 *
 * <h2>数据组织</h2>
 * <pre>
 * 行式存储:        列式存储(本类):
 * Row0: [c0, c1, c2]   Column0: [r0, r1, r2]
 * Row1: [c0, c1, c2]   Column1: [r0, r1, r2]
 * Row2: [c0, c1, c2]   Column2: [r0, r1, r2]
 * </pre>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>批量读取 Parquet、ORC 等列式格式的数据
 *   <li>向量化算子(过滤、投影、聚合)的输入/输出
 *   <li>批量写入列式存储文件
 *   <li>与 Arrow 等列式内存格式的数据交换
 * </ul>
 *
 * <h2>性能优势</h2>
 * <ul>
 *   <li>提高 CPU 缓存命中率(连续访问同类型数据)
 *   <li>支持 SIMD 指令并行处理多个值
 *   <li>减少类型转换和虚函数调用开销
 *   <li>便于应用列压缩算法
 * </ul>
 *
 * <p><b>注意:</b> 此类是可序列化的,可用于分布式计算框架的数据交换。
 *
 * @see ColumnVector 列向量的基础接口
 * @see ColumnarRow 列批次中的行视图
 * @see VectorizedRowIterator 列批次的行迭代器
 */
public class VectorizedColumnBatch implements Serializable {

    private static final long serialVersionUID = 8180323238728166155L;

    /** 此批次中的有效行数 */
    private int numRows;

    /** 列向量数组,每个元素代表一列的数据 */
    public final ColumnVector[] columns;

    /**
     * 构造一个向量化列批次。
     *
     * @param vectors 列向量数组,数组长度即为列数
     */
    public VectorizedColumnBatch(ColumnVector[] vectors) {
        this.columns = vectors;
    }

    /**
     * 设置此批次中的有效行数。
     *
     * <p>在填充数据时,可能实际填充的行数少于向量容量,此方法用于标记有效数据的行数。
     *
     * @param numRows 有效行数
     */
    public void setNumRows(int numRows) {
        this.numRows = numRows;
    }

    /**
     * 获取此批次中的有效行数。
     *
     * @return 有效行数
     */
    public int getNumRows() {
        return numRows;
    }

    /**
     * 获取列数(Arity)。
     *
     * @return 列向量数组的长度
     */
    public int getArity() {
        return columns.length;
    }

    /**
     * 检查指定位置的值是否为 NULL。
     *
     * @param rowId 行索引
     * @param colId 列索引
     * @return 如果该位置的值为 NULL 返回 true,否则返回 false
     */
    public boolean isNullAt(int rowId, int colId) {
        return columns[colId].isNullAt(rowId);
    }

    /**
     * 获取指定位置的布尔值。
     *
     * @param rowId 行索引
     * @param colId 列索引
     * @return 布尔值
     */
    public boolean getBoolean(int rowId, int colId) {
        return ((BooleanColumnVector) columns[colId]).getBoolean(rowId);
    }

    /**
     * 获取指定位置的字节值。
     *
     * @param rowId 行索引
     * @param colId 列索引
     * @return 字节值
     */
    public byte getByte(int rowId, int colId) {
        return ((ByteColumnVector) columns[colId]).getByte(rowId);
    }

    /**
     * 获取指定位置的短整型值。
     *
     * @param rowId 行索引
     * @param colId 列索引
     * @return 短整型值
     */
    public short getShort(int rowId, int colId) {
        return ((ShortColumnVector) columns[colId]).getShort(rowId);
    }

    /**
     * 获取指定位置的整型值。
     *
     * @param rowId 行索引
     * @param colId 列索引
     * @return 整型值
     */
    public int getInt(int rowId, int colId) {
        return ((IntColumnVector) columns[colId]).getInt(rowId);
    }

    /**
     * 获取指定位置的长整型值。
     *
     * @param rowId 行索引
     * @param colId 列索引
     * @return 长整型值
     */
    public long getLong(int rowId, int colId) {
        return ((LongColumnVector) columns[colId]).getLong(rowId);
    }

    /**
     * 获取指定位置的单精度浮点值。
     *
     * @param rowId 行索引
     * @param colId 列索引
     * @return 单精度浮点值
     */
    public float getFloat(int rowId, int colId) {
        return ((FloatColumnVector) columns[colId]).getFloat(rowId);
    }

    /**
     * 获取指定位置的双精度浮点值。
     *
     * @param rowId 行索引
     * @param colId 列索引
     * @return 双精度浮点值
     */
    public double getDouble(int rowId, int colId) {
        return ((DoubleColumnVector) columns[colId]).getDouble(rowId);
    }

    /**
     * 获取指定位置的字符串值。
     *
     * @param rowId 行索引
     * @param pos 列索引
     * @return 二进制字符串
     */
    public BinaryString getString(int rowId, int pos) {
        Bytes byteArray = getByteArray(rowId, pos);
        return BinaryString.fromBytes(byteArray.data, byteArray.offset, byteArray.len);
    }

    /**
     * 获取指定位置的二进制数据。
     *
     * <p>如果字节数组的长度等于数据长度,直接返回原数组;否则复制一份指定长度的新数组返回。
     *
     * @param rowId 行索引
     * @param pos 列索引
     * @return 字节数组
     */
    public byte[] getBinary(int rowId, int pos) {
        Bytes byteArray = getByteArray(rowId, pos);
        if (byteArray.len == byteArray.data.length) {
            return byteArray.data;
        } else {
            byte[] ret = new byte[byteArray.len];
            System.arraycopy(byteArray.data, byteArray.offset, ret, 0, byteArray.len);
            return ret;
        }
    }

    /**
     * 获取指定位置的字节数组引用(包含数据、偏移量和长度)。
     *
     * @param rowId 行索引
     * @param colId 列索引
     * @return 字节数组引用
     */
    public Bytes getByteArray(int rowId, int colId) {
        return ((BytesColumnVector) columns[colId]).getBytes(rowId);
    }

    /**
     * 获取指定位置的十进制值。
     *
     * @param rowId 行索引
     * @param colId 列索引
     * @param precision 精度(总位数)
     * @param scale 小数位数
     * @return 十进制值
     */
    public Decimal getDecimal(int rowId, int colId, int precision, int scale) {
        return ((DecimalColumnVector) (columns[colId])).getDecimal(rowId, precision, scale);
    }

    /**
     * 获取指定位置的时间戳值。
     *
     * @param rowId 行索引
     * @param colId 列索引
     * @param precision 时间戳精度
     * @return 时间戳值
     */
    public Timestamp getTimestamp(int rowId, int colId, int precision) {
        return ((TimestampColumnVector) (columns[colId])).getTimestamp(rowId, precision);
    }

    /**
     * 获取指定位置的数组值。
     *
     * @param rowId 行索引
     * @param colId 列索引
     * @return 内部数组表示
     */
    public InternalArray getArray(int rowId, int colId) {
        return ((ArrayColumnVector) columns[colId]).getArray(rowId);
    }

    /**
     * 获取指定位置的行值(嵌套行)。
     *
     * @param rowId 行索引
     * @param colId 列索引
     * @return 内部行表示
     */
    public InternalRow getRow(int rowId, int colId) {
        return ((RowColumnVector) columns[colId]).getRow(rowId);
    }

    /**
     * 获取指定位置的 Variant 值。
     *
     * <p>Variant 是半结构化数据类型,内部表示为包含 value 和 metadata 的行。
     *
     * @param rowId 行索引
     * @param colId 列索引
     * @return Variant 值
     */
    public Variant getVariant(int rowId, int colId) {
        InternalRow row = getRow(rowId, colId);
        byte[] value = row.getBinary(0);
        byte[] metadata = row.getBinary(1);
        return new GenericVariant(value, metadata);
    }

    /**
     * 获取指定位置的 Map 值。
     *
     * @param rowId 行索引
     * @param colId 列索引
     * @return 内部 Map 表示
     */
    public InternalMap getMap(int rowId, int colId) {
        return ((MapColumnVector) columns[colId]).getMap(rowId);
    }

    /**
     * 复制此列批次,使用新的列向量数组。
     *
     * <p>复制的批次保留原批次的行数设置。
     *
     * @param vectors 新的列向量数组
     * @return 新的列批次实例
     */
    public VectorizedColumnBatch copy(ColumnVector[] vectors) {
        VectorizedColumnBatch vectorizedColumnBatch = new VectorizedColumnBatch(vectors);
        vectorizedColumnBatch.setNumRows(numRows);
        return vectorizedColumnBatch;
    }
}
