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

package org.apache.paimon.data.columnar.heap;

import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.data.columnar.RowColumnVector;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;

/**
 * 堆Row列向量实现类。
 *
 * <p>这个类表示一个可空的堆Row列向量，用于在列式存储中管理Row（行/结构体）类型的数据。
 * Row由多个字段列向量组成，每个字段可以是不同的类型。
 *
 * <h2>内存布局</h2>
 * <pre>
 * HeapRowVector 结构:
 * ┌─────────────────────┐
 * │ Null Bitmap (可选)  │  每个 bit 表示一个 Row 是否为 null
 * ├─────────────────────┤
 * │ Field 1 Vector      │  第1个字段的列向量
 * │ [v1, v2, v3, ...]   │
 * ├─────────────────────┤
 * │ Field 2 Vector      │  第2个字段的列向量
 * │ [v1, v2, v3, ...]   │
 * ├─────────────────────┤
 * │ Field 3 Vector      │  第3个字段的列向量
 * │ [v1, v2, v3, ...]   │
 * └─────────────────────┘
 *
 * 每个 Row 由相同索引位置的所有字段值组成
 * </pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建字段列向量: (id:INT, name:STRING, age:INT)
 * HeapIntVector idVector = new HeapIntVector(100);
 * HeapBytesVector nameVector = new HeapBytesVector(100);
 * HeapIntVector ageVector = new HeapIntVector(100);
 *
 * // 创建 Row 列向量
 * HeapRowVector rowVector = new HeapRowVector(100, idVector, nameVector, ageVector);
 *
 * // 设置第0行的数据
 * idVector.setInt(0, 1);
 * nameVector.putByteArray(0, "Alice".getBytes(), 0, 5);
 * ageVector.setInt(0, 25);
 *
 * // 读取第0行
 * ColumnarRow row = rowVector.getRow(0);
 * // row.getInt(0) == 1
 * // row.getString(1) == "Alice"
 * // row.getInt(2) == 25
 *
 * // 追加一行（增加计数，实际值由各字段向量设置）
 * rowVector.appendRow();
 * }</pre>
 *
 * <h2>性能特点</h2>
 * <ul>
 *   <li><b>列式存储</b>: 每个字段独立存储为列向量，利于列式压缩和扫描</li>
 *   <li><b>访问效率</b>: O(1) 时间复杂度访问任意行</li>
 *   <li><b>类型灵活</b>: 每个字段可以是不同类型的列向量</li>
 *   <li><b>向量化处理</b>: 支持批量操作和向量化计算</li>
 *   <li><b>空值传播</b>: 追加空行时，自动为所有字段追加空值</li>
 * </ul>
 *
 * <h2>适用场景</h2>
 * <ul>
 *   <li>存储 SQL ROW/STRUCT 类型</li>
 *   <li>嵌套数据结构</li>
 *   <li>列式文件格式（Parquet, ORC）中的结构体列</li>
 * </ul>
 *
 * @see AbstractStructVector 结构体向量的基类
 * @see WritableColumnVector 可写列向量接口
 * @see RowColumnVector Row列向量接口
 * @see ColumnarRow 列式存储的Row表示
 */
public class HeapRowVector extends AbstractStructVector
        implements WritableColumnVector, RowColumnVector {

    /** 向量化列批次，用于提供统一的批次访问接口。 */
    private VectorizedColumnBatch vectorizedColumnBatch;

    /**
     * 构造一个堆Row列向量。
     *
     * @param len Row的数量（行数）
     * @param fields 字段列向量数组，每个元素代表一个字段
     */
    public HeapRowVector(int len, ColumnVector... fields) {
        super(len, fields);
        vectorizedColumnBatch = new VectorizedColumnBatch(children);
    }

    /**
     * 获取指定位置的Row。
     *
     * <p>返回一个列式Row视图，该Row指向各字段列向量的指定位置。
     *
     * @param i Row的索引位置
     * @return 指定位置的列式Row
     */
    @Override
    public ColumnarRow getRow(int i) {
        ColumnarRow columnarRow = new ColumnarRow(vectorizedColumnBatch);
        columnarRow.setRowId(i);
        return columnarRow;
    }

    /**
     * 获取向量化列批次。
     *
     * @return 向量化列批次对象
     */
    @Override
    public VectorizedColumnBatch getBatch() {
        return vectorizedColumnBatch;
    }

    /**
     * 为堆向量预留空间。
     *
     * <p>Row向量本身不存储数据，所以这个方法什么也不做。
     *
     * @param newCapacity 新的容量（未使用）
     */
    @Override
    void reserveForHeapVector(int newCapacity) {
        // Nothing to store.
    }

    /**
     * 追加一行。
     *
     * <p>增加行计数。注意这只是增加计数，实际的字段值需要通过各字段列向量分别设置。
     */
    public void appendRow() {
        reserve(elementsAppended + 1);
        elementsAppended++;
    }

    /**
     * 设置字段列向量数组。
     *
     * <p>替换当前的字段列向量并重新创建向量化列批次。
     *
     * @param fields 新的字段列向量数组
     */
    public void setFields(WritableColumnVector[] fields) {
        System.arraycopy(fields, 0, this.children, 0, fields.length);
        this.vectorizedColumnBatch = new VectorizedColumnBatch(children);
    }

    /**
     * 追加一个空行。
     *
     * <p>不仅增加行计数，还为所有字段追加空值。这确保了空值能够正确传播到所有字段。
     */
    @Override
    public void appendNull() {
        super.appendNull();
        for (ColumnVector child : children) {
            if (child instanceof WritableColumnVector) {
                ((WritableColumnVector) child).appendNull();
            }
        }
    }
}
