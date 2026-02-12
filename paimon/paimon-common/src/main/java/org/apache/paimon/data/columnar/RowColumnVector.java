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

import org.apache.paimon.data.InternalRow;

/**
 * 行列向量接口,用于访问 ROW/STRUCT 类型的列数据。
 *
 * <p>此接口提供对嵌套 ROW 类型数据的访问,每个元素都是一个结构化的行,
 * 行的各个字段分别存储在独立的子列向量中。
 *
 * <h2>数据组织</h2>
 * <pre>
 * ROW<name STRING, age INT> 列的存储:
 * Row 0: {name: "Alice", age: 30}
 * Row 1: {name: "Bob", age: 25}
 *
 * 子向量0 (name): ["Alice", "Bob"]
 * 子向量1 (age):  [30, 25]
 * </pre>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>嵌套结构化数据
 *   <li>复杂对象的列式存储
 *   <li>多字段组合类型
 * </ul>
 *
 * @see ColumnVector 列向量基础接口
 * @see org.apache.paimon.data.InternalRow 内部行接口
 * @see VectorizedColumnBatch 向量化列批次
 * @see org.apache.paimon.data.columnar.heap.HeapRowVector 堆内存实现
 */
public interface RowColumnVector extends ColumnVector {
    /**
     * 获取指定位置的行值。
     *
     * @param i 行索引(从0开始)
     * @return 内部行对象
     */
    InternalRow getRow(int i);

    /**
     * 获取存储嵌套行数据的列批次。
     *
     * <p>嵌套行的各个字段作为独立的列向量存储在返回的批次中。
     *
     * @return 嵌套行的列批次
     */
    VectorizedColumnBatch getBatch();
}
