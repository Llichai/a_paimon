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

import org.apache.paimon.data.InternalArray;

/**
 * 数组列向量接口,用于访问 ARRAY 类型的列数据。
 *
 * <p>此接口提供对嵌套 ARRAY 类型数据的访问,每个元素都是一个数组,
 * 数组元素存储在单独的子列向量中。
 *
 * <h2>数据组织</h2>
 * <pre>
 * ARRAY<INT> 列的存储:
 * Row 0: [1, 2, 3]      -> offset=0, length=3
 * Row 1: [4, 5]         -> offset=3, length=2
 * Row 2: [6, 7, 8, 9]   -> offset=5, length=4
 *
 * 底层元素向量: [1, 2, 3, 4, 5, 6, 7, 8, 9]
 * </pre>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>存储一对多关系的数据
 *   <li>嵌套数据结构
 *   <li>变长列表数据
 * </ul>
 *
 * @see ColumnVector 列向量基础接口
 * @see org.apache.paimon.data.InternalArray 内部数组接口
 * @see org.apache.paimon.data.columnar.heap.HeapArrayVector 堆内存实现
 */
public interface ArrayColumnVector extends ColumnVector {
    /**
     * 获取指定位置的数组值。
     *
     * @param i 行索引(从0开始)
     * @return 内部数组对象
     */
    InternalArray getArray(int i);

    /**
     * 获取存储数组元素的子列向量。
     *
     * @return 元素列向量
     */
    ColumnVector getColumnVector();
}
