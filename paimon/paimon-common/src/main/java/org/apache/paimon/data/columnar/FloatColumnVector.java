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

/**
 * 单精度浮点型列向量接口,用于访问单精度浮点型列数据。
 *
 * <p>此接口扩展了 {@link ColumnVector} 基础接口,提供 float 类型的类型化访问方法。
 * 主要用于存储 FLOAT 等 SQL 类型的列数据。
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>存储和访问 FLOAT 类型的列数据
 *   <li>科学计算中的浮点数值
 *   <li>向量化执行中的批量浮点运算
 * </ul>
 *
 * <h2>性能优化</h2>
 * <ul>
 *   <li>4字节存储,节省空间
 *   <li>支持 SIMD 浮点运算
 * </ul>
 *
 * @see ColumnVector 列向量基础接口
 * @see org.apache.paimon.data.columnar.heap.HeapFloatVector 堆内存实现
 * @see org.apache.paimon.data.columnar.writable.WritableFloatVector 可写实现
 */
public interface FloatColumnVector extends ColumnVector {
    /**
     * 获取指定位置的单精度浮点值。
     *
     * @param i 行索引(从0开始)
     * @return 单精度浮点值
     */
    float getFloat(int i);
}
